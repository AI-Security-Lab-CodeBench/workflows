from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from dagster import AssetKey, DagsterInstance, DagsterRunStatus
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.config_assets import ConfigRunIOManager, current_configuration_record
from codebench_pipeline.dataset_cli import main
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.input_provenance import current_materialization
from codebench_pipeline.rules_artifacts import RulesCaptureArtifact
from codebench_pipeline.rules_learning import CaptureResult
from codebench_pipeline.rules_settings import RULES_REGISTRATION, RegisteredRules
from codebench_pipeline.storage import IntegrityError

_KEY = AssetKey(["codebench", "rules_capture"])
_CATEGORY = "CAT-4"
_JSON = TypeAdapter(dict[str, JsonValue])


@dataclass
class CaptureCompletion:
    normal: Callable[[dict[str, JsonValue]], dict[str, JsonValue]]
    model: str
    allow_recovery: bool = False
    recovered: list[tuple[str, ...]] = field(default_factory=list)
    requests: int = 0

    def __call__(self, request: dict[str, JsonValue]) -> dict[str, JsonValue]:
        self.requests += 1
        messages = request["messages"]
        assert isinstance(messages, list)
        user_message = messages[1]
        assert isinstance(user_message, dict)
        content = user_message["content"]
        assert isinstance(content, str)
        batch = _JSON.validate_json(content)
        if "batch_id" not in batch and (batch.get("category") != _CATEGORY or len(messages) == 2):
            return self.normal(request)
        calls: list[JsonValue] = []
        finish = "length"
        if self.allow_recovery and "batch_id" in batch:
            tools = request["tools"]
            assert isinstance(tools, list) and request["tool_choice"] == "auto"
            (tool,) = tools
            assert isinstance(tool, dict)
            function = tool["function"]
            assert isinstance(function, dict)
            accepted = batch["accepted_rule_ids"]
            self.recovered.append(tuple(TypeAdapter(list[str]).validate_python(accepted)))
            finish = "tool_calls"
            calls.append({
                "id": "recovered", "type": "function",
                "function": {"name": function["name"], "arguments": "{}"},
            })
        return {"id": "synthetic", "model": self.model, "choices": [{
            "index": 0, "finish_reason": finish,
            "message": {"role": "assistant", "content": None, "tool_calls": calls},
        }]}


def exercise_capture_resume(
    home: Path, instance: DagsterInstance, completion: CaptureCompletion, monkeypatch: pytest.MonkeyPatch,
) -> None:
    definitions = build_definitions(home)
    job = definitions.resolve_job_def("capture_rules")
    manager = ConfigRunIOManager(home=str(home))
    registered = manager.materialized_scope(
        current_configuration_record(instance, RULES_REGISTRATION, CODEBENCH_DATASET_ID), RegisteredRules,
    ).registration

    def register(runtime_origin: str) -> None:
        payload = registered.model_copy(update={
            "runtime": registered.runtime.model_copy(update={"base_url": runtime_origin}),
        }).model_dump_json()
        assert definitions.resolve_job_def("register_rules").execute_in_process(
            instance=instance, partition_key=CODEBENCH_DATASET_ID,
            run_config={"ops": {"codebench__rules_registration": {"config": {
                "registration_json": payload, "expected_sha256": sha256(payload.encode()).hexdigest(),
            }}}},
        ).success

    def cli(source: str) -> None:
        monkeypatch.setattr("sys.argv", [
            "dataset-cli", "rules-capture", "--home", str(home), "--category", _CATEGORY, "--resume-run", source,
        ])
        main()

    def legacy_failure(description: str, *, metadata: dict[str, str]) -> IntegrityError:
        return IntegrityError(description)

    with monkeypatch.context() as historical:
        historical.setattr("codebench_pipeline.rules_assets.Failure", legacy_failure)
        legacy = job.execute_in_process(instance=instance, partition_key=_CATEGORY, raise_on_error=False)
    modern = job.execute_in_process(instance=instance, partition_key=_CATEGORY, raise_on_error=False)
    assert not legacy.success and not modern.success
    legacy_output = manager.run_output_path(legacy.run_id, _KEY, _CATEGORY)
    legacy_file = legacy_output.with_name(f"{legacy_output.name}.attempt-0.evidence.json")
    initial = CaptureResult.model_validate_json(legacy_file.read_bytes()).model_dump_json(exclude={
        "batches": {"__all__": {"descriptor_sha256", "prior_runs", "recovery_reason"}},
    }).encode()
    legacy_file.write_bytes(initial)
    previous = CaptureResult.model_validate_json(initial)
    assert not previous.complete and len(previous.packages) == 1

    calls = completion.requests
    with pytest.raises(IntegrityError, match="not a failed capture_rules run"):
        cli(str(uuid4()))
    legacy_file.unlink()
    with pytest.raises(IntegrityError, match="missing, unreadable, or malformed"):
        cli(legacy.run_id)
    legacy_file.write_bytes(b'{"batches": null}')
    with pytest.raises(IntegrityError, match="missing, unreadable, or malformed"):
        cli(legacy.run_id)
    legacy_file.write_bytes(initial)

    modern_output = manager.run_output_path(modern.run_id, _KEY, _CATEGORY)
    modern_file = modern_output.with_name(f"{modern_output.name}.attempt-0.evidence.json")
    modern_file.write_bytes(modern_file.read_bytes() + b"\n")
    with pytest.raises(IntegrityError, match="differs from its failed attempt digest"):
        cli(modern.run_id)

    register("http://127.0.0.1:1")
    with pytest.raises(IntegrityError, match="settings differ from the registered"):
        cli(legacy.run_id)
    assert completion.requests == calls

    register(registered.runtime.base_url)
    completion.allow_recovery = True
    cli(legacy.run_id)
    assert completion.requests == calls + 1
    assert completion.recovered == [(previous.packages[0].rule.id,)]
    assert legacy_file.read_bytes() == initial
    assert not legacy_output.exists()
    source = instance.get_run_by_id(legacy.run_id)
    assert source is not None and source.status == DagsterRunStatus.FAILURE
    recovered_record = current_materialization(instance, _KEY, _CATEGORY)
    assert recovered_record is not None and recovered_record.run_id != legacy.run_id
    recovered = manager.materialized_scope(recovered_record, RulesCaptureArtifact)
    assert recovered.result.complete
    assert recovered.result.rules == previous.rules
    assert recovered.result.batches[0].prior_runs == (previous.batches[0].run,)
