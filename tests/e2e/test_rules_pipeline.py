import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import NamedTuple

import httpx2
import pandas as pd
import pytest
from dagster import (
    AssetKey,
    DagsterInstance,
    Definitions,
    MarkdownMetadataValue,
    MetadataValue,
    TextMetadataValue,
    asset,
    materialize,
)
from pydantic import JsonValue
from rules_capture_recovery_fixture import CaptureCompletion, exercise_capture_resume
from rules_report_http_fixture import assert_report_document
from starlette.testclient import TestClient
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.artifact_views import ArtifactDataService, ArtifactPartitions, ArtifactTable
from codebench_pipeline.config_assets import GLOBAL_CONFIG, ConfigRunIOManager, current_configuration_record
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager
from codebench_pipeline.dataset_artifacts import DatasetArtifactCatalog
from codebench_pipeline.dataset_cli import configure_rules, main
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.rules_artifacts import RULES_EVAL_KEY
from codebench_pipeline.rules_registration_config import (
    HistoricalRegisteredRules,
    HistoricalRulesPolicy,
    RulesPolicy,
)
from codebench_pipeline.rules_settings import RULES_PARTITIONS, RULES_REGISTRATION
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.train_assets import register_dataset_partitions
from codebench_pipeline.train_config import TrainConfig, TrainSource

_GUIDE = Path(__file__).resolve().parents[2] / "business-configs/bashguard/guide.json"
_RUNTIME = f"sha256:{'a' * 64}"
_MODEL = "synthetic/qwen"
_TRAIN = {"pwd": "CAT-3", "date": "CAT-4", "echo train": "CAT-6", "echo background": "CAT-7"}
_SELECTED = ("CAT-3", "CAT-4", "CAT-6")
_PROPOSALS = {"CAT-3": "pwd", "CAT-4": "date"}
_TEST = {"pwd": "CAT-3", "pwd -L": "CAT-3", "whoami": "CAT-5", "echo held": "CAT-6"}


def _labels(category: str) -> dict[str, str]:
    return {"category": category, "risk": f"{category}.1", "severity": "low", "action": "allow"}


class _HTTP(NamedTuple):
    origin: str
    calls: list[tuple[str, dict]]


@contextmanager
def _services(complete: Callable[[dict[str, JsonValue]], dict[str, JsonValue]]) -> Iterator[_HTTP]:
    calls: list[tuple[str, dict]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            size = int(self.headers["Content-Length"])
            if self.path == "/v1/analyze":
                assert 0 < size <= 2 << 20
            request = json.loads(self.rfile.read(size))
            calls.append((self.path, request))
            match self.path:
                case "/v1/chat/completions":
                    response = complete(request)
                case "/v1/evaluate":
                    response = _evaluate(request)
                case "/v1/analyze":
                    response = _analyze(request)
                case _:
                    self.send_error(404)
                    return
            encoded = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _message: str, *args: object) -> None:
            return

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever)
        worker.start()
        try:
            yield _HTTP(f"http://127.0.0.1:{server.server_port}", calls)
        finally:
            server.shutdown()
            worker.join()


def _complete(request: dict) -> dict:
    messages = request["messages"]
    batch = json.loads(messages[1]["content"])
    assert request["model"] == _MODEL
    assert request["chat_template_kwargs"] == {"enable_thinking": False}
    assert "Bashguard authoring reference, guide schema bashguard-guide/v1" in messages[0]["content"]
    assert all(command not in json.dumps(messages[1:]) for command in ("pwd -L", "whoami", "echo held"))
    if "next_rule_id" not in batch:
        tool = "finalize"
        arguments = {
            "justification": "Drop only the duplicate literal; the first rule covers its tests.",
            "drop_ids": ["cat-3.capture-0001-02"], "add": [],
        }
    else:
        category = batch["category"]
        assert len(batch["rows"]) == 1
        row = batch["rows"][0]
        assert row["gold"] == _labels(category)
        assert "command" not in row and "context" not in row
        assert row["literal_cli_eligible"]
        assert row["calls"][0][0]["role"] in {"program", "opaque"}
        assert "echo train" not in json.dumps(messages)
        assert "echo background" not in json.dumps(messages)
        if category in _PROPOSALS and (len(messages) == 2 or (category == "CAT-3" and len(messages) == 4)):
            command = _PROPOSALS[category]
            tool = "add_rule"
            rule_id = batch["next_rule_id"] if len(messages) == 2 else json.loads(messages[-1]["content"])["next_rule_id"]
            arguments = {
                "rule": {
                    "id": rule_id, "scope": "simple", "when": f"argv == ['{command}']",
                    "labels": _labels(category), "rationale": f"Literal {command} operation.",
                    "sources": [batch["sources"][0]["url"]],
                },
                "tests": [
                    {"command": command, "expected": _labels(category)},
                    {"command": "echo SYNTHETIC_NEAR_MISS", "expected": {"category": "other"}},
                ],
            }
        else:
            tool = "finish"
            arguments = {"justification": "The exact literal is covered."}
    return {
        "id": f"completion-{len(messages)}", "model": _MODEL,
        "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
            "role": "assistant",
            "tool_calls": [{"id": f"call-{len(messages)}", "type": "function", "function": {
                "name": tool, "arguments": json.dumps(arguments),
            }}],
        }}],
    }


def _analyze(request: dict) -> dict:
    assert request["required_runtime_version"] == _RUNTIME
    assert 0 < len(request["rows"]) <= 128
    results = []
    for row in request["rows"]:
        assert set(row) == {"id", "command", "context"}
        command = row["command"]
        assert command in _TRAIN
        words = []
        offset = 0
        for value in command.split():
            start = command.index(value, offset)
            offset = start + len(value)
            words.append({"start": start, "end": offset, "raw": value, "value": value, "static": True})
        results.append({"id": row["id"], "analysis": {
            "eligible": True, "reasons": [], "nodes": [],
            "calls": [{"start": 0, "end": len(command), "words": words}],
        }})
    return {"runtime_version": _RUNTIME, "results": results}


def _evaluate(request: dict) -> dict:
    assert request["required_runtime_version"] == _RUNTIME
    bundle = request["bundle"]
    if not bundle["rules"]:
        assert bundle == {"version": 1, "kind": "empty_learned", "rules": []}
    assert all(set(row) == {"id", "command", "context"} for row in request["rows"])
    digest = sha256(json.dumps(bundle, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    results = []
    for row in request["rows"]:
        hits = [rule for rule in bundle["rules"] if rule["when"] == f"argv == ['{row['command']}']"]
        results.append({"id": row["id"], "result": {
            "status": "matched" if hits else "abstain",
            "basis": "literal_cli_form", "bundle_sha256": digest,
            "labels": hits[0]["labels"] if hits else None,
            "rule_ids": [rule["id"] for rule in hits],
            "rationales": [rule["rationale"] for rule in hits],
            "sources": [source for rule in hits for source in rule["sources"]],
            "reasons": [] if hits else ["no_proven_rule"],
        }})
    return {"runtime_version": _RUNTIME, "bundle_sha256": digest, "results": results}


def _split(
    blobs: dict[str, bytes], number: int, commands: dict[str, str], *, modify: str | None = None,
) -> TrainConfig:
    sources = []
    for category in sorted(set(commands.values())):
        number += 1
        commit = f"{number:040x}"
        rows = "".join(
            f"{command},,,low,{category},{category}.1,fixture,,{'modify' if command == modify else 'allow'}\n"
            for command in commands if commands[command] == category
        )
        data = ("command,description,util,severity,category,risk,source,seed,class\n" + rows).encode()
        blobs[f"/fixture/data/{commit}/commands.csv"] = data
        sources.append(TrainSource(
            repository="fixture/data", branch=f"feat/synthetic-{number}", commit=commit, path="commands.csv",
            category=category, sha256=sha256(data).hexdigest(),
        ))
    return TrainConfig(sources=sources)


def test_rules_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    blobs: dict[str, bytes] = {}
    completion = CaptureCompletion(_complete, _MODEL)

    def reply(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, stream=httpx2.ByteStream(blobs[request.url.path]))

    with (
        _services(completion) as services,
        dataset_instance(tmp_path) as instance,
        httpx2.Client(transport=httpx2.MockTransport(reply)) as repository,
    ):
        spec = fixture_spec(tmp_path)
        labeling = spec.annotation
        assert labeling.batch is not None
        spec = spec.model_copy(update={
            "annotation": labeling.model_copy(update={
                "model": labeling.model.model_copy(update={
                    "model": _MODEL, "base_url": f"{services.origin}/v1", "temperature": 0.0,
                }),
                "batch": labeling.batch.model_copy(update={
                    "inference_mode": "plain", "reasoning_effort": "none", "capacity": None,
                }),
            }),
            "train": _split(blobs, 0, _TRAIN), "test": _split(blobs, 10, _TEST, modify="whoami"),
        })
        registry = DatasetRegistry(tmp_path)
        registry.register(spec)
        register_dataset_partitions(instance, spec)
        monkeypatch.setattr("codebench_pipeline.repository_tree._repository_client", lambda: repository)
        definitions = build_definitions(tmp_path)
        manager = DataFrameIOManager(storage=spec.storage)

        def run(job: str, partition: str = "codebench", **options: object):
            return definitions.resolve_job_def(job).execute_in_process(
                instance=instance, partition_key=partition, **options,
            )

        assert run("refresh_dataset_configuration").success
        assert run("load_train_category", "CAT-3").success
        assert spec.train is not None
        revised = _split(blobs, 20, {"date": "CAT-4"}).sources[0]
        spec = spec.model_copy(update={"train": TrainConfig(sources=[
            revised if source.category == "CAT-4" else source for source in spec.train.sources
        ])})
        registry.register(spec)
        definitions = build_definitions(tmp_path)
        assert run("refresh_dataset_configuration").success

        old_guide = '{"instructions":"Synthetic historical guide","examples":[{}]}'
        previous = HistoricalRulesPolicy.model_validate({
            "runtime": {"base_url": services.origin, "version": "sha256:" + "b" * 64},
            "prompt_guide": {"path": "guide.json", "sha256": sha256(old_guide.encode()).hexdigest()},
            "categories": _SELECTED,
            "train": {"digest": "e" * 64, "row_count": 13},
            "test": {"digest": "f" * 64, "row_count": 7},
        })
        config_io = ConfigRunIOManager(home=str(tmp_path))

        @asset(key=RULES_REGISTRATION, partitions_def=RULES_PARTITIONS, io_manager_key="config_run_io_manager")
        def legacy_registration() -> HistoricalRegisteredRules:
            return HistoricalRegisteredRules(
                registration=previous, digest=sha256(previous.model_dump_json().encode()).hexdigest(), guide=old_guide,
            )

        assert materialize(
            [legacy_registration], instance=instance, partition_key="codebench",
            resources={"config_run_io_manager": config_io},
        ).success
        revision = registry.current_revision("codebench")
        monkeypatch.setattr("sys.argv", [
            "dataset-cli", "rules-register", "--home", str(tmp_path),
            "--migrate", "--runtime-version", _RUNTIME, "--guide-sha256", sha256(_GUIDE.read_bytes()).hexdigest(),
        ])
        main()
        assert registry.current_revision("codebench") == revision
        record = current_configuration_record(instance, GLOBAL_CONFIG, "codebench")
        approved = config_io.registered(record.run_id, materialization=record).rules_learning
        assert approved is not None
        assert approved.registration.categories == _SELECTED
        assert approved.registration.runtime.base_url == services.origin
        assert approved.registration.runtime.version == _RUNTIME
        assert set(approved.registration.model_dump()) == {"categories", "runtime", "prompt_guide"}
        assert set(instance.get_dynamic_partitions("codebench_train_categories")) == set(_TRAIN.values())
        assert run("capture_rules", "CAT-3").success
        assert completion.requests == 3
        assert {
            row["command"]
            for path, request in services.calls if path == "/v1/evaluate"
            for row in request["rows"] if row["id"].startswith("train:")
        } == {"pwd"}

        assert run("load_train_category", "CAT-4").success
        assert run("load_train_category", "CAT-6").success
        assert run("load_train_category", "CAT-7").success
        assert run("merge_train_data").success
        train = manager.reference_at(["train", "merged_rows"], "codebench")
        allowed_snapshot = train.digest
        test: AssetReference | None = None
        read_version = DataFrameIOManager.read_version

        def read_pinned(storage: DataFrameIOManager, reference: AssetReference):
            if reference in (train, test):
                assert reference.digest == allowed_snapshot
            return read_version(storage, reference)

        monkeypatch.setattr(DataFrameIOManager, "read_version", read_pinned)
        assert not run("learn_rules", raise_on_error=False).success
        assert completion.requests == 3
        exercise_capture_resume(tmp_path, instance, completion, monkeypatch)
        assert run("capture_rules", "CAT-6").success
        capture_requests = completion.requests
        assert {row["command"] for path, req in services.calls if path == "/v1/analyze" for row in req["rows"]} == {
            command for command, category in _TRAIN.items() if category in _SELECTED
        }
        assert run("learn_rules").success
        assert completion.requests == capture_requests + 1
        for path, request in services.calls:
            if path == "/v1/evaluate":
                assert {row["command"] for row in request["rows"]}.isdisjoint({"pwd -L", "whoami", "echo held"})
        assert any(
            {row["command"] for row in request["rows"]} == _TRAIN.keys()
            and len(request["bundle"]["rules"]) == 2
            for path, request in services.calls if path == "/v1/evaluate"
        )
        services.calls.clear()
        for category in sorted(set(_TEST.values())):
            assert run("load_test_category", category).success
        assert run("merge_test_data").success
        test = manager.reference_at(["test", "merged_rows"], "codebench")
        allowed_snapshot = test.digest
        assert run("evaluate_rules").success
        event = instance.get_latest_materialization_event(RULES_EVAL_KEY)
        assert event is not None and event.asset_materialization is not None
        metadata = event.asset_materialization.metadata
        assert metadata["Train digest"] == TextMetadataValue(train.digest)
        assert metadata["Test digest"] == TextMetadataValue(test.digest)
        assert [path for path, _ in services.calls] == ["/v1/evaluate"]
        request = services.calls[0][1]
        assert {row["command"] for row in request["rows"]} == _TEST.keys()
        assert len(request["bundle"]["rules"]) == 2
        _assert_public_metrics(instance, tmp_path, definitions)
        assert_report_document(tmp_path, instance, manager)


def _assert_public_metrics(instance: DagsterInstance, home: Path, definitions: Definitions) -> None:
    service = ArtifactDataService(DatasetArtifactCatalog(home, instance), definitions)
    origin = "http://127.0.0.1:3000"
    with TestClient(make_dataset_api(None, None, 3000, artifacts=service), base_url=origin) as client:
        headers = {"Origin": origin}
        key = ["codebench", "rules_eval"]
        discovery = client.post("/artifacts/partitions", headers=headers, json={"asset_key": key})
        assert discovery.status_code == 200
        partitions = ArtifactPartitions.model_validate_json(discovery.content).partitions
        assert partitions == ("codebench",)
        response = client.post("/artifacts/data", headers=headers,
            json={"asset_key": key, "partition_key": partitions[0], "query": {"limit": 200}},
        )
        assert response.status_code == 200
        capture = ["codebench", "rules_capture"]
        discovery = client.post("/artifacts/partitions", headers=headers, json={"asset_key": capture})
        assert discovery.status_code == 200
        assert ArtifactPartitions.model_validate_json(discovery.content).partitions == ("CAT-3", "CAT-4", "CAT-6")
        captured = client.post("/artifacts/data", headers=headers, json={
            "asset_key": capture, "partition_key": "CAT-3", "query": {"limit": 10},
        })
        assert captured.status_code == 200
        assert {row.values["rule_id"] for row in ArtifactTable.model_validate_json(captured.content).page.rows} == {
            "cat-3.capture-0001-01", "cat-3.capture-0001-02",
        }
    result = ArtifactTable.model_validate_json(response.content)
    assert result.data_schema.source == "declared"
    cells = {(row.values["cohort"], row.values["axis"], row.values["label"], row.values["metric"]):
             row.values["value"] for row in result.page.rows}
    assert cells["overall", "outcome", "", "correct"] == 1
    assert cells["overall", "outcome", "", "abstain"] == 3
    assert cells["trained", "outcome", "", "support"] == 3
    assert cells["unseen", "outcome", "", "support"] == 1
    assert cells["overall", "overlap", "", "rows"] == 1
    assert cells["overall", "action", "modify", "support"] == 1
    event = instance.get_latest_materialization_event(RULES_EVAL_KEY)
    assert event is not None and event.asset_materialization is not None
    chart = event.asset_materialization.metadata["Outcome distribution"]
    assert isinstance(chart, MarkdownMetadataValue)
    assert "Unseen categories (n=1)" in chart.value and "█" in chart.value


def test_empty_learned_rules_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    train_commands = {"echo train": "CAT-6", "echo background": "CAT-7"}
    test_commands = {"echo train": "CAT-6", "whoami": "CAT-5"}
    blobs: dict[str, bytes] = {}

    def reply(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, stream=httpx2.ByteStream(blobs[request.url.path]))

    with (
        _services(_complete) as services,
        dataset_instance(tmp_path) as instance,
        httpx2.Client(transport=httpx2.MockTransport(reply)) as repository,
    ):
        spec = fixture_spec(tmp_path)
        labeling = spec.annotation
        assert labeling.batch is not None
        spec = spec.model_copy(update={
            "annotation": labeling.model_copy(update={
                "model": labeling.model.model_copy(update={
                    "model": _MODEL, "base_url": f"{services.origin}/v1", "temperature": 0.0,
                }),
                "batch": labeling.batch.model_copy(update={
                    "inference_mode": "plain", "reasoning_effort": "none", "capacity": None,
                }),
            }),
            "train": _split(blobs, 0, train_commands),
            "test": _split(blobs, 10, test_commands, modify="echo train"),
        })
        DatasetRegistry(tmp_path).register(spec)
        register_dataset_partitions(instance, spec)
        monkeypatch.setattr("codebench_pipeline.repository_tree._repository_client", lambda: repository)
        policy = RulesPolicy.model_validate({
            "categories": ("CAT-6",),
            "runtime": {"base_url": services.origin, "version": _RUNTIME},
            "prompt_guide": {"path": "guide.json", "sha256": sha256(_GUIDE.read_bytes()).hexdigest()},
        })
        payload = policy.model_dump_json().encode()
        configure_rules(
            tmp_path, payload=payload, expected_sha256=sha256(payload).hexdigest(),
            runtime_version=None, guide_sha256=None,
        )
        definitions = build_definitions(tmp_path)

        def run(name: str, partition: str = "codebench") -> None:
            assert definitions.resolve_job_def(name).execute_in_process(
                instance=instance, partition_key=partition,
            ).success

        for split, commands in (("train", train_commands), ("test", test_commands)):
            for category in sorted(set(commands.values())):
                run(f"load_{split}_category", category)
            run(f"merge_{split}_data")
        run("capture_rules", "CAT-6")
        run("learn_rules")
        run("evaluate_rules")

        finalized_key = ["codebench", "rules_finalize"]
        manager = DataFrameIOManager(storage=spec.storage)
        train = manager.reference_at(["train", "merged_rows"], "codebench")
        test = manager.reference_at(["test", "merged_rows"], "codebench")
        manifest = manager.read_version(manager.reference_at(["codebench", "rules_eval_report"], "codebench"))
        assert isinstance(manifest, pd.DataFrame)
        (report_url,) = manifest["report_url"]
        final = instance.get_latest_materialization_event(AssetKey(finalized_key))
        scored = instance.get_latest_materialization_event(RULES_EVAL_KEY)
        assert final is not None and final.asset_materialization is not None
        assert scored is not None and scored.asset_materialization is not None
        frozen = final.asset_materialization.metadata
        metrics = scored.asset_materialization.metadata
        assert frozen["rules"] == MetadataValue.int(0)
        assert frozen["Train digest"] == metrics["Train digest"] == MetadataValue.text(train.digest)
        assert frozen["Train rows"] == metrics["Train rows"] == MetadataValue.int(len(train_commands))
        assert metrics["Test digest"] == MetadataValue.text(test.digest)
        assert metrics["Test rows"] == MetadataValue.int(len(test_commands))
        assert metrics["Abstentions"] == metrics["Held-out support"] == MetadataValue.int(len(test_commands))
        assert metrics["Exact matches"] == metrics["Wrong matches"] == metrics["Evaluator errors"] == MetadataValue.int(0)
        chart = metrics["Outcome distribution"]
        assert isinstance(chart, MarkdownMetadataValue)
        assert f"  Abstain       {len(test_commands)}  {'█' * 30}" in chart.value
        train_eval, test_eval = [request for path, request in services.calls if path == "/v1/evaluate"]
        empty = {"version": 1, "kind": "empty_learned", "rules": []}
        assert train_eval["bundle"] == test_eval["bundle"] == empty
        assert frozen["bundle digest"] == metrics["Bundle digest"] == MetadataValue.text(
            sha256(json.dumps(empty, separators=(",", ":")).encode()).hexdigest()
        )
        for request, commands, split in ((train_eval, train_commands, "train"), (test_eval, test_commands, "test")):
            assert {row["command"] for row in request["rows"]} == commands.keys()
            assert all(row["id"].startswith(f"{split}:") for row in request["rows"])

        service = ArtifactDataService(DatasetArtifactCatalog(tmp_path, instance), definitions)
        origin = "http://localhost:3000"
        api = make_dataset_api(DatasetDataService(tmp_path, instance), None, 3000, artifacts=service)
        with TestClient(api, base_url=origin, headers={"Origin": origin}) as client:
            for key in (["codebench", "rules_dedup"], finalized_key):
                discovery = client.post("/artifacts/partitions", json={"asset_key": key})
                assert discovery.status_code == 200
                assert ArtifactPartitions.model_validate_json(discovery.content).partitions == ("codebench",)
                response = client.post("/artifacts/data", json={
                    "asset_key": key, "partition_key": "codebench", "query": {"limit": 10},
                })
                assert response.status_code == 200
                table = ArtifactTable.model_validate_json(response.content)
                assert table.page.total_rows == 0 and not table.page.rows
                assert table.data_schema.source == "declared"
            report = client.get(str(report_url).removeprefix("/codebench/api"))
            assert report.status_code == 200
            measurement_script = '<script id="measurements" type="application/json">'
            rows = json.loads(report.text.split(measurement_script, 1)[1].split("</script>", 1)[0])
            overall = {
                row["metric"]: row["value"] for row in rows if row["cohort"] == "overall" and row["axis"] == "outcome"
            }
            assert overall == {
                "support": 2, "correct": 0, "wrong": 0, "abstain": 2, "error": 0,
                "precision": None, "coverage": 0, "recall": 0,
            }
            assert {
                row["cohort"]: row["value"]
                for row in rows if row["axis"] == "outcome" and row["metric"] == "abstain"
            } == {"overall": 2, "trained": 1, "unseen": 1, "uncontaminated": 1}
