import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import NamedTuple

import httpx2
import pytest
from dagster import DagsterInstance, Definitions, MarkdownMetadataValue
from pydantic import JsonValue
from rules_capture_recovery_fixture import CaptureCompletion, exercise_capture_resume
from starlette.testclient import TestClient
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.artifact_views import ArtifactDataService, ArtifactPartitions, ArtifactTable
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager
from codebench_pipeline.dataset_artifacts import DatasetArtifactCatalog
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.rules_artifacts import RULES_EVAL_KEY
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.train_assets import register_dataset_partitions
from codebench_pipeline.train_config import TrainConfig, TrainSource

_GUIDE = Path(__file__).resolve().parents[2] / "business-configs/bashguard/guide.json"
_RUNTIME = f"sha256:{'a' * 64}"
_MODEL = "synthetic/qwen"
_TRAIN = {"pwd": "CAT-3", "date": "CAT-4"}
_TEST = {"pwd": "CAT-3", "pwd -L": "CAT-3", "whoami": "CAT-5"}


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
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append((self.path, request))
            match self.path:
                case "/v1/chat/completions":
                    response = complete(request)
                case "/v1/evaluate":
                    response = _evaluate(request)
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
    assert all(command not in json.dumps(messages[1:]) for command in ("pwd -L", "whoami"))
    if "next_rule_id" not in batch:
        tool = "finalize"
        arguments = {
            "justification": "Drop only the duplicate literal; the first rule covers its tests.",
            "drop_ids": ["cat-3.capture-0001-02"], "add": [],
        }
    else:
        category = batch["category"]
        command = batch["rows"][0]["command"]
        if len(messages) == 2 or (category == "CAT-3" and len(messages) == 4):
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


def _evaluate(request: dict) -> dict:
    assert request["required_runtime_version"] == _RUNTIME
    bundle = request["bundle"]
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


def test_rules_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    blobs: dict[str, bytes] = {}
    completion = CaptureCompletion(_complete, _MODEL)

    def split(number: int, commands: dict[str, str]) -> TrainConfig:
        sources = []
        for category in sorted(set(commands.values())):
            number += 1
            commit = f"{number:040x}"
            rows = "".join(
                f"{command},,,low,{category},{category}.1,fixture,,allow\n"
                for command in commands if commands[command] == category
            )
            data = ("command,description,util,severity,category,risk,source,seed,class\n" + rows).encode()
            blobs[f"/fixture/data/{commit}/commands.csv"] = data
            sources.append(TrainSource(
                repository="fixture/data", branch=f"feat/synthetic-{number}", commit=commit, path="commands.csv",
                category=category, sha256=sha256(data).hexdigest(),
            ))
        return TrainConfig(sources=sources)

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
            "train": split(0, _TRAIN), "test": split(10, _TEST),
        })
        DatasetRegistry(tmp_path).register(spec)
        register_dataset_partitions(instance, spec)
        monkeypatch.setattr("codebench_pipeline.repository_tree._repository_client", lambda: repository)
        definitions = build_definitions(tmp_path)
        manager = DataFrameIOManager(storage=spec.storage)

        def run(job: str, partition: str = "codebench", **options: object):
            return definitions.resolve_job_def(job).execute_in_process(
                instance=instance, partition_key=partition, **options,
            )

        assert run("refresh_dataset_configuration").success
        for category in sorted(set(_TRAIN.values())):
            assert run("load_train_category", category).success
        assert run("merge_train_data").success
        train = manager.reference_at(["train", "merged_rows"], "codebench")
        for category in sorted(set(_TEST.values())):
            assert run("load_test_category", category).success
        assert run("merge_test_data").success
        test = manager.reference_at(["test", "merged_rows"], "codebench")
        payload = json.dumps({
            "runtime": {"base_url": services.origin, "version": _RUNTIME},
            "prompt_guide": {"path": "guide.json", "sha256": sha256(_GUIDE.read_bytes()).hexdigest()},
            "categories": sorted(set(_TRAIN.values())),
            "train": {"digest": train.digest, "row_count": len(_TRAIN)},
            "test": {"digest": test.digest, "row_count": len(_TEST)},
        })
        assert run("register_rules", run_config={"ops": {"codebench__rules_registration": {"config": {
            "registration_json": payload, "expected_sha256": sha256(payload.encode()).hexdigest(),
        }}}}).success
        allowed_snapshot = train.digest
        read_version = DataFrameIOManager.read_version

        def read_pinned(storage: DataFrameIOManager, reference: AssetReference):
            if reference in (train, test):
                assert reference.digest == allowed_snapshot
            return read_version(storage, reference)

        monkeypatch.setattr(DataFrameIOManager, "read_version", read_pinned)
        assert run("capture_rules", "CAT-3").success
        assert not run("learn_rules", raise_on_error=False).success
        assert completion.requests == 3
        exercise_capture_resume(tmp_path, instance, completion, monkeypatch)
        capture_requests = completion.requests
        assert run("learn_rules").success
        assert completion.requests == capture_requests + 1
        for path, request in services.calls:
            if path == "/v1/evaluate":
                assert {row["command"] for row in request["rows"]}.isdisjoint({"pwd -L", "whoami"})
        services.calls.clear()
        allowed_snapshot = test.digest
        assert run("evaluate_rules").success
        assert [path for path, _ in services.calls] == ["/v1/evaluate"]
        request = services.calls[0][1]
        assert {row["command"] for row in request["rows"]} == _TEST.keys()
        assert len(request["bundle"]["rules"]) == 2
        _assert_public_metrics(instance, tmp_path, definitions)


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
        response = client.post(
            "/artifacts/data", headers=headers, json={"asset_key": key, "partition_key": partitions[0]},
        )
        assert response.status_code == 200
        private = client.post(
            "/artifacts/partitions", headers=headers, json={"asset_key": ["codebench", "rules_capture"]},
        )
        assert private.status_code == 200 and not ArtifactPartitions.model_validate_json(private.content).partitions
    result = ArtifactTable.model_validate_json(response.content)
    assert result.data_schema.source == "declared"
    cells = {(row.values["cohort"], row.values["axis"], row.values["metric"]): row.values["value"]
             for row in result.page.rows}
    assert cells["overall", "outcome", "correct"] == 1
    assert cells["overall", "outcome", "abstain"] == 2
    assert cells["overall", "overlap", "rows"] == 1
    event = instance.get_latest_materialization_event(RULES_EVAL_KEY)
    assert event is not None and event.asset_materialization is not None
    chart = event.asset_materialization.metadata["Outcome distribution"]
    assert isinstance(chart, MarkdownMetadataValue)
    assert "Unseen categories (n=1)" in chart.value and "█" in chart.value
