import os
from logging import INFO
from pathlib import Path
from threading import Barrier
from uuid import UUID

import pytest
from dagster import DagsterInstance, OpExecutionContext, job, op
from pydantic import JsonValue, TypeAdapter
from test_batch_labeling import (
    QUOTE,
    URL,
    batch_config,
    live_mcp_fixture,
    remote_model,
    request_parts,
    row_retrieval,
    tool_reply,
    unsupported,
)
from test_dataset_rules import make_candidate, make_rules
from test_keenable import result
from test_keenable_planner import completion
from test_rules_learning import Step, _agent, _client, _package, _train

from codebench_pipeline.agent_trace import graph_log
from codebench_pipeline.batch_labeling import annotate_batches
from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_planner import SearchPlanner
from codebench_pipeline.keenable_policy import CurrentKeenableDeclaration, UnrestrictedSearch
from codebench_pipeline.keenable_transport import DirectResponse, Request
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.rules_learning import capture_rules, deduplicate_rules

_PREFIX = "CODEBENCH_GRAPH_V1 "
_PRIVATE = "synthetic-private-observation"
_JSON = TypeAdapter(dict[str, JsonValue])


def _events(instance: DagsterInstance, run_id: str, step: str) -> list[dict[str, JsonValue]]:
    records: list[dict[str, JsonValue]] = []
    identities: set[str] = set()
    for event in instance.all_logs(run_id):
        if not event.user_message.startswith(_PREFIX):
            continue
        assert event.step_key == step and event.level == INFO
        assert _PRIVATE not in event.user_message and "https://" not in event.user_message
        record = _JSON.validate_json(event.user_message.removeprefix(_PREFIX))
        assert set(record) == {"graph", "id", "start_ns", "end_ns", "status", "nodes"}
        identity, start, end, nodes = (record[key] for key in ("id", "start_ns", "end_ns", "nodes"))
        assert isinstance(identity, str) and UUID(hex=identity).version == 4
        assert UUID(hex=identity).hex == identity
        assert identity not in identities
        identities.add(identity)
        assert type(start) is int and type(end) is int and 10**18 <= start <= end
        assert isinstance(nodes, list)
        for node in nodes:
            assert isinstance(node, dict) and set(node) == {"name", "start_ns", "end_ns", "status"}
            node_start, node_end = node["start_ns"], node["end_ns"]
            assert type(node_start) is int and type(node_end) is int
            assert start <= node_start <= node_end <= end
            start = node_end
        records.append(record)
    return records


def _nodes(record: dict[str, JsonValue]) -> list[tuple[JsonValue, JsonValue]]:
    nodes = record["nodes"]
    assert isinstance(nodes, list)
    return [(node["name"], node["status"]) for node in nodes if isinstance(node, dict)]


def test_labeler_workers_keep_each_rows_visits_under_the_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_wave = Barrier(2)
    candidates = [make_candidate(_PRIVATE), make_candidate("grounded")]
    config = batch_config(tmp_path).model_copy(
        update={"inference_mode": "plain", "capacity": None, "row_concurrency": 2}
    )
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://synthetic.invalid")

    def reply(_url: str, body: bytes, _key: str | None, _timeout: float) -> HTTPResponse:
        (row,), observations = request_parts(body)
        identity = row["candidate"]["candidate_id"]
        if not observations:
            first_wave.wait(timeout=5)
        if identity == _PRIVATE:
            return tool_reply("finalize", {"outcome": unsupported(row)})
        return row_retrieval(body)

    monkeypatch.setattr("codebench_pipeline.batch_labeling.plain_complete", reply)

    @op
    def label_rows(context: OpExecutionContext) -> list[str]:
        with graph_log(context.log.info, os.environ.get):
            attempts = annotate_batches(candidates, make_rules(), remote_model(), config, tmp_path)
        return [item.status for item in attempts]

    @job
    def labeling_job() -> None:
        label_rows()

    with DagsterInstance.ephemeral() as instance, live_mcp_fixture({URL: QUOTE}):
        run = labeling_job.execute_in_process(instance=instance)
        records = _events(instance, run.run_id, "label_rows")

    assert run.output_for_node("label_rows") == ["failure", "success"]
    assert [(item["graph"], item["status"]) for item in records] == [("labeler", "ok")] * 2
    assert sorted([_nodes(item) for item in records], key=len) == [
        [("complete", "ok"), ("tools", "ok")],
        [("complete", "ok"), ("tools", "ok")] * 3,
    ]


def test_capture_and_nomination_judge_emit_distinct_graph_names(monkeypatch: pytest.MonkeyPatch) -> None:
    train = _train("pwd -P", "pwd --physical")
    first = _package("pwd -P", "cat-6.capture-0001-01", _PRIVATE)
    second = _package("pwd -P|pwd --physical", "cat-6.capture-0001-02", positive="pwd --physical")
    capture_steps: tuple[Step, ...] = (
        ("add_rule", first),
        ("add_rule", second),
        ("finish", {"justification": "Compare variants."}),
    )
    judge_step: Step = ("finalize", {
        "drop_ids": [], "add": [], "justification": "Keep the two reviewed forms as separate rules.",
    })
    client = _client([])
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://synthetic.invalid")

    @op
    def learn_rules(context: OpExecutionContext) -> int:
        with graph_log(context.log.info, os.environ.get):
            capture = capture_rules(_agent(iter(capture_steps)), train, client)
            dedup = deduplicate_rules(_agent(iter((judge_step,))), train, client, (capture,))
        assert capture.complete and dedup.complete
        return len(dedup.decisions)

    @job
    def learning_job() -> None:
        learn_rules()

    with DagsterInstance.ephemeral() as instance:
        run = learning_job.execute_in_process(instance=instance)
        records = _events(instance, run.run_id, "learn_rules")

    assert run.output_for_node("learn_rules") == 1
    assert [(item["graph"], item["status"]) for item in records] == [
        ("rules_capture", "ok"), ("rules_dedup", "ok")
    ]
    assert [_nodes(item) for item in records] == [[("complete", "ok"), ("tools", "ok")] * n for n in (3, 1)]


@pytest.mark.parametrize("mode", ["enabled", "disabled", "standalone", "failure"])
def test_keenable_planner_records_only_bound_enabled_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    declaration = CurrentKeenableDeclaration(
        schema_version="3",
        dataset_id="codebench",
        generation="graph-fixture",
        sources=(),
        search_plan=UnrestrictedSearch(mode="adaptive", max_queries=2, unrestricted_cases=("plan",)),
    )
    planner = SearchPlanner("https://model.test/v1", "test-model", None, tmp_path / "receipts")
    options = iter(("plan-reference", "plan-inventory"))
    if mode == "disabled":
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    else:
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://synthetic.invalid")

    def decide(_url: str, _body: bytes, _key: str | None, _timeout: float) -> HTTPResponse:
        return completion({"option": next(options)})

    def search(_request: Request) -> DirectResponse:
        if mode == "failure":
            raise RuntimeError(_PRIVATE)
        return result(f"https://source.test/{_PRIVATE}")

    monkeypatch.setattr("codebench_pipeline.keenable_planner.complete", decide)
    monkeypatch.setattr("codebench_pipeline.keenable.request_direct", search)

    @op
    def plan_searches(context: OpExecutionContext) -> str:
        collection = KeenableCollection(tmp_path, declaration, planner=planner)
        if mode == "standalone":
            catalog = collection.discover_with_credential("synthetic-key")
        else:
            with graph_log(context.log.info, os.environ.get):
                catalog = collection.discover_with_credential("synthetic-key")
        assert catalog.plan is not None
        return catalog.plan.stop

    @job
    def planner_job() -> None:
        plan_searches()

    with DagsterInstance.ephemeral() as instance:
        run = planner_job.execute_in_process(instance=instance, raise_on_error=False)
        records = _events(instance, run.run_id, "plan_searches")

    assert run.success == (mode != "failure")
    if run.success:
        assert run.output_for_node("plan_searches") == "budget"
    if mode in {"disabled", "standalone"}:
        assert records == []
        return
    (trace,) = records
    assert trace["graph"] == "keenable_planner"
    if mode == "failure":
        assert trace["status"] == "error"
        assert _nodes(trace) == [("choose", "ok"), ("search", "error")]
    else:
        assert trace["status"] == "ok"
        assert _nodes(trace) == [("choose", "ok"), ("search", "ok")] * 2 + [("choose", "ok")]
