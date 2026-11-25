import json
import logging
import os
import sys
from collections import Counter
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Final
from unittest.mock import patch

from dagster import (
    DagsterEventType,
    DagsterInstance,
    EventLogEntry,
    GraphDefinition,
    JobDefinition,
    Nothing,
    OpExecutionContext,
    Out,
    build_reconstructable_job,
    execute_job,
    multiprocess_executor,
    op,
)

from codebench_pipeline.agent_trace import graph_log
from codebench_pipeline.batch_labeling import BatchExecution, annotate_batches
from codebench_pipeline.grounding_tools import GroundingCredentials
from codebench_pipeline.keenable import KeenableCollection, load_declaration
from codebench_pipeline.keenable_planner import SearchPlanner
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.trace_projection import GRAPH_PREFIX, project_run
from tests.test_batch_labeling import QUOTE, URL, batch_config, live_mcp_fixture, remote_model, row_retrieval
from tests.test_dataset_rules import make_candidate, make_rules
from tests.test_keenable import DECLARATION, result
from tests.test_keenable_planner import completion

_MODEL_KEY: Final = "synthetic-trace-model-key"
_GROUNDING_KEY: Final = "fixture-key"
_SEARCH_KEY: Final = "synthetic-trace-search-key"
_ROW_IDS: Final = ("synthetic-trace-row-one", "synthetic-trace-row-two")
_COMMAND: Final = "probe inspect synthetic-trace-command"
_PAGE_MARKER: Final = "synthetic-trace-private-page"
_PAGES: Final = (
    f"https://operator.example/{_PAGE_MARKER}/unexpected-output",
    f"https://operator.example/{_PAGE_MARKER}/quoting",
)
_SENTINELS: Final = (_MODEL_KEY, _GROUNDING_KEY, _SEARCH_KEY, *_ROW_IDS, _COMMAND, _PAGE_MARKER, QUOTE, URL)
_SHAPE: Final = Counter(
    {
        ("label_rows", "labeler", ("complete", "tools", "complete", "tools", "complete", "tools")): 2,
        ("plan_discovery", "keenable_planner", ("choose", "search", "choose", "search", "choose")): 1,
    }
)


@contextmanager
def _synthetic_step(context: OpExecutionContext) -> Generator[None]:
    with (
        graph_log(context.log.info, os.environ.get),
        patch("socket.getaddrinfo", side_effect=AssertionError("Unexpected network lookup in trace QA")),
        patch(
            "socket.socket.connect", side_effect=AssertionError("Unexpected network connection in trace QA")
        ),
    ):
        yield


def _model_reply(_url: str, body: bytes, _key: str | None, _timeout: float) -> HTTPResponse:
    return row_retrieval(body)


@op(out=Out(Nothing))
def label_rows(context: OpExecutionContext) -> None:
    with TemporaryDirectory(prefix="trace-labeler-") as directory:
        root = Path(directory)
        config = batch_config(root).model_copy(
            update={"inference_mode": "plain", "capacity": None, "row_concurrency": 2},
        )
        rows = [make_candidate(identity).model_copy(update={"command": _COMMAND}) for identity in _ROW_IDS]
        with (
            _synthetic_step(context),
            patch("codebench_pipeline.batch_labeling.plain_complete", side_effect=_model_reply),
            live_mcp_fixture({URL: QUOTE}),
        ):
            attempts = annotate_batches(
                rows,
                make_rules(),
                remote_model(),
                config,
                root,
                execution=BatchExecution(_MODEL_KEY, GroundingCredentials(keenable=_GROUNDING_KEY)),
            )
        assert [attempt.status for attempt in attempts] == ["success", "success"]


@op(out=Out(Nothing))
def plan_discovery(context: OpExecutionContext) -> None:
    with TemporaryDirectory(prefix="trace-planner-") as directory:
        root = Path(directory)
        planner = SearchPlanner("https://model.invalid/v1", "test-model", None, root, key=_MODEL_KEY)
        choices = (
            completion({"option": "workloads-unexpected-output"}),
            completion({"option": "workloads-quoting"}),
            completion({"option": None}),
        )
        with (
            _synthetic_step(context),
            patch("codebench_pipeline.keenable_planner.complete", side_effect=choices),
            patch(
                "codebench_pipeline.keenable.request_direct", side_effect=tuple(result(url) for url in _PAGES)
            ),
        ):
            catalog = KeenableCollection(root, load_declaration(DECLARATION), planner=planner)
            receipt = catalog.discover_with_credential(_SEARCH_KEY)
        plan = receipt.plan
        assert plan is not None
        assert (plan.mode, plan.stop, len(plan.decisions), len(plan.feedback)) == (
            "adaptive",
            "planner_stop",
            3,
            2,
        )


def build_flow_trace_job() -> JobDefinition:
    return JobDefinition(
        graph_def=GraphDefinition(name="flow_trace_qa", node_defs=[label_rows, plan_discovery]),
        executor_def=multiprocess_executor,
    )


def _assert_no_payload(text: str) -> None:
    if any(sentinel in text for sentinel in _SENTINELS):
        raise AssertionError("A trace contained a synthetic sensitive payload sentinel")


def _assert_trace(entries: Sequence[EventLogEntry]) -> None:
    workers = {
        entry.step_key: entry.dagster_event.pid
        for entry in entries
        if entry.dagster_event_type == DagsterEventType.STEP_WORKER_STARTED
        and entry.dagster_event is not None
    }
    assert workers.keys() == {"label_rows", "plan_discovery"}
    assert len(set(workers.values())) == 2 and os.getpid() not in workers.values()

    graph_steps: Counter[str | None] = Counter()
    for entry in entries:
        message = entry.user_message
        _assert_no_payload(message)
        if message.startswith(GRAPH_PREFIX):
            assert entry.level == logging.INFO
            graph_steps[entry.step_key] += 1
    assert graph_steps == Counter({"label_rows": 2, "plan_discovery": 1})

    (terminal,) = [entry for entry in entries if entry.dagster_event_type == DagsterEventType.RUN_SUCCESS]
    projection = project_run(terminal, entries, enabled_at=0)
    _assert_no_payload(json.dumps(projection.spans))
    assert projection.rejected_graphs == 0

    spans = projection.spans
    assert all(span["status"]["code"] == 1 for span in spans)
    root = spans[0]
    steps = {span["spanId"]: span["name"] for span in spans if span.get("parentSpanId") == root["spanId"]}
    assert Counter(steps.values()) == Counter({"label_rows": 1, "plan_discovery": 1})
    projected = Counter(
        (
            steps[parent],
            graph["name"].partition(" #")[0],
            tuple(node["name"] for node in spans if node.get("parentSpanId") == graph["spanId"]),
        )
        for graph in spans
        if (parent := graph.get("parentSpanId")) in steps
    )
    assert projected == _SHAPE


def _run_trace(scratch: Path, *, deployed: bool = False) -> None:
    tests = Path(__file__).resolve().parents[1]
    deployment = os.environ["DAGSTER_HOME"] if deployed else None
    env = {key: os.environ[key] for key in ("PATH", "LD_LIBRARY_PATH") if key in os.environ}
    env.update(
        HOME=str(scratch),
        TMPDIR=str(scratch),
        PYTHONPATH=os.pathsep.join(map(str, (tests.parent, tests))),
        PYTHONDONTWRITEBYTECODE="1",
        DAGSTER_DISABLE_TELEMETRY="1",
        MLFLOW_TRACKING_URI="http://synthetic.invalid",
    )
    with (
        patch.dict(os.environ, env, clear=True),
        DagsterInstance.local_temp(str(scratch))
        if deployment is None
        else DagsterInstance.from_config(deployment) as instance,
        execute_job(
            build_reconstructable_job("tests.e2e.test_flow_trace", "build_flow_trace_job"),
            instance=instance,
            run_config={"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}},
            raise_on_error=False,
        ) as run,
    ):
        print(f"CODEBENCH_TRACE_QA_RUN_ID={run.run_id}", flush=True)
        assert run.success, "Synthetic Dagster job did not succeed"
        _assert_trace(instance.all_logs(run.run_id))


def test_real_graphs_are_traced_under_their_worker_steps(tmp_path: Path) -> None:
    _run_trace(tmp_path)


if __name__ == "__main__":
    if sys.argv[1:] != ["--deployed"]:
        sys.exit("Use --deployed to explicitly write a synthetic run to the configured Dagster instance.")
    with TemporaryDirectory(prefix="codebench-trace-smoke-") as temporary:
        _run_trace(Path(temporary), deployed=True)
