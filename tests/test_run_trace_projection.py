import json
import os
import unittest
from collections import deque
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from time import time, time_ns
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from dagster import (
    DagsterEventType,
    DagsterInstance,
    GraphDefinition,
    JobDefinition,
    OpExecutionContext,
    RetryRequested,
    build_sensor_context,
    execute_job,
    multiprocess_executor,
    op,
    reconstructable,
)
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.trace_sensor import mlflow_trace_sensor

_JSON = TypeAdapter(dict[str, JsonValue])
_PRIVATE = "private-command-partition-prompt"
_PREFIX = "CODEBENCH_GRAPH_V1 "


@op
def terminal() -> None:
    return None


cli_job = JobDefinition(graph_def=GraphDefinition("capture_rules", node_defs=[terminal]))


@op
def worker_step(context: OpExecutionContext) -> None:
    context.log.info(_PRIVATE)
    for _ in range(2):
        start = time_ns()
        context.log.info(_PREFIX + json.dumps({
            "graph": "rules_capture", "id": uuid4().hex, "status": "ok",
            "start_ns": start, "end_ns": start + 2,
            "nodes": [
                {"name": "complete", "start_ns": start, "end_ns": start + 1, "status": "ok"},
                {"name": "tools", "start_ns": start + 1, "end_ns": start + 2, "status": "ok"},
            ],
        }))
    start = time_ns()
    context.log.info(_PREFIX + json.dumps({
        "graph": "labeler", "id": uuid4().hex, "status": "ok",
        "start_ns": start, "end_ns": start, "nodes": [], "prompt": _PRIVATE,
    }))
    context.log.info(f'{_PREFIX}{{"graph":"{_PRIVATE}"}}')
    if context.retry_number == 0:
        raise RetryRequested(max_retries=1) from RuntimeError(_PRIVATE)
    raise RuntimeError(_PRIVATE)


def worker_job() -> JobDefinition:
    return JobDefinition(
        graph_def=GraphDefinition("worker_job", node_defs=[worker_step]),
        executor_def=multiprocess_executor,
    )


class MlflowWire:
    def __init__(self) -> None:
        self.experiment_exists = False
        self.creates = 0
        self.outage = False
        self.posts: list[dict[str, JsonValue]] = []
        self.acks: deque[tuple[int, str, bytes]] = deque()

    @contextmanager
    def serve(self) -> Generator[str]:
        wire = self

        class Handler(BaseHTTPRequestHandler):
            def reply(self, code: int, body: bytes, media_type: str = "application/json") -> None:
                self.send_response(code)
                self.send_header("Content-Type", media_type)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                parsed = urlsplit(self.path)
                if parsed.path != "/api/2.0/mlflow/experiments/get-by-name" or parse_qs(parsed.query) != {
                    "experiment_name": ["CodeBench"]
                }:
                    self.reply(400, b"{}")
                elif wire.outage:
                    self.reply(503, b"{}")
                elif wire.experiment_exists:
                    self.reply(200, b'{"experiment":{"experiment_id":"42"}}')
                else:
                    self.reply(404, b'{"error_code":"RESOURCE_DOES_NOT_EXIST"}')

            def do_POST(self) -> None:
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                if self.path == "/api/2.0/mlflow/experiments/create" and _JSON.validate_json(raw) == {
                    "name": "CodeBench"
                }:
                    wire.creates += 1
                    wire.experiment_exists = True
                    self.reply(200, b'{"experiment_id":"42"}')
                elif (
                    self.path == "/v1/traces" and wire.experiment_exists
                    and self.headers["X-MLflow-Experiment-Id"] == "42"
                    and self.headers["Content-Type"] == "application/json"
                ):
                    wire.posts.append(_JSON.validate_json(raw))
                    code, content_type, body = wire.acks.popleft() if wire.acks else (200, "application/json", b"{}")
                    self.reply(code, body, content_type)
                else:
                    self.reply(400, b"{}")

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                yield f"http://127.0.0.1:{server.server_port}"
            finally:
                server.shutdown()
                worker.join()


def read_spans(post: dict[str, JsonValue]) -> list[dict[str, JsonValue]]:
    resources = post["resourceSpans"]
    assert isinstance(resources, list) and isinstance(resources[0], dict)
    scopes = resources[0]["scopeSpans"]
    assert isinstance(scopes, list) and isinstance(scopes[0], dict)
    return TypeAdapter(list[dict[str, JsonValue]]).validate_python(scopes[0]["spans"])


class RunTraceTests(unittest.TestCase):
    def test_cli_uses_deployment_cutoff_even_without_a_cursor(self) -> None:
        cutoffs: list[datetime] = []

        @op
        def begin_observation() -> None:
            cutoffs.append(datetime.now(UTC))

        running_before_deployment = JobDefinition(
            graph_def=GraphDefinition("running_before_deployment", node_defs=[begin_observation]),
        )

        with TemporaryDirectory() as directory, dataset_instance(Path(directory)) as instance:
            cli_job.execute_in_process(instance=instance)
            running_before_deployment.execute_in_process(instance=instance)
            current = cli_job.execute_in_process(instance=instance, tags={"dagster/partition": _PRIVATE})
            wire = MlflowWire()
            with wire.serve() as uri, patch.dict(os.environ, {
                "MLFLOW_TRACKING_URI": uri,
                "CODEBENCH_MLFLOW_TRACE_START_AT": cutoffs[0].isoformat(),
            }):
                sensor = mlflow_trace_sensor()
                with build_sensor_context(instance=instance, last_sensor_start_time=time()) as ctx:
                    tick = sensor.evaluate_tick(ctx)
                self.assertIn("acknowledged 1", tick.skip_message or "")
                self.assertFalse(tick.run_requests or tick.asset_events)
                self.assertEqual(wire.creates, 1)
                self.assertEqual(len(wire.posts), 1)
                self.assertNotIn(_PRIVATE, json.dumps(wire.posts))
                root, step = read_spans(wire.posts[0])
                self.assertEqual(root["name"], f"capture_rules [{current.run_id}]")
                attributes = root["attributes"]
                assert isinstance(attributes, list)
                self.assertIn(
                    {"key": "mlflow.traceTag.dagster.run_id", "value": {"stringValue": current.run_id}},
                    attributes,
                )
                self.assertEqual(step["parentSpanId"], root["spanId"])
                self.assertEqual(step["name"], "terminal")
                self.assertEqual(step["traceId"], root["traceId"])

    def test_worker_graphs_parent_to_each_retry_without_exporting_logs_or_errors(self) -> None:
        with TemporaryDirectory() as directory, DagsterInstance.local_temp(directory) as instance:
            enable = time()
            with execute_job(reconstructable(worker_job), instance=instance) as result:
                self.assertFalse(result.success)
                self.assertIn(DagsterEventType.STEP_WORKER_STARTED, [event.event_type for event in result.all_events])
                run_id = result.run_id
            wire = MlflowWire()
            with wire.serve() as uri, patch.dict(os.environ, {
                "MLFLOW_TRACKING_URI": uri, "CODEBENCH_MLFLOW_TRACE_START_AT": "",
            }):
                with build_sensor_context(instance=instance, last_sensor_start_time=enable) as ctx:
                    tick = mlflow_trace_sensor().evaluate_tick(ctx)
            self.assertIn("acknowledged 1", tick.skip_message or "")
            self.assertEqual(len(wire.posts), 1)
            self.assertNotIn(_PRIVATE, json.dumps(wire.posts))
            root, *children = read_spans(wire.posts[0])
            self.assertEqual(root["name"], f"worker_job [{run_id}]")
            self.assertEqual(root["status"], {"code": 2})
            by_name = {span["name"]: span for span in children if isinstance(span["name"], str)}
            for key, graph_ordinals, outcome in (("worker_step", (1, 2), "retry"), ("worker_step #2", (3, 4), "error")):
                attempt = by_name[key]
                self.assertEqual(attempt["parentSpanId"], root["spanId"])
                self.assertEqual(attempt["attributes"], [{"key": "codebench.status", "value": {"stringValue": outcome}}])
                for ordinal in graph_ordinals:
                    graph = by_name[f"rules_capture #{ordinal}"]
                    self.assertEqual(graph["parentSpanId"], attempt["spanId"])
                    self.assertEqual(
                        [span["name"] for span in children if span["parentSpanId"] == graph["spanId"]],
                        ["complete", "tools"],
                    )
            self.assertEqual(len(children), 14)

    def test_outage_and_partial_or_invalid_ack_keep_only_unacknowledged_runs_pending(self) -> None:
        wire = MlflowWire()
        with (
            TemporaryDirectory() as directory, DagsterInstance.local_temp(directory) as instance,
            wire.serve() as uri, patch.dict(os.environ, {
                "MLFLOW_TRACKING_URI": uri, "CODEBENCH_MLFLOW_TRACE_START_AT": "",
            }),
        ):
            enable = time()
            sensor = mlflow_trace_sensor()
            wire.outage = True
            with build_sensor_context(instance=instance, last_sensor_start_time=enable) as ctx:
                unavailable = sensor.evaluate_tick(ctx)
            self.assertIn("deferred", unavailable.skip_message or "")
            first = cli_job.execute_in_process(instance=instance)
            second = cli_job.execute_in_process(instance=instance)
            wire.outage = False
            wire.acks.extend((
                (200, "application/json", b"{}"),
                (200, "application/json", b'{"partialSuccess":{"rejectedSpans":"1","errorMessage":"private"}}'),
                (200, "text/html", b"<html>proxy response</html>"),
                (200, "application/json", b'{"partialSuccess":{"rejectedSpans":"0"}}'),
            ))
            cursor = unavailable.cursor
            for signal in ("deferred", "deferred", "acknowledged 1", "no new terminal runs"):
                with build_sensor_context(instance=instance, cursor=cursor, last_sensor_start_time=time()) as ctx:
                    tick = sensor.evaluate_tick(ctx)
                self.assertIn(signal, tick.skip_message or "")
                self.assertNotIn("private", tick.skip_message or "")
                self.assertFalse(tick.run_requests or tick.asset_events)
                cursor = tick.cursor
            self.assertEqual(wire.creates, 1)
            self.assertEqual([read_spans(post)[0]["name"] for post in wire.posts], [
                f"capture_rules [{first.run_id}]", *([f"capture_rules [{second.run_id}]"] * 3),
            ])
            self.assertEqual(wire.posts[1], wire.posts[2])
            self.assertEqual(wire.posts[1], wire.posts[3])
