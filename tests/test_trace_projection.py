import json
import logging
import unittest

from dagster import DagsterEvent, DagsterEventType, EventLogEntry
from pydantic import JsonValue

from codebench_pipeline.agent_trace import GraphName
from codebench_pipeline.trace_projection import GRAPH_PREFIX, Projection, project_run


def _project(*nodes: JsonValue, graph_name: GraphName = "keenable_planner") -> Projection:
    run_id = "01961007-bce2-4995-8d11-f546ee29f1d1"
    job = "observation"
    key = "planner"
    step = EventLogEntry(
        None,
        logging.INFO,
        "",
        run_id,
        1.0,
        step_key=key,
        dagster_event=DagsterEvent(DagsterEventType.STEP_START.value, job),
    )
    graph = EventLogEntry(
        None,
        logging.INFO,
        GRAPH_PREFIX
        + json.dumps(
            {
                "graph": graph_name,
                "id": "01961007bce249958d11f546ee29f1d2",
                "start_ns": 2_000_000_000,
                "end_ns": 2_000_000_099,
                "status": "error",
                "nodes": nodes,
            }
        ),
        run_id,
        3.0,
        step_key=key,
    )
    terminal = EventLogEntry(
        None,
        logging.INFO,
        "",
        run_id,
        4.0,
        job_name=job,
        dagster_event=DagsterEvent(DagsterEventType.RUN_FAILURE.value, job),
    )
    return project_run(terminal, [step, graph, terminal], enabled_at=0)


class TraceProjectionTests(unittest.TestCase):
    def test_call_types_and_timing_survive_projection_under_numbered_cycles(self) -> None:
        nodes: tuple[JsonValue, ...] = (
            {
                "name": "choose",
                "start_ns": 2_000_000_001,
                "end_ns": 2_000_000_010,
                "status": "ok",
                "calls": [
                    {
                        "kind": "llm",
                        "name": "chat_completions",
                        "start_ns": 2_000_000_002,
                        "end_ns": 2_000_000_006,
                        "status": "ok",
                    },
                ],
            },
            {
                "name": "search",
                "start_ns": 2_000_000_011,
                "end_ns": 2_000_000_025,
                "status": "ok",
                "calls": [
                    {
                        "kind": "tool",
                        "name": "search",
                        "start_ns": 2_000_000_012,
                        "end_ns": 2_000_000_018,
                        "status": "error",
                    },
                    {
                        "kind": "tool",
                        "name": "search",
                        "start_ns": 2_000_000_018,
                        "end_ns": 2_000_000_023,
                        "status": "ok",
                    },
                ],
            },
            {"name": "choose", "start_ns": 2_000_000_030, "end_ns": 2_000_000_039, "status": "error"},
        )
        projection = _project(*nodes)

        self.assertEqual(projection.rejected_graphs, 0)
        _, graph, first, choose, llm, search, failed_tool, tool, second, failed_choice, attempt = (
            projection.spans
        )
        self.assertEqual(graph.get("parentSpanId"), attempt["spanId"])
        self.assertEqual(graph["status"]["code"], 2)
        for span, kind in ((graph, '"AGENT"'), (first, '"CHAIN"'), (second, '"CHAIN"')):
            self.assertIn({"key": "mlflow.spanType", "value": {"stringValue": kind}}, span["attributes"])
        self.assertEqual(
            [
                (
                    span["name"],
                    span.get("parentSpanId"),
                    span["startTimeUnixNano"],
                    span["endTimeUnixNano"],
                    span["status"]["code"],
                )
                for span in (first, second)
            ],
            [
                ("Cycle #1", graph["spanId"], "2000000001", "2000000025", 1),
                ("Cycle #2", graph["spanId"], "2000000030", "2000000039", 2),
            ],
        )
        self.assertEqual(
            [(span["name"], span.get("parentSpanId")) for span in (choose, search, failed_choice)],
            [("choose", first["spanId"]), ("search", first["spanId"]), ("choose", second["spanId"])],
        )
        self.assertEqual(
            [
                (
                    call["name"],
                    call.get("parentSpanId"),
                    call["startTimeUnixNano"],
                    call["endTimeUnixNano"],
                    call["status"]["code"],
                    next(
                        item["value"]["stringValue"]
                        for item in call["attributes"]
                        if item["key"] == "mlflow.spanType"
                    ),
                )
                for call in (llm, failed_tool, tool)
            ],
            [
                ("LLM: chat_completions", choose["spanId"], "2000000002", "2000000006", 1, '"LLM"'),
                ("Tool: search", search["spanId"], "2000000012", "2000000018", 2, '"TOOL"'),
                ("Tool: search", search["spanId"], "2000000018", "2000000023", 1, '"TOOL"'),
            ],
        )
        ids = [span["spanId"] for span in projection.spans]
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(ids, [span["spanId"] for span in _project(*nodes).spans])
        self.assertNotIn("codebench.business_outcome", {item["key"] for item in llm["attributes"]})

    def test_business_results_are_visible_on_calls_cycles_and_graphs(self) -> None:
        rule_id = "cat-2.capture-0001-01"
        digest = "a" * 64
        rejected: JsonValue = {
            "kind": "llm",
            "name": "chat_completions",
            "start_ns": 2_000_000_001,
            "end_ns": 2_000_000_002,
            "status": "ok",
            "result": {"status": "rejected", "count": 0},
        }
        projection = _project(
            {
                "name": "complete",
                "start_ns": 2_000_000_001,
                "end_ns": 2_000_000_002,
                "status": "ok",
                "calls": [rejected],
            },
            {"name": "complete", "start_ns": 2_000_000_001, "end_ns": 2_000_000_002, "status": "ok"},
            {
                "name": "tools",
                "start_ns": 2_000_000_002,
                "end_ns": 2_000_000_009,
                "status": "ok",
                "calls": [
                    {
                        "kind": "tool",
                        "name": "finalize",
                        "start_ns": 2_000_000_003,
                        "end_ns": 2_000_000_008,
                        "status": "ok",
                        "result": {
                            "status": "produced",
                            "dropped_rule_ids": [rule_id],
                            "count": 1,
                            "receipts": [{"receipt": 3, "status": "returned", "bundle_sha256": digest}],
                        },
                    }
                ],
            },
            graph_name="rules_dedup",
        )

        _, graph, retry_cycle, _, rejected_call, cycle, _, _, call, _ = projection.spans
        codes = [span["status"]["code"] for span in (graph, retry_cycle, rejected_call, cycle, call)]
        self.assertEqual(codes, [2, 1, 1, 1, 1])
        for span, key in ((retry_cycle, "business_outcomes"), (rejected_call, "business_outcome")):
            attrs = {item["key"]: item["value"]["stringValue"] for item in span["attributes"]}
            self.assertEqual(attrs[f"codebench.{key}"], '["rejected"]' if key.endswith("s") else "rejected")
        expected = (
            (graph, {"business_outcomes": ["rejected", "produced"], "cycle_count": 2}),
            (cycle, {"actions": ["finalize"], "result_counts": {"finalize": [1]}}),
            (call, {"action": "finalize", "business_outcome": "produced", "count": 1}),
        )
        for span, fields in expected:
            attrs = {item["key"]: item["value"]["stringValue"] for item in span["attributes"]}
            summary = json.loads(attrs["mlflow.spanOutputs"])
            self.assertEqual({key: summary[key] for key in fields}, fields)
            for key, value in fields.items():
                self.assertEqual(
                    attrs[f"codebench.{key}"], value if isinstance(value, str) else json.dumps(value)
                )
            self.assertEqual(json.loads(attrs["codebench.dropped_rule_ids"]), [rule_id])
            self.assertEqual(json.loads(attrs["codebench.receipt_numbers"]), [3])
            self.assertEqual(json.loads(attrs["codebench.bundle_sha256s"]), [digest])

    def test_rejects_unsafe_call_records_without_exporting_their_contents(self) -> None:
        private = "private-url-or-argument"
        bad_receipts: JsonValue = [{"receipt": 1, "status": "returned", "bundle_sha256": private}]
        cases: tuple[tuple[str, dict[str, JsonValue]], ...] = (
            ("unknown name", {"name": private}),
            ("name belongs to a different kind", {"name": "chat_completions"}),
            ("unknown kind", {"kind": private}),
            ("extra payload", {"arguments": private}),
            ("non-digest payload reference", {"inputs": "../" + private}),
            ("unknown result status", {"result": {"status": private}}),
            ("extra result payload", {"result": {"status": "returned", "message": private}}),
            ("unsafe rule ID", {"result": {"status": "returned", "accepted_rule_ids": [private]}}),
            ("unsafe count", {"result": {"status": "returned", "count": True}}),
            ("unsafe receipt", {"result": {"status": "returned", "receipts": bad_receipts}}),
            ("start outside node", {"start_ns": 2_000_000_001}),
            ("end outside node", {"end_ns": 2_000_000_010}),
            ("inverted interval", {"end_ns": 2_000_000_002}),
        )
        for label, override in cases:
            with self.subTest(label=label):
                projection = _project(
                    {
                        "name": "search",
                        "start_ns": 2_000_000_002,
                        "end_ns": 2_000_000_009,
                        "status": "error",
                        "calls": [
                            {
                                "kind": "tool",
                                "name": "search",
                                "start_ns": 2_000_000_003,
                                "end_ns": 2_000_000_008,
                                "status": "error",
                                **override,
                            }
                        ],
                    }
                )
                self.assertEqual(projection.rejected_graphs, 1)
                self.assertEqual(len(projection.spans), 2)
                self.assertNotIn(private, json.dumps(projection.spans))
