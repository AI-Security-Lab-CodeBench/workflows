import json
import logging
import os
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import Final, NotRequired, TypedDict
from uuid import UUID

from dagster import DagsterEventType, EventLogEntry

from codebench_pipeline.rules_trace_outputs import Outcome, RulesTraceOutputs
from codebench_pipeline.trace_graph_record import GRAPH_PREFIX as GRAPH_PREFIX
from codebench_pipeline.trace_graph_record import (
    CallSummary,
    GroupSummary,
    describe_call,
    describe_group,
    parse_graph,
)
from codebench_pipeline.trace_payloads import PAYLOAD_DIR, load_payload

TERMINAL_EVENTS: Final = (
    DagsterEventType.RUN_SUCCESS,
    DagsterEventType.RUN_FAILURE,
    DagsterEventType.RUN_CANCELED,
)
_NAME: Final = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}")


class _StringValue(TypedDict):
    stringValue: str


class _Attribute(TypedDict):
    key: str
    value: _StringValue


class _Status(TypedDict):
    code: int


class Span(TypedDict):
    traceId: str
    spanId: str
    parentSpanId: NotRequired[str]
    name: str
    startTimeUnixNano: str
    endTimeUnixNano: str
    status: _Status
    attributes: list[_Attribute]


@dataclass(frozen=True, slots=True)
class Projection:
    spans: list[Span]
    rejected_graphs: int


class MissingTerminalEvent(RuntimeError):
    """The terminal index and the run log disagree."""


@dataclass(frozen=True, slots=True)
class _Attempt:
    span_id: str
    name: str
    start_ns: int
    span_type: str | None = None


def _ns(timestamp: float) -> int:
    return round(timestamp * 1_000_000_000)


def _span_id(identity: str) -> str:
    return sha256(identity.encode()).hexdigest()[:16]


def _attribute(key: str, value: str) -> _Attribute:
    return {"key": key, "value": {"stringValue": value}}


def _business_output(
    span: Span, summary: CallSummary | GroupSummary, summary_key: str = "mlflow.spanOutputs"
) -> Span:
    span["attributes"].append(_attribute(summary_key, json.dumps(summary)))
    span["attributes"].extend(
        _attribute(f"codebench.{key}", value if isinstance(value, str) else json.dumps(value))
        for key, value in summary.items()
        if key not in ("execution_status", "calls", "receipts")
    )
    return span


def _safe_name(name: str | None, fallback: str) -> str:
    return name if name and _NAME.fullmatch(name) else fallback


class _RunSpans:
    """Collect one terminal run; retain only metadata from its persisted log."""

    def __init__(self, terminal: EventLogEntry) -> None:
        try:
            run_id = UUID(terminal.run_id)
        except ValueError:
            self.trace_id = sha256(terminal.run_id.encode()).hexdigest()[:32]
            self.run_id = self.trace_id
        else:
            self.trace_id = run_id.hex
            self.run_id = str(run_id)
        self.root_id = _span_id(f"{self.trace_id}:run")
        self.spans: list[Span] = []
        self.attempts: dict[str, _Attempt] = {}
        self.rules_outputs = RulesTraceOutputs()
        self.attempt_counts: Counter[str] = Counter()
        self.graph_counts: Counter[str] = Counter()
        self.graph_ids: set[str] = set()
        directory = os.environ.get(PAYLOAD_DIR)
        self.payload_directory = Path(directory) if directory else None
        self.rejected_graphs = 0

    def span(self, attempt: _Attempt, end_ns: int, outcome: Outcome, parent: str | None = None) -> Span:
        span: Span = {
            "traceId": self.trace_id,
            "spanId": attempt.span_id,
            "name": attempt.name,
            "startTimeUnixNano": str(attempt.start_ns),
            "endTimeUnixNano": str(max(end_ns, attempt.start_ns)),
            "status": {"code": 1 if outcome == "ok" else 0 if outcome == "skipped" else 2},
            "attributes": [_attribute("codebench.status", outcome)],
        }
        if parent is not None:
            span["parentSpanId"] = parent
        if attempt.span_type is not None:
            span["attributes"].append(_attribute("mlflow.spanType", json.dumps(attempt.span_type)))
        return span

    def new_attempt(self, key: str, timestamp: int) -> _Attempt:
        self.attempt_counts[key] += 1
        count = self.attempt_counts[key]
        static_step_key, _, _ = key.partition("[")
        name = _safe_name(static_step_key, "step")
        return _Attempt(
            _span_id(f"{self.trace_id}:step:{key}:{count}"),
            f"{name} #{count}" if count > 1 else name,
            timestamp,
        )

    def graph_event(self, entry: EventLogEntry, key: str) -> None:
        if entry.level not in (logging.INFO, "INFO") or not entry.user_message.startswith(GRAPH_PREFIX):
            return
        graph = parse_graph(entry.user_message)
        attempt = self.attempts.get(key)
        if graph is None or attempt is None:
            self.rejected_graphs += 1
            return
        if graph.id in self.graph_ids:
            return
        self.graph_ids.add(graph.id)
        self.graph_counts[graph.graph] += 1
        graph_id = _span_id(f"{self.trace_id}:graph:{graph.id}")
        invocation = _Attempt(
            graph_id, f"{graph.graph} #{self.graph_counts[graph.graph]}", graph.start_ns, "AGENT"
        )
        starts = [index for index, node in enumerate(graph.nodes) if node.name in ("complete", "choose")]
        self.spans.append(
            _business_output(
                self.span(invocation, graph.end_ns, graph.status, attempt.span_id),
                describe_group(graph.nodes, graph.status, len(starts)),
            )
        )
        for cycle, (start, stop) in enumerate(pairwise([0, *starts, len(graph.nodes)])):
            nodes = graph.nodes[start:stop]
            parent = graph_id
            if cycle:
                parent = _span_id(f"{graph_id}:cycle:{cycle}")
                cycle_span = _Attempt(
                    parent, f"Cycle #{cycle}", min(node.start_ns for node in nodes), "CHAIN"
                )
                end_ns = max(node.end_ns for node in nodes)
                outcome = "error" if any(node.status == "error" for node in nodes) else "ok"
                self.spans.append(
                    _business_output(
                        self.span(cycle_span, end_ns, outcome, graph_id),
                        describe_group(nodes, outcome),
                    )
                )
            for index, node in enumerate(nodes, start):
                visit = _Attempt(_span_id(f"{graph_id}:node:{index}"), node.name, node.start_ns)
                self.spans.append(self.span(visit, node.end_ns, node.status, parent))
                for call_index, call in enumerate(node.calls):
                    call_id = _span_id(f"{visit.span_id}:call:{call_index}")
                    invocation = _Attempt(
                        call_id, f"{call.label}: {call.name}", call.start_ns, call.label.upper()
                    )
                    leaf = self.span(invocation, call.end_ns, call.status, visit.span_id)
                    summary = describe_call(call)
                    if call.kind == "llm" and call.context_window is not None:
                        leaf["attributes"].append(
                            _attribute("codebench.context_window", call.context_window.model_dump_json())
                        )
                    if call.inputs is None and call.outputs is None:
                        self.spans.append(_business_output(leaf, summary))
                        leaf["attributes"].append(_attribute("codebench.payload_capture", "not_recorded"))
                        continue
                    _business_output(leaf, summary, "codebench.call_summary")
                    for key, reference, absent in (
                        ("spanInputs", call.inputs, "no_recorded_input"),
                        ("spanOutputs", call.outputs, "no_recorded_output"),
                    ):
                        payload = (
                            load_payload(self.payload_directory, reference) if reference is not None
                            else json.dumps({"capture_status": absent})
                        )
                        leaf["attributes"].append(_attribute(f"mlflow.{key}", payload))
                    self.spans.append(leaf)

    def step_event(self, entry: EventLogEntry) -> None:
        key = entry.step_key
        if key is None:
            return
        event_type = entry.dagster_event_type
        timestamp = _ns(entry.timestamp)
        match event_type:
            case None:
                self.graph_event(entry, key)
            case DagsterEventType.STEP_START | DagsterEventType.STEP_RESTARTED:
                if key in self.attempts:
                    self.end_step(key, timestamp, "interrupted")
                self.attempts[key] = self.new_attempt(key, timestamp)
            case DagsterEventType.ASSET_MATERIALIZATION:
                if key in self.attempts and (materialization := entry.asset_materialization) is not None:
                    self.rules_outputs.record(key, materialization)
            case DagsterEventType.STEP_SUCCESS:
                self.end_step(key, timestamp, "ok")
            case DagsterEventType.STEP_FAILURE:
                self.end_step(key, timestamp, "error")
            case DagsterEventType.STEP_UP_FOR_RETRY:
                self.end_step(key, timestamp, "retry")
            case DagsterEventType.STEP_SKIPPED:
                self.end_step(key, timestamp, "skipped")
            case _:
                return

    def end_step(self, key: str, timestamp: int, outcome: Outcome) -> None:
        attempt = self.attempts.pop(key, None) or self.new_attempt(key, timestamp)
        span = self.span(attempt, timestamp, outcome, self.root_id)
        if (product := self.rules_outputs.finish(key, outcome)) is not None:
            span["attributes"].append(_attribute("mlflow.spanOutputs", json.dumps(product)))
            span["attributes"].extend(
                _attribute(f"codebench.output.{field}", str(value))
                for field, value in product.items()
                if field != "execution_status"
            )
        self.spans.append(span)


def project_run(terminal: EventLogEntry, entries: Iterable[EventLogEntry], enabled_at: float) -> Projection:
    """Project one completed execution; an empty projection is before enablement."""
    run = _RunSpans(terminal)
    started = _ns(terminal.timestamp)
    for entry in entries:
        if entry.dagster_event_type == DagsterEventType.RUN_START:
            if entry.timestamp < enabled_at:
                return Projection([], 0)
            started = min(started, _ns(entry.timestamp))
        if entry.step_key is not None:
            started = min(started, _ns(entry.timestamp))
        run.step_event(entry)
        if entry.dagster_event_type == terminal.dagster_event_type and entry.timestamp == terminal.timestamp:
            break
    else:
        raise MissingTerminalEvent("The indexed terminal event is missing from the run log.")

    ended = _ns(terminal.timestamp)
    if started < _ns(enabled_at):
        return Projection([], 0)
    status: Outcome
    match terminal.dagster_event_type:
        case DagsterEventType.RUN_SUCCESS:
            status = "ok"
        case DagsterEventType.RUN_CANCELED:
            status = "canceled"
        case _:
            status = "error"
    for key in tuple(run.attempts):
        run.end_step(key, ended, "canceled" if status == "canceled" else "interrupted")

    job = _safe_name(terminal.job_name, "dagster_job")
    title = f"{job} [{run.run_id}]"
    root = run.span(_Attempt(run.root_id, title, started), ended, status)
    root["attributes"].extend(
        [
            _attribute("dagster.run_id", run.run_id),
            _attribute("dagster.job", job),
            _attribute("mlflow.spanInputs", json.dumps(job)),
            _attribute("mlflow.spanOutputs", json.dumps(run.rules_outputs.summarize(status))),
            _attribute("mlflow.traceTag.mlflow.traceName", title),
            _attribute("mlflow.traceTag.dagster.run_id", run.run_id),
            _attribute("mlflow.traceTag.dagster.job", job),
        ]
    )
    return Projection([root, *run.spans], run.rejected_graphs)
