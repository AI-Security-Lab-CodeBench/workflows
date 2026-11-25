import json
import logging
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256
from typing import Annotated, Final, Literal, NotRequired, TypedDict
from uuid import UUID

from dagster import DagsterEventType, EventLogEntry
from pydantic import BaseModel, ConfigDict, Field, ValidationError

TERMINAL_EVENTS: Final = (
    DagsterEventType.RUN_SUCCESS,
    DagsterEventType.RUN_FAILURE,
    DagsterEventType.RUN_CANCELED,
)
GRAPH_PREFIX: Final = "CODEBENCH_GRAPH_V1 "
_NAME: Final = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}")
_GRAPH_ID: Final = re.compile(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}")

type EpochNs = Annotated[int, Field(ge=1, le=2**63 - 1)]
type Outcome = Literal["ok", "error", "canceled", "interrupted", "retry", "skipped"]


class _Node(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    name: Literal["complete", "tools", "choose", "search"]
    start_ns: EpochNs
    end_ns: EpochNs
    status: Literal["ok", "error"]


class _Graph(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    graph: Literal["labeler", "rules_capture", "rules_dedup", "keenable_planner"]
    id: str
    start_ns: EpochNs
    end_ns: EpochNs
    status: Literal["ok", "error"]
    nodes: list[_Node]


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


def _ns(timestamp: float) -> int:
    return round(timestamp * 1_000_000_000)


def _span_id(identity: str) -> str:
    return sha256(identity.encode()).hexdigest()[:16]


def _attribute(key: str, value: str) -> _Attribute:
    return {"key": key, "value": {"stringValue": value}}


def _graph(message: str) -> _Graph | None:
    try:
        graph = _Graph.model_validate_json(message[len(GRAPH_PREFIX):])
    except ValidationError:
        return None
    if (
        not _GRAPH_ID.fullmatch(graph.id)
        or graph.end_ns < graph.start_ns
        or any(not graph.start_ns <= node.start_ns <= node.end_ns <= graph.end_ns for node in graph.nodes)
    ):
        return None
    return graph


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
        self.attempt_counts: Counter[str] = Counter()
        self.graph_counts: Counter[str] = Counter()
        self.graph_ids: set[str] = set()
        self.rejected_graphs = 0

    def span(
        self, attempt: _Attempt, end_ns: int, outcome: Outcome, parent: str | None = None
    ) -> Span:
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
        graph = _graph(entry.user_message)
        attempt = self.attempts.get(key)
        if graph is None or attempt is None:
            self.rejected_graphs += 1
            return
        if graph.id in self.graph_ids:
            return
        self.graph_ids.add(graph.id)
        self.graph_counts[graph.graph] += 1
        graph_id = _span_id(f"{self.trace_id}:graph:{graph.id}")
        name = f"{graph.graph} #{self.graph_counts[graph.graph]}"
        self.spans.append(
            self.span(_Attempt(graph_id, name, graph.start_ns), graph.end_ns, graph.status, attempt.span_id)
        )
        for index, node in enumerate(graph.nodes):
            node_id = _span_id(f"{graph_id}:node:{index}")
            self.spans.append(
                self.span(_Attempt(node_id, node.name, node.start_ns), node.end_ns, node.status, graph_id)
            )

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
                prior = self.attempts.get(key)
                if prior is not None:
                    self.spans.append(self.span(prior, timestamp, "interrupted", self.root_id))
                self.attempts[key] = self.new_attempt(key, timestamp)
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
        self.spans.append(self.span(attempt, timestamp, outcome, self.root_id))


def project_run(
    terminal: EventLogEntry, entries: Iterable[EventLogEntry], enabled_at: float
) -> Projection:
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
    for attempt in run.attempts.values():
        run.spans.append(run.span(attempt, ended, "canceled" if status == "canceled" else "interrupted", run.root_id))

    job = _safe_name(terminal.job_name, "dagster_job")
    title = f"{job} [{run.run_id}]"
    root = run.span(_Attempt(run.root_id, title, started), ended, status)
    root["attributes"].extend([
        _attribute("dagster.run_id", run.run_id),
        _attribute("dagster.job", job),
        _attribute("mlflow.spanInputs", json.dumps(job)),
        _attribute("mlflow.spanOutputs", json.dumps(status)),
        _attribute("mlflow.traceTag.mlflow.traceName", title),
        _attribute("mlflow.traceTag.dagster.run_id", run.run_id),
        _attribute("mlflow.traceTag.dagster.job", job),
    ])
    return Projection([root, *run.spans], run.rejected_graphs)
