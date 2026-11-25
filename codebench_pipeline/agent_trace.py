import json
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass
from functools import wraps
from time import monotonic_ns, time_ns
from typing import Final, Literal, TypedDict
from uuid import uuid4

type GraphName = Literal["labeler", "rules_capture", "rules_dedup", "keenable_planner"]
type _NodeName = Literal["complete", "tools", "choose", "search"]
type _Status = Literal["ok", "error"]

_LOG_INFO: Final = ContextVar[Callable[[str], None] | None]("graph_log_info", default=None)


class _NodeRecord(TypedDict):
    name: _NodeName
    start_ns: int
    end_ns: int
    status: _Status


class _GraphRecord(TypedDict):
    graph: GraphName
    id: str
    start_ns: int
    end_ns: int
    status: _Status
    nodes: list[_NodeRecord]


@dataclass(frozen=True, slots=True)
class GraphTrace:
    _record: _GraphRecord | None
    _origin_ns: int = 0

    def wrap[S, R](self, name: _NodeName, node: Callable[[S], R]) -> Callable[[S], R]:
        """Time the actual node call without inspecting its input or result."""
        record = self._record
        if record is None:
            return node

        @wraps(node)
        def invoke(state: S) -> R:
            start = self._origin_ns + monotonic_ns()
            entry = _NodeRecord(name=name, start_ns=start, end_ns=start, status="error")
            record["nodes"].append(entry)
            try:
                result = node(state)
                entry["status"] = "ok"
                return result
            finally:
                entry["end_ns"] = self._origin_ns + monotonic_ns()

        return invoke


@contextmanager
def graph_log(info: Callable[[str], None], environment: Callable[[str], str | None]) -> Generator[None]:
    """Bind an asset step's INFO logger for graphs invoked by that step."""
    try:
        tracking_uri = environment("MLFLOW_TRACKING_URI")
    except KeyError:
        tracking_uri = None
    token = _LOG_INFO.set(info if tracking_uri and tracking_uri.strip() else None)
    try:
        yield
    finally:
        _LOG_INFO.reset(token)


def graph_log_context() -> Context:
    """Capture only the trace logger for propagation across a worker boundary."""
    context = Context()
    context.run(_LOG_INFO.set, _LOG_INFO.get())
    return context


@contextmanager
def record_graph(name: GraphName) -> Generator[GraphTrace]:
    info = _LOG_INFO.get()
    if info is None:
        yield GraphTrace(None)
        return

    start = time_ns()
    origin = start - monotonic_ns()
    record = _GraphRecord(graph=name, id=uuid4().hex, start_ns=start, end_ns=start, status="error", nodes=[])
    try:
        yield GraphTrace(record, origin)
        record["status"] = "ok"
    finally:
        record["end_ns"] = origin + monotonic_ns()
        info("CODEBENCH_GRAPH_V1 " + json.dumps(record, separators=(",", ":")))
