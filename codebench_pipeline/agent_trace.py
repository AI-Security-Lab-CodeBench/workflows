import json
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass
from functools import wraps
from time import monotonic_ns, time_ns
from typing import Final, Literal, NotRequired, TypedDict
from uuid import uuid4

type GraphName = Literal["labeler", "rules_capture", "rules_dedup", "keenable_planner"]
type LlmName = Literal["chat_completions"]
type ToolName = Literal[
    "query",
    "page_fetch",
    "finalize",
    "search_documentation",
    "fetch_documentation",
    "add_rule",
    "propose_candidate",
    "page_evaluation",
    "finish",
    "finish_all_covered",
    "finish_with_abstentions",
    "finish_without_validated_rules",
    "evaluate_rule",
    "keep_rules",
    "search",
]
type _NodeName = Literal["complete", "tools", "choose", "search"]
type _Status = Literal["ok", "error"]

_LOG_INFO: Final = ContextVar[Callable[[str], None] | None]("graph_log_info", default=None)


class ActionReceipt(TypedDict):
    receipt: int
    status: Literal["returned"]
    bundle_sha256: NotRequired[str]
    corpus_digest: NotRequired[str]


class ActionResult(TypedDict):
    status: Literal["rejected", "produced", "abstained", "returned"]
    accepted_rule_ids: NotRequired[list[str]]
    dropped_rule_ids: NotRequired[list[str]]
    count: NotRequired[int]
    receipts: NotRequired[list[ActionReceipt]]


class _CallTiming(TypedDict):
    start_ns: int
    end_ns: int
    status: _Status
    result: NotRequired[ActionResult]


class _LlmCall(_CallTiming):
    kind: Literal["llm"]
    name: LlmName


class _ToolCall(_CallTiming):
    kind: Literal["tool"]
    name: ToolName


type _Call = _LlmCall | _ToolCall


class _NodeRecord(TypedDict):
    name: _NodeName
    start_ns: int
    end_ns: int
    status: _Status
    calls: list[_Call]


class _GraphRecord(TypedDict):
    graph: GraphName
    id: str
    start_ns: int
    end_ns: int
    status: _Status
    nodes: list[_NodeRecord]


@dataclass(frozen=True, slots=True)
class _ActiveNode:
    origin_ns: int
    calls: list[_Call]


_ACTIVE_NODE: Final = ContextVar[_ActiveNode | None]("graph_active_node", default=None)


@dataclass(slots=True)
class CallOutcome:
    """Record verified results independently of transport or execution timing."""

    _entry: _Call | None = None

    def record(self, result: ActionResult) -> None:
        if self._entry is not None:
            self._entry["result"] = result


@contextmanager
def _record_call(entry: _Call) -> Generator[CallOutcome]:
    active = _ACTIVE_NODE.get()
    if active is None:
        yield CallOutcome()
        return
    outcome = CallOutcome(_entry=entry)
    start = active.origin_ns + monotonic_ns()
    entry["start_ns"] = start
    entry["end_ns"] = start
    active.calls.append(entry)
    try:
        yield outcome
        entry["status"] = "ok"
    finally:
        entry["end_ns"] = active.origin_ns + monotonic_ns()


def llm_call() -> AbstractContextManager[CallOutcome]:
    return _record_call(_LlmCall(kind="llm", name="chat_completions", start_ns=0, end_ns=0, status="error"))


def tool_call(name: ToolName) -> AbstractContextManager[CallOutcome]:
    return _record_call(_ToolCall(kind="tool", name=name, start_ns=0, end_ns=0, status="error"))


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
            entry = _NodeRecord(name=name, start_ns=start, end_ns=start, status="error", calls=[])
            record["nodes"].append(entry)
            token = _ACTIVE_NODE.set(_ActiveNode(self._origin_ns, entry["calls"]))
            try:
                result = node(state)
                entry["status"] = "ok"
                return result
            finally:
                _ACTIVE_NODE.reset(token)
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
