import re
from collections.abc import Sequence
from typing import Annotated, ClassVar, Final, Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from codebench_pipeline.agent_trace import ActionReceipt, GraphName, LlmName, ToolName
from codebench_pipeline.models import Digest

GRAPH_PREFIX: Final = "CODEBENCH_GRAPH_V1 "
_GRAPH_ID: Final = re.compile(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}")

type _EpochNs = Annotated[int, Field(ge=1, le=2**63 - 1)]
type _Count = Annotated[int, Field(ge=0, le=2**31 - 1)]
type _ReceiptNumber = Annotated[int, Field(ge=1, le=2**31 - 1)]
type _RuleId = Annotated[str, Field(pattern=r"^cat-[1-9][0-9]{0,8}\.(capture|judge)-[0-9]{4,9}-[0-9]{2,9}$")]
type _BusinessOutcome = Literal["produced", "rejected", "abstained", "returned"]
type _Execution = Literal["ok", "error"]


class _GraphMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _Receipt(_GraphMetadata):
    receipt: _ReceiptNumber
    status: Literal["returned"]
    bundle_sha256: Digest | None = None
    corpus_digest: Digest | None = None


class _Result(_GraphMetadata):
    status: _BusinessOutcome
    accepted_rule_ids: tuple[_RuleId, ...] = ()
    dropped_rule_ids: tuple[_RuleId, ...] = ()
    count: _Count | None = None
    receipts: tuple[_Receipt, ...] = ()


class _Timing(_GraphMetadata):
    start_ns: _EpochNs
    end_ns: _EpochNs
    status: _Execution


class _MeasuredCall(_Timing):
    result: _Result | None = None


class _LlmCall(_MeasuredCall):
    kind: Literal["llm"]
    name: LlmName
    label: ClassVar[str] = "LLM"


class _ToolCall(_MeasuredCall):
    kind: Literal["tool"]
    name: ToolName
    label: ClassVar[str] = "Tool"


type Call = Annotated[_LlmCall | _ToolCall, Field(discriminator="kind")]


class Node(_Timing):
    name: Literal["complete", "tools", "choose", "search"]
    calls: tuple[Call, ...] = ()


class Graph(_Timing):
    graph: GraphName
    id: str
    nodes: list[Node]


class _Identifiers(TypedDict, total=False):
    accepted_rule_ids: list[str]
    dropped_rule_ids: list[str]
    receipt_numbers: list[int]
    bundle_sha256s: list[str]
    corpus_digests: list[str]


class CallSummary(_Identifiers, total=False):
    action: LlmName | ToolName
    execution_status: _Execution
    business_outcome: _BusinessOutcome
    count: int
    receipts: list[ActionReceipt]


class GroupSummary(_Identifiers, total=False):
    actions: list[LlmName | ToolName]
    execution_status: _Execution
    business_outcomes: list[_BusinessOutcome]
    result_counts: dict[LlmName | ToolName, list[int]]
    calls: list[CallSummary]
    call_count: int
    node_count: int
    cycle_count: int


def parse_graph(message: str) -> Graph | None:
    """Read allowlisted trace metadata without exposing invalid log content."""
    try:
        graph = Graph.model_validate_json(message[len(GRAPH_PREFIX) :])
    except ValidationError:
        return None
    if (
        not _GRAPH_ID.fullmatch(graph.id)
        or graph.end_ns < graph.start_ns
        or any(not graph.start_ns <= node.start_ns <= node.end_ns <= graph.end_ns for node in graph.nodes)
        or any(
            not node.start_ns <= call.start_ns <= call.end_ns <= node.end_ns
            for node in graph.nodes
            for call in node.calls
        )
    ):
        return None
    return graph


def _identifiers(results: Sequence[_Result]) -> _Identifiers:
    accepted = list(dict.fromkeys(rule_id for result in results for rule_id in result.accepted_rule_ids))
    dropped = list(dict.fromkeys(rule_id for result in results for rule_id in result.dropped_rule_ids))
    receipts = [receipt for result in results for receipt in result.receipts]
    numbers = list(dict.fromkeys(receipt.receipt for receipt in receipts))
    bundles = list(
        dict.fromkeys(receipt.bundle_sha256 for receipt in receipts if receipt.bundle_sha256 is not None)
    )
    corpora = list(
        dict.fromkeys(receipt.corpus_digest for receipt in receipts if receipt.corpus_digest is not None)
    )
    summary: _Identifiers = {}
    if accepted:
        summary["accepted_rule_ids"] = accepted
    if dropped:
        summary["dropped_rule_ids"] = dropped
    if numbers:
        summary["receipt_numbers"] = numbers
    if bundles:
        summary["bundle_sha256s"] = bundles
    if corpora:
        summary["corpus_digests"] = corpora
    return summary


def describe_call(call: Call) -> CallSummary:
    result = call.result
    if result is None:
        return {"action": call.name, "execution_status": call.status}
    summary: CallSummary = {
        "action": call.name,
        "execution_status": call.status,
        "business_outcome": result.status,
        **_identifiers((result,)),
    }
    if result.count is not None:
        summary["count"] = result.count
    if result.receipts:
        receipts: list[ActionReceipt] = []
        for receipt in result.receipts:
            item = ActionReceipt(receipt=receipt.receipt, status=receipt.status)
            if receipt.bundle_sha256 is not None:
                item["bundle_sha256"] = receipt.bundle_sha256
            if receipt.corpus_digest is not None:
                item["corpus_digest"] = receipt.corpus_digest
            receipts.append(item)
        summary["receipts"] = receipts
    return summary


def describe_group(nodes: Sequence[Node], status: _Execution, cycle_count: int | None = None) -> GroupSummary:
    calls = [call for node in nodes for call in node.calls]
    results = [result for call in calls if (result := call.result) is not None]
    summary: GroupSummary = {
        "actions": list(dict.fromkeys(call.name for call in calls)),
        "execution_status": status,
        "call_count": len(calls),
        "node_count": len(nodes),
        **_identifiers(results),
    }
    if cycle_count is not None:
        summary["cycle_count"] = cycle_count
    else:
        summary["calls"] = [describe_call(call) for call in calls]
    if results:
        outcomes: dict[_BusinessOutcome, None] = dict.fromkeys(result.status for result in results)
        summary["business_outcomes"] = list(outcomes)
    counts: dict[LlmName | ToolName, list[int]] = {}
    for call in calls:
        if call.result is not None and call.result.count is not None:
            counts.setdefault(call.name, []).append(call.result.count)
    if counts:
        summary["result_counts"] = counts
    return summary
