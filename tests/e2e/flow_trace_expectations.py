import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from typing import Final

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.trace_projection import Span
from tests.e2e.flow_trace_rules_fixture import FIRST_RULE_ID, SECOND_RULE_ID

_MODEL: Final = ("LLM: chat_completions", "LLM")
_OUTPUTS: Final = TypeAdapter(dict[str, JsonValue])

type Execution = tuple[str, str]
type Cycle = tuple[tuple[str, Execution], ...]
type Graph = tuple[Cycle, ...]

_LABELER: Final[Graph] = (
    (("complete", _MODEL), ("tools", ("Tool: query", "TOOL"))),
    (("complete", _MODEL), ("tools", ("Tool: page_fetch", "TOOL"))),
    (("complete", _MODEL), ("tools", ("Tool: finalize", "TOOL"))),
)
_PLANNER: Final[Graph] = (
    (("choose", _MODEL), ("search", ("Tool: search", "TOOL"))),
    (("choose", _MODEL), ("search", ("Tool: search", "TOOL"))),
    (("choose", _MODEL),),
)
_CAPTURE: Final[Graph] = (
    (("complete", _MODEL), ("tools", ("Tool: add_rule", "TOOL"))),
    (("complete", _MODEL), ("tools", ("Tool: add_rule", "TOOL"))),
    (("complete", _MODEL), ("tools", ("Tool: add_rule", "TOOL"))),
    (("complete", _MODEL), ("tools", ("Tool: finish", "TOOL"))),
)
_DEDUP: Final[Graph] = (
    (("complete", _MODEL), ("tools", ("Tool: evaluate_rule", "TOOL"))),
    (("complete", _MODEL), ("tools", ("Tool: finalize", "TOOL"))),
)
_EXPECTED: Final = {
    "label_rows": {"labeler #1": _LABELER, "labeler #2": _LABELER},
    "plan_discovery": {"keenable_planner #1": _PLANNER},
    "learn_rules": {"rules_capture #1": _CAPTURE, "rules_dedup #1": _DEDUP},
}
_RESULTS: Final[dict[tuple[str, int, str], dict[str, JsonValue]]] = {
    ("labeler", 3, "tools"): {"business_outcome": "produced", "count": 1},
    ("keenable_planner", 1, "choose"): {"business_outcome": "produced", "count": 1},
    ("keenable_planner", 2, "choose"): {"business_outcome": "produced", "count": 1},
    ("keenable_planner", 3, "choose"): {"business_outcome": "abstained", "count": 0},
    ("rules_capture", 1, "tools"): {"business_outcome": "rejected", "accepted_rule_ids": None},
    ("rules_capture", 2, "tools"): {
        "business_outcome": "produced",
        "accepted_rule_ids": [FIRST_RULE_ID],
        "receipt_numbers": [1],
    },
    ("rules_capture", 3, "tools"): {
        "business_outcome": "produced",
        "accepted_rule_ids": [SECOND_RULE_ID],
        "receipt_numbers": [2],
    },
    ("rules_capture", 4, "tools"): {
        "business_outcome": "returned",
        "accepted_rule_ids": [FIRST_RULE_ID, SECOND_RULE_ID],
    },
    ("rules_dedup", 1, "tools"): {
        "business_outcome": "returned",
        "accepted_rule_ids": None,
        "receipt_numbers": [3],
    },
    ("rules_dedup", 2, "tools"): {
        "business_outcome": "produced",
        "accepted_rule_ids": None,
        "dropped_rule_ids": [FIRST_RULE_ID],
        "receipt_numbers": [4],
    },
}


def assert_span_tree(spans: Sequence[Span]) -> None:
    by_id = {span["spanId"]: span for span in spans}
    assert len(by_id) == len(spans)
    children: defaultdict[str | None, list[Span]] = defaultdict(list)
    for span in spans:
        parent_id = span.get("parentSpanId")
        if parent_id is not None:
            parent = by_id[parent_id]
            assert (
                int(parent["startTimeUnixNano"])
                <= int(span["startTimeUnixNano"])
                <= int(span["endTimeUnixNano"])
                <= int(parent["endTimeUnixNano"])
            )
        children[parent_id].append(span)

    (root,) = children[None]
    assert root["status"]["code"] == 1
    steps = children[root["spanId"]]
    assert Counter(step["name"] for step in steps) == Counter(iter(_EXPECTED))
    for step in steps:
        assert step["status"]["code"] == 1
        graphs = children[step["spanId"]]
        expected = _EXPECTED[step["name"]]
        assert Counter(graph["name"] for graph in graphs) == Counter(iter(expected))
        for graph in graphs:
            assert graph["status"]["code"] == 1
            graph_name = graph["name"].partition(" #")[0]
            cycles = children[graph["spanId"]]
            expected_cycles = expected[graph["name"]]
            assert [cycle["name"] for cycle in cycles] == [
                f"Cycle #{number}" for number in range(1, len(expected_cycles) + 1)
            ]
            for number, (cycle, expected_nodes) in enumerate(zip(cycles, expected_cycles, strict=True), 1):
                assert cycle["status"]["code"] == 1
                cycle_attributes = {
                    attribute["key"]: attribute["value"]["stringValue"] for attribute in cycle["attributes"]
                }
                cycle_output = _OUTPUTS.validate_json(cycle_attributes["mlflow.spanOutputs"])
                assert cycle_output["execution_status"] == "ok"
                nodes = children[cycle["spanId"]]
                assert [node["name"] for node in nodes] == [name for name, _ in expected_nodes]
                call_outputs: list[dict[str, JsonValue]] = []
                for node, (_, expected_execution) in zip(nodes, expected_nodes, strict=True):
                    assert node["status"]["code"] == 1
                    (execution,) = children[node["spanId"]]
                    attributes = {
                        attribute["key"]: attribute["value"]["stringValue"]
                        for attribute in execution["attributes"]
                    }
                    expected_name, expected_kind = expected_execution
                    assert execution["name"] == expected_name
                    assert json.loads(attributes["mlflow.spanType"]) == expected_kind
                    assert not children[execution["spanId"]]
                    output = _OUTPUTS.validate_json(attributes["mlflow.spanOutputs"])
                    call_outputs.append(output)
                    result = _RESULTS.get((graph_name, number, node["name"]), {})
                    assert execution["status"]["code"] == 1
                    assert output["execution_status"] == "ok"
                    assert {field: output.get(field) for field in result} == result
                    if "business_outcome" in result:
                        outcomes = cycle_output["business_outcomes"]
                        assert isinstance(outcomes, list) and result["business_outcome"] in outcomes
                    for field in ("accepted_rule_ids", "dropped_rule_ids"):
                        if field in result:
                            assert cycle_output.get(field) == result[field]
                assert cycle_output["calls"] == call_outputs
