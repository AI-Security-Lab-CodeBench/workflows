from collections.abc import Sequence
from typing import Final

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.batch_labeling_protocol import Completion
from codebench_pipeline.rules_agents import Feedback
from codebench_pipeline.trace_projection import Span
from tests.e2e.flow_trace_rules_fixture import FIRST_RULE_ID, SECOND_RULE_ID, Exchange
from tests.rules_learning_fixture import COMPILE, ModelRequest

_JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])


def span_attributes(span: Span) -> dict[str, str]:
    return {attribute["key"]: attribute["value"]["stringValue"] for attribute in span["attributes"]}


def assert_rules_payloads(cycles: Sequence[tuple[Span, ...]], exchanges: Sequence[Exchange]) -> None:
    previous_call_id: str | None = None
    previous_feedback: dict[str, JsonValue] | None = None
    for (llm, tool), (sent, received) in zip(cycles, exchanges, strict=True):
        llm_attrs = span_attributes(llm)
        request = _JSON_OBJECT.validate_json(llm_attrs["mlflow.spanInputs"])
        response = _JSON_OBJECT.validate_json(llm_attrs["mlflow.spanOutputs"])
        assert request == sent
        assert response == received

        (choice,) = Completion.model_validate(response).choices
        (call,) = choice.message.tool_calls
        tool_attrs = span_attributes(tool)
        tool_input = _JSON_OBJECT.validate_json(tool_attrs["mlflow.spanInputs"])
        assert tool_input == {"tool_call_id": call.id, "arguments": call.function.arguments}
        output = _JSON_OBJECT.validate_json(tool_attrs["mlflow.spanOutputs"])
        assert output["role"] == "tool" and output["tool_call_id"] == call.id
        content = output["content"]
        assert isinstance(content, str)
        result = Feedback.model_validate_json(content)

        if previous_call_id is not None:
            previous_assistant, previous_tool = ModelRequest.model_validate_json(
                llm_attrs["mlflow.spanInputs"]
            ).messages[-2:]
            (previous_call,) = previous_assistant.tool_calls
            assert isinstance(previous_call, dict) and previous_call["id"] == previous_call_id
            assert previous_feedback == {
                "role": previous_tool.role,
                "tool_call_id": previous_tool.tool_call_id,
                "content": previous_tool.content,
            }
        previous_call_id, previous_feedback = call.id, output

        if call.id == "call-1" and call.function.name == "add_rule":
            assert result.diagnostic == COMPILE
        if call.function.name == "finish":
            assert _JSON_OBJECT.validate_json(content) == {"recorded": [FIRST_RULE_ID, SECOND_RULE_ID]}
        if call.function.name == "finalize":
            (receipt,) = result.receipts
            assert receipt.receipt == 4 and receipt.train_matches == 2 and receipt.fixture_failures == 0
