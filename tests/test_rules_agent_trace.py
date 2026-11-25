import json
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from email.message import Message
from io import BytesIO
from urllib.error import HTTPError

import pytest

from codebench_pipeline import rules_agents
from codebench_pipeline.agent_trace import ActionResult
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.rules_learning import capture_rules
from tests.rules_learning_fixture import Step, rule_package, scripted_agent, synthetic_evaluator, train_corpus


@dataclass(slots=True)
class _TraceCall:
    name: str
    inputs: object
    outputs: object = None
    result: ActionResult | None = None

    def output(self, value: object) -> None:
        self.outputs = value

    def record(self, result: ActionResult) -> None:
        self.result = result


@pytest.fixture
def trace_calls(monkeypatch: pytest.MonkeyPatch) -> list[_TraceCall]:
    calls: list[_TraceCall] = []

    @contextmanager
    def record(name: str = "chat_completions", *, inputs: object = None) -> Generator[_TraceCall]:
        call = _TraceCall(name, inputs)
        calls.append(call)
        yield call

    monkeypatch.setattr(rules_agents, "llm_call", record)
    monkeypatch.setattr(rules_agents, "tool_call", record)
    return calls


def test_rules_trace_matches_wire_requests_responses_and_tool_feedback(trace_calls: list[_TraceCall]) -> None:
    steps: tuple[Step, ...] = (
        ("add_rule", {}),
        ("add_rule", rule_package("syntax error", "cat-6.capture-0001-01")),
        ("finish", {"justification": "No reviewed train evidence."}),
    )
    script = scripted_agent(iter(steps))
    exchanges: list[tuple[bytes, bytes]] = []

    def request(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        response = script.request(url, raw, key, timeout)
        exchanges.append((raw, response.body))
        return response

    result = capture_rules(replace(script, request=request), train_corpus("pwd -P"), synthetic_evaluator([]))

    assert result.complete
    model_calls = trace_calls[::2]
    tool_calls = trace_calls[1::2]
    assert [(call.inputs, call.outputs) for call in model_calls] == exchanges
    assert [call.result for call in model_calls] == [{"status": "produced", "count": 1}] * 3
    for tool, (_, response) in zip(tool_calls, exchanges, strict=True):
        sent = json.loads(response)["choices"][0]["message"]["tool_calls"][0]
        assert tool.name == sent["function"]["name"]
        assert tool.inputs == {"tool_call_id": sent["id"], "arguments": sent["function"]["arguments"]}
    for tool, (request_body, _), code in zip(
        tool_calls[:2], exchanges[1:], ("tool_input", "cel_compile"), strict=True
    ):
        feedback = json.loads(request_body)["messages"][-1]
        assert json.loads(feedback["content"])["diagnostic"]["code"] == code
        assert tool.outputs == feedback
        assert tool.result == {"status": "rejected"}
    assert tool_calls[-1].outputs == {"role": "tool", "tool_call_id": "call-3", "content": "{}"}


@pytest.mark.parametrize("failure", ["sensitive", "provider_http", "endpoint_error"])
def test_rules_trace_uses_safe_diagnostics_instead_of_provider_secrets(
    trace_calls: list[_TraceCall],
    failure: str,
) -> None:
    step: Step = ("finish", {"justification": "No reviewed train evidence."})
    script = scripted_agent(iter((step,)))
    key = "synthetic-key-never-echo"
    first = True

    def request(url: str, raw: bytes, credential: str | None, timeout: float) -> HTTPResponse:
        nonlocal first
        assert credential == key
        if not first:
            return script.request(url, raw, None, timeout)
        first = False
        match failure:
            case "sensitive":
                return HTTPResponse(key.encode(), None)
            case "provider_http":
                raise HTTPError(url, 401, "Unauthorized", Message(), BytesIO(key.encode()))
            case _:
                raise OSError(key)

    result = capture_rules(
        replace(script, request=request, api_key=key), train_corpus("pwd -P"), synthetic_evaluator([])
    )

    assert key not in repr(trace_calls)
    (batch,) = result.batches
    (diagnostic,) = batch.run.diagnostics
    if failure == "sensitive":
        assert diagnostic.code == "response_sensitive"
        assert trace_calls[0].outputs == {"omitted": "response_sensitive"}
        assert result.complete
    else:
        assert diagnostic.code == failure
        assert trace_calls[0].outputs == {"diagnostic": diagnostic.model_dump(mode="json", exclude_none=True)}
        assert not result.complete
