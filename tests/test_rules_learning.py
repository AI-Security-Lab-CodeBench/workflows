import json
from collections.abc import Iterator
from dataclasses import replace
from email.message import Message
from hashlib import sha256
from io import BytesIO
from urllib.error import HTTPError

import httpx2
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.rules_agents import Feedback, RuleAgent
from codebench_pipeline.rules_contracts import (
    Bundle,
    Diagnostic,
    EvaluationInput,
    Fixture,
    Labels,
    NativeEvaluation,
    NativeFailure,
    NativeResult,
    NativeRow,
    NativeRule,
    Other,
    Package,
)
from codebench_pipeline.rules_corpus import Corpus, CorpusRow, CorpusSource
from codebench_pipeline.rules_engine import BashguardClient, evaluate_package
from codebench_pipeline.rules_learning import capture_rules, deduplicate_rules
from codebench_pipeline.rules_similarity import CapturedRule, FinishInput, capture_batches, nominate_rules
from codebench_pipeline.typed_config import ConfigArtifact

_MODEL = "qwen38-27b-fp8"
_PIN = "pinned-synthetic"
_SOURCE = "https://example.test/manifest"
_GOLD = Labels(category="CAT-6", risk="CAT-6.1", severity="low", action="allow")
_GUIDE = '{"instructions":"Synthetic test guide","examples":["positive and negative"]}'
_COMPILE = Diagnostic(
    code="cel_compile", phase="compile", rule_id="cat-6.capture-0001-01", field="when",
    message="<input>:1:20: expected ']'\n | argv == ['pwd', '-P'\n | ...................^", line=1, column=20,
)
_JSON = TypeAdapter(dict[str, JsonValue])
type Step = tuple[str, ConfigArtifact | dict[str, JsonValue]]


class _EvaluatorInput(ConfigArtifact):
    required_runtime_version: str
    bundle: Bundle
    rows: tuple[EvaluationInput, ...]


class _Message(ConfigArtifact):
    role: str
    content: str | None = None
    tool_call_id: str | None = None
    tool_calls: tuple[JsonValue, ...] = ()


class _Request(ConfigArtifact):
    model: str
    messages: tuple[_Message, ...]
    tools: tuple[dict[str, JsonValue], ...]
    tool_choice: JsonValue
    parallel_tool_calls: bool
    stream: bool
    max_completion_tokens: int
    temperature: float
    chat_template_kwargs: dict[str, bool]


def _feedback(request: _Request) -> Feedback:
    last = request.messages[-1]
    assert last.role == "tool" and last.content is not None
    assert last.tool_call_id == f"call-{(len(request.messages) - 2) // 2}"
    return Feedback.model_validate_json(last.content)


def _agent(steps: Iterator[Step], observed: list[_Request] | None = None) -> RuleAgent:
    requests = [] if observed is None else observed

    def complete(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        request = _Request.model_validate_json(raw)
        assert url == "https://model.test/v1/chat/completions" and key is None and timeout == 30
        assert request.chat_template_kwargs == {"enable_thinking": False}
        assert request.model == _MODEL and request.temperature == 0 and request.max_completion_tokens == 8192
        assert not request.parallel_tool_calls and not request.stream
        requests.append(request)
        name, args = next(steps)
        return HTTPResponse(json.dumps({
            "id": f"result-{len(requests)}", "model": _MODEL,
            "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
                "role": "assistant", "content": None, "tool_calls": [{
                    "id": f"call-{len(requests)}", "type": "function", "function": {
                        "name": name, "arguments": args.model_dump_json() if isinstance(args, ConfigArtifact)
                        else json.dumps(args),
                    },
                }],
            }}],
        }).encode(), None)

    return RuleAgent(
        model=ModelConfig(
            provider="openai_compatible", model=_MODEL, base_url="https://model.test/v1",
            temperature=0, timeout_seconds=30, max_output_tokens=8192,
        ),
        backend_revision=_MODEL, guide=_GUIDE, guide_sha256=sha256(_GUIDE.encode()).hexdigest(),
        request=complete,
    )

def _client(calls: list[_EvaluatorInput]) -> BashguardClient:
    def endpoint(request: httpx2.Request) -> httpx2.Response:
        payload = _EvaluatorInput.model_validate_json(request.content)
        assert payload.required_runtime_version == _PIN
        calls.append(payload)
        if any(rule.when == "syntax error" for rule in payload.bundle.rules):
            return httpx2.Response(422, content=NativeFailure(runtime_version=_PIN, error=_COMPILE).model_dump_json())
        digest = sha256(payload.bundle.model_dump_json().encode()).hexdigest()
        results: list[NativeRow] = []
        for row in payload.rows:
            matches = tuple(
                rule for rule in payload.bundle.rules if rule.when == "true" or row.command in rule.when.split("|")
            )
            results.append(NativeRow(id=row.id, result=NativeResult(
                status="matched" if matches else "abstain", basis="literal_cli_form",
                labels=matches[0].labels if matches else None, rule_ids=tuple(rule.id for rule in matches),
                rationales=(), sources=(), reasons=() if matches else ("no_proven_rule",), bundle_sha256=digest,
            )))
        return httpx2.Response(200, content=NativeEvaluation(
            runtime_version=_PIN, bundle_sha256=digest, results=tuple(results),
        ).model_dump_json())

    return BashguardClient("http://engine.test", _PIN, httpx2.MockTransport(endpoint))


def _train(*commands: str) -> Corpus:
    return Corpus(
        split="train", digest="a" * 64,
        rows=tuple(CorpusRow(id=f"train:{index}", command=command, gold=_GOLD) for index, command in enumerate(commands)),
        sources=(CorpusSource(category="CAT-6", url=_SOURCE, sha256="b" * 64),),
    )


def _package(command: str, rule_id: str, rationale: str = "physical directory path") -> Package:
    return Package(
        rule=NativeRule(id=rule_id, when=command, labels=_GOLD, rationale=rationale, sources=(_SOURCE,)),
        tests=(Fixture(command="pwd -P", expected=_GOLD), Fixture(command="pwd -L", expected=Other())),
    )


def test_capture_repairs_native_compile_and_fixture_failures_from_next_model_request() -> None:
    requests: list[_Request] = []
    evaluations: list[_EvaluatorInput] = []
    train = _train("pwd -P", "pwd -L", "pwd -P")
    rule_id = capture_batches(train)[0].next_rule_id
    valid = _package("pwd -P", rule_id)

    def steps() -> Iterator[Step]:
        yield "add_rule", _package("syntax error", rule_id)
        assert _feedback(requests[-1]).diagnostic == _COMPILE
        yield "add_rule", _package("true", rule_id)
        rejected = _feedback(requests[-1])
        assert rejected.diagnostic is not None and rejected.diagnostic.code == "fixture_mismatch"
        assert rejected.receipts[0].train_wrong_matches == 0 and rejected.receipts[0].fixture_failures == 1
        assert rejected.examples[0].expected == Other() and rejected.examples[0].actual == _GOLD
        yield "page_evaluation", {"receipt": 1, "kind": "fixtures", "offset": 1, "limit": 1}
        page = _feedback(requests[-1]).page
        assert page is not None and page.rows[0].input.command == "pwd -L"
        yield "add_rule", valid
        accepted = _feedback(requests[-1])
        assert accepted.recorded == (rule_id,) and accepted.receipts[0].train_total == 3
        yield "page_evaluation", {"receipt": 2, "offset": 1, "limit": 1}
        page = _feedback(requests[-1]).page
        assert page is not None and page.next_offset == 2 and page.rows[0].id == train.rows[1].id
        yield "finish", {"justification": "The logical form remains uncovered."}

    result = capture_rules(_agent(steps(), requests), train, _client(evaluations))
    assert result.complete and result.packages == (valid,) and len(evaluations) == 3
    assert {row.id for row in evaluations[-1].rows} >= {row.id for row in train.rows}
    assert result.rules[0].singleton.hit_sets[0].row_ids == ("train:0", "train:2")


def test_judge_rejects_old_ids_and_reruns_replacement_after_readonly_preview() -> None:
    requests: list[_Request] = []
    calls: list[_EvaluatorInput] = []
    client = _client(calls)
    train = _train("pwd -P", "pwd -Q")
    first = _package("pwd -P", "cat-6.capture-0001-01")
    second = _package("pwd -P|pwd -Q", "cat-6.capture-0001-02").model_copy(update={
        "tests": (Fixture(command="pwd -Q", expected=_GOLD), Fixture(command="pwd -L", expected=Other())),
    })
    capture_steps: tuple[Step, ...] = (
        ("add_rule", first), ("add_rule", second), ("finish", {"justification": "Compare variants."}),
    )
    capture = capture_rules(_agent(iter(capture_steps)), train, client)
    fresh = _package("pwd -P", "cat-6.judge-0001-01")
    rejected_fresh = _package("true", fresh.rule.id)
    why = "Drop the second rule and its -Q fixture: the unique -Q form is intentionally not retained."

    def steps() -> Iterator[Step]:
        assert all("modify" not in str(tool["function"]) for tool in requests[-1].tools)
        yield "evaluate_rule", fresh
        assert _feedback(requests[-1]).receipts[0].train_matches == 1
        yield "finalize", {"drop_ids": [second.rule.id], "add": [_JSON.validate_json(second.model_dump_json())],
                           "justification": why}
        rejected = _feedback(requests[-1]).diagnostic
        assert rejected is not None and rejected.code == "fresh_id" and rejected.rule_id == second.rule.id
        yield "finalize", {"drop_ids": [second.rule.id], "add": [_JSON.validate_json(rejected_fresh.model_dump_json())],
                           "justification": why}
        failed = _feedback(requests[-1])
        assert failed.diagnostic is not None and failed.diagnostic.code == "fixture_mismatch"
        assert failed.receipts[0].fixture_failures == 1 and failed.examples[0].actual == _GOLD
        yield "finalize", {"drop_ids": [second.rule.id], "add": [_JSON.validate_json(fresh.model_dump_json())],
                           "justification": why}

    result = deduplicate_rules(_agent(steps(), requests), train, client, capture)
    decision = result.decisions[0].finalized
    assert result.complete and decision is not None and result.packages == (first, fresh)
    assert decision.submission == type(decision.submission)(drop=(second.rule.id,), add=(fresh,), justification=why)
    assert decision.combined.rows[1].actual == Other()
    assert [tuple(rule.id for rule in call.bundle.rules) for call in calls[2:]] == [
        (fresh.rule.id,), (fresh.rule.id,), (fresh.rule.id,), (first.rule.id, fresh.rule.id),
    ]
    keep_steps: tuple[Step, ...] = (("finalize", {
        "drop_ids": [], "add": [], "justification": "Keep both interpretations pending better evidence.",
    }),)
    kept = deduplicate_rules(_agent(iter(keep_steps)), train, client, capture)
    no_change = kept.decisions[0].finalized
    assert kept.complete and kept.packages == capture.packages and no_change is not None
    assert not no_change.submission.drop and not no_change.submission.add
    assert no_change.submission.justification == "Keep both interpretations pending better evidence."


def test_nominations_union_distinct_tuple_semantics_and_singleton_evidence() -> None:
    client = _client([])
    train = _train("pwd -P", "pwd -L")
    strong = _package("pwd -P", "a")
    overlap = _package("pwd -P", "b", "alpha beta gamma")
    semantic = _package("pwd -L", "c")
    semantic = semantic.model_copy(update={"rule": semantic.rule.model_copy(update={
        "labels": _GOLD.model_copy(update={"risk": "CAT-6.2"}),
    })})
    nominations = {(pair.left_id, pair.right_id): pair for pair in nominate_rules(tuple(
        CapturedRule(package=package, singleton=evaluate_package(client, package, train))
        for package in (strong, overlap, semantic)
    ))}
    assert set(nominations) == {("a", "b"), ("a", "c")}
    assert not nominations["a", "b"].semantic and nominations["a", "b"].shared_hits == ("train:0",)
    assert nominations["a", "c"].semantic and not nominations["a", "c"].same_tuple
    assert not nominations["a", "c"].shared_hits
    assert nominations["a", "c"].left_only_hits == ("train:0",)
    assert nominations["a", "c"].right_only_hits == ("train:1",)


def test_capture_turn_budget_forces_terminal_and_records_exhaustion() -> None:
    calls: list[_EvaluatorInput] = []
    requests: list[_Request] = []
    train = _train("pwd -P")

    def steps() -> Iterator[Step]:
        for _ in range(8):
            yield "page_evaluation", {"receipt": 42}

    result = capture_rules(_agent(steps(), requests), train, _client(calls), category="CAT-6")
    batch = result.batches[0]
    assert not result.complete and batch.run.status == "exhausted" and batch.justification is None
    assert len(requests) == len(batch.run.request_sha256) == 8 and not calls
    assert requests[-1].tool_choice == {"type": "function", "function": {"name": "finish"}}
    assert tuple(item.code for item in batch.run.diagnostics) == ("unknown_receipt",) * 7 + ("unknown_tool",)
    assert tuple(item.turn for item in batch.run.failures) == tuple(range(1, 9))


def test_provider_rejection_reports_status_without_echo_and_preserves_later_batches() -> None:
    train = _train(*(f"pwd -{index:02}" for index in range(17)))
    script = _agent(iter((("finish", FinishInput(justification="No proposed rules for this batch.")),)))
    first_attempt = iter((True, False))

    def request(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        if next(first_attempt):
            raise HTTPError(url, 400, "Bad request", Message(), BytesIO(b"maximum context; private train row"))
        return script.request(url, raw, key, timeout)

    result = capture_rules(replace(script, request=request), train, _client([]))
    assert not result.complete and result.finished_batches == 1 and len(result.batches) == 2
    assert set(train_row.id for train_row in train.rows) == {key for batch in result.batches for key in batch.row_ids}
    first = result.batches[0].run
    assert first.status == "context_limit" and first.diagnostics[0].code == "provider_context_limit"
    assert "HTTP 400" in first.diagnostics[0].message and "private train row" not in result.model_dump_json()
