import json
from collections.abc import Iterator
from dataclasses import replace
from email.message import Message
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import httpx2
import pytest
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.rules_agents import Feedback, RuleAgent
from codebench_pipeline.rules_contracts import (
    Bundle,
    Diagnostic,
    EvaluationError,
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
        assert request.model == _MODEL and request.temperature == 0
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


def test_first_capture_request_presents_full_shipped_guide_with_8192_output_tokens() -> None:
    guide_bytes = (Path(__file__).resolve().parents[1] / "business-configs/bashguard/guide.json").read_bytes()
    guide = guide_bytes.decode("utf-8")
    requests: list[_Request] = []
    script = _agent(iter((("finish", FinishInput(justification="Synthetic examples reviewed.")),)), requests)
    agent = replace(script, guide=guide, guide_sha256=sha256(guide_bytes).hexdigest())
    train = _train(*(f"pwd -P {index}" for index in range(16)))

    result = capture_rules(agent, train, _client([]))

    assert result.complete, result.batches[0].run.diagnostics
    (request,) = requests
    system, batch = request.messages
    assert system.role == "system" and system.content is not None
    assert system.content.partition("\nPinned authoring guide:\n")[2].encode() == guide_bytes
    assert batch.content is not None
    assert _JSON.validate_json(batch.content)["rows"] == [
        _JSON.validate_json(row.model_dump_json()) for row in capture_batches(train)[0].rows
    ]
    assert request.max_completion_tokens == 8192 and request.tool_choice == "required" and request.tools


def test_capture_retries_provider_context_rejection_without_losing_latest_tool_pair() -> None:
    train = _train("pwd -P")
    rule_id = capture_batches(train)[0].next_rule_id
    steps: tuple[Step, ...] = (
        ("add_rule", _package("syntax error", rule_id)),
        ("page_evaluation", {"receipt": 42}),
        ("finish", FinishInput(justification="No valid rules were proposed.")),
    )
    script = _agent(iter(steps))
    attempts: list[bytes] = []

    def request(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        attempts.append(raw)
        if len(attempts) == 3:
            raise HTTPError(url, 400, "Bad request", Message(), BytesIO(b"maximum context length"))
        return script.request(url, raw, key, timeout)

    result = capture_rules(replace(script, request=request), train, _client([]))

    run = result.batches[0].run
    assert result.complete, run.diagnostics
    _, _, rejected_raw, retried_raw = attempts
    rejected, retried = (_Request.model_validate_json(raw) for raw in (rejected_raw, retried_raw))
    assert len(retried_raw) < len(rejected_raw)
    assert retried == rejected.model_copy(update={"messages": rejected.messages[:2] + rejected.messages[-2:]})
    feedback = retried.messages[-1].content
    assert feedback is not None
    diagnostic = Feedback.model_validate_json(feedback).diagnostic
    assert diagnostic is not None and diagnostic.code == "unknown_receipt"
    assert run.request_sha256 == tuple(sha256(raw).hexdigest() for raw in attempts)
    assert [(failure.turn, failure.diagnostic.code) for failure in run.failures] == [
        (1, "cel_compile"), (2, "unknown_receipt"), (3, "provider_context_limit"),
    ]


def test_capture_reports_transport_cap_separately_without_contacting_provider() -> None:
    agent = replace(_agent(iter(())), max_request_bytes=1024)

    result = capture_rules(agent, _train("pwd -P"), _client([]))

    run = result.batches[0].run
    assert not result.complete and not run.request_sha256
    assert run.status == "request_too_large" and run.diagnostics[0].code == "request_too_large"


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

    calls.clear()
    result = deduplicate_rules(_agent(steps(), requests), train, client, (capture,))
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
    kept = deduplicate_rules(_agent(iter(keep_steps)), train, client, (capture,))
    no_change = kept.decisions[0].finalized
    assert kept.complete and kept.packages == capture.packages and no_change is not None
    assert not no_change.submission.drop and not no_change.submission.add
    assert no_change.submission.justification == "Keep both interpretations pending better evidence."


def test_dedup_rejects_category_rule_that_mislabels_other_train_category() -> None:
    category = _train("pwd -P")
    client = _client([])
    package = _package("pwd -P", capture_batches(category)[0].next_rule_id)
    author: tuple[Step, ...] = (
        ("add_rule", package), ("finish", {"justification": "Covered the literal."}),
    )
    captured = capture_rules(_agent(iter(author)), category, client)
    conflicting = CorpusRow(
        id="train:1", command="pwd -P",
        gold=Labels(category="CAT-7", risk="CAT-7.1", severity="low", action="allow"),
    )
    train = Corpus(split="train", digest="b" * 64, rows=(*category.rows, conflicting), sources=category.sources)
    requests: list[_Request] = []

    with pytest.raises(EvaluationError, match="wrong_match") as failure:
        deduplicate_rules(_agent(iter(()), requests), train, client, (captured,))
    assert captured.packages == (package,)
    assert failure.value.diagnostic.rule_id == package.rule.id
    assert not requests


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
        for _ in range(9):
            yield "page_evaluation", {"receipt": 42}

    result = capture_rules(_agent(steps(), requests), train, _client(calls), category="CAT-6")
    batch = result.batches[0]
    assert not result.complete and batch.run.status == "exhausted" and batch.justification is None
    assert len(requests) == len(batch.run.request_sha256) == 9 and not calls
    assert requests[-2].tool_choice == "required"
    assert requests[-1].tool_choice == {"type": "function", "function": {"name": "finish"}}
    assert len(requests[-1].tools) == 1
    assert tuple(item.code for item in batch.run.diagnostics) == ("unknown_receipt",) * 7 + ("unknown_tool",) * 2
    assert tuple(item.turn for item in batch.run.failures) == tuple(range(1, 10))


@pytest.mark.parametrize(("response_body", "diagnostic"), [
    (b"discarded" * 6000, "response_too_large"),
    (b"discarded synthetic-key-never-echo", "response_sensitive"),
    (b'{"id":"cut","model":"qwen38-27b-fp8","choices":[{"index":0,"finish_reason":"length",'
     b'"message":{"role":"assistant","tool_calls":[{"id":"cut-call","type":"function","function":'
     b'{"name":"finish","arguments":"{\\"justification\\":\\"Do not accept this truncated turn.\\"}"}}]}}]}',
     "output_limit"),
])
def test_capture_retries_invalid_finish_using_only_closure_state(response_body: bytes, diagnostic: str) -> None:
    train = _train("pwd -P", "pwd -L")
    requests: list[_Request] = []
    evaluations: list[_EvaluatorInput] = []
    rule_id = capture_batches(train)[0].next_rule_id
    valid = _package("pwd -P", rule_id)
    why = "The logical form is not covered."
    failure: Step = ("page_evaluation", {"receipt": 42})
    steps: tuple[Step, ...] = (
        ("add_rule", valid), *((failure,) * 6),
        ("finish", {"justification": why}),
    )
    script = _agent(iter(steps))

    def request(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        requests.append(_Request.model_validate_json(raw))
        assert key == "synthetic-key-never-echo"
        if len(requests) == 8:
            return HTTPResponse(response_body, None)
        response = script.request(url, raw, None, timeout)
        if len(requests) == 9:
            return replace(response, body=response.body.replace(b'"tool_calls", "message"', b'"stop", "message"'))
        return response

    agent = replace(script, request=request, api_key="synthetic-key-never-echo")
    result = capture_rules(agent, train, _client(evaluations))

    batch = result.batches[0]
    assert result.complete, batch.run.diagnostics
    assert batch.justification == why and result.packages == (valid,)
    assert len(evaluations) == 1 and batch.rules[0].singleton.corpus_digest == train.digest
    assert [d.code for d in batch.run.diagnostics] == ["unknown_receipt"] * 6 + [diagnostic]
    assert "synthetic-key-never-echo" not in result.model_dump_json()
    first, retry = requests[-2:]
    assert first.tool_choice == "required"
    assert retry.tool_choice == {"type": "function", "function": {"name": "finish"}}
    assert first.tools == retry.tools and len(retry.tools) == 1
    assert first.max_completion_tokens == retry.max_completion_tokens < requests[0].max_completion_tokens
    payload = first.messages[-1].content
    assert payload is not None
    summary = _JSON.validate_json(payload)
    assert summary["accepted_rule_ids"] == [rule_id] and summary["uncovered_row_ids"] == ["train:1"]
    assert summary["diagnostic_codes"] == ["unknown_receipt"]
    assert all(_GUIDE not in (message.content or "") and message.role != "tool" for message in retry.messages)
    assert all("synthetic-key-never-echo" not in (message.content or "") for message in retry.messages)


def test_capture_can_acknowledge_empty_batch_on_first_closure_call() -> None:
    train = _train("pwd -P")
    requests: list[_Request] = []
    wait: Step = ("page_evaluation", {"receipt": 42})
    why = "No rule was recorded; the batch remains uncovered."
    steps: tuple[Step, ...] = (*((wait,) * 7), ("finish", FinishInput(justification=why)))

    result = capture_rules(_agent(iter(steps), requests), train, _client([]))

    (batch,) = result.batches
    assert result.complete and not batch.rules and batch.justification == why
    assert len(requests) == 8 and requests[-1].tool_choice == "required" and len(requests[-1].tools) == 1
    state = requests[-1].messages[-1].content
    assert state is not None
    summary = _JSON.validate_json(state)
    assert summary["accepted_rule_ids"] == [] and summary["uncovered_row_ids"] == ["train:0"]


_TRUNCATED = HTTPResponse(
    b'{"id":"cut","model":"qwen38-27b-fp8","choices":[{"index":0,"finish_reason":"length",'
    b'"message":{"role":"assistant"}}]}', None,
)


@pytest.mark.parametrize("accept_recovery", [True, False])
def test_double_truncated_capture_closes_only_with_supported_native_reason(accept_recovery: bool) -> None:
    train = _train("pwd -P", "pwd -L")
    evaluations: list[_EvaluatorInput] = []
    batch_id = capture_batches(train)[0].id
    valid = tuple(_package("pwd -P", f"{batch_id}-{index:02d}") for index in range(1, 6))
    wait: Step = ("page_evaluation", {"receipt": 42})
    terminal = "finish_with_abstentions" if accept_recovery else "finish_all_covered"
    steps: tuple[Step, ...] = (*(("add_rule", package) for package in valid), wait, wait, (terminal, {}))
    script = _agent(iter(steps))
    attempts: list[_Request] = []

    def request(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        attempts.append(_Request.model_validate_json(raw))
        if len(attempts) in (8, 9):
            return _TRUNCATED
        return script.request(url, raw, key, timeout)

    result = capture_rules(replace(script, request=request), train, _client(evaluations))

    (batch,) = result.batches
    assert result.complete is accept_recovery
    assert result.packages == valid and len(evaluations) == 5
    assert [d.code for d in batch.run.diagnostics if d.code == "output_limit"] == ["output_limit"] * 2
    assert len(attempts) == 10
    if accept_recovery:
        assert batch.justification == "Retained 5 validated rule(s); covered 1 row(s) and abstained on 1 row(s)."
    else:
        assert batch.justification is None and batch.run.diagnostics[-1].code == "unknown_tool"


@pytest.mark.parametrize("tamper", [None, "order", "descriptor", "receipt", "terminal"])
def test_capture_resume_replays_pins_and_only_requests_unfinished_closure(tamper: str | None) -> None:
    train = _train("pwd -P", *(f"pwd -Q{index:02}" for index in range(15)), "pwd -Z")
    first_batch, second_batch = capture_batches(train)
    first = _package("pwd -P", first_batch.next_rule_id)
    second = _package("pwd -Z", second_batch.next_rule_id).model_copy(update={
        "tests": (Fixture(command="pwd -Z", expected=_GOLD), Fixture(command="pwd -L", expected=Other())),
    })
    initial: tuple[Step, ...] = (
        ("add_rule", first), ("finish", {"justification": "Leave the Q forms uncovered."}), ("add_rule", second),
    )
    original_requests: list[_Request] = []
    script = _agent(iter(initial), original_requests)

    def interrupted(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        if len(original_requests) == 3:
            raise HTTPError(url, 503, "Offline", Message(), BytesIO())
        return script.request(url, raw, key, timeout)

    previous = capture_rules(replace(script, request=interrupted), train, _client([]))
    done, unfinished = previous.batches
    assert not previous.complete and previous.finished_batches == 1 and previous.packages == (first, second)
    selected_batches = previous.batches
    match tamper:
        case "order":
            selected_batches = (done.model_copy(update={"row_ids": done.row_ids[::-1]}), unfinished)
        case "descriptor":
            selected_batches = (done.model_copy(update={"descriptor_sha256": "c" * 64}), unfinished)
        case "receipt":
            entry = unfinished.rules[0]
            forged = entry.model_copy(update={"singleton": entry.singleton.model_copy(update={"bundle_sha256": "d" * 64})})
            selected_batches = (done, unfinished.model_copy(update={"rules": (forged,)}))
        case "terminal":
            selected_batches = (done.model_copy(update={"justification": None}), unfinished)
        case None:
            selected_batches = (done.model_copy(update={"descriptor_sha256": None}), unfinished)
        case unexpected:
            pytest.fail(f"Unexpected test case: {unexpected}")
    evidence = previous.model_copy(update={"batches": selected_batches})
    calls: list[_EvaluatorInput] = []
    requests: list[_Request] = []
    recovery: tuple[Step, ...] = (("finish_all_covered", {}),)
    agent = _agent(iter(recovery), requests)
    if tamper is not None:
        with pytest.raises(EvaluationError, match={
            "order": "capture_resume_batch", "descriptor": "capture_resume_batch",
            "receipt": "capture_resume_receipt", "terminal": "capture_resume_closure",
        }[tamper]):
            capture_rules(agent, train, _client(calls), resume=evidence)
        assert not requests
        return

    result = capture_rules(agent, train, _client(calls), resume=evidence)

    assert result.complete and result.packages == (first, second)
    assert tuple(call.bundle.rules for call in calls) == ((first.rule,), (second.rule,))
    (request,) = requests
    slate = request.messages[-1].content
    assert slate is not None
    assert _JSON.validate_json(slate)["diagnostic_codes"] == ["provider_http"]
    assert result.batches[0] == selected_batches[0]
    assert result.batches[1].prior_runs == (unfinished.run,)
    assert result.batches[1].justification == "Retained 1 validated rule(s); covered 1 row(s) and abstained on 0 row(s)."


@pytest.mark.parametrize(("justification", "diagnostic"), [
    ("Both candidates match the sole observed form; retain them rather than assume broader equivalence.", None),
    (" ", "tool_input"),
])
def test_judge_truncation_requires_explicit_justified_keep_and_combined_gate(
    justification: str, diagnostic: str | None,
) -> None:
    train = _train("pwd -P", "pwd -L")
    evaluations: list[_EvaluatorInput] = []
    client = _client(evaluations)
    first = _package("pwd -P", "cat-6.capture-0001-01")
    second = _package("pwd -P", "cat-6.capture-0001-02")
    author: tuple[Step, ...] = (
        ("add_rule", first), ("add_rule", second), ("finish", {"justification": "Compare duplicate hits."}),
    )
    captured = capture_rules(_agent(iter(author)), train, client)
    observed: list[_Request] = []
    page: Step = ("page_evaluation", {"receipt": 1, "kind": "matches", "limit": 1})

    def steps() -> Iterator[Step]:
        yield page
        evidence = _feedback(observed[-1]).page
        assert evidence is not None and tuple(row.id for row in evidence.rows) == ("train:0",)
        assert evidence.total == 1 and evidence.next_offset is None
        yield from (page,) * 5
        yield "keep_rules", {"justification": justification}

    script = _agent(steps(), observed)
    attempts: list[_Request] = []

    def request(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        attempts.append(_Request.model_validate_json(raw))
        if len(attempts) in (7, 8):
            return _TRUNCATED
        return script.request(url, raw, key, timeout)

    evaluations.clear()
    result = deduplicate_rules(replace(script, request=request), train, client, (captured,))

    (decision,) = result.decisions
    assert result.packages == captured.packages
    assert [d.code for d in decision.run.diagnostics] == (
        ["output_limit", "output_limit"] + ([] if diagnostic is None else [diagnostic])
    )
    assert result.complete is (diagnostic is None)
    if diagnostic is None:
        assert decision.finalized is not None and decision.finalized.submission.justification == justification
        assert not decision.finalized.submission.add and not decision.finalized.submission.drop
        assert tuple(call.bundle.rules for call in evaluations) == ((first.rule,), (second.rule,), (first.rule, second.rule))
        assert not decision.finalized.combined.fixtures
    else:
        assert len(evaluations) == 2 and decision.finalized is None


def test_judge_closure_rejects_empty_justification_then_evaluates_actual_diff() -> None:
    train = _train("pwd -P")
    requests: list[_Request] = []
    evaluations: list[_EvaluatorInput] = []
    client = _client(evaluations)
    first = _package("pwd -P", "cat-6.capture-0001-01")
    second = _package("pwd -P", "cat-6.capture-0001-02")
    capture_steps: tuple[Step, ...] = (
        ("add_rule", first), ("add_rule", second), ("finish", {"justification": "Compare duplicates."}),
    )
    captured = capture_rules(_agent(iter(capture_steps)), train, client)
    fresh = _package("pwd -P", "cat-6.judge-0001-01")
    why = "Consolidate both duplicates; the retained fixture checks the same train behavior."
    wait: Step = ("page_evaluation", {"receipt": 1})
    steps: tuple[Step, ...] = (
        *((wait,) * 6),
        ("finalize", {"drop_ids": [], "add": [], "justification": " "}),
        ("finalize", {
            "drop_ids": [first.rule.id, second.rule.id], "add": [_JSON.validate_json(fresh.model_dump_json())],
            "justification": why,
        }),
    )

    result = deduplicate_rules(_agent(iter(steps), requests), train, client, (captured,))

    (decision,) = result.decisions
    receipt = decision.finalized
    assert result.complete and receipt is not None and result.packages == (fresh,)
    assert receipt.submission.drop == (first.rule.id, second.rule.id) and receipt.submission.justification == why
    assert all(item.corpus_digest == train.digest for item in (*receipt.additions, receipt.combined))
    assert [item.code for item in decision.run.diagnostics] == ["tool_input"]
    closure, repair = requests[-2:]
    assert closure.tool_choice == "required"
    assert repair.tool_choice == {"type": "function", "function": {"name": "finalize"}}
    assert closure.tools == repair.tools and len(repair.tools) == 1
    assert repair.max_completion_tokens == requests[0].max_completion_tokens == 8192
    assert closure.messages[:2] == repair.messages[:2] == requests[0].messages


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
