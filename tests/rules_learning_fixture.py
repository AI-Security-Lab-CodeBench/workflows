import json
from collections.abc import Callable, Iterator
from hashlib import sha256
from typing import Final

import httpx2
from pydantic import JsonValue

from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.rules_agents import Feedback, RuleAgent
from codebench_pipeline.rules_capture_facts import (
    AnalysisBatch,
    AnalysisRow,
    NativeAnalysis,
    NativeCall,
    NativeWord,
)
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
from codebench_pipeline.rules_engine import BashguardClient
from codebench_pipeline.typed_config import ConfigArtifact

_MODEL: Final = "qwen38-27b-fp8"
_PIN: Final = "pinned-synthetic"
CAT_SHAPE: Final = "argv.size() == 4 && argv[0] == 'cat' && argv[1] == '-n' && argv[2] == '--'"
SOURCE: Final = "https://example.test/manifest"
GOLD: Final = Labels(category="CAT-6", risk="CAT-6.1", severity="low", action="allow")
GUIDE: Final = '{"instructions":"Synthetic test guide","examples":["positive and negative"]}'
COMPILE: Final = Diagnostic(
    code="cel_compile",
    phase="compile",
    rule_id="cat-6.capture-0001-01",
    field="when",
    message="<input>:1:20: expected ']'\n | argv == ['pwd', '-P'\n | ...................^",
    line=1,
    column=20,
)
type Step = tuple[str, ConfigArtifact | dict[str, JsonValue]]


class EvaluatorInput(ConfigArtifact):
    required_runtime_version: str
    bundle: Bundle
    rows: tuple[EvaluationInput, ...]


class _AnalyzerInput(ConfigArtifact):
    required_runtime_version: str
    rows: tuple[EvaluationInput, ...]


class _Message(ConfigArtifact):
    role: str
    content: str | None = None
    tool_call_id: str | None = None
    tool_calls: tuple[JsonValue, ...] = ()


class ModelRequest(ConfigArtifact):
    model: str
    messages: tuple[_Message, ...]
    tools: tuple[dict[str, JsonValue], ...]
    tool_choice: JsonValue
    parallel_tool_calls: bool
    stream: bool
    max_completion_tokens: int
    temperature: float
    chat_template_kwargs: dict[str, bool]


def feedback(request: ModelRequest) -> Feedback:
    last = request.messages[-1]
    assert last.role == "tool" and last.content is not None
    assert last.tool_call_id == f"call-{(len(request.messages) - 2) // 2}"
    return Feedback.model_validate_json(last.content)


def scripted_agent(steps: Iterator[Step], observed: list[ModelRequest] | None = None) -> RuleAgent:
    requests = [] if observed is None else observed

    def complete(url: str, raw: bytes, key: str | None, timeout: float) -> HTTPResponse:
        request = ModelRequest.model_validate_json(raw)
        assert url == "https://model.test/v1/chat/completions" and key is None and timeout == 30
        assert request.chat_template_kwargs == {"enable_thinking": False}
        assert request.model == _MODEL and request.temperature == 0
        assert not request.parallel_tool_calls and not request.stream
        requests.append(request)
        name, args = next(steps)
        arguments = args.model_dump_json() if isinstance(args, ConfigArtifact) else json.dumps(args)
        call = {
            "id": f"call-{len(requests)}",
            "type": "function",
            "function": {"name": name, "arguments": arguments},
        }
        message = {"role": "assistant", "content": None, "tool_calls": [call]}
        choice = {"index": 0, "finish_reason": "tool_calls", "message": message}
        completion = {"id": f"result-{len(requests)}", "model": _MODEL, "choices": [choice]}
        return HTTPResponse(json.dumps(completion).encode(), None)

    return RuleAgent(
        model=ModelConfig(
            provider="openai_compatible",
            model=_MODEL,
            base_url="https://model.test/v1",
            temperature=0,
            timeout_seconds=30,
            max_output_tokens=8192,
        ),
        backend_revision=_MODEL,
        guide=GUIDE,
        guide_sha256=sha256(GUIDE.encode()).hexdigest(),
        request=complete,
    )


def synthetic_evaluator(
    calls: list[EvaluatorInput],
    analyses: dict[str, AnalysisRow] | None = None,
    *,
    reject_value: str = "",
    matches_rule: Callable[[NativeRule, str], bool] | None = None,
) -> BashguardClient:
    def endpoint(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/v1/analyze":
            analysis = _AnalyzerInput.model_validate_json(request.content)
            assert analysis.required_runtime_version == _PIN
            return httpx2.Response(
                200,
                content=AnalysisBatch(
                    runtime_version=_PIN,
                    results=tuple(
                        analyses[row.id]
                        if analyses is not None and row.id in analyses
                        else simple_analysis(row.id, row.command)
                        for row in analysis.rows
                    ),
                ).model_dump_json(),
            )
        payload = EvaluatorInput.model_validate_json(request.content)
        assert payload.required_runtime_version == _PIN
        calls.append(payload)
        if any(rule.when == "syntax error" for rule in payload.bundle.rules):
            return httpx2.Response(
                422, content=NativeFailure(runtime_version=_PIN, error=COMPILE).model_dump_json()
            )
        for rule in payload.bundle.rules:
            if reject_value and reject_value in rule.when and rule.capture_guarded:
                return httpx2.Response(
                    422,
                    content=NativeFailure(
                        runtime_version=_PIN,
                        error=Diagnostic(
                            code="raw_literal_overfit",
                            phase="compile",
                            rule_id=rule.id,
                            field="when",
                            message="Use documented public tokens, not raw literals.",
                        ),
                    ).model_dump_json(),
                )
        digest = sha256(payload.bundle.model_dump_json().encode()).hexdigest()
        results: list[NativeRow] = []
        for row in payload.rows:
            matches = tuple(
                rule
                for rule in payload.bundle.rules
                if (matches_rule is not None and matches_rule(rule, row.command))
                or rule.when == "true"
                or row.command in rule.when.split("|")
                or (
                    rule.when == CAT_SHAPE
                    and len(row.command.split()) == 4
                    and row.command.split()[:3] == ["cat", "-n", "--"]
                )
            )
            results.append(
                NativeRow(
                    id=row.id,
                    result=NativeResult(
                        status="matched" if matches else "abstain",
                        basis="literal_cli_form",
                        labels=matches[0].labels if matches else None,
                        rule_ids=tuple(rule.id for rule in matches),
                        rationales=(),
                        sources=(),
                        reasons=() if matches else ("no_proven_rule",),
                        bundle_sha256=digest,
                    ),
                )
            )
        return httpx2.Response(
            200,
            content=NativeEvaluation(
                runtime_version=_PIN,
                bundle_sha256=digest,
                results=tuple(results),
            ).model_dump_json(),
        )

    return BashguardClient("http://engine.test", _PIN, httpx2.MockTransport(endpoint))


def simple_analysis(row_id: str, command: str) -> AnalysisRow:
    offset = 0
    words: list[NativeWord] = []
    for token in command.split():
        start = command.index(token, offset)
        offset = start + len(token)
        words.append(NativeWord(start=start, end=offset, raw=token, value=token, static=True))
    return AnalysisRow(
        id=row_id,
        analysis=NativeAnalysis(
            eligible=True,
            reasons=(),
            nodes=(),
            calls=(NativeCall(start=0, end=len(command), words=tuple(words)),),
        ),
    )


def train_corpus(*commands: str) -> Corpus:
    return Corpus(
        split="train",
        digest="a" * 64,
        rows=tuple(
            CorpusRow(id=f"train:{index}", command=command, gold=GOLD)
            for index, command in enumerate(commands)
        ),
        sources=(CorpusSource(category="CAT-6", url=SOURCE, sha256="b" * 64),),
    )


def rule_package(
    command: str,
    rule_id: str,
    rationale: str = "physical directory path",
    *,
    positive: str = "pwd -P",
) -> Package:
    return Package(
        rule=NativeRule(
            id=rule_id,
            when=command,
            labels=GOLD,
            rationale=rationale,
            sources=(SOURCE,),
            capture_allow_shapes=(tuple(positive.split()),),
            capture_guarded=True,
        ),
        tests=(Fixture(command=positive, expected=GOLD), Fixture(command="pwd -L", expected=Other())),
    )
