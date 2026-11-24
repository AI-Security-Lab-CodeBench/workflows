from collections.abc import Callable
from hashlib import sha256
from typing import Literal

import httpx2
import pandas as pd
import pytest
from pydantic import ValidationError

from codebench_pipeline.rules_contracts import (
    Bundle,
    Diagnostic,
    EvaluationError,
    EvaluationInput,
    FinalizeError,
    FinalizeSubmission,
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
from codebench_pipeline.rules_corpus import Corpus, CorpusRow, build_corpus
from codebench_pipeline.rules_engine import BashguardClient, evaluate_corpus, evaluate_package, finalize_batch
from codebench_pipeline.rules_metrics import TrainFingerprint, score_holdout, score_package
from codebench_pipeline.typed_config import ConfigArtifact

_PIN = "synthetic-runtime"
_GOLD = Labels(category="CAT-6", risk="CAT-6.1", severity="low", action="allow")
_UNSEEN = Labels(category="CAT-4", risk="CAT-4.1", severity="high", action="deny")
_SOURCE = "https://example.test/synthetic-policy"


class _Request(ConfigArtifact):
    required_runtime_version: str
    bundle: Bundle
    rows: tuple[EvaluationInput, ...]


def _rule(rule_id: str = "read-pwd", command: str = "pwd -P", labels: Labels = _GOLD) -> NativeRule:
    return NativeRule(id=rule_id, when=command, labels=labels, rationale="Fórm <trusted>", sources=(_SOURCE,))


def _train(*commands: str) -> Corpus:
    return Corpus(
        split="train",
        digest="a" * 64,
        rows=tuple(
            CorpusRow(id=f"row:{i}", command=command, gold=_GOLD) for i, command in enumerate(commands)
        ),
    )


def _response(request: httpx2.Request) -> NativeEvaluation:
    assert request.method == "POST" and request.url.path == "/v1/evaluate"
    payload = _Request.model_validate_json(request.content)
    assert payload.required_runtime_version == _PIN
    raw_bundle = request.content.split(b',"bundle":', 1)[1].rsplit(b',"rows":', 1)[0]
    digest = sha256(raw_bundle).hexdigest()
    output: list[NativeRow] = []
    for row in reversed(payload.rows):
        matching = tuple(rule for rule in payload.bundle.rules if rule.when == row.command)
        labels = tuple(rule.labels for rule in matching)
        matched = bool(labels) and all(label == labels[0] for label in labels)
        reason = "conflicting_rules" if labels else "no_proven_rule"
        error = None
        if row.command == "printf runtime":
            error = Diagnostic(
                code="cel_runtime_error", message="index out of bounds", field="when", rule_id="r"
            )
            reason = "rule_evaluation_error:r"
        output.append(
            NativeRow(
                id=row.id,
                error=error,
                result=NativeResult(
                    status="matched" if matched else "abstain",
                    basis="literal_cli_form",
                    labels=labels[0] if matched else None,
                    rule_ids=tuple(rule.id for rule in matching) if matched else (),
                    rationales=(),
                    sources=(),
                    reasons=() if matched else (reason,),
                    bundle_sha256=digest,
                ),
            )
        )
    return NativeEvaluation(runtime_version=_PIN, bundle_sha256=digest, results=tuple(output))


def _wire(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, content=_response(request).model_dump_json())


def _client(handler: Callable[[httpx2.Request], httpx2.Response] = _wire) -> BashguardClient:
    return BashguardClient("http://evaluator.test", _PIN, httpx2.MockTransport(handler))


def test_corpus_preserves_duplicate_rows_and_derives_pinned_sources() -> None:
    frame = pd.DataFrame(
        [("pwd -P", "CAT-6", "CAT-6.1", "low", "allow")] * 207,
        columns=["command", "category", "risk", "severity", "class"],
    )
    frame.attrs["train_sources"] = {
        "CAT-6": [
            {
                "repository": "example/data",
                "branch": "main",
                "commit": "1" * 40,
                "path": "splits/train score.csv",
                "category": "CAT-6",
                "sha256": "2" * 64,
            }
        ]
    }

    corpus = build_corpus(frame, split="train", digest="a" * 64)

    assert {row.id for row in corpus.rows} == {f"train:{'a' * 64}:{i}" for i in range(207)}
    assert all(row.gold == _GOLD for row in corpus.rows)
    assert {row.command_sha256 for row in corpus.rows} == {sha256(b"pwd -P").hexdigest()}
    assert corpus.sources_for("CAT-6") == (
        f"https://github.com/example/data/blob/{'1' * 40}/splits/train%20score.csv",
    )
    with pytest.raises(ValidationError, match="frozen"):
        setattr(corpus.rows[0].gold, "action", "deny")


def test_train_corpus_refuses_to_fabricate_sources() -> None:
    frame = pd.DataFrame(
        [("pwd -P", "CAT-6", "CAT-6.1", "low", "allow")],
        columns=["command", "category", "risk", "severity", "class"],
    )
    with pytest.raises(ValidationError):
        build_corpus(frame, split="train", digest="a" * 64)


@pytest.mark.parametrize("command", ["pwd -P", "printf " + "x" * 50_000])
def test_wire_batches_by_rows_and_bytes_without_transmitting_gold(command: str) -> None:
    sizes: list[int] = []

    def endpoint(request: httpx2.Request) -> httpx2.Response:
        parsed = _Request.model_validate_json(request.content)
        sizes.append(len(parsed.rows))
        assert len(parsed.rows) <= 128 and len(request.content) <= 2 << 20
        return _wire(request)

    corpus = _train(*(command,) * 130)
    receipt = evaluate_corpus(_client(endpoint), Bundle(rules=(_rule(),)), corpus)

    expected = (128, 2) if command == "pwd -P" else (41, 41, 41, 7)
    assert sizes == list(expected)
    assert tuple(row.id for row in receipt.rows) == tuple(row.id for row in corpus.rows)
    assert tuple(row.id for row in receipt.page(offset=127, limit=2)) == ("row:127", "row:128")


@pytest.mark.parametrize(
    ("fault", "code"),
    [
        ("pin", "runtime_version_mismatch"),
        ("sha", "response_digest"),
        ("row_sha", "response_digest"),
        ("missing", "response_rows"),
        ("duplicate", "response_rows"),
    ],
)
def test_rejects_corruption_on_later_response_batches(
    fault: Literal["pin", "sha", "row_sha", "missing", "duplicate"],
    code: str,
) -> None:
    def endpoint(request: httpx2.Request) -> httpx2.Response:
        reply = _response(request)
        if len(reply.results) == 2:
            last = reply.results[-1]
            match fault:
                case "pin":
                    reply = reply.model_copy(update={"runtime_version": "another-build"})
                case "sha":
                    reply = reply.model_copy(update={"bundle_sha256": "b" * 64})
                case "row_sha":
                    result = last.result.model_copy(update={"bundle_sha256": "b" * 64})
                    reply = reply.model_copy(
                        update={"results": (reply.results[0], last.model_copy(update={"result": result}))}
                    )
                case "missing":
                    reply = reply.model_copy(update={"results": (last,)})
                case "duplicate":
                    reply = reply.model_copy(update={"results": (last, last)})
        return httpx2.Response(200, content=reply.model_dump_json())

    with pytest.raises(EvaluationError, match=code):
        evaluate_corpus(_client(endpoint), Bundle(rules=(_rule(),)), _train(*("pwd -P",) * 130))


def test_compile_diagnostic_preserves_native_location_and_caret() -> None:
    diagnostic = Diagnostic(
        code="invalid_bundle",
        phase="compile",
        rule_id="read-pwd",
        field="when",
        line=1,
        column=9,
        message="syntax error\n | argv ==\n | ........^",
    )

    def endpoint(_request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            422, content=NativeFailure(runtime_version=_PIN, error=diagnostic).model_dump_json()
        )

    with pytest.raises(EvaluationError) as caught:
        evaluate_corpus(_client(endpoint), Bundle(rules=(_rule(),)), _train("pwd -P"))
    assert caught.value.diagnostic == diagnostic


def test_runtime_error_never_passes_a_negative_fixture() -> None:
    package = Package(rule=_rule(), tests=(Fixture(command="printf runtime", expected=Other()),))
    receipt = evaluate_package(_client(), package, _train("pwd -P"))
    assert not receipt.fixtures_passed
    assert receipt.fixtures[0].actual is None
    assert score_package(receipt).fixtures_failed == 1
    assert receipt.hit_sets[0].row_ids == ("row:0",)


def test_finalize_rejects_conflicts_introduced_by_combining_packages() -> None:
    old = Package(rule=_rule("old", labels=_UNSEEN), tests=(Fixture(command="pwd -P", expected=_UNSEEN),))
    replacement = Package(rule=_rule("new"), tests=(Fixture(command="pwd -P", expected=_GOLD),))
    diff = FinalizeSubmission(add=(replacement,), justification="New synthetic specialization")
    with pytest.raises(FinalizeError, match="fixture_mismatch") as caught:
        finalize_batch(_client(), (old,), diff, _train("pwd -P"))
    assert caught.value.evidence[0].fixtures_passed
    error = caught.value.evidence[1].fixtures[0].error
    assert error is not None and error.code == "conflicting_rules"


def test_finalize_does_not_inherit_old_fixtures_or_preserve_old_coverage() -> None:
    keep = Package(rule=_rule("keep"), tests=(Fixture(command="pwd -L", expected=Other()),))
    drop = Package(rule=_rule("drop", "printf old"), tests=(Fixture(command="printf old", expected=_GOLD),))
    add = Package(rule=_rule("new", "pwd -L"), tests=(Fixture(command="pwd -L", expected=_GOLD),))
    diff = FinalizeSubmission(add=(add,), drop=("drop",), justification="Retire legacy signature")

    receipt = finalize_batch(_client(), (keep, drop), diff, _train("pwd -P", "printf old"))

    assert receipt.packages == (keep, add)
    assert tuple(row.input.command for row in receipt.combined.fixtures) == ("pwd -L",)
    assert receipt.combined.rows[1].actual == Other()


def test_heldout_metrics_distinguish_overlap_errors_and_unsupported_precision() -> None:
    samples = (("pwd -P", _GOLD), ("pwd -P", _UNSEEN), ("pwd -L", _UNSEEN), ("printf runtime", _GOLD))
    test = Corpus(
        split="test",
        digest="b" * 64,
        rows=tuple(
            CorpusRow(id=str(i), command=command, gold=gold) for i, (command, gold) in enumerate(samples)
        ),
    )
    receipt = evaluate_corpus(_client(), Bundle(rules=(_rule(),)), test)

    metrics = score_holdout(receipt, TrainFingerprint.from_corpus(_train("pwd -P")))

    assert (
        metrics.overall.correct,
        metrics.overall.wrong,
        metrics.overall.abstain,
        metrics.overall.error,
    ) == (1, 1, 1, 1)
    assert (metrics.overall.precision, metrics.overall.coverage, metrics.overall.recall) == (0.5, 0.5, 0.25)
    assert (metrics.contaminated_rows, metrics.contaminated_commands) == (2, 1)
    assert metrics.uncontaminated.precision is None and metrics.uncontaminated.support == 2
    allow, deny = tuple(metric for metric in metrics.classes if metric.axis == "action")
    assert (allow.fpr, allow.negative_support, deny.precision, deny.recall) == (0.5, 2, None, 0.0)
