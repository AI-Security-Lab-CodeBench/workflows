import pandas as pd
import pytest
from pydantic import ValidationError

from codebench_pipeline.rules_contracts import (
    EvaluatedRow,
    EvaluationInput,
    EvaluationReceipt,
    Fixture,
    Labels,
    NativeResult,
    NativeRow,
    NativeRule,
    Split,
)
from codebench_pipeline.rules_corpus import build_corpus
from codebench_pipeline.rules_metrics import HeldOutMetrics, TrainFingerprint, score_holdout

_LABELS = Labels(category="CAT-6", risk="CAT-6.1", severity="low", action="allow")


def _frame(*samples: tuple[str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [(command, "CAT-6", "CAT-6.1", "low", action) for command, action in samples],
        columns=["command", "category", "risk", "severity", "class"],
    )


def test_holdout_scores_modify_as_wrong_or_abstain_without_losing_axis_credit() -> None:
    corpus = build_corpus(
        _frame(("pwd", "allow"), ("pwd -P", "modify"), ("pwd -L", "modify")),
        split="test",
        digest="b" * 64,
    )
    rows = tuple(
        EvaluatedRow.from_native(
            (EvaluationInput(id=row.id, command=row.command), row.gold),
            NativeRow(
                id=row.id,
                result=NativeResult(
                    status="matched" if matched else "abstain",
                    basis="literal_cli_form",
                    labels=_LABELS if matched else None,
                    rule_ids=("pwd-rule",) if matched else (),
                    rationales=(),
                    sources=(),
                    reasons=() if matched else ("no_proven_rule",),
                    bundle_sha256="c" * 64,
                ),
            ),
        )
        for row, matched in zip(corpus.rows, (True, True, False), strict=True)
    )
    receipt = EvaluationReceipt(
        runtime_version="synthetic",
        bundle_sha256="c" * 64,
        split=corpus.split,
        corpus_digest=corpus.digest,
        rows=rows,
        hit_sets=(),
    )
    train = TrainFingerprint(digest="a" * 64, categories=("CAT-6",), command_hashes=())

    metrics = score_holdout(receipt, train)

    assert EvaluationReceipt.model_validate_json(receipt.model_dump_json()) == receipt
    assert HeldOutMetrics.model_validate_json(metrics.model_dump_json()) == metrics
    assert receipt.rows[0].expected == _LABELS
    assert (
        metrics.overall.support,
        metrics.overall.correct,
        metrics.overall.wrong,
        metrics.overall.abstain,
        metrics.overall.error,
    ) == (3, 1, 1, 1, 0)
    assert metrics.trained == metrics.uncontaminated == metrics.overall
    assert {
        (item.axis, item.value): (item.support, item.predicted, item.true_positive, item.false_positive)
        for item in metrics.classes
    } == {
        ("category", "CAT-6"): (3, 2, 2, 0),
        ("risk", "CAT-6.1"): (3, 2, 2, 0),
        ("severity", "low"): (3, 2, 2, 0),
        ("action", "allow"): (1, 2, 1, 1),
        ("action", "modify"): (2, 0, 0, 0),
    }
    modify = next(item for item in metrics.classes if item.value == "modify")
    assert (modify.coverage, modify.precision, modify.recall) == (0.5, None, 0.0)
    assert {
        (cell.expected.action, cell.actual.action if cell.actual else None, cell.outcome): cell.count
        for cell in metrics.confusion
    } == {
        ("allow", "allow", "correct"): 1,
        ("modify", "allow", "wrong"): 1,
        ("modify", None, "abstain"): 1,
    }


@pytest.mark.parametrize(("split", "action"), [("train", "modify"), ("test", "unknown")])
def test_corpus_rejects_actions_outside_its_split_contract(split: Split, action: str) -> None:
    with pytest.raises(ValidationError, match="action"):
        build_corpus(_frame(("pwd", action)), split=split, digest="a" * 64)


@pytest.mark.parametrize("action", ["modify", "unknown"])
def test_native_rules_predictions_and_fixtures_reject_unsupported_actions(action: str) -> None:
    labels = {**_LABELS.model_dump(), "action": action}
    with pytest.raises(ValidationError, match=r"labels\.action"):
        NativeRule.model_validate(
            {
                "id": "pwd-rule",
                "when": "pwd",
                "labels": labels,
                "rationale": "Synthetic literal.",
                "sources": ("https://example.test/policy",),
            }
        )
    with pytest.raises(ValidationError, match=r"labels\.action"):
        NativeResult.model_validate(
            {
                "status": "matched",
                "basis": "literal_cli_form",
                "labels": labels,
                "rule_ids": ("pwd-rule",),
                "rationales": (),
                "sources": (),
                "reasons": (),
                "bundle_sha256": "c" * 64,
            }
        )
    with pytest.raises(ValidationError) as caught:
        Fixture.model_validate({"command": "pwd", "expected": labels})
    assert any(
        error["loc"][0] == "expected" and error["loc"][-1] == "action" and error["type"] == "literal_error"
        for error in caught.value.errors()
    )
