from collections import Counter
from hashlib import sha256
from typing import Literal, assert_never

from codebench_pipeline.models import Digest
from codebench_pipeline.rules_contracts import (
    Category,
    Diagnostic,
    EvaluatedRow,
    EvaluationError,
    EvaluationReceipt,
    Labels,
    Other,
)
from codebench_pipeline.rules_corpus import Corpus
from codebench_pipeline.typed_config import ConfigArtifact

Outcome = Literal["correct", "wrong", "abstain", "error"]
Axis = Literal["category", "risk", "severity", "action"]


class OutcomeMetrics(ConfigArtifact):
    support: int
    correct: int
    wrong: int
    abstain: int
    error: int
    precision: float | None
    coverage: float | None
    recall: float | None


class ClassMetrics(ConfigArtifact):
    axis: Axis
    value: str
    support: int
    negative_support: int
    predicted: int
    true_positive: int
    false_positive: int
    precision: float | None
    coverage: float | None
    recall: float | None
    fpr: float | None


class ConfusionCell(ConfigArtifact):
    expected: Labels
    actual: Labels | None
    outcome: Outcome
    count: int


class PackageMetrics(ConfigArtifact):
    runtime_version: str
    bundle_sha256: Digest
    train_digest: Digest
    fixtures_passed: int
    fixtures_failed: int
    train: OutcomeMetrics
    classes: tuple[ClassMetrics, ...]
    confusion: tuple[ConfusionCell, ...]


class HeldOutMetrics(ConfigArtifact):
    runtime_version: str
    bundle_sha256: Digest
    train_digest: Digest
    test_digest: Digest
    trained_categories: tuple[str, ...]
    contaminated_rows: int
    contaminated_commands: int
    overall: OutcomeMetrics
    uncontaminated: OutcomeMetrics
    trained: OutcomeMetrics
    unseen: OutcomeMetrics
    classes: tuple[ClassMetrics, ...]
    confusion: tuple[ConfusionCell, ...]


class TrainFingerprint(ConfigArtifact):
    """Train lineage needed for holdout scoring, without exposing commands or gold rows."""

    digest: Digest
    categories: tuple[Category, ...]
    command_hashes: tuple[Digest, ...]

    @classmethod
    def from_corpus(cls, train: Corpus, *, categories: tuple[str, ...] | None = None) -> "TrainFingerprint":
        if train.split != "train":
            raise EvaluationError(Diagnostic(code="train_only", message="Fingerprint requires train"))
        return cls(
            digest=train.digest,
            categories=tuple(
                sorted(
                    set(categories if categories is not None else (row.gold.category for row in train.rows))
                )
            ),
            command_hashes=tuple(sorted({row.command_sha256 for row in train.rows})),
        )


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _outcome(row: EvaluatedRow) -> Outcome:
    if row.error is not None:
        return "error"
    match row.actual:
        case Labels():
            return "correct" if row.passed else "wrong"
        case Other():
            return "abstain"
        case None:
            return "error"
    assert_never(row.actual)


def _summary(rows: tuple[EvaluatedRow, ...]) -> OutcomeMetrics:
    counts = Counter(_outcome(row) for row in rows)
    matches = counts["correct"] + counts["wrong"]
    return OutcomeMetrics(
        support=len(rows),
        correct=counts["correct"],
        wrong=counts["wrong"],
        abstain=counts["abstain"],
        error=counts["error"],
        precision=_ratio(counts["correct"], matches),
        coverage=_ratio(matches, len(rows)),
        recall=_ratio(counts["correct"], len(rows)),
    )


def _decisions(rows: tuple[EvaluatedRow, ...]) -> tuple[tuple[Labels, Labels | None, Outcome], ...]:
    decisions: list[tuple[Labels, Labels | None, Outcome]] = []
    for row in rows:
        if not isinstance(row.expected, Labels):
            raise EvaluationError(
                Diagnostic(code="corpus_gold", message="Expected complete corpus gold labels")
            )
        actual = row.actual if row.error is None and isinstance(row.actual, Labels) else None
        decisions.append((row.expected, actual, _outcome(row)))
    return tuple(decisions)


def _label(labels: Labels, axis: Axis) -> str:
    match axis:
        case "category":
            return labels.category
        case "risk":
            return labels.risk
        case "severity":
            return labels.severity
        case "action":
            return labels.action
    assert_never(axis)


def _classes(rows: tuple[tuple[Labels, Labels | None, Outcome], ...]) -> tuple[ClassMetrics, ...]:
    metrics: list[ClassMetrics] = []
    axes: tuple[Axis, ...] = ("category", "risk", "severity", "action")
    for axis in axes:
        pairs = tuple(
            (_label(gold, axis), _label(prediction, axis) if prediction else None)
            for gold, prediction, _ in rows
        )
        support = Counter(gold for gold, _ in pairs)
        predictions = Counter(prediction for _, prediction in pairs if prediction is not None)
        true = Counter(gold for gold, prediction in pairs if gold == prediction)
        labeled = Counter(gold for gold, prediction in pairs if prediction is not None)
        for value in sorted(support.keys() | predictions.keys()):
            negatives = len(rows) - support[value]
            false = predictions[value] - true[value]
            metrics.append(
                ClassMetrics(
                    axis=axis,
                    value=value,
                    support=support[value],
                    negative_support=negatives,
                    predicted=predictions[value],
                    true_positive=true[value],
                    false_positive=false,
                    precision=_ratio(true[value], predictions[value]),
                    coverage=_ratio(labeled[value], support[value]),
                    recall=_ratio(true[value], support[value]),
                    fpr=_ratio(false, negatives),
                )
            )
    return tuple(metrics)


def _confusion(rows: tuple[tuple[Labels, Labels | None, Outcome], ...]) -> tuple[ConfusionCell, ...]:
    counts = Counter(rows)
    return tuple(
        ConfusionCell(expected=gold, actual=actual, outcome=outcome, count=counts[gold, actual, outcome])
        for gold, actual, outcome in sorted(
            counts,
            key=lambda item: (
                item[0].model_dump_json(),
                item[2],
                item[1].model_dump_json() if item[1] else "",
            ),
        )
    )


def score_package(receipt: EvaluationReceipt) -> PackageMetrics:
    if receipt.split != "train":
        raise EvaluationError(Diagnostic(code="train_only", message="Package scoring requires train"))
    decisions = _decisions(receipt.rows)
    passed = sum(row.passed for row in receipt.fixtures)
    return PackageMetrics(
        runtime_version=receipt.runtime_version,
        bundle_sha256=receipt.bundle_sha256,
        train_digest=receipt.corpus_digest,
        fixtures_passed=passed,
        fixtures_failed=len(receipt.fixtures) - passed,
        train=_summary(receipt.rows),
        classes=_classes(decisions),
        confusion=_confusion(decisions),
    )


def score_holdout(receipt: EvaluationReceipt, train: TrainFingerprint) -> HeldOutMetrics:
    """Report exact command overlap separately and recompute exact-tuple metrics without those rows."""
    if receipt.split != "test" or train.digest == receipt.corpus_digest:
        raise EvaluationError(
            Diagnostic(code="holdout_split", message="Expected distinct train and test snapshots")
        )
    hashes = set(train.command_hashes)
    trained = set(train.categories)
    overlap = Counter[str]()
    clean: list[EvaluatedRow] = []
    known: list[EvaluatedRow] = []
    unseen: list[EvaluatedRow] = []
    decisions = _decisions(receipt.rows)
    for row, (gold, _, _) in zip(receipt.rows, decisions, strict=True):
        digest = sha256(row.input.command.encode()).hexdigest()
        if digest in hashes:
            overlap[digest] += 1
        else:
            clean.append(row)
        (known if gold.category in trained else unseen).append(row)
    return HeldOutMetrics(
        runtime_version=receipt.runtime_version,
        bundle_sha256=receipt.bundle_sha256,
        train_digest=train.digest,
        test_digest=receipt.corpus_digest,
        trained_categories=tuple(sorted(trained)),
        contaminated_rows=overlap.total(),
        contaminated_commands=len(overlap),
        overall=_summary(receipt.rows),
        uncontaminated=_summary(tuple(clean)),
        trained=_summary(tuple(known)),
        unseen=_summary(tuple(unseen)),
        classes=_classes(decisions),
        confusion=_confusion(decisions),
    )
