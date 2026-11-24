import re
from collections import Counter
from itertools import combinations, pairwise
from math import ceil, sqrt
from typing import Final, Literal

import numpy as np
from numpy.typing import NDArray
from pydantic import Field

from codebench_pipeline.models import Digest
from codebench_pipeline.rules_contracts import (
    Category,
    Diagnostic,
    EvaluatedRow,
    EvaluationError,
    EvaluationReceipt,
    FinalizeReceipt,
    Labels,
    Package,
)
from codebench_pipeline.rules_corpus import Corpus, CorpusRow, CorpusSource
from codebench_pipeline.typed_config import ConfigArtifact

SIMILARITY_REVISION: Final = "rationale-word-bigram-tfidf-lsa-sqrt24-cosine075-or-any-train-hit-v1"
_MIN_COSINE: Final = 0.75
_STOPWORDS: Final = frozenset(
    "a an and are as at be by can for from has have in is it of on only or that the this to when with".split()
)
_WORDS = re.compile(r"[^\W_]+")
_TOKENS = re.compile(r"[A-Za-z0-9_./-]+|[^\w\s]")
_BATCH_SIZE: Final = 16
JUDGE_WIDTH: Final = 4


class AgentFailure(ConfigArtifact):
    turn: int
    diagnostic: Diagnostic


class AgentRun(ConfigArtifact):
    model: str
    backend_revision: str
    guide_sha256: Digest
    request_sha256: tuple[Digest, ...]
    status: Literal["finished", "exhausted", "context_limit", "endpoint_error"]
    failures: tuple[AgentFailure, ...]

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        return tuple(failure.diagnostic for failure in self.failures)


class PageInput(ConfigArtifact):
    receipt: int = Field(ge=1)
    kind: Literal["fixtures", "rows"] = "rows"
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=3, ge=1, le=5)


class ReceiptView(ConfigArtifact):
    receipt: int
    runtime_version: str
    bundle_sha256: Digest
    corpus_digest: Digest
    fixture_total: int
    fixture_failures: int
    train_total: int
    train_matches: int
    train_wrong_matches: int
    train_errors: int


class EvidencePage(ConfigArtifact):
    receipt: int
    kind: Literal["fixtures", "rows"]
    offset: int
    total: int
    next_offset: int | None
    rows: tuple[EvaluatedRow, ...]


def review_examples(receipts: tuple[EvaluationReceipt, ...]) -> tuple[EvaluatedRow, ...]:
    return tuple(
        row for receipt in receipts
        for row in (*receipt.fixtures, *receipt.rows)
        if row.error is not None or ((row.rule_ids or row in receipt.fixtures) and not row.passed)
    )[:3]


class FinishInput(ConfigArtifact):
    justification: str = Field(pattern=r"\S", max_length=2000)


class DiffInput(FinishInput):
    drop_ids: tuple[str, ...] = Field(max_length=JUDGE_WIDTH)
    add: tuple[Package, ...] = Field(max_length=JUDGE_WIDTH)


class CaptureBatch(ConfigArtifact):
    id: str
    category: Category
    rows: tuple[CorpusRow, ...]
    sources: tuple[CorpusSource, ...]
    next_rule_id: str


def capture_batches(train: Corpus, category: Category | None = None) -> tuple[CaptureBatch, ...]:
    """Sort literal token sequences per category and take bounded, disjoint neighborhoods."""
    if train.split != "train" or not train.rows or len({row.id for row in train.rows}) != len(train.rows):
        raise EvaluationError(Diagnostic(code="train_only", message="Expected nonempty train with distinct row IDs"))
    batches: list[CaptureBatch] = []
    categories = {row.gold.category for row in train.rows if category is None or row.gold.category == category}
    for selected in sorted(categories):
        rows = sorted(
            (row for row in train.rows if row.gold.category == selected),
            key=lambda row: (tuple(_TOKENS.findall(row.command)), row.id),
        )
        sources = tuple(source for source in train.sources if source.category == selected)
        if not sources:
            raise EvaluationError(Diagnostic(code="train_sources", message=f"No pinned manifest sources for {selected}"))
        for number, start in enumerate(range(0, len(rows), _BATCH_SIZE), 1):
            key = f"{selected.lower()}.capture-{number:04d}"
            batches.append(CaptureBatch(
                id=key, category=selected, rows=tuple(rows[start:start + _BATCH_SIZE]),
                sources=sources, next_rule_id=f"{key}-01",
            ))
    return tuple(batches)


class CapturedRule(ConfigArtifact):
    package: Package
    singleton: EvaluationReceipt


class CaptureDecision(ConfigArtifact):
    batch_id: str
    row_ids: tuple[str, ...]
    justification: str | None
    rules: tuple[CapturedRule, ...]
    run: AgentRun


class Nomination(ConfigArtifact):
    left_id: str
    right_id: str
    left_labels: Labels
    right_labels: Labels
    semantic_cosine: float
    semantic: bool
    same_tuple: bool
    shared_hits: tuple[str, ...]
    left_only_hits: tuple[str, ...]
    right_only_hits: tuple[str, ...]

    def in_scope(self, ids: set[str]) -> bool:
        return self.left_id in ids and self.right_id in ids


class JudgeCandidate(ConfigArtifact):
    package: Package
    singleton: ReceiptView


class JudgePair(ConfigArtifact):
    left_id: str
    right_id: str
    same_tuple: bool
    semantic_cosine: float
    shared_hit_count: int
    left_only_hit_count: int
    right_only_hit_count: int
    shared_examples: tuple[str, ...]
    left_only_examples: tuple[str, ...]
    right_only_examples: tuple[str, ...]


class JudgeInput(ConfigArtifact):
    candidates: tuple[JudgeCandidate, ...]
    nominations: tuple[JudgePair, ...]
    new_id_prefixes: tuple[str, ...]
    sources: tuple[CorpusSource, ...]


class JudgeDecision(ConfigArtifact):
    batch_id: str
    candidate_ids: tuple[str, ...]
    nominations: tuple[Nomination, ...]
    run: AgentRun
    finalized: FinalizeReceipt | None


class DedupResult(ConfigArtifact):
    corpus_digest: Digest
    runtime_version: str
    similarity_revision: str = SIMILARITY_REVISION
    nominations: tuple[Nomination, ...]
    decisions: tuple[JudgeDecision, ...]
    remaining_nominations: tuple[Nomination, ...]
    packages: tuple[Package, ...]

    @property
    def complete(self) -> bool:
        return not self.remaining_nominations and all(item.finalized is not None for item in self.decisions)


def judge_group(pending: tuple[Nomination, ...]) -> tuple[set[str], tuple[Nomination, ...]]:
    first = pending[0]
    ids = {first.left_id, first.right_id}
    for pair in pending[1:]:
        edge = {pair.left_id, pair.right_id}
        if edge & ids and len(ids | edge) <= JUDGE_WIDTH:
            ids.update(edge)
    return ids, tuple(pair for pair in pending if pair.in_scope(ids))


def _concept_cosines(descriptions: tuple[str, ...]) -> NDArray[np.float64]:
    """Fit word/bigram TF-IDF, then cosine in a truncated corpus-local SVD.

    This is not a pretrained encoder: it models co-occurrence, not general paraphrase,
    and sparsity and low-rank projection can miss or invent associations. Nominate only.
    """
    documents: list[Counter[str]] = []
    for description in descriptions:
        words = tuple(word for word in _WORDS.findall(description.casefold()) if word not in _STOPWORDS)
        documents.append(Counter((*words, *(" ".join(pair) for pair in pairwise(words)))))
    vocabulary = tuple(sorted({term for document in documents for term in document}))
    if not vocabulary:
        return np.zeros((len(documents), len(documents)), dtype=np.float64)
    counts = np.array([[document[term] for term in vocabulary] for document in documents], dtype=np.float64)
    idf = 1 + np.log((1 + len(documents)) / (1 + np.count_nonzero(counts, axis=0)))
    features = np.log1p(counts) * idf
    feature_norms = np.sqrt(np.diag(features @ features.T))[:, np.newaxis]
    features /= np.where(feature_norms > 0, feature_norms, 1)
    left, singular, _ = np.linalg.svd(features, full_matrices=False)
    rank = min(24, max(2, ceil(sqrt(len(documents)))))
    while rank < len(singular) and np.isclose(singular[rank], singular[rank - 1], rtol=1e-9, atol=1e-12):
        rank += 1
    vectors = left[:, :rank] * singular[:rank]
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    vectors /= np.where(norms > 1e-12, norms, 1)
    return vectors @ vectors.T


def nominate_rules(captured: tuple[CapturedRule, ...]) -> tuple[Nomination, ...]:
    if len(captured) < 2:
        return ()
    ordered = sorted(captured, key=lambda entry: entry.package.rule.id)
    rules = tuple(entry.package.rule for entry in ordered)
    cosines = _concept_cosines(tuple(rule.rationale for rule in rules))
    hits = tuple(
        {row_id for hit in entry.singleton.hit_sets if hit.rule_id == rule.id for row_id in hit.row_ids}
        for entry, rule in zip(ordered, rules, strict=True)
    )
    nominations: list[Nomination] = []
    for left, right in combinations(range(len(rules)), 2):
        cosine = round(float(np.clip(cosines[left, right], -1, 1)), 8)
        shared = hits[left] & hits[right]
        semantic = cosine >= _MIN_COSINE
        if not (shared or semantic):
            continue
        left_rule, right_rule = rules[left], rules[right]
        nominations.append(Nomination(
            left_id=left_rule.id, right_id=right_rule.id, left_labels=left_rule.labels,
            right_labels=right_rule.labels, semantic_cosine=cosine, semantic=semantic,
            same_tuple=left_rule.labels == right_rule.labels,
            shared_hits=tuple(sorted(shared)), left_only_hits=tuple(sorted(hits[left] - hits[right])),
            right_only_hits=tuple(sorted(hits[right] - hits[left])),
        ))
    return tuple(sorted(
        nominations,
        key=lambda item: (-len(item.shared_hits), -item.semantic_cosine, item.left_id, item.right_id),
    ))
