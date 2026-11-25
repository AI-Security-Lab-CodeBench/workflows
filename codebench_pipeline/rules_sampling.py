from collections import defaultdict
from collections.abc import Iterator
from fractions import Fraction
from heapq import merge
from itertools import islice
from math import ceil
from typing import Final, assert_never

from codebench_pipeline.models import Digest
from codebench_pipeline.rules_contracts import Category
from codebench_pipeline.rules_corpus import Corpus, CorpusRow
from codebench_pipeline.rules_registration_config import SampleCount, SamplePercent, SampleSelection, SplitPin
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import ConfigArtifact

SAMPLING_VERSION: Final = "stratified-v2"


class RulesSample(ConfigArtifact):
    version: str
    selection: SampleSelection
    required_categories: tuple[Category, ...]
    source_digest: Digest
    corpus: Corpus

    @property
    def pin(self) -> SplitPin:
        return SplitPin(digest=self.corpus.digest, row_count=len(self.corpus.rows))

    def for_capture(self, category: Category) -> Corpus:
        rows = tuple(row for row in self.corpus.rows if row.gold.category == category)
        if self.corpus.split != "train" or not rows:
            raise IntegrityError(f"The train sample has no rows for capture category {category}")
        return Corpus(
            split="train", digest=self.corpus.digest, rows=rows,
            sources=tuple(source for source in self.corpus.sources if source.category == category),
        )


def _interleave[Key: (str, tuple[str, str, str])](
    groups: dict[Key, tuple[int, Iterator[CorpusRow]]], required: frozenset[Key] = frozenset(),
) -> Iterator[CorpusRow]:
    """Merge evenly spaced group slots; a singleton's slot lands halfway through."""
    def ranked(key: Key, size: int, rows: Iterator[CorpusRow]) -> Iterator[tuple[Fraction, Key, CorpusRow]]:
        for index, row in enumerate(rows, start=int(key in required)):
            yield Fraction(2 * index + 1, 2 * size), key, row

    for key in sorted(required):
        yield next(groups[key][1])
    streams = (ranked(key, size, rows) for key, (size, rows) in groups.items())
    for _, _, row in merge(*streams, key=lambda slot: (slot[0], slot[1])):
        yield row


def _stratum_order(rows: list[CorpusRow]) -> Iterator[CorpusRow]:
    strata: dict[tuple[str, str, str], list[CorpusRow]] = defaultdict(list)
    for row in rows:
        strata[row.gold.risk, row.gold.severity, row.gold.action].append(row)
    yield from _interleave({
        key: (len(items), iter(sorted(items, key=lambda row: (fingerprint(row.id), row.id))))
        for key, items in strata.items()
    })


def _stratified_order(rows: tuple[CorpusRow, ...], required: tuple[Category, ...]) -> Iterator[CorpusRow]:
    categories: dict[Category, list[CorpusRow]] = defaultdict(list)
    for row in rows:
        categories[row.gold.category].append(row)
    if not set(required) <= categories.keys():
        raise IntegrityError("Train sample cannot cover all registered capture categories; check rows or raise the budget")
    yield from _interleave(
        {key: (len(items), _stratum_order(items)) for key, items in categories.items()}, frozenset(required),
    )


def _target(selection: SampleSelection, population: int) -> int:
    match selection:
        case SampleCount(count=count):
            return min(count, population)
        case SamplePercent(percent=percent):
            return ceil(population * Fraction(str(percent)) / 100)
    assert_never(selection)


def sample_corpus(
    source: Corpus, selection: SampleSelection, *, required_categories: tuple[Category, ...] = (),
) -> RulesSample:
    if not source.rows or (source.split != "train" and required_categories):
        raise IntegrityError("Sampling requires a nonempty split; capture categories apply only to train")
    required_categories = tuple(sorted(set(required_categories)))
    target = _target(selection, len(source.rows))
    if target < len(required_categories):
        raise IntegrityError("Train sample cannot cover all registered capture categories; raise the budget")
    rows = tuple(islice(_stratified_order(source.rows, required_categories), target))
    digest = fingerprint([
        SAMPLING_VERSION, source.split, source.digest, selection.model_dump_json(),
        list(required_categories), [row.id for row in rows],
    ])
    return RulesSample(
        version=SAMPLING_VERSION, selection=selection, required_categories=required_categories,
        source_digest=source.digest,
        corpus=source.model_copy(update={"rows": rows, "digest": digest}),
    )
