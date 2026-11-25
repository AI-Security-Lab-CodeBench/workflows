from hashlib import sha256
from urllib.parse import quote

from pandas import DataFrame
from pydantic import TypeAdapter

from codebench_pipeline.models import Digest
from codebench_pipeline.rules_contracts import Category, Gold, Labels, ModifyGold, Split
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_config import CategorySources, TrainSource
from codebench_pipeline.typed_config import ConfigArtifact

_SOURCES = TypeAdapter(dict[str, list[TrainSource]])
_LABELS = TypeAdapter(tuple[str, str, str, str, str])


class CorpusRow(ConfigArtifact):
    id: str
    command: str
    gold: Gold

    @property
    def command_sha256(self) -> str:
        return sha256(self.command.encode()).hexdigest()


class CorpusSource(ConfigArtifact):
    category: Category
    url: str
    sha256: Digest


class Corpus(ConfigArtifact):
    split: Split
    digest: Digest
    rows: tuple[CorpusRow, ...]
    sources: tuple[CorpusSource, ...] = ()
    category_sources_digests: dict[Category, Digest] = {}

    def sources_for(self, category: str) -> tuple[str, ...]:
        return tuple(source.url for source in self.sources if source.category == category)


def _merged_sources(frame: DataFrame, categories: set[str], split: Split) -> dict[str, CategorySources]:
    grouped = _SOURCES.validate_python(frame.attrs.get(f"{split}_sources"), strict=True)
    if not categories.issubset(grouped):
        raise IntegrityError(f"{split} corpus categories are missing pinned source provenance")
    for category, entries in grouped.items():
        if not entries or any(entry.category != category for entry in entries):
            raise IntegrityError(f"Invalid pinned {split} source provenance for {category}")
    return {category: CategorySources(sources=entries) for category, entries in grouped.items()}


def _corpus_source(entry: TrainSource) -> CorpusSource:
    return CorpusSource(
        category=entry.category,
        url=f"https://github.com/{entry.repository}/blob/{entry.commit}/{quote(entry.path)}",
        sha256=entry.sha256,
    )


def build_corpus(frame: DataFrame, *, split: Split, digest: Digest) -> Corpus:
    """Build split identities by immutable snapshot and original source order, including duplicates."""
    rows: list[CorpusRow] = []
    for index, raw in enumerate(
        frame.loc[:, ["command", "category", "risk", "severity", "class"]].itertuples(index=False, name=None)
    ):
        command, row_category, risk, severity, action = _LABELS.validate_python(raw, strict=True)
        action = action.strip().lower()
        model = ModifyGold if split == "test" and action == "modify" else Labels
        gold = model.model_validate(
            {
                "category": row_category.split(maxsplit=1)[0] if row_category.strip() else "",
                "risk": risk.split(maxsplit=1)[0] if risk.strip() else "",
                "severity": severity.strip().lower(),
                "action": action,
            }
        )
        rows.append(CorpusRow(id=f"{split}:{digest}:{index}", command=command, gold=gold))
    grouped = _merged_sources(frame, {row.gold.category for row in rows}, split)
    sources = tuple(
        _corpus_source(entry) for category in sorted(grouped) for entry in grouped[category].sources
    ) if split == "train" else ()
    return Corpus(
        split=split, digest=digest, rows=tuple(rows), sources=sources,
        category_sources_digests={key: value.digest for key, value in grouped.items()},
    )
