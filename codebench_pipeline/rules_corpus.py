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
    sources_digest: Digest | None = None

    def sources_for(self, category: str) -> tuple[str, ...]:
        return tuple(source.url for source in self.sources if source.category == category)


def _train_sources(frame: DataFrame, categories: set[str]) -> tuple[CorpusSource, ...]:
    grouped = _SOURCES.validate_python(frame.attrs.get("train_sources"), strict=True)
    if not categories.issubset(grouped):
        raise IntegrityError("Train corpus categories are missing pinned source provenance")
    sources: list[CorpusSource] = []
    for category in sorted(categories):
        entries = grouped[category]
        if not entries or any(entry.category != category for entry in entries):
            raise IntegrityError(f"Invalid pinned train source provenance for {category}")
        sources.extend(_corpus_source(entry) for entry in entries)
    return tuple(sources)


def _corpus_source(entry: TrainSource) -> CorpusSource:
    return CorpusSource(
        category=entry.category,
        url=f"https://github.com/{entry.repository}/blob/{entry.commit}/{quote(entry.path)}",
        sha256=entry.sha256,
    )


def build_corpus(frame: DataFrame, *, split: Split, digest: Digest, category: Category | None = None) -> Corpus:
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
    if category is not None:
        if split != "train" or not rows or any(row.gold.category != category for row in rows):
            raise IntegrityError(f"Expected a nonempty train table containing only {category}")
        pinned = CategorySources.model_validate({"sources": frame.attrs.get("train_sources")}, strict=True)
        if (
            not pinned.sources or any(source.category != category for source in pinned.sources)
            or frame.attrs.get("train_sources_digest") != pinned.digest
        ):
            raise IntegrityError(f"Invalid pinned train source provenance for {category}")
        return Corpus(
            split=split, digest=digest, rows=tuple(rows), sources=tuple(map(_corpus_source, pinned.sources)),
            sources_digest=pinned.digest,
        )
    sources = _train_sources(frame, {row.gold.category for row in rows}) if split == "train" else ()
    return Corpus(split=split, digest=digest, rows=tuple(rows), sources=sources)
