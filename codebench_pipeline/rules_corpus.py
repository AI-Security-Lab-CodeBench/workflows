from hashlib import sha256
from urllib.parse import quote

from pandas import DataFrame
from pydantic import TypeAdapter

from codebench_pipeline.models import Digest
from codebench_pipeline.rules_contracts import Category, Labels, Split
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_config import TrainSource
from codebench_pipeline.typed_config import ConfigArtifact

_SOURCES = TypeAdapter(dict[str, list[TrainSource]])
_LABELS = TypeAdapter(tuple[str, str, str, str, str])


class CorpusRow(ConfigArtifact):
    id: str
    command: str
    gold: Labels

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
        sources.extend(
            CorpusSource(
                category=entry.category,
                url=f"https://github.com/{entry.repository}/blob/{entry.commit}/{quote(entry.path)}",
                sha256=entry.sha256,
            )
            for entry in entries
        )
    return tuple(sources)


def build_corpus(frame: DataFrame, *, split: Split, digest: str) -> Corpus:
    """Build split identities by immutable snapshot and original source order, including duplicates."""
    rows: list[CorpusRow] = []
    for index, raw in enumerate(
        frame.loc[:, ["command", "category", "risk", "severity", "class"]].itertuples(index=False, name=None)
    ):
        command, category, risk, severity, action = _LABELS.validate_python(raw, strict=True)
        gold = Labels.model_validate(
            {
                "category": category.split(maxsplit=1)[0] if category.strip() else "",
                "risk": risk.split(maxsplit=1)[0] if risk.strip() else "",
                "severity": severity.strip().lower(),
                "action": action.strip().lower(),
            }
        )
        rows.append(CorpusRow(id=f"{split}:{digest}:{index}", command=command, gold=gold))
    sources = _train_sources(frame, {row.gold.category for row in rows}) if split == "train" else ()
    return Corpus(split=split, digest=digest, rows=tuple(rows), sources=sources)
