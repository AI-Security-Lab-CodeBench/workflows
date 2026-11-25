from collections import defaultdict
from pathlib import PureWindowsPath
from typing import Annotated, Final, Literal, Self, assert_never

from pydantic import AfterValidator, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import Commit, check_path
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import ConfigArtifact

TrainGroupColumn = Literal["risk", "severity", "class"]
SplitCategory = Annotated[str, Field(pattern=r"^CAT-[1-9][0-9]*$")]
_DEFAULT_GROUP_BY: Final[tuple[TrainGroupColumn, ...]] = ("risk", "severity", "class")


def _distinct_categories(value: list[SplitCategory]) -> list[SplitCategory]:
    if len(set(value)) != len(value):
        raise PydanticCustomError("train_categories", "Train categories must be distinct")
    return value


_CategoryList = Annotated[list[SplitCategory], Field(min_length=1), AfterValidator(_distinct_categories)]


class _TrainSourcePin(ConfigArtifact):
    repository: Annotated[
        str,
        Field(
            pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?/"
            r"[A-Za-z0-9_.-]*[A-Za-z0-9_-][A-Za-z0-9_.-]*$"
        ),
    ]
    branch: str = Field(min_length=1)
    commit: Commit
    path: str

    @field_validator("branch")
    @classmethod
    def branch_ref(cls, value: str) -> str:
        if (
            value == "@"
            or value.startswith("-")
            or value.endswith(".")
            or ".." in value
            or "@{" in value
            or any(
                char.isspace() or ord(char) < 32 or ord(char) == 127 or char in "~^:?*[\\" for char in value
            )
            or any(not part or part.startswith(".") or part.endswith(".lock") for part in value.split("/"))
        ):
            raise PydanticCustomError("train_branch", "Expected a Git branch name")
        return value

    @field_validator("path")
    @classmethod
    def repository_path(cls, value: str) -> str:
        try:
            check_path(value)
        except IntegrityError as error:
            raise PydanticCustomError("train_path", "Expected a safe relative repository path") from error
        if PureWindowsPath(value).drive or ".git" in value.casefold().split("/"):
            raise PydanticCustomError("train_path", "Expected a safe relative repository path")
        return value


class TrainSource(_TrainSourcePin):
    category: SplitCategory
    sha256: Digest
    categories: _CategoryList | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def partition_belongs_to_source(self) -> Self:
        if self.categories is not None and self.category not in self.categories:
            raise PydanticCustomError("train_categories", "Partition category must belong to the source")
        return self


class MixedTrainSource(_TrainSourcePin):
    sha256: Digest
    categories: _CategoryList


class CategorySources(ConfigArtifact):
    sources: list[TrainSource]

    @property
    def digest(self) -> str:
        return fingerprint(self.model_dump_json())


class TrainConfig(ConfigArtifact):
    sources: list[TrainSource | MixedTrainSource]
    group_by: list[TrainGroupColumn] = Field(default_factory=lambda: list(_DEFAULT_GROUP_BY), min_length=1)

    @field_validator("sources")
    @classmethod
    def distinct_sources(cls, value: list[TrainSource | MixedTrainSource]) -> list[TrainSource | MixedTrainSource]:
        if any(isinstance(source, TrainSource) and source.categories is not None for source in value):
            raise PydanticCustomError("train_categories", "Declare only one of category or categories")
        identities = {(source.repository.casefold(), source.commit, source.path) for source in value}
        if len(identities) != len(value):
            raise PydanticCustomError("train_sources", "Train sources must name distinct pinned files")
        return value

    @field_validator("group_by")
    @classmethod
    def distinct_group_columns(cls, value: list[TrainGroupColumn]) -> list[TrainGroupColumn]:
        if len(set(value)) != len(value):
            raise PydanticCustomError("train_group_by", "Train grouping columns must be distinct")
        return value

    def sources_by_category(self) -> dict[str, CategorySources]:
        groups: defaultdict[str, list[TrainSource]] = defaultdict(list)
        for source in self.sources:
            match source:
                case TrainSource():
                    groups[source.category].append(source)
                    continue
                case MixedTrainSource():
                    for category in source.categories:
                        groups[category].append(
                            TrainSource(
                                repository=source.repository,
                                branch=source.branch,
                                commit=source.commit,
                                path=source.path,
                                sha256=source.sha256,
                                category=category,
                                categories=source.categories,
                            )
                        )
                    continue
            assert_never(source)
        return {category: CategorySources(sources=sources) for category, sources in groups.items()}


class SplitSections(ConfigArtifact):
    train: TrainConfig
    test: TrainConfig


class CountSettings(ConfigArtifact):
    group_by: list[TrainGroupColumn]
    sources_digest: Digest


class MergeSettings(ConfigArtifact):
    category_digests: dict[SplitCategory, Digest]


class SplitMarginalCount(ConfigArtifact):
    category: SplitCategory
    label: TrainGroupColumn
    value: str
    count: Annotated[int, Field(gt=0)]


class SplitStats(ConfigArtifact):
    total_rows: Annotated[int, Field(ge=0)]
    marginal_counts: tuple[SplitMarginalCount, ...]
