import json
from typing import Final, Self

from pydantic import ConfigDict, Field, JsonValue, model_validator

from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.keenable_policy import KeenableDeclaration
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_branch_config import RepositoryAcquisitionConfig, RepositoryBranchConfig
from codebench_pipeline.repository_config import (
    DEFAULT_SOURCE_WEIGHTS,
    RepositoryBranch,
    RepositoryConfig,
    SourceWeight,
)
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import TypedConfig


class SourceDeclaration(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_list: str | None = Field(default=None, exclude_if=lambda path: path is None)
    declaration: dict[str, JsonValue]
    digest: Digest

    @model_validator(mode="after")
    def verify_digest(self) -> "SourceDeclaration":
        if self.digest != fingerprint(self.declaration):
            raise ValueError("Source declaration digest does not match its contents")
        return self

    def repository_config(self, branch: RepositoryBranch) -> RepositoryAcquisitionConfig:
        if "origins" in self.declaration:
            return RepositoryBranchConfig.model_validate(self.declaration).runtime(branch)
        return RepositoryConfig.model_validate(self.declaration)

    def keenable_config(self) -> KeenableDeclaration:
        return KeenableDeclaration.model_validate_json(json.dumps(self.declaration))

    def require_current(self, branch: Branch) -> None:
        if self.source_list is not None or (branch != "keenable" and "origins" not in self.declaration):
            raise IntegrityError(
                "Historical declared collection contains source-list paths; re-register the path-free dataset configuration"
            )


class DeclaredCollection(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    existing_datasets: SourceDeclaration
    documentation_crawl: SourceDeclaration
    keenable_use_cases: SourceDeclaration
    merge_keenable_weight: SourceWeight | None = Field(default=None, exclude_if=lambda weight: weight is None)

    @property
    def keenable_source_weight(self) -> float:
        if self.merge_keenable_weight is not None:
            return self.merge_keenable_weight
        dataset = self.repository_config("dataset")
        docs = self.repository_config("documentation")
        dataset_weight = (
            dataset.keenable_source_weight
            if isinstance(dataset, RepositoryConfig)
            else DEFAULT_SOURCE_WEIGHTS["keenable"]
        )
        if isinstance(docs, RepositoryConfig) and docs.keenable_source_weight != dataset_weight:
            raise IntegrityError("Collection declarations disagree on the Keenable source weight")
        return dataset_weight

    @model_validator(mode="after")
    def verify_collection(self) -> Self:
        dataset = self.repository_config("dataset")
        docs = self.repository_config("documentation")
        dataset_origins = dataset.origins("dataset")
        docs_origins = docs.origins("documentation")
        origins = (*dataset_origins, *docs_origins)
        if len({origin.origin_id for origin in origins}) != len(origins):
            raise IntegrityError("Repository origin IDs must be unique across branches")
        if len({origin.repository.casefold() for origin in origins}) != len(origins):
            raise IntegrityError("Repository origins must be independent across branches")
        dataset_weights = [dataset.source_weight("dataset", origin.origin_id) for origin in dataset_origins]
        docs_weights = [docs.source_weight("documentation", origin.origin_id) for origin in docs_origins]
        if not self.keenable_source_weight < min(dataset_weights) <= max(dataset_weights) < min(docs_weights):
            raise IntegrityError("Source weights must keep documentation above datasets above Keenable")
        return self

    def for_branch(self, branch: Branch) -> SourceDeclaration:
        return {
            "dataset": self.existing_datasets,
            "documentation": self.documentation_crawl,
            "keenable": self.keenable_use_cases,
        }[branch]

    def require_current_declarations(self) -> None:
        self.existing_datasets.require_current("dataset")
        self.documentation_crawl.require_current("documentation")
        self.keenable_use_cases.require_current("keenable")

    def repository_config(self, branch: RepositoryBranch) -> RepositoryAcquisitionConfig:
        return self.for_branch(branch).repository_config(branch)

    @property
    def keenable_declaration(self) -> KeenableDeclaration:
        return self.keenable_use_cases.keenable_config()


class DeclaredSourceStageConfig(TypedConfig):
    branch: Branch
    source: SourceDeclaration
    planner_binding: Digest | None = None

    @model_validator(mode="after")
    def require_current_source(self) -> Self:
        self.source.require_current(self.branch)
        if self.branch == "keenable":
            self.source.keenable_config()
        else:
            self.source.repository_config(self.branch)
        return self

    @property
    def repository_config(self) -> RepositoryAcquisitionConfig:
        if self.branch == "keenable":
            raise IntegrityError("Keenable does not have repository acquisition configuration")
        return self.source.repository_config(self.branch)

    @property
    def declaration_digest(self) -> str:
        if self.branch == "keenable":
            if self.planner_binding is not None:
                return fingerprint([self.source.digest, self.planner_binding])
            return self.source.digest
        return self.repository_config.branch_digest(self.branch)


class CollectionMergeConfig(TypedConfig):
    keenable_source_weight: SourceWeight = DEFAULT_SOURCE_WEIGHTS["keenable"]


CAT3_DATASET_ID: Final = "cloud_infrastructure_commands_grounded_v1"
