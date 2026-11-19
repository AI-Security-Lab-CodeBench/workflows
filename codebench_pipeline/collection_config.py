from typing import Final

from pydantic import ConfigDict, JsonValue, model_validator

from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import DEFAULT_SOURCE_WEIGHTS, SourceWeight
from codebench_pipeline.storage import fingerprint
from codebench_pipeline.typed_config import TypedConfig


class SourceDeclaration(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_list: str
    declaration: dict[str, JsonValue]
    digest: Digest

    @model_validator(mode="after")
    def verify_digest(self) -> "SourceDeclaration":
        if self.digest != fingerprint(self.declaration):
            raise ValueError("Source declaration digest does not match its contents")
        return self


class DeclaredCollection(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    existing_datasets: SourceDeclaration
    documentation_crawl: SourceDeclaration
    keenable_use_cases: SourceDeclaration

    def for_branch(self, branch: Branch) -> SourceDeclaration:
        return {
            "dataset": self.existing_datasets,
            "documentation": self.documentation_crawl,
            "keenable": self.keenable_use_cases,
        }[branch]


class DeclaredSourceStageConfig(TypedConfig):
    branch: Branch
    source: SourceDeclaration
    planner_binding: Digest | None = None

    @property
    def declaration_digest(self) -> str:
        if self.branch == "keenable":
            if self.planner_binding is not None:
                return fingerprint([self.source.digest, self.planner_binding])
            return self.source.digest
        from codebench_pipeline.repository_config import RepositoryConfig

        return RepositoryConfig.model_validate(self.source.declaration).branch_digest(self.branch)


class CollectionMergeConfig(TypedConfig):
    keenable_source_weight: SourceWeight = DEFAULT_SOURCE_WEIGHTS["keenable"]


CAT3_DATASET_ID: Final = "cloud_infrastructure_commands_grounded_v1"
