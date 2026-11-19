from pathlib import Path
from typing import Self

from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import (
    RepositoryBranch,
    RepositoryConfig,
    RepositoryId,
    RepositoryModel,
    RepositoryOrigin,
    RepositoryOriginScope,
    RepositorySettings,
    repository_origin_matches,
)
from codebench_pipeline.repository_etl import RepositoryParserId, reviewed_parser
from codebench_pipeline.source_transform_contract import TRANSFORM_OPTIONS, SourceTransformConfig
from codebench_pipeline.storage import IntegrityError


class RepositoryParser(RepositoryModel):
    parser_id: RepositoryParserId
    sha256: Digest
    options_json: str = "{}"
    batch_size: int = Field(default=128, ge=1, le=10000, strict=True)

    @model_validator(mode="after")
    def check_options(self) -> Self:
        if self.sha256 != reviewed_parser(self.parser_id).sha256:
            raise PydanticCustomError("parser_pin", "Parser ID and SHA-256 are not a reviewed pair")
        TRANSFORM_OPTIONS.validate_json(self.options_json, strict=True)
        return self

    def runtime(self) -> SourceTransformConfig:
        return reviewed_parser(self.parser_id).model_copy(
            update={"options_json": self.options_json, "batch_size": self.batch_size}
        )


class DeclaredRepositoryOrigin(RepositoryOriginScope):
    transform: RepositoryParser


class RepositoryBranchConfig(RepositorySettings):
    """A source declaration containing only inputs owned by one repository branch."""

    revision: RepositoryId
    origins: tuple[DeclaredRepositoryOrigin, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def check_origins(self) -> Self:
        identities = {origin.origin_id for origin in self.origins}
        if len(identities) != len(self.origins):
            raise PydanticCustomError("repository_origin_ids", "Repository origin IDs must be unique")
        if len({origin.repository.casefold() for origin in self.origins}) != len(self.origins):
            raise PydanticCustomError("repository_origins", "Repository origins must be independent")
        if self.source_weights.keys() - identities:
            raise PydanticCustomError("source_weights", "Source weights must name origins in this branch")
        return self

    def runtime(self, branch: RepositoryBranch) -> "RepositoryBranchRuntime":
        """Resolve parser IDs to deployment pins without opening or importing parser files."""
        if any(not repository_origin_matches(origin, branch) for origin in self.origins):
            raise IntegrityError(f"Repository origin provenance or scope differs from the {branch} branch")
        return RepositoryBranchRuntime(
            branch=branch,
            declaration_revision=self.revision,
            selected_origins=tuple(
                RepositoryOrigin(
                    **origin.model_dump(exclude={"transform"}),
                    transform=origin.transform.runtime(),
                )
                for origin in self.origins
            ),
            maximum_file_bytes=self.maximum_file_bytes,
            workers=self.workers,
            source_weights=self.source_weights,
        )


class RepositoryBranchRuntime(RepositorySettings):
    """Resolved acquisition input for one branch, with deployment-owned parser pins."""

    branch: RepositoryBranch
    declaration_revision: RepositoryId
    selected_origins: tuple[RepositoryOrigin, ...]

    def origins(self, branch: RepositoryBranch) -> tuple[RepositoryOrigin, ...]:
        if branch != self.branch:
            raise IntegrityError(f"Repository declaration belongs to the {self.branch} branch")
        return self.selected_origins

    def branch_digest(self, branch: RepositoryBranch) -> str:
        return self.digest_for_origins(branch, self.origins(branch))


type RepositoryAcquisitionConfig = RepositoryBranchRuntime | RepositoryConfig


def load_repository_branch(path: Path) -> RepositoryBranchConfig:
    return RepositoryBranchConfig.model_validate_json(path.read_bytes())
