from pathlib import Path
from typing import Self

from pydantic import ConfigDict, Field, RootModel, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.merge_config import SourceWeight
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import (
    DEFAULT_CAPTURE_WORKERS,
    DEFAULT_MAXIMUM_FILE_BYTES,
    REPOSITORY_USE,
    CaptureWorkers,
    MaximumFileBytes,
    RepositoryBranch,
    RepositoryConfig,
    RepositoryId,
    RepositoryLicense,
    RepositoryModel,
    RepositoryOrigin,
    RepositoryOriginIdentity,
    RepositoryOriginScope,
    RepositoryScope,
    RepositorySettings,
    RightsFile,
    repository_origin_matches,
    selector_format,
)
from codebench_pipeline.repository_etl import (
    RepositoryParser,
    RepositoryParserId,
    repository_parser_transform,
    reviewed_parser,
)
from codebench_pipeline.source_transform_contract import TRANSFORM_OPTIONS, SourceTransformConfig
from codebench_pipeline.storage import IntegrityError


def check_origin_identities(origins: tuple[RepositoryOriginIdentity, ...]) -> None:
    if len({origin.origin_id for origin in origins}) != len(origins):
        raise PydanticCustomError("repository_origin_ids", "Repository origin IDs must be unique")
    if len({origin.repository.casefold() for origin in origins}) != len(origins):
        raise PydanticCustomError("repository_origins", "Repository origins must be independent")


class RepositoryRights(RepositoryModel):
    license_id: RepositoryLicense
    license_file: RightsFile
    notice_files: tuple[RightsFile, ...] = ()
    attribution: str = Field(min_length=1)


class DeclaredRepositoryOrigin(RepositoryOriginIdentity):
    file_selector: RepositoryScope
    rights: RepositoryRights
    parser: RepositoryParser

    @model_validator(mode="after")
    def check_origin(self) -> Self:
        self.check_scope_paths(self.file_selector, (self.rights.license_file, *self.rights.notice_files))
        if self.parser.sha256 != reviewed_parser(self.parser.parser_id).sha256:
            raise PydanticCustomError("parser_pin", "Parser ID and SHA-256 are not a reviewed pair")
        return self

    def runtime(self) -> RepositoryOrigin:
        return RepositoryOrigin(
            origin_id=self.origin_id,
            repository=self.repository,
            commit=self.commit,
            tree_digest=self.tree_digest,
            scope=self.file_selector,
            included_paths=self.included_paths,
            excluded_paths=self.excluded_paths,
            license_id=self.rights.license_id,
            license=self.rights.license_file,
            notices=self.rights.notice_files,
            attribution=self.rights.attribution,
            source_type=self.source_type,
            provenance_type=self.provenance_type,
            format=selector_format(self.file_selector),
            use=REPOSITORY_USE,
            transform=repository_parser_transform(self.parser),
        )


class _RepositoryBranchDeclaration[Origin: DeclaredRepositoryOrigin](RepositoryModel):
    refresh_revision: RepositoryId
    origins: tuple[Origin, ...] = Field(min_length=1)
    maximum_file_bytes: MaximumFileBytes = Field(default=DEFAULT_MAXIMUM_FILE_BYTES, strict=True)
    capture_workers: CaptureWorkers = Field(default=DEFAULT_CAPTURE_WORKERS, strict=True)

    @model_validator(mode="after")
    def check_origins(self) -> Self:
        check_origin_identities(self.origins)
        return self

    def runtime(self, branch: RepositoryBranch) -> "RepositoryBranchRuntime":
        return RepositoryBranchRuntime(
            branch=branch,
            declaration_revision=self.refresh_revision,
            selected_origins=tuple(origin.runtime() for origin in self.origins),
            maximum_file_bytes=self.maximum_file_bytes,
            workers=self.capture_workers,
        )


class _HistoricalWeightedOrigin(DeclaredRepositoryOrigin):
    trust_weight: SourceWeight | None = Field(default=None, exclude_if=lambda weight: weight is None)


class RepositoryBranchDeclaration(_RepositoryBranchDeclaration[DeclaredRepositoryOrigin]):
    """The authoring contract for one repository branch."""


class HistoricalWeightedRepositoryBranch(_RepositoryBranchDeclaration[_HistoricalWeightedOrigin]):
    """Decode a pre-merge repository declaration for historical inspection."""


class _HistoricalRepositoryParser(RepositoryModel):
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


class _HistoricalRepositoryOrigin(RepositoryOriginScope):
    transform: _HistoricalRepositoryParser

    def runtime(self) -> RepositoryOrigin:
        return RepositoryOrigin(
            origin_id=self.origin_id,
            repository=self.repository,
            commit=self.commit,
            tree_digest=self.tree_digest,
            scope=self.scope,
            included_paths=self.included_paths,
            excluded_paths=self.excluded_paths,
            license_id=self.license_id,
            license=self.license,
            notices=self.notices,
            attribution=self.attribution,
            source_type=self.source_type,
            provenance_type=self.provenance_type,
            format=self.format,
            use=self.use,
            transform=self.transform.runtime(),
        )


class _HistoricalRepositoryBranch(RepositorySettings):
    """Read earlier path-free registrations without changing their recorded declaration."""

    revision: RepositoryId
    origins: tuple[_HistoricalRepositoryOrigin, ...] = Field(min_length=1)
    source_weights: dict[RepositoryId, SourceWeight] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_origins(self) -> Self:
        check_origin_identities(self.origins)
        if self.source_weights.keys() - {origin.origin_id for origin in self.origins}:
            raise PydanticCustomError("source_weights", "Source weights must name origins in this branch")
        return self

    def runtime(self, branch: RepositoryBranch) -> "RepositoryBranchRuntime":
        return RepositoryBranchRuntime(
            branch=branch,
            declaration_revision=self.revision,
            selected_origins=tuple(origin.runtime() for origin in self.origins),
            maximum_file_bytes=self.maximum_file_bytes,
            workers=self.workers,
        )


class RepositoryBranchConfig(RootModel[RepositoryBranchDeclaration | _HistoricalRepositoryBranch]):
    """Decode declarations stored in registrations, including the prior path-free schema."""

    model_config = ConfigDict(frozen=True)

    def runtime(self, branch: RepositoryBranch) -> "RepositoryBranchRuntime":
        return self.root.runtime(branch)


class RepositoryBranchRuntime(RepositorySettings):
    """Resolved acquisition input for one branch, with deployment-owned parser pins."""

    branch: RepositoryBranch
    declaration_revision: RepositoryId
    selected_origins: tuple[RepositoryOrigin, ...]

    @model_validator(mode="after")
    def check_branch(self) -> Self:
        if any(not repository_origin_matches(origin, self.branch) for origin in self.selected_origins):
            raise IntegrityError(
                f"Repository origin provenance or scope differs from the {self.branch} branch"
            )
        return self

    def origins(self, branch: RepositoryBranch) -> tuple[RepositoryOrigin, ...]:
        if branch != self.branch:
            raise IntegrityError(f"Repository declaration belongs to the {self.branch} branch")
        return self.selected_origins

    def branch_digest(self, branch: RepositoryBranch) -> str:
        return self.digest_for_origins(branch, self.origins(branch))


type RepositoryAcquisitionConfig = RepositoryBranchRuntime | RepositoryConfig


def load_repository_branch(path: Path) -> RepositoryBranchDeclaration:
    return RepositoryBranchDeclaration.model_validate_json(path.read_bytes())
