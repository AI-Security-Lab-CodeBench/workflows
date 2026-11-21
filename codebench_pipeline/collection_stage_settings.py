from collections.abc import Callable
from dataclasses import dataclass
from typing import Self

from pydantic import BaseModel

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.dataset_branches import Branch, stage_name
from codebench_pipeline.discovery_config import DiscoveryInference
from codebench_pipeline.keenable_inspection import InspectionTaxonomy
from codebench_pipeline.keenable_policy import KeenableSnippetSettings
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_branch_config import RepositoryAcquisitionConfig
from codebench_pipeline.repository_config import (
    CaptureWorkers,
    Commit,
    MaximumFileBytes,
    RepositoryBranch,
    RepositoryId,
    RepositoryName,
    RepositoryOriginScope,
)
from codebench_pipeline.repository_etl import RepositoryParserPin
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import ConfigArtifact


class RepositoryStageBinding(ConfigArtifact):
    branch: RepositoryBranch
    declaration_digest: Digest

    def require_branch(self, branch: RepositoryBranch) -> None:
        if self.branch != branch:
            raise IntegrityError(f"Repository declaration belongs to the {self.branch} branch")


class RepositoryCatalogSettings(RepositoryStageBinding):
    origins: tuple[RepositoryOriginScope, ...]
    maximum_file_bytes: MaximumFileBytes

    @classmethod
    def project(cls, config: RepositoryAcquisitionConfig, branch: RepositoryBranch) -> Self:
        return cls(
            branch=branch,
            declaration_digest=config.branch_digest(branch),
            origins=tuple(
                RepositoryOriginScope.model_validate(origin.model_dump(exclude={"transform"}), strict=True)
                for origin in config.origins(branch)
            ),
            maximum_file_bytes=config.maximum_file_bytes,
        )


class RepositoryCaptureOrigin(ConfigArtifact):
    origin_id: RepositoryId
    repository: RepositoryName
    commit: Commit

    @classmethod
    def from_scope(cls, origin: RepositoryOriginScope) -> Self:
        return cls(origin_id=origin.origin_id, repository=origin.repository, commit=origin.commit)


class RepositoryCaptureSettings(RepositoryStageBinding):
    origins: tuple[RepositoryCaptureOrigin, ...]
    maximum_file_bytes: MaximumFileBytes
    workers: CaptureWorkers

    @classmethod
    def project(cls, config: RepositoryAcquisitionConfig, branch: RepositoryBranch) -> Self:
        return cls(
            branch=branch,
            declaration_digest=config.branch_digest(branch),
            origins=tuple(RepositoryCaptureOrigin.from_scope(origin) for origin in config.origins(branch)),
            maximum_file_bytes=config.maximum_file_bytes,
            workers=config.workers,
        )


class RepositoryExtractionOrigin(ConfigArtifact):
    origin_id: RepositoryId
    parser: RepositoryParserPin


class RepositoryExtractionSettings(RepositoryStageBinding):
    origins: tuple[RepositoryExtractionOrigin, ...]

    @classmethod
    def project(cls, config: RepositoryAcquisitionConfig, branch: RepositoryBranch) -> Self:
        return cls(
            branch=branch,
            declaration_digest=config.branch_digest(branch),
            origins=tuple(
                RepositoryExtractionOrigin(
                    origin_id=origin.origin_id,
                    parser=RepositoryParserPin.from_transform(origin.transform),
                )
                for origin in config.origins(branch)
            ),
        )


class DocumentationCaptureSettings(RepositoryCatalogSettings):
    workers: CaptureWorkers

    @classmethod
    def project(cls, config: RepositoryAcquisitionConfig, branch: RepositoryBranch) -> Self:
        catalog = RepositoryCatalogSettings.project(config, branch)
        return cls(
            branch=catalog.branch,
            declaration_digest=catalog.declaration_digest,
            origins=catalog.origins,
            maximum_file_bytes=catalog.maximum_file_bytes,
            workers=config.workers,
        )

    def capture(self) -> RepositoryCaptureSettings:
        return RepositoryCaptureSettings(
            branch=self.branch,
            declaration_digest=self.declaration_digest,
            origins=tuple(RepositoryCaptureOrigin.from_scope(origin) for origin in self.origins),
            maximum_file_bytes=self.maximum_file_bytes,
            workers=self.workers,
        )


class KeenableCandidateSettings(ConfigArtifact):
    declaration_digest: Digest
    search: KeenableSnippetSettings
    taxonomy: tuple[InspectionTaxonomy, ...] = ()
    inference: DiscoveryInference | None = None

    @classmethod
    def project(
        cls,
        source: DeclaredSourceStageConfig,
        rules: CategoryRules | None = None,
        inference: DiscoveryInference | None = None,
    ) -> Self:
        search = source.source.keenable_search()
        taxonomy: tuple[InspectionTaxonomy, ...] = ()
        if search.authored_seeds:
            if rules is None or inference is None:
                raise IntegrityError(
                    "Authored inspection seeds require taxonomy and mapped discovery inference"
                )
            taxonomy = InspectionTaxonomy.project(search.authored_seeds, rules)
        return cls(
            declaration_digest=source.declaration_digest,
            search=search,
            taxonomy=taxonomy,
            inference=inference if taxonomy else None,
        )


type CollectionOperationSettings = (
    RepositoryCatalogSettings
    | RepositoryCaptureSettings
    | RepositoryExtractionSettings
    | DocumentationCaptureSettings
    | KeenableCandidateSettings
)


@dataclass(frozen=True, slots=True)
class CollectionOperation:
    branch: Branch
    stage: str
    contract: type[CollectionOperationSettings]
    project: Callable[[DeclaredSourceStageConfig], CollectionOperationSettings]

    @property
    def name(self) -> str:
        return stage_name(self.stage, self.branch)

    @property
    def mapping_scope(self) -> str:
        return f"{self.branch}_{self.stage}"


def _repository_operation[
    Settings: (RepositoryCatalogSettings | RepositoryCaptureSettings | RepositoryExtractionSettings)
](
    branch: RepositoryBranch,
    stage: str,
    contract: type[Settings],
) -> CollectionOperation:
    return CollectionOperation(
        branch, stage, contract, lambda source: contract.project(source.repository_config, branch)
    )


COLLECTION_OPERATIONS = (
    _repository_operation("dataset", "enumerate_corpus_files", RepositoryCatalogSettings),
    _repository_operation("dataset", "capture_corpus_files", RepositoryCaptureSettings),
    _repository_operation("dataset", "command_candidates", RepositoryExtractionSettings),
    _repository_operation(
        "documentation",
        "capture_documentation",
        DocumentationCaptureSettings,
    ),
    _repository_operation(
        "documentation",
        "command_candidates",
        RepositoryExtractionSettings,
    ),
    CollectionOperation(
        "keenable", "command_candidates", KeenableCandidateSettings, KeenableCandidateSettings.project
    ),
)
_COLLECTION_OPERATIONS = {
    (operation.branch, operation.stage): operation for operation in COLLECTION_OPERATIONS
}


def collection_operation(branch: Branch, stage: str) -> CollectionOperation:
    return _COLLECTION_OPERATIONS[branch, stage]


def collection_settings_branch(settings: BaseModel) -> Branch | None:
    if isinstance(settings, RepositoryStageBinding):
        return settings.branch
    if isinstance(settings, KeenableCandidateSettings):
        return "keenable"
    return None
