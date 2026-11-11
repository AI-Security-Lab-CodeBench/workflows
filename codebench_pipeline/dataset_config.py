from pathlib import Path
from typing import Literal, assert_never

from pydantic import ConfigDict, Field, field_validator, model_validator

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_documentation_config import DocumentationConfig as DocumentationConfig
from codebench_pipeline.dataset_seed_config import (
    SearchSeeds,
    SourceSeed,
    unique_sources,
)
from codebench_pipeline.dataset_source_models import is_documentation_source
from codebench_pipeline.model_config import ModelConfig, ModelRoles
from codebench_pipeline.models import Identifier
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.typed_config import TypedConfig


class StrictStageConfig(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CatalogInputs(StrictStageConfig):
    mode: Literal["provided_sources", "keenable"]
    documents: list[SourceSeed] = Field(default_factory=list)
    search_request: SearchSeeds | None = None

    @field_validator("documents")
    @classmethod
    def distinct_sources(cls, value: list[SourceSeed]) -> list[SourceSeed]:
        return unique_sources(value)

    @model_validator(mode="after")
    def matching_input(self) -> "CatalogInputs":
        if self.mode == "provided_sources" and self.search_request is not None:
            raise ValueError("Provided sources cannot include a search request")
        if self.mode == "keenable" and (self.search_request is None or self.documents):
            raise ValueError("Keenable discovery requires only a search request")
        return self


class DatasetSeeds(CatalogInputs):
    """Source/search starting points, not synthetic command prompts or RNG seeds."""

    revision: str = Field(default="1", min_length=1)


class CatalogConfig(CatalogInputs):
    collection_revision: str = Field(default="1", min_length=1)
    model: ModelConfig


class SnapshotConfig(StrictStageConfig):
    maximum_bytes: int = Field(default=10_485_760, gt=0)
    documents: list[SourceSeed] = Field(default_factory=list)

    @field_validator("documents")
    @classmethod
    def distinct_sources(cls, value: list[SourceSeed]) -> list[SourceSeed]:
        return unique_sources(value)


class AdapterConfig(StrictStageConfig):
    model: ModelConfig
    mode: Literal["infer", "provided"] = "infer"
    sample_characters: int = Field(default=8000, ge=100, le=50000)


class ExtractionConfig(StrictStageConfig):
    context_characters: int = Field(default=100, ge=0, le=2000)


class FilteringConfig(StrictStageConfig):
    maximum_command_characters: int = Field(default=32768, gt=0)
    quarantine_dynamic_shell: bool = True


class AnnotationConfig(StrictStageConfig):
    model: ModelConfig
    maximum_context_characters: int = Field(default=131072, ge=1024, le=1048576)


class ReviewConfig(AnnotationConfig):
    batch_size: int = Field(default=16, ge=1, le=64)


class PublicationConfig(StrictStageConfig):
    limit_per_risk: int = Field(default=250, ge=1, le=250)


class DatasetFileConfig(StrictStageConfig):
    dataset_id: Identifier
    revision: str = Field(min_length=1)
    rules: CategoryRules
    models: ModelRoles
    seeds: DatasetSeeds
    documentation_sources: DatasetSeeds | None = None
    documentation: DocumentationConfig
    snapshot_maximum_bytes: int = Field(default=10_485_760, gt=0)
    adapters: Literal["infer", "provided"] = "infer"
    adapter_sample_characters: int = Field(default=8000, ge=100, le=50000)
    extraction: ExtractionConfig = ExtractionConfig()
    filtering: FilteringConfig = FilteringConfig()
    publication: PublicationConfig = PublicationConfig()
    storage: StorageConfig | None = None

    @model_validator(mode="after")
    def matching_sources(self) -> "DatasetFileConfig":
        for seeds in (self.seeds, self.documentation_sources):
            if (
                seeds is not None
                and seeds.search_request is not None
                and not {group.risk for group in seeds.search_request.searches}
                <= {risk.id for risk in self.rules.risks}
            ):
                raise ValueError("Discovery query risk is outside the category")
        if self.documentation_sources is not None:
            for document in self.documentation_sources.documents:
                if not is_documentation_source(document.source_type, document.provenance_type):
                    raise ValueError(
                        "Documentation sources require original documentation or forum provenance, not corpora"
                    )
        return self


class DatasetBuildSpec(StrictStageConfig):
    dataset_id: Identifier
    revision: str
    rules: CategoryRules
    catalog: CatalogConfig
    snapshots: SnapshotConfig = SnapshotConfig()
    documentation_catalog: CatalogConfig | None = None
    documentation_snapshots: SnapshotConfig = SnapshotConfig()
    adapters: AdapterConfig
    extraction: ExtractionConfig
    filtering: FilteringConfig
    documentation: DocumentationConfig
    annotation: AnnotationConfig
    review: ReviewConfig
    publication: PublicationConfig
    storage: StorageConfig


def catalog_for_branch(spec: DatasetBuildSpec, branch: Branch) -> CatalogConfig:
    match branch:
        case "dataset":
            return spec.catalog
        case "documentation":
            return spec.documentation_catalog or CatalogConfig(
                mode="provided_sources", model=spec.catalog.model
            )
    assert_never(branch)


def snapshots_for_branch(spec: DatasetBuildSpec, branch: Branch) -> SnapshotConfig:
    match branch:
        case "dataset":
            return spec.snapshots
        case "documentation":
            return spec.documentation_snapshots
    assert_never(branch)


def load_build_config(path: Path, home: Path | None = None) -> DatasetBuildSpec:
    supplied = DatasetFileConfig.model_validate_json(path.read_bytes())
    parent = path.resolve().parent
    documents = [
        document.model_copy(update={"local_path": str((parent / document.local_path).resolve())})
        for document in supplied.seeds.documents
    ]
    documentation_documents = [
        document.model_copy(update={"local_path": str((parent / document.local_path).resolve())})
        for document in (
            supplied.documentation_sources.documents if supplied.documentation_sources is not None else []
        )
    ]
    storage = supplied.storage or StorageConfig(
        local_root=str(((home or Path(".codebench-datasets")) / "dataframes").resolve())
    )
    if storage.backend == "local" and not Path(storage.local_root).is_absolute():
        storage = storage.model_copy(update={"local_root": str((parent / storage.local_root).resolve())})
    return DatasetBuildSpec(
        dataset_id=supplied.dataset_id,
        revision=supplied.revision,
        rules=supplied.rules,
        catalog=CatalogConfig(
            collection_revision=supplied.seeds.revision,
            mode=supplied.seeds.mode,
            documents=documents,
            search_request=supplied.seeds.search_request,
            model=supplied.models.selector,
        ),
        snapshots=SnapshotConfig(
            maximum_bytes=supplied.snapshot_maximum_bytes,
            documents=documents,
        ),
        documentation_catalog=(
            CatalogConfig(
                collection_revision=supplied.documentation_sources.revision,
                mode=supplied.documentation_sources.mode,
                documents=documentation_documents,
                search_request=supplied.documentation_sources.search_request,
                model=supplied.models.selector,
            )
            if supplied.documentation_sources is not None
            else None
        ),
        documentation_snapshots=SnapshotConfig(
            maximum_bytes=supplied.snapshot_maximum_bytes,
            documents=documentation_documents,
        ),
        adapters=AdapterConfig(
            model=supplied.models.adapter,
            mode=supplied.adapters,
            sample_characters=supplied.adapter_sample_characters,
        ),
        extraction=supplied.extraction,
        filtering=supplied.filtering,
        documentation=(
            supplied.documentation.model_copy(update={"model": supplied.models.selector})
            if supplied.documentation.mode == "keenable" and supplied.documentation.model is None
            else supplied.documentation
        ),
        annotation=AnnotationConfig(model=supplied.models.labeler),
        review=ReviewConfig(model=supplied.models.reviewer),
        publication=supplied.publication,
        storage=storage,
    )
