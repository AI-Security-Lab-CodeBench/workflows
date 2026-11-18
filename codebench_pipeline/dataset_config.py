from pathlib import Path
from typing import Annotated, Literal, assert_never

from pydantic import ConfigDict, Field, field_validator, model_validator

from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.category_rules import CategoryRules, Text
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
    mode: Literal["provided_sources"] = "provided_sources"
    documents: list[SourceSeed] = Field(default_factory=list)
    manifest: str | None = None
    search_request: SearchSeeds | None = None

    @field_validator("documents")
    @classmethod
    def distinct_sources(cls, value: list[SourceSeed]) -> list[SourceSeed]:
        return unique_sources(value)

    @model_validator(mode="after")
    def matching_input(self) -> "CatalogInputs":
        if self.search_request is not None:
            raise ValueError("Source discovery belongs to the local agent; supply a source manifest")
        return self


class DatasetSeeds(CatalogInputs):
    """Category source inputs prepared by the local agent."""

    revision: str = Field(default="1", min_length=1)

    @model_validator(mode="after")
    def one_source_input(self) -> "DatasetSeeds":
        if self.manifest is not None and self.documents:
            raise ValueError("Choose either a source manifest or inline source records")
        return self


class CatalogConfig(CatalogInputs):
    collection_revision: str = Field(default="1", min_length=1)
    model: ModelConfig | None = None
    manifest_header_json: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @property
    def configured(self) -> bool:
        return bool(self.documents) or self.manifest is not None or self.manifest_header_json is not None


SnapshotBytes = Annotated[int, Field(gt=0)]
SnapshotWorkers = Annotated[int, Field(ge=1, le=16)]
BundleVersion = Annotated[int, Field(strict=True, ge=1, le=1)]


class SnapshotSettings(StrictStageConfig):
    maximum_bytes: SnapshotBytes = 10_485_760
    workers: SnapshotWorkers = 8


class SnapshotConfig(StrictStageConfig):
    maximum_bytes: SnapshotBytes = 10_485_760
    documents: list[SourceSeed] = Field(default_factory=list)
    workers: SnapshotWorkers = 8

    @field_validator("documents")
    @classmethod
    def distinct_sources(cls, value: list[SourceSeed]) -> list[SourceSeed]:
        return unique_sources(value)


class AdapterConfig(StrictStageConfig):
    model: ModelConfig | None = None
    mode: Literal["provided"] = "provided"
    sample_characters: int = Field(default=8000, ge=100, le=50000)


class ExtractionConfig(StrictStageConfig):
    context_characters: int = Field(default=100, ge=0, le=2000)


class FilteringConfig(StrictStageConfig):
    maximum_command_characters: int = Field(default=32768, gt=0)
    quarantine_dynamic_shell: bool = True


class AnnotationConfig(StrictStageConfig):
    model: ModelConfig
    maximum_context_characters: int = Field(default=131072, ge=1024, le=1048576)
    workers: int = Field(default=1, ge=1, le=8)
    batch: BatchLabelingConfig | None = None

    @model_validator(mode="after")
    def batch_provider(self) -> "AnnotationConfig":
        if self.batch is not None and self.model.provider != "openai_compatible":
            raise ValueError("Tool-grounded batches require an OpenAI-compatible provider")
        return self


class ReviewConfig(AnnotationConfig):
    batch_size: int = Field(default=16, ge=1, le=64)


class PublicationConfig(StrictStageConfig):
    limit_per_risk: int = Field(default=250, ge=1, le=250)
    review_columns: list[str] = Field(
        default_factory=lambda: ["command", "category_id", "risk_id", "severity", "class"]
    )

    @field_validator("review_columns")
    @classmethod
    def unique_review_columns(cls, value: list[str]) -> list[str]:
        if not value or any(not column.strip() for column in value) or len(set(value)) != len(value):
            raise ValueError("Review columns must be nonempty and distinct")
        return value


class DatasetFileConfig(StrictStageConfig):
    dataset_id: Identifier
    revision: str = Field(min_length=1)
    rules: CategoryRules | None = None
    business_config: Text | None = None
    models: ModelRoles
    seeds: DatasetSeeds
    documentation_sources: DatasetSeeds | None = None
    documentation: DocumentationConfig
    snapshot_maximum_bytes: int = Field(default=10_485_760, gt=0)
    adapters: Literal["provided"] = "provided"
    adapter_sample_characters: int = Field(default=8000, ge=100, le=50000)
    labeling_workers: int = Field(default=1, ge=1, le=8)
    labeling_batch: BatchLabelingConfig | None = None
    extraction: ExtractionConfig = ExtractionConfig()
    filtering: FilteringConfig = FilteringConfig()
    publication: PublicationConfig = PublicationConfig()
    storage: StorageConfig | None = None

    @model_validator(mode="after")
    def matching_sources(self) -> "DatasetFileConfig":
        if (self.rules is None) == (self.business_config is None):
            raise ValueError("Choose exactly one of rules or business_config")
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
    bundle_version: BundleVersion | None = Field(default=None, exclude_if=lambda value: value is None)
    rules: CategoryRules
    business_config: Text | None = None
    catalog: CatalogConfig
    snapshots: SnapshotConfig = SnapshotConfig()
    documentation_catalog: CatalogConfig | None = None
    documentation_snapshots: SnapshotConfig = SnapshotConfig()
    adapters: AdapterConfig
    extraction: ExtractionConfig
    documentation_adapters: AdapterConfig | None = Field(default=None, exclude_if=lambda value: value is None)
    documentation_extraction: ExtractionConfig | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    filtering: FilteringConfig
    documentation: DocumentationConfig
    annotation: AnnotationConfig
    review: ReviewConfig | None = None
    publication: PublicationConfig
    storage: StorageConfig

    @field_validator("business_config")
    @classmethod
    def absolute_business_reference(cls, value: str | None) -> str | None:
        if value is not None and not Path(value).is_absolute():
            raise ValueError("Registered business_config must be an absolute path")
        return value


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
    settings = {"dataset": spec.snapshots, "documentation": spec.documentation_snapshots}[branch]
    if spec.bundle_version is None:
        return settings
    return settings.model_copy(update={"documents": catalog_for_branch(spec, branch).documents})


def load_build_config(path: Path, home: Path | None = None) -> DatasetBuildSpec:
    from codebench_pipeline.dataset_bundle import load_dataset_bundle

    return load_dataset_bundle(path, home)
