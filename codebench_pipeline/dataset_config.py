from pathlib import Path
from typing import Annotated, Literal, assert_never

from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.category_rules import CategoryRules, Text
from codebench_pipeline.collection_config import DeclaredCollection
from codebench_pipeline.config_directory import ConfigDirectorySnapshot
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_documentation_config import DocumentationConfig as DocumentationConfig
from codebench_pipeline.dataset_seed_config import (
    SearchSeeds,
    SourceSeed,
    unique_sources,
)
from codebench_pipeline.discovery_config import DiscoveryOptions
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Identifier
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.train_config import TrainConfig
from codebench_pipeline.typed_config import ConfigArtifact, TypedConfig


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


class CatalogConfig(CatalogInputs):
    collection_revision: str = Field(default="1", min_length=1)
    model: ModelConfig | None = None
    manifest_header_json: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @property
    def configured(self) -> bool:
        return bool(self.documents) or self.manifest is not None or self.manifest_header_json is not None


SnapshotBytes = Annotated[int, Field(gt=0)]
SnapshotWorkers = Annotated[int, Field(ge=1, le=16)]


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


class DeduplicationPolicy(StrictStageConfig, ConfigArtifact):
    normalize_safe_whitespace: bool = True


RequestBytes = Annotated[int, Field(ge=1024, le=1048576)]
ReleaseCap = Annotated[int, Field(ge=1, le=250)]
AnnotationWorkers = Annotated[int, Field(ge=1, le=16)]


class AnnotationConfig(StrictStageConfig):
    model: ModelConfig
    maximum_context_characters: RequestBytes = 131072
    workers: AnnotationWorkers = 1
    batch: BatchLabelingConfig | None = None

    @property
    def max_request_bytes(self) -> int:
        return self.maximum_context_characters

    @model_validator(mode="after")
    def batch_provider(self) -> "AnnotationConfig":
        if self.batch is not None and self.model.provider != "openai_compatible":
            raise ValueError("Tool-grounded batches require an OpenAI-compatible provider")
        return self


class ReviewConfig(AnnotationConfig):
    batch_size: int = Field(default=16, ge=1, le=64)


class PublicationConfig(StrictStageConfig):
    limit_per_risk: ReleaseCap = 250
    review_columns: list[str] = Field(
        default_factory=lambda: ["command", "category_id", "risk_id", "severity", "class"]
    )

    @field_validator("review_columns")
    @classmethod
    def unique_review_columns(cls, value: list[str]) -> list[str]:
        if not value or any(not column.strip() for column in value) or len(set(value)) != len(value):
            raise ValueError("Review columns must be nonempty and distinct")
        return value


class DatasetBuildSpec(StrictStageConfig):
    dataset_id: Identifier
    revision: str
    refresh_external_inputs: bool = False
    rules: CategoryRules
    business_config: Text | None = None
    config_directory: ConfigDirectorySnapshot | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
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
    deduplication: DeduplicationPolicy = Field(
        default=DeduplicationPolicy(), exclude_if=lambda policy: policy == DeduplicationPolicy()
    )
    documentation: DocumentationConfig | None = Field(default=None, exclude_if=lambda value: value is None)
    declared_collection: DeclaredCollection | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    annotation: AnnotationConfig
    discovery: DiscoveryOptions = Field(
        default=DiscoveryOptions(), exclude_if=lambda options: options == DiscoveryOptions()
    )
    review: ReviewConfig | None = None
    publication: PublicationConfig
    storage: StorageConfig
    train: TrainConfig | None = Field(default=None, exclude_if=lambda value: value is None)
    test: TrainConfig | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def distinct_split_inputs(self) -> "DatasetBuildSpec":
        if self.train and self.test:
            train_files = {source.sha256 for source in self.train.sources}
            if any(source.sha256 in train_files for source in self.test.sources):
                raise ValueError("The same pinned CSV cannot be in both train and test")
        return self

    @property
    def has_declared_collection(self) -> bool:
        return self.declared_collection is not None

    @model_validator(mode="after")
    def matching_collection_inputs(self) -> "DatasetBuildSpec":
        if self.config_directory is not None and (
            self.has_declared_collection and self.config_directory.category_id != self.rules.category_id
        ):
            raise ValueError("Configuration directory scope differs from the declared dataset category")
        if self.has_declared_collection:
            if "documentation" in self.model_fields_set or self.refresh_external_inputs:
                raise ValueError("Declared collection requires pinned inputs and no labeling documentation")
            if (
                self.catalog.configured
                or self.documentation_catalog is not None
                or (self.snapshots.documents or self.documentation_snapshots.documents)
            ):
                raise ValueError("Declared collection cannot include prepared source inventories")
            require_declared_annotation(self.annotation)
        elif self.documentation is None:
            raise ValueError("Prepared source inputs require labeling documentation")
        return self

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
        case "keenable":
            return CatalogConfig()
    assert_never(branch)


def snapshots_for_branch(spec: DatasetBuildSpec, branch: Branch) -> SnapshotConfig:
    settings = {"dataset": spec.snapshots, "documentation": spec.documentation_snapshots}[branch]
    if spec.refresh_external_inputs:
        return settings
    return settings.model_copy(update={"documents": catalog_for_branch(spec, branch).documents})


def load_build_config(path: Path, home: Path | None = None) -> DatasetBuildSpec:
    from codebench_pipeline.declared_dataset_bundle import DeclaredDatasetBundle

    bundle = DeclaredDatasetBundle.model_validate_json(path.read_bytes())
    return bundle.resolve(path.resolve().parent, home=home)


def require_declared_annotation(annotation: AnnotationConfig) -> BatchLabelingConfig:
    batch = annotation.batch
    if annotation.model.provider != "openai_compatible" or batch is None:
        raise PydanticCustomError(
            "declared_labeling", "Declared collection requires OpenAI-compatible batch labeling"
        )
    if not batch.backend_revision.strip() or (
        batch.inference_mode == "guarded" and not Path(batch.require_capacity().state_directory).is_absolute()
    ):
        raise PydanticCustomError(
            "declared_labeling_capacity",
            "Labeling requires a pinned backend revision; guarded mode requires an absolute capacity directory",
        )
    return batch
