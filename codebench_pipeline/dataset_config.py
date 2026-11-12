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
from codebench_pipeline.source_manifests import (
    manifest_documentation,
    manifest_sources,
    resolve_source_paths,
    validate_transformations,
)
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


class SnapshotConfig(StrictStageConfig):
    maximum_bytes: int = Field(default=10_485_760, gt=0)
    documents: list[SourceSeed] = Field(default_factory=list)
    workers: int = Field(default=8, ge=1, le=16)

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
    adapters: Literal["provided"] = "provided"
    adapter_sample_characters: int = Field(default=8000, ge=100, le=50000)
    labeling_workers: int = Field(default=1, ge=1, le=8)
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
    review: ReviewConfig | None = None
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
        resolve_source_paths(document, parent)
        for document in supplied.seeds.documents
    ]
    documentation_documents = [
        resolve_source_paths(document, parent)
        for document in (
            supplied.documentation_sources.documents if supplied.documentation_sources is not None else []
        )
    ]
    storage = supplied.storage or StorageConfig(
        local_root=str(((home or Path(".codebench-datasets")) / "dataframes").resolve())
    )
    if storage.backend == "local" and not Path(storage.local_root).is_absolute():
        storage = storage.model_copy(update={"local_root": str((parent / storage.local_root).resolve())})
    return resolve_business_inputs(DatasetBuildSpec(
        dataset_id=supplied.dataset_id,
        revision=supplied.revision,
        rules=supplied.rules,
        catalog=CatalogConfig(
            collection_revision=supplied.seeds.revision,
            mode=supplied.seeds.mode,
            documents=documents,
            manifest=str((parent / supplied.seeds.manifest).resolve())
            if supplied.seeds.manifest is not None else None,
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
                manifest=str((parent / supplied.documentation_sources.manifest).resolve())
                if supplied.documentation_sources.manifest is not None else None,
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
        documentation=supplied.documentation.model_copy(
            update={"manifest": str((parent / supplied.documentation.manifest).resolve())}
            if supplied.documentation.manifest is not None else {}
        ),
        annotation=AnnotationConfig(model=supplied.models.labeler, workers=supplied.labeling_workers),
        review=None,
        publication=supplied.publication,
        storage=storage,
    ))


def resolve_business_inputs(spec: DatasetBuildSpec) -> DatasetBuildSpec:
    updates: dict[str, CatalogConfig | SnapshotConfig | DocumentationConfig] = {}
    for catalog_name, snapshot_name in (
        ("catalog", "snapshots"), ("documentation_catalog", "documentation_snapshots")
    ):
        catalog = spec.catalog if catalog_name == "catalog" else spec.documentation_catalog
        if catalog is None:
            continue
        documents = (
            manifest_sources(Path(catalog.manifest), spec.rules.category_id)
            if catalog.manifest is not None else catalog.documents
        )
        validate_transformations(documents)
        if catalog_name == "documentation_catalog" and any(
            not is_documentation_source(document.source_type, document.provenance_type)
            for document in documents
        ):
            raise ValueError("Documentation manifest cannot contain dataset corpora")
        updates[catalog_name] = catalog.model_copy(update={"documents": documents})
        snapshots = spec.snapshots if snapshot_name == "snapshots" else spec.documentation_snapshots
        updates[snapshot_name] = snapshots.model_copy(update={"documents": documents})
    if spec.documentation.manifest is not None:
        updates["documentation"] = spec.documentation.model_copy(
            update={"snippets": manifest_documentation(
                Path(spec.documentation.manifest), spec.rules.category_id
            )}
        )
    return spec.model_copy(update=updates)
