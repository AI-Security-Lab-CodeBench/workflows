from pathlib import Path
from typing import assert_never

from pydantic import Field, RootModel, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.business_config import load_business_rules
from codebench_pipeline.category_rules import CategoryRules, Text
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_config import (
    AdapterConfig,
    AnnotationConfig,
    BundleVersion,
    CatalogConfig,
    DatasetBuildSpec,
    DatasetFileConfig,
    DatasetSeeds,
    ExtractionConfig,
    FilteringConfig,
    PublicationConfig,
    SnapshotConfig,
    SnapshotSettings,
    StrictStageConfig,
)
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_source_models import is_documentation_source
from codebench_pipeline.legacy_dataset_config import resolve_legacy_file
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Identifier
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.source_manifests import (
    load_documentation_manifest,
    load_source_manifest,
    resolve_source_paths,
    validate_transformations,
)
from codebench_pipeline.storage import IntegrityError


class BranchSources(DatasetSeeds):
    model: ModelConfig | None = None


class CollectionSection(StrictStageConfig):
    sources: BranchSources = BranchSources()
    snapshot: SnapshotSettings = SnapshotSettings()
    adapters: AdapterConfig = AdapterConfig()
    extraction: ExtractionConfig = ExtractionConfig()

    def resolve(
        self, parent: Path, category_id: str, *, branch: Branch
    ) -> tuple[CatalogConfig, SnapshotConfig]:
        manifest = (
            load_source_manifest((parent / self.sources.manifest).resolve(), category_id)
            if self.sources.manifest is not None
            else None
        )
        documents = (
            manifest.sources
            if manifest is not None
            else [resolve_source_paths(document, parent) for document in self.sources.documents]
        )
        if branch == "documentation" and any(
            not is_documentation_source(document.source_type, document.provenance_type)
            for document in documents
        ):
            raise IntegrityError("Documentation sources require original documentation or forum provenance")
        validate_transformations(documents)
        return (
            CatalogConfig(
                collection_revision=self.sources.revision,
                documents=documents,
                model=self.sources.model,
                manifest_header_json=manifest.model_dump_json(exclude={"sources"})
                if manifest is not None
                else None,
            ),
            SnapshotConfig(maximum_bytes=self.snapshot.maximum_bytes, workers=self.snapshot.workers),
        )


class BundleDocumentation(DocumentationConfig):
    @model_validator(mode="after")
    def one_snippet_input(self) -> "BundleDocumentation":
        if self.manifest is not None and self.snippets:
            raise PydanticCustomError("documentation_input", "Choose a manifest or inline snippets")
        if self.manifest_header_json is not None:
            raise PydanticCustomError("documentation_input", "Manifest headers are resolved at registration")
        return self

    def resolve(self, parent: Path, category_id: str) -> DocumentationConfig:
        manifest = (
            load_documentation_manifest((parent / self.manifest).resolve(), category_id)
            if self.manifest is not None
            else None
        )
        return DocumentationConfig(
            mode=self.mode,
            snippets=manifest.snippets if manifest is not None else self.snippets,
            revision=self.revision,
            maximum_bytes=self.maximum_bytes,
            model=self.model,
            manifest_header_json=manifest.model_dump_json(exclude={"snippets"})
            if manifest is not None
            else None,
        )


class FinalizationSection(StrictStageConfig):
    rules: CategoryRules | None = None
    business_config: Text | None = None
    documentation: BundleDocumentation
    annotation: AnnotationConfig
    filtering: FilteringConfig = FilteringConfig()
    publication: PublicationConfig = PublicationConfig()

    @model_validator(mode="after")
    def one_policy(self) -> "FinalizationSection":
        if (self.rules is None) == (self.business_config is None):
            raise PydanticCustomError("category_policy", "Choose exactly one of rules or business_config")
        return self

    def resolve_policy(self, parent: Path) -> CategoryRules:
        if self.rules is not None:
            return self.rules
        assert self.business_config is not None
        return load_business_rules(parent / self.business_config)


class DatasetBundleConfig(StrictStageConfig):
    version: BundleVersion
    dataset_id: Identifier
    revision: str = Field(min_length=1)
    existing_datasets: CollectionSection = CollectionSection()
    documentation_crawl: CollectionSection = CollectionSection()
    finalization: FinalizationSection
    storage: StorageConfig | None = None

    @model_validator(mode="after")
    def documentation_origin(self) -> "DatasetBundleConfig":
        if any(
            not is_documentation_source(document.source_type, document.provenance_type)
            for document in self.documentation_crawl.sources.documents
        ):
            raise PydanticCustomError(
                "documentation_origin",
                "Documentation sources require original documentation or forum provenance",
            )
        return self

    def resolve(self, parent: Path, storage: StorageConfig) -> DatasetBuildSpec:
        rules = self.finalization.resolve_policy(parent)
        catalog, snapshots = self.existing_datasets.resolve(parent, rules.category_id, branch="dataset")
        docs_catalog, docs_snapshots = self.documentation_crawl.resolve(
            parent, rules.category_id, branch="documentation"
        )
        return DatasetBuildSpec(
            dataset_id=self.dataset_id,
            revision=self.revision,
            bundle_version=self.version,
            rules=rules,
            catalog=catalog,
            snapshots=snapshots,
            documentation_catalog=docs_catalog,
            documentation_snapshots=docs_snapshots,
            adapters=self.existing_datasets.adapters,
            extraction=self.existing_datasets.extraction,
            documentation_adapters=self.documentation_crawl.adapters,
            documentation_extraction=self.documentation_crawl.extraction,
            filtering=self.finalization.filtering,
            documentation=self.finalization.documentation.resolve(parent, rules.category_id),
            annotation=self.finalization.annotation,
            publication=self.finalization.publication,
            storage=storage,
        )


class DatasetFile(RootModel[DatasetBundleConfig | DatasetFileConfig]):
    pass


def load_dataset_bundle(path: Path, home: Path | None) -> DatasetBuildSpec:
    supplied = DatasetFile.model_validate_json(path.read_bytes()).root
    parent = path.resolve().parent
    storage = supplied.storage or StorageConfig(
        local_root=str(((home or Path(".codebench-datasets")) / "dataframes").resolve())
    )
    if storage.backend == "local" and not Path(storage.local_root).is_absolute():
        storage = storage.model_copy(update={"local_root": str((parent / storage.local_root).resolve())})
    match supplied:
        case DatasetFileConfig():
            return resolve_legacy_file(supplied, parent, storage)
        case DatasetBundleConfig():
            return supplied.resolve(parent, storage)
    assert_never(supplied)
