from pathlib import Path
from typing import Literal

from pydantic import Field, JsonValue, TypeAdapter

from codebench_pipeline.business_config import load_business_rules
from codebench_pipeline.category_rules import Text
from codebench_pipeline.collection_config import DeclaredCollection, SourceDeclaration
from codebench_pipeline.dataset_config import (
    AdapterConfig,
    AnnotationConfig,
    CatalogConfig,
    DatasetBuildSpec,
    ExtractionConfig,
    FilteringConfig,
    PublicationConfig,
    StrictStageConfig,
)
from codebench_pipeline.models import Identifier
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.storage import IntegrityError, fingerprint


class SourceListReference(StrictStageConfig):
    source_list: Text


class RegisteredAnnotation(StrictStageConfig):
    mode: Literal["registered"]


class GuardedFinalization(StrictStageConfig):
    business_config: Text
    annotation: AnnotationConfig | RegisteredAnnotation
    filtering: FilteringConfig = FilteringConfig()
    publication: PublicationConfig = PublicationConfig()


class DeclaredDatasetBundle(StrictStageConfig):
    dataset_id: Identifier
    revision: str = Field(min_length=1)
    existing_datasets: SourceListReference
    documentation_crawl: SourceListReference
    keenable_use_cases: SourceListReference
    finalization: GuardedFinalization
    storage: StorageConfig | None = None

    def resolve(self, parent: Path, storage: StorageConfig, *, home: Path | None = None) -> DatasetBuildSpec:
        from codebench_pipeline.dataset_registry import DatasetRegistry
        from codebench_pipeline.keenable_policy import load_declaration
        from codebench_pipeline.repository import load_repository_config

        annotation = self.finalization.annotation
        if isinstance(annotation, RegisteredAnnotation):
            registry = DatasetRegistry(home or Path(".codebench-datasets"))
            try:
                registered = registry.get_snapshot(self.dataset_id)
            except FileNotFoundError as error:
                raise IntegrityError(
                    "Declared collection requires existing registered annotation settings; select the canonical dataset --home"
                ) from error
            annotation = registered.annotation
            if self.storage is None:
                storage = registered.storage
        policy_path = (parent / self.finalization.business_config).resolve()
        rules = load_business_rules(policy_path, "CAT-3")
        category = policy_path.parent
        dataset_path = source_path(category, self.existing_datasets)
        documentation_path = source_path(category, self.documentation_crawl)
        keenable_path = source_path(category, self.keenable_use_cases)
        dataset = load_repository_config(dataset_path)
        documentation = (
            dataset if documentation_path == dataset_path else load_repository_config(documentation_path)
        )
        keenable = load_declaration(keenable_path)
        if keenable.dataset != self.dataset_id:
            raise IntegrityError("Keenable source list belongs to a different dataset partition")
        return DatasetBuildSpec(
            dataset_id=self.dataset_id,
            revision=self.revision,
            rules=rules,
            catalog=CatalogConfig(),
            adapters=AdapterConfig(),
            extraction=ExtractionConfig(),
            filtering=self.finalization.filtering,
            annotation=annotation,
            publication=self.finalization.publication,
            storage=storage,
            declared_collection=DeclaredCollection(
                existing_datasets=source_declaration(dataset_path, dataset.model_dump_json()),
                documentation_crawl=source_declaration(documentation_path, documentation.model_dump_json()),
                keenable_use_cases=source_declaration(keenable_path, keenable.model_dump_json()),
            ),
        )


def source_path(category: Path, reference: SourceListReference) -> Path:
    path = (category / reference.source_list).resolve()
    if Path(reference.source_list).is_absolute() or not path.is_relative_to(category):
        raise IntegrityError("Source list must be a category-relative declaration")
    return path


def source_declaration(path: Path, normalized_json: str) -> SourceDeclaration:
    declaration = TypeAdapter(dict[str, JsonValue]).validate_json(normalized_json)
    return SourceDeclaration(source_list=str(path), declaration=declaration, digest=fingerprint(declaration))
