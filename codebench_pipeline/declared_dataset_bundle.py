from pathlib import Path
from typing import Literal

from pydantic import Field, JsonValue, TypeAdapter

from codebench_pipeline.business_config import load_business_rules
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
from codebench_pipeline.repository_branch_config import load_repository_branch
from codebench_pipeline.repository_config import SourceWeight
from codebench_pipeline.storage import IntegrityError, fingerprint


class RegisteredAnnotation(StrictStageConfig):
    mode: Literal["registered"]


class GuardedFinalization(StrictStageConfig):
    annotation: AnnotationConfig | RegisteredAnnotation
    filtering: FilteringConfig = FilteringConfig()
    publication: PublicationConfig = PublicationConfig()
    keenable_source_weight: SourceWeight | None = None


class DeclaredDatasetBundle(StrictStageConfig):
    dataset_id: Identifier
    category_id: Literal["CAT-3"]
    revision: str = Field(min_length=1)
    finalization: GuardedFinalization
    storage: StorageConfig | None = None

    def resolve(self, parent: Path, storage: StorageConfig, *, home: Path | None = None) -> DatasetBuildSpec:
        from codebench_pipeline.dataset_registry import DatasetRegistry
        from codebench_pipeline.keenable_policy import load_declaration

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
        category = (parent.parent / "categories" / self.category_id.replace("-", "").lower()).resolve()
        rules = load_business_rules(source_path(category, "category.json"), self.category_id)
        dataset = load_repository_branch(source_path(category, "collection/existing-datasets.json"))
        documentation = load_repository_branch(source_path(category, "collection/documentation-crawl.json"))
        keenable = load_declaration(source_path(category, "keenable/declaration.json"))
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
                existing_datasets=source_declaration(dataset.model_dump_json()),
                documentation_crawl=source_declaration(documentation.model_dump_json()),
                keenable_use_cases=source_declaration(keenable.model_dump_json()),
                merge_keenable_weight=self.finalization.keenable_source_weight,
            ),
        )


def source_path(category: Path, relative: str) -> Path:
    path = (category / relative).resolve()
    if not path.is_relative_to(category):
        raise IntegrityError("Business declaration escapes its category directory")
    return path


def source_declaration(normalized_json: str) -> SourceDeclaration:
    declaration = TypeAdapter(dict[str, JsonValue]).validate_json(normalized_json)
    return SourceDeclaration(declaration=declaration, digest=fingerprint(declaration))
