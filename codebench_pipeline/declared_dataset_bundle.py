from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, JsonValue, field_validator, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.batch_labeling_config import DeclaredBatchLabelingConfig
from codebench_pipeline.business_config import compile_business_rules
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import CollectionMergeConfig, DeclaredCollection, SourceDeclaration
from codebench_pipeline.config_directory import ConfigDirectorySnapshot
from codebench_pipeline.dataset_config import (
    AdapterConfig,
    AnnotationConfig,
    AnnotationWorkers,
    CatalogConfig,
    DatasetBuildSpec,
    DeduplicationPolicy,
    ExtractionConfig,
    FilteringConfig,
    PublicationConfig,
    ReleaseCap,
    RequestBytes,
    StrictStageConfig,
    require_declared_annotation,
)
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Identifier
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.storage import IntegrityError


def compile_directory(
    directory: ConfigDirectorySnapshot, dataset_id: Identifier, merge: CollectionMergeConfig
) -> tuple[CategoryRules, DeclaredCollection]:
    from codebench_pipeline.collection_config import parse_collection_keenable

    category = directory.category_directory
    rules = compile_business_rules(category / "category.json", directory.category_id, directory.read)
    dataset = RepositoryBranchDeclaration.model_validate_json(
        directory.read(category / "collection/existing-datasets.json")
    )
    documentation = RepositoryBranchDeclaration.model_validate_json(
        directory.read(category / "collection/documentation-crawl.json")
    )
    keenable = parse_collection_keenable(directory.read(category / "keenable/declaration.json"))
    if keenable.dataset != dataset_id:
        raise IntegrityError("Keenable source list belongs to a different dataset partition")
    return rules, DeclaredCollection(
        existing_datasets=SourceDeclaration.from_declaration(dataset),
        documentation_crawl=SourceDeclaration.from_declaration(documentation),
        keenable_use_cases=SourceDeclaration.from_declaration(keenable),
        merge=merge,
    )


class FromRegistration(StrictStageConfig):
    from_registration: Literal[True]

    @field_validator("from_registration", mode="before")
    @classmethod
    def requires_true(cls, value: JsonValue) -> JsonValue:
        if value is not True:
            raise PydanticCustomError("from_registration", "from_registration must be true")
        return value


class LabelingFromRegistration(FromRegistration):
    inference_mode: Literal["plain"] | None = None


class DeclaredLabeling(StrictStageConfig):
    model: ModelConfig
    max_request_bytes: RequestBytes = 131072
    workers: AnnotationWorkers = 1
    batch: DeclaredBatchLabelingConfig

    def registered(self) -> AnnotationConfig:
        return AnnotationConfig(
            model=self.model,
            maximum_context_characters=self.max_request_bytes,
            workers=self.workers,
            batch=self.batch.registered(),
        )

    @model_validator(mode="after")
    def declared_transport(self) -> "DeclaredLabeling":
        require_declared_annotation(self.registered())
        return self


class DeclaredReview(StrictStageConfig):
    review_columns: list[str] = Field(default_factory=lambda: PublicationConfig().review_columns)

    @field_validator("review_columns")
    @classmethod
    def command_identity(cls, value: list[str]) -> list[str]:
        if "command" not in value:
            raise PydanticCustomError("review_command_identity", "review_columns must include command")
        return PublicationConfig.unique_review_columns(value)


class DeclaredRelease(StrictStageConfig):
    release_cap_per_risk: ReleaseCap = 250


class DeclaredS3Storage(StrictStageConfig):
    backend: Literal["s3"]
    bucket: str
    endpoint_url: str | None = Field(default=None, repr=False)
    region: str = "us-east-1"
    access_key_env: str | None = None
    secret_key_env: str | None = None
    key_prefix: str = "datasets"
    preview_rows: Annotated[int, Field(ge=0, le=100)] = 10


class DeclaredDatasetBundle(StrictStageConfig):
    dataset_id: Identifier
    category_id: Literal["CAT-3"]
    revision_label: str = Field(min_length=1)
    labeling: DeclaredLabeling | LabelingFromRegistration
    storage: DeclaredS3Storage | FromRegistration
    filtering: FilteringConfig = FilteringConfig()
    deduplication: DeduplicationPolicy = DeduplicationPolicy()
    merge: CollectionMergeConfig = CollectionMergeConfig()
    review: DeclaredReview = DeclaredReview()
    release: DeclaredRelease = DeclaredRelease()

    def resolve(
        self,
        parent: Path,
        *,
        home: Path | None = None,
        observed_directory: ConfigDirectorySnapshot | None = None,
    ) -> DatasetBuildSpec:
        from codebench_pipeline.dataset_registry import DatasetRegistry

        dataset_home = home if home is not None else Path(".codebench-datasets")
        registry = DatasetRegistry(dataset_home)
        registered = None
        match self.labeling:
            case LabelingFromRegistration(inference_mode=mode):
                try:
                    registered = registry.get_snapshot(self.dataset_id)
                except FileNotFoundError as error:
                    raise IntegrityError(
                        "Declared collection requires existing registered annotation settings; select the canonical dataset --home"
                    ) from error
                annotation = registered.annotation
                batch = require_declared_annotation(annotation)
                if mode is not None:
                    annotation = annotation.model_copy(
                        update={"batch": batch.model_copy(update={"inference_mode": mode, "capacity": None})}
                    )
            case DeclaredLabeling() as labeling:
                annotation = labeling.registered()
        match self.storage:
            case FromRegistration():
                try:
                    storage = (
                        registered.storage if registered is not None else registry.storage(self.dataset_id)
                    )
                except FileNotFoundError as error:
                    raise IntegrityError(
                        "Declared collection requires existing registered storage settings on S3; "
                        "select the canonical dataset --home or declare S3 storage explicitly"
                    ) from error
                if storage.backend != "s3":
                    raise IntegrityError(
                        "Registered storage is local; declare S3 storage explicitly for a new declared run"
                    )
            case DeclaredS3Storage() as s3:
                storage = StorageConfig(
                    backend="s3",
                    bucket=s3.bucket,
                    endpoint_url=s3.endpoint_url,
                    region=s3.region,
                    access_key_env=s3.access_key_env,
                    secret_key_env=s3.secret_key_env,
                    prefix=s3.key_prefix,
                    sample_rows=s3.preview_rows,
                )
        directory = (
            ConfigDirectorySnapshot.capture(parent.parent, self.category_id)
            if observed_directory is None
            else observed_directory.require_complete(self.category_id)
        )
        rules, collection = compile_directory(directory, self.dataset_id, self.merge)
        return DatasetBuildSpec(
            dataset_id=self.dataset_id,
            revision=self.revision_label,
            rules=rules,
            config_directory=directory,
            catalog=CatalogConfig(),
            adapters=AdapterConfig(),
            extraction=ExtractionConfig(),
            filtering=self.filtering,
            deduplication=self.deduplication,
            annotation=annotation,
            publication=PublicationConfig(
                limit_per_risk=self.release.release_cap_per_risk,
                review_columns=self.review.review_columns,
            ),
            storage=storage,
            declared_collection=collection,
        )
