from typing import Literal

from dagster import AssetKey, Config
from pydantic import BaseModel, ConfigDict, Field

from codebench_pipeline.batch_labeling_config import BackendRevision, BatchPromptConfig
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import CollectionMergeConfig, DeclaredSourceStageConfig
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, Branch, collection_branches, stage_name
from codebench_pipeline.dataset_config import (
    AdapterConfig,
    AnnotationConfig,
    AnnotationWorkers,
    CatalogConfig,
    DatasetBuildSpec,
    DocumentationConfig,
    ExtractionConfig,
    FilteringConfig,
    PublicationConfig,
    RequestBytes,
    SnapshotConfig,
    catalog_for_branch,
    snapshots_for_branch,
)
from codebench_pipeline.grounding_tools import GroundingConfig
from codebench_pipeline.keenable_policy import Case
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Identifier
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import TypedConfig


class ConfigArtifact(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class PolicyArtifact(ConfigArtifact):
    category_rules: CategoryRules


class PreparedSourceSettings(ConfigArtifact):
    source_catalog: CatalogConfig
    source_documents: SnapshotConfig
    document_adapters: AdapterConfig
    command_candidates: ExtractionConfig


class ExistingDatasetsConfig(PreparedSourceSettings, PolicyArtifact):
    pass


class DocumentationCrawlConfig(PreparedSourceSettings, PolicyArtifact):
    source_catalog: CatalogConfig = Field(alias="documentation/source_catalog")
    source_documents: SnapshotConfig = Field(alias="documentation/source_documents")
    document_adapters: AdapterConfig = Field(alias="documentation/document_adapters")
    command_candidates: ExtractionConfig = Field(alias="documentation/command_candidates")


def prepared_source_settings(spec: DatasetBuildSpec, branch: Branch) -> PreparedSourceSettings:
    if branch == "keenable":
        raise IntegrityError("Keenable requires a declared source configuration")
    adapters, extraction = (
        (spec.documentation_adapters or spec.adapters, spec.documentation_extraction or spec.extraction)
        if branch == "documentation"
        else (spec.adapters, spec.extraction)
    )
    return PreparedSourceSettings(
        source_catalog=catalog_for_branch(spec, branch),
        source_documents=snapshots_for_branch(spec, branch),
        document_adapters=adapters,
        command_candidates=extraction,
    )


def require_mapped_config[Settings: Config](key: AssetKey, op: Settings, mapped: Settings) -> Settings:
    """Keep launch config compatible while using the same mapped value for computation and cache."""
    if op != mapped:
        raise IntegrityError(f"Op config for {key.to_user_string()} differs from same-run mapped config")
    return mapped


class CollectionSettings(ConfigArtifact):
    collection: DeclaredSourceStageConfig


class KeenableGrantPreview(ConfigArtifact):
    effective_cases: tuple[Case, ...]
    effective_byte_capacity: int


class DeclaredBranchConfig(CollectionSettings, PolicyArtifact):
    keenable_preview: KeenableGrantPreview | None = Field(
        default=None, exclude_if=lambda preview: preview is None
    )


class DeclaredMergeConfig(ConfigArtifact):
    merged_command_candidates: CollectionMergeConfig


class FilterSettings(ConfigArtifact):
    filtered_commands: FilteringConfig


class FilterConfig(FilterSettings, PolicyArtifact):
    pass


class LabelingDocumentationConfig(PolicyArtifact):
    labeling_documentation: DocumentationConfig


class ScopedCapacityConfig(ConfigArtifact):
    context_length: int = Field(default=262144, gt=0)
    max_fraction: float = Field(default=0.5, gt=0, le=0.5, allow_inf_nan=False)
    max_snapshot_age_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)
    poll_seconds: float = Field(default=5, gt=0, allow_inf_nan=False)
    maximum_wait_seconds: float = Field(default=900, gt=0, allow_inf_nan=False)


class ScopedBatchConfig(BatchPromptConfig, ConfigArtifact):
    backend_revision: BackendRevision
    inference_mode: Literal["guarded", "plain"] = Field(
        default="guarded", exclude_if=lambda mode: mode == "guarded"
    )
    grounding: GroundingConfig
    capacity: ScopedCapacityConfig


class ScopedAnnotationConfig(ConfigArtifact):
    model: ModelConfig
    max_request_bytes: RequestBytes = 131072
    workers: AnnotationWorkers = 1
    batch: ScopedBatchConfig | None = None


def labeling_settings(config: AnnotationConfig) -> ScopedAnnotationConfig:
    batch = config.batch
    return ScopedAnnotationConfig(
        model=config.model,
        max_request_bytes=config.max_request_bytes,
        workers=config.workers,
        batch=ScopedBatchConfig(
            instructions=batch.instructions,
            batch_size=batch.batch_size,
            reasoning_effort=batch.reasoning_effort,
            maximum_model_turns=batch.maximum_model_turns,
            maximum_grounding_rounds=batch.maximum_grounding_rounds,
            backend_revision=batch.backend_revision,
            inference_mode=batch.inference_mode,
            grounding=batch.grounding,
            capacity=ScopedCapacityConfig(
                context_length=batch.capacity.context_length,
                max_fraction=batch.capacity.max_fraction,
                max_snapshot_age_seconds=batch.capacity.max_snapshot_age_seconds,
                poll_seconds=batch.capacity.poll_seconds,
                maximum_wait_seconds=batch.capacity.maximum_wait_seconds,
            ),
        )
        if batch is not None
        else None,
    )


class GuardedLabelingSettings(ConfigArtifact):
    labeling_attempts: ScopedAnnotationConfig


class GuardedLabelingConfig(GuardedLabelingSettings, PolicyArtifact):
    pass


class PreparedLabelingConfig(PolicyArtifact):
    labeling_attempts: AnnotationConfig


class ReviewQueueSettings(ConfigArtifact):
    human_review_queue: PublicationConfig


class ReviewQueueConfig(ReviewQueueSettings, PolicyArtifact):
    pass


class DeclaredScopes(ConfigArtifact):
    existing_datasets: CollectionSettings
    documentation_crawl: CollectionSettings
    keenable_use_cases: CollectionSettings
    merged_command_candidates: DeclaredMergeConfig
    filtered_commands: FilterSettings
    labeling_attempts: GuardedLabelingSettings
    human_review_queue: ReviewQueueSettings


class DeclaredGlobalConfig(ConfigArtifact):
    dataset_id: Identifier
    revision: str
    rules: CategoryRules
    scopes: DeclaredScopes

    @classmethod
    def from_spec(cls, spec: DatasetBuildSpec) -> "DeclaredGlobalConfig":
        if spec.declared_collection is None:
            raise IntegrityError("Registered dataset has no collection declarations")
        return cls(
            dataset_id=spec.dataset_id,
            revision=spec.revision,
            rules=spec.rules,
            scopes=DeclaredScopes(
                existing_datasets=collection_asset_settings(spec, "dataset"),
                documentation_crawl=collection_asset_settings(spec, "documentation"),
                keenable_use_cases=collection_asset_settings(spec, "keenable"),
                merged_command_candidates=DeclaredMergeConfig(
                    merged_command_candidates=CollectionMergeConfig(
                        keenable_source_weight=spec.declared_collection.keenable_source_weight
                    )
                ),
                filtered_commands=FilterSettings(filtered_commands=spec.filtering),
                labeling_attempts=GuardedLabelingSettings(
                    labeling_attempts=labeling_settings(spec.annotation)
                ),
                human_review_queue=ReviewQueueSettings(human_review_queue=spec.publication),
            ),
        )


def finalization_stage_configs(spec: DatasetBuildSpec) -> dict[str, Config]:
    merge: TypedConfig = TypedConfig()
    if spec.declared_collection is not None:
        merge = CollectionMergeConfig(keenable_source_weight=spec.declared_collection.keenable_source_weight)
    return {
        "merged_command_candidates": merge,
        "filtered_commands": spec.filtering,
        **({"labeling_documentation": spec.documentation} if spec.documentation is not None else {}),
        "labeling_attempts": spec.annotation,
        "proposed_annotations": TypedConfig(),
        "labeling_failures": TypedConfig(),
        "deduplicated_annotations": TypedConfig(),
        "category_eligible_annotations": TypedConfig(),
        "human_review_queue": spec.publication,
        "human_review_decisions": HumanReviewConfig(),
        "dataset_release": HumanReviewConfig(),
    }


def collection_stage_configs(spec: DatasetBuildSpec, branch: Branch) -> dict[str, Config]:
    if spec.declared_collection is not None:
        config = declared_source_config(spec, branch)
        return {stage_name(name, branch): config for name in BRANCH_BY_NAMESPACE[branch].declared_stages}
    settings = prepared_source_settings(spec, branch)
    return {
        stage_name(name, branch): config
        for name, config in {
            "source_catalog": settings.source_catalog,
            "source_documents": settings.source_documents,
            "document_adapters": settings.document_adapters,
            "command_candidates": settings.command_candidates,
        }.items()
    }


def collection_asset_settings(spec: DatasetBuildSpec, branch: Branch) -> CollectionSettings:
    return CollectionSettings(collection=declared_source_config(spec, branch))


def stage_configs(spec: DatasetBuildSpec) -> dict[str, Config]:
    return {
        **{
            stage: config
            for branch in collection_branches(declared=spec.has_declared_collection)
            for stage, config in collection_stage_configs(spec, branch.namespace).items()
        },
        **finalization_stage_configs(spec),
    }


def declared_source_config(spec: DatasetBuildSpec, branch: Branch) -> DeclaredSourceStageConfig:
    if spec.declared_collection is None:
        raise IntegrityError("Registered dataset has no collection declarations")
    source = spec.declared_collection.for_branch(branch)
    if branch == "keenable":
        declaration = source.keenable_config()
        if declaration.search_plan is not None and declaration.search_plan.mode == "adaptive":
            model, batch = spec.annotation.model, spec.annotation.batch
            if model.provider != "openai_compatible" or model.base_url is None or batch is None:
                raise IntegrityError(
                    "Adaptive discovery requires a registered remote model and capacity settings"
                )
            return DeclaredSourceStageConfig(
                branch=branch,
                source=source,
                planner_binding=fingerprint(
                    [model.base_url, model.model, batch.capacity.model_dump_json(), batch.backend_revision]
                ),
            )
    return DeclaredSourceStageConfig(branch=branch, source=source)
