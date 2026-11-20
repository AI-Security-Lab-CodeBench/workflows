from typing import Literal

from dagster import Config
from pydantic import BaseModel, ConfigDict, Field

from codebench_pipeline.batch_labeling_config import BackendRevision, BatchLabelingConfig, BatchPromptConfig
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import (
    AdaptivePlannerConfig,
    CollectionMergeConfig,
    DeclaredCollection,
    DeclaredSourceStageConfig,
)
from codebench_pipeline.config_directory import ConfigDirectorySnapshot
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, COLLECTION_BRANCHES, Branch, stage_name
from codebench_pipeline.dataset_config import (
    AnnotationConfig,
    AnnotationWorkers,
    DatasetBuildSpec,
    FilteringConfig,
    PublicationConfig,
    RequestBytes,
    require_guarded_annotation,
)
from codebench_pipeline.grounding_tools import GroundingConfig
from codebench_pipeline.inference_capacity import ScopedCapacityConfig
from codebench_pipeline.keenable_policy import Case
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import TypedConfig


class ConfigArtifact(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class PolicyArtifact(ConfigArtifact):
    category_rules: CategoryRules


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


class ScopedBatchConfig(BatchPromptConfig, ConfigArtifact):
    backend_revision: BackendRevision
    inference_mode: Literal["guarded"] = Field(default="guarded", exclude_if=lambda mode: mode == "guarded")
    grounding: GroundingConfig
    capacity: ScopedCapacityConfig

    def bind_capacity(self, state_directory: str, expected_model_path: str) -> BatchLabelingConfig:
        return BatchLabelingConfig.model_validate(
            {
                **self.model_dump(exclude={"capacity"}),
                "capacity": self.capacity.bind(state_directory, expected_model_path),
            }
        )


class ScopedAnnotationConfig(ConfigArtifact):
    model: ModelConfig
    max_request_bytes: RequestBytes = 131072
    workers: AnnotationWorkers = 1
    batch: ScopedBatchConfig


def labeling_settings(config: AnnotationConfig) -> ScopedAnnotationConfig:
    batch = require_guarded_annotation(config)
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
            grounding=batch.grounding,
            capacity=ScopedCapacityConfig.from_capacity(batch.capacity),
        ),
    )


class GuardedLabelingSettings(ConfigArtifact):
    labeling_attempts: ScopedAnnotationConfig
    collection: DeclaredCollection

    @classmethod
    def from_spec(cls, spec: DatasetBuildSpec) -> "GuardedLabelingSettings":
        collection = spec.declared_collection
        if collection is None:
            raise IntegrityError("Guarded labeling requires a declared collection")
        collection.require_current_declarations()
        return cls(labeling_attempts=labeling_settings(spec.annotation), collection=collection)


class GuardedLabelingConfig(GuardedLabelingSettings, PolicyArtifact):
    @classmethod
    def from_spec(cls, spec: DatasetBuildSpec) -> "GuardedLabelingConfig":
        settings = GuardedLabelingSettings.from_spec(spec)
        return cls(
            category_rules=spec.rules,
            labeling_attempts=settings.labeling_attempts,
            collection=settings.collection,
        )


class GuardedLabelingCacheConfig(TypedConfig):
    labeling_attempts: ScopedAnnotationConfig
    declaration_digests: tuple[Digest, ...]

    @classmethod
    def from_settings(cls, settings: GuardedLabelingSettings) -> "GuardedLabelingCacheConfig":
        return cls(
            labeling_attempts=settings.labeling_attempts,
            declaration_digests=tuple(
                settings.collection.for_branch(branch.namespace).digest for branch in COLLECTION_BRANCHES
            ),
        )


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
    registration_digest: Digest
    config_directory: ConfigDirectorySnapshot | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    rules: CategoryRules
    scopes: DeclaredScopes

    def source_config(self, branch: Branch) -> DeclaredSourceStageConfig:
        return {
            "dataset": self.scopes.existing_datasets,
            "documentation": self.scopes.documentation_crawl,
            "keenable": self.scopes.keenable_use_cases,
        }[branch].collection

    @classmethod
    def from_spec(
        cls, spec: DatasetBuildSpec, *, directory: ConfigDirectorySnapshot | None = None
    ) -> "DeclaredGlobalConfig":
        if spec.declared_collection is None:
            raise IntegrityError("Registered dataset has no collection declarations")
        return cls(
            dataset_id=spec.dataset_id,
            revision=spec.revision,
            registration_digest=fingerprint(spec.model_dump_json()),
            config_directory=spec.config_directory if directory is None else directory,
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
                labeling_attempts=GuardedLabelingSettings.from_spec(spec),
                human_review_queue=ReviewQueueSettings(human_review_queue=spec.publication),
            ),
        )


def collection_stage_configs(registered: DeclaredGlobalConfig, branch: Branch) -> dict[str, Config]:
    config = registered.source_config(branch)
    return {stage_name(name, branch): config for name in BRANCH_BY_NAMESPACE[branch].declared_stages}


def collection_asset_settings(spec: DatasetBuildSpec, branch: Branch) -> CollectionSettings:
    return CollectionSettings(collection=declared_source_config(spec, branch))


def stage_configs(settings: DatasetBuildSpec | DeclaredGlobalConfig) -> dict[str, Config]:
    registered = (
        DeclaredGlobalConfig.from_spec(settings) if isinstance(settings, DatasetBuildSpec) else settings
    )
    scopes = registered.scopes
    return {
        **{
            stage: config
            for branch in COLLECTION_BRANCHES
            for stage, config in collection_stage_configs(registered, branch.namespace).items()
        },
        "merged_command_candidates": scopes.merged_command_candidates.merged_command_candidates,
        "filtered_commands": scopes.filtered_commands.filtered_commands,
        "labeling_attempts": GuardedLabelingCacheConfig.from_settings(scopes.labeling_attempts),
        "proposed_annotations": TypedConfig(),
        "labeling_failures": TypedConfig(),
        "deduplicated_annotations": TypedConfig(),
        "category_eligible_annotations": TypedConfig(),
        "human_review_queue": scopes.human_review_queue.human_review_queue,
        "human_review_decisions": HumanReviewConfig(),
        "dataset_release": HumanReviewConfig(),
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
                planner=AdaptivePlannerConfig(
                    base_url=model.base_url,
                    model=model.model,
                    api_key_env=model.api_key_env,
                    capacity=ScopedCapacityConfig.from_capacity(batch.capacity),
                    backend_revision=batch.backend_revision,
                ),
            )
    return DeclaredSourceStageConfig(branch=branch, source=source)
