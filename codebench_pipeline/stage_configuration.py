from pydantic import BaseModel, Field

from codebench_pipeline.batch_labeling_config import (
    BackendRevision,
    BatchLabelingConfig,
    BatchPromptConfig,
    InferenceMode,
)
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import (
    CollectionMergeConfig,
    DeclaredCollection,
    DeclaredSourceStageConfig,
)
from codebench_pipeline.collection_stage_settings import (
    COLLECTION_OPERATIONS,
    CollectionOperation,
    CollectionOperationSettings,
    KeenableCandidateSettings,
)
from codebench_pipeline.config_directory import ConfigDirectorySnapshot
from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, Branch
from codebench_pipeline.dataset_config import (
    AnnotationConfig,
    AnnotationWorkers,
    DatasetBuildSpec,
    DeduplicationPolicy,
    FilteringConfig,
    PublicationConfig,
    RequestBytes,
    require_declared_annotation,
)
from codebench_pipeline.discovery_config import DiscoveryInference
from codebench_pipeline.grounding_tools import GroundingConfig
from codebench_pipeline.inference_capacity import ScopedCapacityConfig
from codebench_pipeline.labeling_stage_settings import (
    LabelingRules,
    LabelingSources,
    ProposalRules,
    RiskRoute,
)
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.release_stage_settings import (
    CategoryEligibilityConfig,
    DatasetReleaseConfig,
    PublicationTaxonomy,
    ReleaseGroundingConfig,
    ReviewQueueConfig,
)
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import ConfigArtifact, TypedConfig


class CollectionSettings(ConfigArtifact):
    collection: DeclaredSourceStageConfig


class DeclaredMergeConfig(ConfigArtifact):
    merged_command_candidates: CollectionMergeConfig


class FilterSettings(ConfigArtifact):
    filtered_commands: FilteringConfig


class FilterConfig(FilterSettings):
    routes: tuple[RiskRoute, ...]

    @classmethod
    def project(cls, registered: "DeclaredGlobalConfig", rules: CategoryRules) -> "FilterConfig":
        return cls(
            filtered_commands=registered.scopes.filtered_commands.filtered_commands,
            routes=tuple(RiskRoute.from_rule(risk) for risk in rules.risks),
        )


class DeduplicationConfig(ConfigArtifact):
    deduplication: DeduplicationPolicy


class ScopedBatchConfig(BatchPromptConfig, ConfigArtifact):
    backend_revision: BackendRevision
    inference_mode: InferenceMode = Field(default="guarded", exclude_if=lambda mode: mode == "guarded")
    grounding: GroundingConfig
    capacity: ScopedCapacityConfig | None = Field(default=None, exclude_if=lambda capacity: capacity is None)

    def bind_capacity(
        self, state_directory: str | None = None, expected_model_path: str | None = None
    ) -> BatchLabelingConfig:
        capacity = None
        if self.inference_mode == "guarded":
            if self.capacity is None or state_directory is None or expected_model_path is None:
                raise IntegrityError("Guarded labeling requires scoped capacity settings and runtime paths")
            capacity = self.capacity.bind(state_directory, expected_model_path)
        return BatchLabelingConfig.model_validate(
            {**self.model_dump(exclude={"capacity"}), "capacity": capacity}
        )


class ScopedAnnotationConfig(ConfigArtifact):
    model: ModelConfig
    max_request_bytes: RequestBytes = 131072
    workers: AnnotationWorkers = 1
    batch: ScopedBatchConfig


def labeling_settings(config: AnnotationConfig) -> ScopedAnnotationConfig:
    batch = require_declared_annotation(config)
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
            capacity=ScopedCapacityConfig.from_capacity(batch.require_capacity())
            if batch.inference_mode == "guarded"
            else None,
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


class GuardedLabelingConfig(ConfigArtifact):
    labeling_attempts: ScopedAnnotationConfig
    rules: LabelingRules
    collection: LabelingSources

    @classmethod
    def project(cls, settings: GuardedLabelingSettings, rules: CategoryRules) -> "GuardedLabelingConfig":
        return cls(
            rules=LabelingRules.from_rules(rules),
            labeling_attempts=settings.labeling_attempts,
            collection=LabelingSources.from_collection(settings.collection),
        )

    @classmethod
    def from_spec(cls, spec: DatasetBuildSpec) -> "GuardedLabelingConfig":
        return cls.project(GuardedLabelingSettings.from_spec(spec), spec.rules)


class ReviewQueueSettings(ConfigArtifact):
    human_review_queue: PublicationConfig


class DeclaredScopes(ConfigArtifact):
    existing_datasets: CollectionSettings
    documentation_crawl: CollectionSettings
    keenable_use_cases: CollectionSettings
    discovery: DiscoveryInference | None = Field(default=None, exclude_if=lambda value: value is None)
    merged_command_candidates: DeclaredMergeConfig
    filtered_commands: FilterSettings
    labeling_attempts: GuardedLabelingSettings
    deduplicated_annotations: DeduplicationConfig = Field(
        default=DeduplicationConfig(deduplication=DeduplicationPolicy()),
        exclude_if=lambda value: value.deduplication == DeduplicationPolicy(),
    )
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

    def collection_operation_settings(self, operation: CollectionOperation) -> CollectionOperationSettings:
        source = self.source_config(operation.branch)
        if operation.branch != "keenable":
            return operation.project(source)
        return KeenableCandidateSettings.project(source, self.rules, self.scopes.discovery)

    def filter_config(self) -> FilterConfig:
        return FilterConfig.project(self, self.rules)

    def labeling_config(self) -> GuardedLabelingConfig:
        return GuardedLabelingConfig.project(self.scopes.labeling_attempts, self.rules)

    def proposal_config(self) -> ProposalRules:
        return ProposalRules.from_rules(self.rules)

    def eligibility_config(self) -> CategoryEligibilityConfig:
        return CategoryEligibilityConfig(category_id=self.rules.category_id)

    def review_queue_config(self) -> ReviewQueueConfig:
        publication = self.scopes.human_review_queue.human_review_queue
        return ReviewQueueConfig.from_rules(
            self.rules, tuple(publication.review_columns), publication.limit_per_risk
        )

    def release_config(self) -> DatasetReleaseConfig:
        return dataset_release_settings(self, self.rules)

    @classmethod
    def from_spec(
        cls, spec: DatasetBuildSpec, *, directory: ConfigDirectorySnapshot | None = None
    ) -> "DeclaredGlobalConfig":
        if spec.declared_collection is None:
            raise IntegrityError("Registered dataset has no collection declarations")
        labeling = GuardedLabelingSettings.from_spec(spec)
        transport = labeling.labeling_attempts
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
                discovery=DiscoveryInference(
                    model=spec.discovery.resolve_model(transport.model, transport.batch.inference_mode),
                    backend_revision=transport.batch.backend_revision,
                    inference_mode=transport.batch.inference_mode,
                    capacity=transport.batch.capacity,
                ),
                merged_command_candidates=DeclaredMergeConfig(
                    merged_command_candidates=spec.declared_collection.merge_config
                ),
                filtered_commands=FilterSettings(filtered_commands=spec.filtering),
                labeling_attempts=labeling,
                deduplicated_annotations=DeduplicationConfig(deduplication=spec.deduplication),
                human_review_queue=ReviewQueueSettings(human_review_queue=spec.publication),
            ),
        )


def dataset_release_settings(registered: DeclaredGlobalConfig, rules: CategoryRules) -> DatasetReleaseConfig:
    labeling = registered.scopes.labeling_attempts
    return DatasetReleaseConfig(
        taxonomy=PublicationTaxonomy.from_rules(rules),
        limit_per_risk=registered.scopes.human_review_queue.human_review_queue.limit_per_risk,
        collection=LabelingSources.from_collection(labeling.collection),
        grounding=ReleaseGroundingConfig.from_grounding(labeling.labeling_attempts.batch.grounding),
    )


def collection_stage_configs(
    registered: DeclaredGlobalConfig, branch: Branch
) -> dict[str, CollectionOperationSettings]:
    return {
        operation.name: registered.collection_operation_settings(operation)
        for operation in COLLECTION_OPERATIONS
        if operation.branch == branch
    }


def collection_asset_settings(spec: DatasetBuildSpec, branch: Branch) -> CollectionSettings:
    return CollectionSettings(collection=declared_source_config(spec, branch))


def stage_configs(settings: DatasetBuildSpec | DeclaredGlobalConfig) -> dict[str, BaseModel]:
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
        "filtered_commands": registered.filter_config(),
        "labeling_attempts": registered.labeling_config(),
        "proposed_annotations": registered.proposal_config(),
        "labeling_failures": TypedConfig(),
        "deduplicated_annotations": scopes.deduplicated_annotations.deduplication,
        "category_eligible_annotations": registered.eligibility_config(),
        "human_review_queue": registered.review_queue_config(),
        "human_review_decisions": TypedConfig(),
        "dataset_release": registered.release_config(),
    }


def declared_source_config(spec: DatasetBuildSpec, branch: Branch) -> DeclaredSourceStageConfig:
    if spec.declared_collection is None:
        raise IntegrityError("Registered dataset has no collection declarations")
    return DeclaredSourceStageConfig(branch=branch, source=spec.declared_collection.for_branch(branch))
