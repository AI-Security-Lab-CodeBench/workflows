from typing import Final

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    DataVersion,
    DynamicPartitionsDefinition,
    Output,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
)

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    REGISTERED_CONFIG_TYPE,
    RegisteredConfig,
    mapping_key,
    mapping_type,
)
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.grounding_tools import LiveGroundingConfig
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Digest
from codebench_pipeline.rules_contracts import Category
from codebench_pipeline.rules_registration_config import RegisteredRules, RulesPolicy, RulesRuntime, SplitPin
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.train_config import MergeSettings, TrainConfig
from codebench_pipeline.typed_config import ConfigArtifact

RULES_REGISTRATION: Final = AssetKey(["codebench", "rules_registration"])
RULES_PARTITIONS: Final = StaticPartitionsDefinition([CODEBENCH_DATASET_ID])
CAPTURE_PARTITIONS: Final = DynamicPartitionsDefinition(name=f"{CODEBENCH_DATASET_ID}_train_categories")
RULES_CONFIG_IO: Final = "config_run_io_manager"
RULES_GROUP: Final = "rules"


class LearningPolicy(ConfigArtifact):
    categories: tuple[Category, ...]
    runtime_version: str
    guide_sha256: Digest

    @classmethod
    def project(cls, registration: RulesPolicy) -> "LearningPolicy":
        return cls(
            categories=tuple(sorted(registration.categories)),
            runtime_version=registration.runtime.version,
            guide_sha256=registration.prompt_guide.sha256,
        )


class LearningPins(LearningPolicy):
    train: SplitPin

    def matches(self, policy: LearningPolicy) -> bool:
        return (
            self.categories == policy.categories and self.runtime_version == policy.runtime_version
            and self.guide_sha256 == policy.guide_sha256
        )


class AgentSettings(ConfigArtifact):
    model: ModelConfig
    backend_revision: str
    max_request_bytes: int
    guide: str

    @property
    def inference_digest(self) -> str:
        return fingerprint(self.model_dump_json(exclude={"guide"}))


class RulesCaptureSettings(ConfigArtifact):
    category: Category
    sources_digest: Digest
    guide_sha256: Digest
    runtime: RulesRuntime
    agent: AgentSettings
    grounding: LiveGroundingConfig


class RulesFinalizeSettings(ConfigArtifact):
    policy: LearningPolicy
    train_sources: MergeSettings
    runtime: RulesRuntime


class RulesDedupSettings(RulesFinalizeSettings):
    agent: AgentSettings


class RulesEvalSettings(ConfigArtifact):
    policy: LearningPolicy
    test_sources: MergeSettings
    runtime: RulesRuntime


def _approved(config: RegisteredConfig) -> RegisteredRules:
    if config.rules_learning is None:
        raise IntegrityError("No rules policy in global_config; run rules-register first")
    return config.rules_learning


def _agent(global_config: RegisteredConfig, registered: RegisteredRules) -> AgentSettings:
    labeling = global_config.scopes.labeling_attempts.labeling_attempts
    if (
        labeling.batch.inference_mode != "plain"
        or labeling.batch.reasoning_effort != "none"
        or labeling.model.provider != "openai_compatible"
        or labeling.model.temperature != 0
    ):
        raise IntegrityError("Rules agents require registered plain OpenAI-compatible inference")
    return AgentSettings(
        model=labeling.model,
        backend_revision=labeling.batch.backend_revision,
        max_request_bytes=labeling.max_request_bytes,
        guide=registered.guide,
    )


def _output[T: ConfigArtifact](value: T) -> Output[T]:
    return Output(value, data_version=DataVersion(fingerprint(value.model_dump_json())))


def _split_sources(config: TrainConfig | None, split: str) -> MergeSettings:
    if config is None or not config.sources:
        raise IntegrityError(f"No configured {split} sources")
    return MergeSettings(category_digests={key: value.digest for key, value in config.sources_by_category().items()})


def rules_mappings() -> list[AssetsDefinition]:
    inputs = {"global_config": AssetIn(
        key=GLOBAL_CONFIG, dagster_type=REGISTERED_CONFIG_TYPE,
        partition_mapping=SpecificPartitionsPartitionMapping([CODEBENCH_DATASET_ID]),
    )}

    @asset(
        key=mapping_key("rules_capture"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_capture"},
        partitions_def=CAPTURE_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesCaptureSettings),
        ins=inputs,
    )
    def capture_settings(context: AssetExecutionContext, global_config: RegisteredConfig) -> Output[RulesCaptureSettings]:
        approved = _approved(global_config)
        rules = approved.registration
        category = context.partition_key
        if category not in rules.categories:
            raise IntegrityError("Capture partition is not selected by the approved rules policy")
        sources = _split_sources(global_config.train, "train").category_digests
        if category not in sources:
            raise IntegrityError(f"No pinned train sources configured for {category}")
        return _output(RulesCaptureSettings(
            category=category, sources_digest=sources[category],
            guide_sha256=rules.prompt_guide.sha256, runtime=rules.runtime, agent=_agent(global_config, approved),
            grounding=global_config.scopes.labeling_attempts.labeling_attempts.batch.grounding,
        ))

    @asset(
        key=mapping_key("rules_dedup"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_dedup"},
        partitions_def=RULES_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesDedupSettings),
        ins=inputs,
    )
    def dedup_settings(global_config: RegisteredConfig) -> Output[RulesDedupSettings]:
        approved = _approved(global_config)
        rules = approved.registration
        return _output(RulesDedupSettings(
            policy=LearningPolicy.project(rules), train_sources=_split_sources(global_config.train, "train"),
            runtime=rules.runtime, agent=_agent(global_config, approved),
        ))

    @asset(
        key=mapping_key("rules_finalize"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_finalize"},
        partitions_def=RULES_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesFinalizeSettings),
        ins=inputs,
    )
    def finalize_settings(global_config: RegisteredConfig) -> Output[RulesFinalizeSettings]:
        rules = _approved(global_config).registration
        return _output(RulesFinalizeSettings(
            policy=LearningPolicy.project(rules), train_sources=_split_sources(global_config.train, "train"),
            runtime=rules.runtime,
        ))

    @asset(
        key=mapping_key("rules_eval"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_eval"},
        partitions_def=RULES_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesEvalSettings),
        ins=inputs,
    )
    def eval_settings(global_config: RegisteredConfig) -> Output[RulesEvalSettings]:
        rules = _approved(global_config).registration
        return _output(RulesEvalSettings(
            policy=LearningPolicy.project(rules), test_sources=_split_sources(global_config.test, "test"),
            runtime=rules.runtime,
        ))

    return [capture_settings, dedup_settings, finalize_settings, eval_settings]
