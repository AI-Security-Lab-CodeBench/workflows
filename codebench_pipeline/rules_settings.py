from pathlib import Path
from typing import Annotated, Final, Literal, Self
from urllib.parse import urlsplit

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    Config,
    DataVersion,
    DynamicPartitionsDefinition,
    Output,
    PythonObjectDagsterType,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
)
from pydantic import Field, JsonValue, field_validator, model_validator

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    REGISTERED_CONFIG_TYPE,
    RegisteredConfig,
    mapping_key,
    mapping_type,
)
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import sha256
from codebench_pipeline.rules_contracts import Category, Split
from codebench_pipeline.storage import IntegrityError, fingerprint, open_directory, open_regular_file
from codebench_pipeline.typed_config import ConfigArtifact

RULES_REGISTRATION: Final = AssetKey(["codebench", "rules_registration"])
RULES_PARTITIONS: Final = StaticPartitionsDefinition([CODEBENCH_DATASET_ID])
CAPTURE_PARTITIONS: Final = DynamicPartitionsDefinition(name=f"{CODEBENCH_DATASET_ID}_train_categories")
RULES_CONFIG_IO: Final = "config_run_io_manager"
RULES_GROUP: Final = "rules"
_DEFAULT_CONFIGURATION: Final = Path(__file__).resolve().parents[1] / "business-configs" / "bashguard"
_MAX_CONFIG_BYTES: Final = 1024 * 1024


class SplitPin(ConfigArtifact):
    digest: Digest
    row_count: int = Field(gt=0)


class RulesRuntime(ConfigArtifact):
    base_url: str
    version: Annotated[str, Field(pattern=r"^sha256:[a-f0-9]{64}$")]

    @field_validator("base_url")
    @classmethod
    def require_endpoint(cls, value: str) -> str:
        endpoint = urlsplit(value)
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or endpoint.username is not None
            or endpoint.path not in {"", "/"}
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError("Expected a Bashguard HTTP origin without credentials")
        return value


class GuidePin(ConfigArtifact):
    path: Literal["guide.json"]
    sha256: Digest


class RulesRegistration(ConfigArtifact):
    runtime: RulesRuntime
    prompt_guide: GuidePin
    categories: tuple[Category, ...] = Field(min_length=1)
    train: SplitPin | None = None
    test: SplitPin | None = None

    @model_validator(mode="after")
    def require_distinct_partitions(self) -> Self:
        if len(set(self.categories)) != len(self.categories) or (
            self.train is not None and self.test is not None and self.train.digest == self.test.digest
        ):
            raise IntegrityError("Rules registration requires distinct categories and split digests")
        return self

    def require_pin(self, split: Split) -> SplitPin:
        pin = self.train if split == "train" else self.test
        if pin is None:
            raise IntegrityError(f"Register the merged {split} digest and row count before running this stage")
        return pin


class PromptGuide(ConfigArtifact):
    instructions: str = Field(min_length=1)
    examples: tuple[dict[str, JsonValue], ...] = Field(min_length=1)


class RegisteredRules(ConfigArtifact):
    registration: RulesRegistration
    digest: Digest
    guide: str


_REGISTERED_RULES_TYPE: Final = PythonObjectDagsterType(RegisteredRules, name="RegisteredRules")


class LearningPins(ConfigArtifact):
    train: SplitPin
    categories: tuple[Category, ...]
    runtime_version: str
    guide_sha256: Digest


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


class RulesDedupSettings(ConfigArtifact):
    pins: LearningPins
    category_sources_digests: dict[Category, Digest]
    runtime: RulesRuntime
    agent: AgentSettings


class RulesFinalizeSettings(ConfigArtifact):
    pins: LearningPins
    runtime: RulesRuntime


class RulesEvalSettings(ConfigArtifact):
    pins: LearningPins
    test: SplitPin
    runtime: RulesRuntime


class RulesRegistrationConfig(Config):
    expected_sha256: Digest
    registration_json: str
    guide_json: str


def read_rules_guide() -> str:
    with open_directory(_DEFAULT_CONFIGURATION) as handle, open_regular_file(handle, "guide.json") as stream:
        content = stream.read(_MAX_CONFIG_BYTES + 1)
    if len(content) > _MAX_CONFIG_BYTES:
        raise IntegrityError("Rules guide exceeds the registration size limit")
    return content.decode("utf-8")


@asset(
    key=RULES_REGISTRATION,
    partitions_def=RULES_PARTITIONS,
    group_name=RULES_GROUP,
    tags={"stage": "rules_registration"},
    io_manager_key=RULES_CONFIG_IO,
    dagster_type=_REGISTERED_RULES_TYPE,
)
def rules_registration(config: RulesRegistrationConfig) -> Output[RegisteredRules]:
    """Verify the submitted rules registration and shared guide before publishing either."""
    payload = config.registration_json.encode()
    if len(payload) > _MAX_CONFIG_BYTES or sha256(payload) != config.expected_sha256:
        raise IntegrityError("Rules registration digest mismatch")
    registration = RulesRegistration.model_validate_json(payload)
    guide = config.guide_json.encode()
    if len(guide) > _MAX_CONFIG_BYTES or sha256(guide) != registration.prompt_guide.sha256:
        raise IntegrityError(f"Rules configuration digest mismatch: {registration.prompt_guide.path}")
    PromptGuide.model_validate_json(guide)
    value = RegisteredRules(registration=registration, digest=config.expected_sha256, guide=config.guide_json)
    return _output(value)


def _pins(registration: RulesRegistration) -> LearningPins:
    return LearningPins(
        train=registration.require_pin("train"),
        categories=tuple(sorted(registration.categories)),
        runtime_version=registration.runtime.version,
        guide_sha256=registration.prompt_guide.sha256,
    )


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


def _category_source_digests(config: RegisteredConfig, categories: tuple[Category, ...]) -> dict[Category, Digest]:
    sources = config.train.sources_by_category() if config.train is not None else {}
    if missing := set(categories) - sources.keys():
        raise IntegrityError(f"No pinned train sources configured for: {', '.join(sorted(missing))}")
    return {category: sources[category].digest for category in categories}


def rules_mappings() -> list[AssetsDefinition]:
    category_mapping = SpecificPartitionsPartitionMapping([CODEBENCH_DATASET_ID])
    capture_inputs = {
        "registered": AssetIn(
            key=RULES_REGISTRATION, dagster_type=_REGISTERED_RULES_TYPE, partition_mapping=category_mapping
        ),
        "global_config": AssetIn(
            key=GLOBAL_CONFIG, dagster_type=REGISTERED_CONFIG_TYPE, partition_mapping=category_mapping
        ),
    }
    singleton_inputs = {"registered": AssetIn(key=RULES_REGISTRATION, dagster_type=_REGISTERED_RULES_TYPE)}

    @asset(
        key=mapping_key("rules_capture"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_capture"},
        partitions_def=CAPTURE_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesCaptureSettings),
        ins=capture_inputs,
    )
    def capture_settings(
        context: AssetExecutionContext, registered: RegisteredRules, global_config: RegisteredConfig
    ) -> Output[RulesCaptureSettings]:
        rules = registered.registration
        if context.partition_key not in rules.categories:
            raise IntegrityError("Capture partition is not selected by the pinned rules registration")
        category = context.partition_key
        return _output(RulesCaptureSettings(
            category=category, sources_digest=_category_source_digests(global_config, (category,))[category],
            guide_sha256=rules.prompt_guide.sha256, runtime=rules.runtime,
            agent=_agent(global_config, registered),
        ))

    @asset(
        key=mapping_key("rules_dedup"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_dedup"},
        partitions_def=RULES_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesDedupSettings),
        ins={
            **singleton_inputs,
            "global_config": AssetIn(
                key=GLOBAL_CONFIG, dagster_type=REGISTERED_CONFIG_TYPE, partition_mapping=category_mapping
            ),
        },
    )
    def dedup_settings(registered: RegisteredRules, global_config: RegisteredConfig) -> Output[RulesDedupSettings]:
        rules = registered.registration
        return _output(RulesDedupSettings(
            pins=_pins(rules), category_sources_digests=_category_source_digests(global_config, rules.categories),
            runtime=rules.runtime, agent=_agent(global_config, registered),
        ))

    @asset(
        key=mapping_key("rules_finalize"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_finalize"},
        partitions_def=RULES_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesFinalizeSettings),
        ins=singleton_inputs,
    )
    def finalize_settings(registered: RegisteredRules) -> Output[RulesFinalizeSettings]:
        rules = registered.registration
        return _output(RulesFinalizeSettings(pins=_pins(rules), runtime=rules.runtime))

    @asset(
        key=mapping_key("rules_eval"),
        group_name=RULES_GROUP,
        tags={"stage": "rules_eval"},
        partitions_def=RULES_PARTITIONS,
        io_manager_key=RULES_CONFIG_IO,
        dagster_type=mapping_type(RulesEvalSettings),
        ins=singleton_inputs,
    )
    def eval_settings(registered: RegisteredRules) -> Output[RulesEvalSettings]:
        rules = registered.registration
        return _output(RulesEvalSettings(pins=_pins(rules), test=rules.require_pin("test"), runtime=rules.runtime))

    return [capture_settings, dedup_settings, finalize_settings, eval_settings]
