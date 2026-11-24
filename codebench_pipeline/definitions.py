import os
from pathlib import Path
from typing import Protocol

from dagster import (
    AssetCheckKey,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    AssetSpec,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    IOManagerDefinition,
    PartitionedConfig,
    ResourceDefinition,
    SensorDefinition,
    SensorEvaluationContext,
    SensorResult,
)
from dagster._core.definitions.declarative_automation.operators import (
    AnyDepsCondition,
    EntityMatchesCondition,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.bashguard_audit import BashGuardAnalyzer
from codebench_pipeline.bashguard_audit_asset import (
    AUDIT_IO_MANAGER,
    bashguard_audit_job,
    bashguard_syntax_audit,
)
from codebench_pipeline.config_assets import (
    CONFIG_DIRECTORY,
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    configuration_assets,
    mapping_key,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_assets_annotations import annotation_assets
from codebench_pipeline.dataset_assets_sources import category_rules, filtered_commands
from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, DECLARED_SOURCE_STAGES, asset_branch
from codebench_pipeline.dataset_dependencies import RELEASE_STAGES
from codebench_pipeline.dataset_engine import scoped_code_versions
from codebench_pipeline.dataset_finalization import finalization_assets
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run_configuration import dataset_run_config, release_run_config
from codebench_pipeline.dataset_storage_io import dataset_storage_io
from codebench_pipeline.declared_source_assets import KeenableDiscovery, declared_source_assets
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO
from codebench_pipeline.finalization_table_io import finalization_table_io
from codebench_pipeline.labeling_checkpoint_io import labeling_checkpoint_resource
from codebench_pipeline.labeling_runtime import LabelingRuntime
from codebench_pipeline.repository_legacy import PreparedCaptureCache
from codebench_pipeline.review_assets import human_assets
from codebench_pipeline.review_inputs import REVIEW_INPUT_MANAGER, review_input_manager
from codebench_pipeline.stage_descriptions import describe_stage
from codebench_pipeline.stage_implementation import ImplementationSource
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.train_assets import (
    TRAIN_JOBS,
    TRAIN_MAPPINGS,
    TRAIN_TABLE_IO,
    TRAIN_TABLES,
    train_partition_inputs,
)

DEFAULT_HOME = Path(os.environ.get("CODEBENCH_HOME", ".codebench-datasets")).resolve()

__all__ = ["DEFAULT_HOME", "build_definitions", "dataset_run_config", "defs", "release_run_config"]


class AutomationAsset(Protocol):
    @property
    def key(self) -> AssetKey: ...

    @property
    def code_version(self) -> str | None: ...

    def replace_attributes(
        self,
        *,
        automation_condition: AutomationCondition[AssetKey | AssetCheckKey],
        code_version: str | None,
    ) -> AssetSpec: ...


def automate_dataset_asset(spec: AutomationAsset, config_assets: AssetSelection) -> AssetSpec:
    stage = spec.key.path[-1]
    branch = asset_branch(spec.key.path) if stage in DECLARED_SOURCE_STAGES else None
    configuration = stage.startswith("map_")
    updates = AnyDepsCondition[AssetKey | AssetCheckKey](
        operand=AutomationCondition.data_version_changed() & ~AutomationCondition.executed_with_root_target(),
    )
    changes: AutomationCondition[AssetKey | AssetCheckKey] = updates
    if not configuration:
        changes |= EntityMatchesCondition[AssetKey | AssetCheckKey, AssetKey](
            key=spec.key, operand=AutomationCondition.code_version_changed()
        )
    updated_in_run = AnyDepsCondition[AssetKey | AssetCheckKey](
        operand=AutomationCondition.will_be_requested(), ignore_selection=config_assets
    )
    awaiting_projection = AnyDepsCondition[AssetKey | AssetCheckKey](
        operand=AutomationCondition.will_be_requested(), allow_selection=config_assets
    )
    condition = (
        (
            (AutomationCondition.missing() & ~AutomationCondition.execution_failed())
            | changes.since(AutomationCondition.newly_requested() & ~changes)
            | updated_in_run
        )
        & ~AutomationCondition.any_deps_missing()
        & ~AutomationCondition.any_deps_in_progress()
        & ~awaiting_projection
        & ~AutomationCondition.in_progress()
    )
    if configuration:
        version = spec.code_version
    else:
        version = fingerprint({**scoped_code_versions(stage, ImplementationSource(), branch)})
    return spec.replace_attributes(automation_condition=condition, code_version=version)


def _require_scoped_configuration_edges(
    configuration: list[AssetsDefinition], consumers: list[AssetsDefinition]
) -> None:
    specs = [spec for definition in [*configuration, *consumers] for spec in definition.specs]
    groups = {spec.key: spec.group_name for spec in specs}
    mappings = {definition.key for definition in configuration if definition.key.path[-1].startswith("map_")}
    for spec in specs:
        for dependency in spec.deps:
            parent = dependency.asset_key
            if groups.get(parent) == "configuration" and spec.group_name != "configuration":
                if spec.key not in mappings:
                    raise IntegrityError(
                        f"{spec.key.to_user_string()} bypasses scoped configuration: {parent.to_user_string()}"
                    )
            if parent in mappings and groups[parent] != spec.group_name:
                raise IntegrityError(
                    f"{parent.to_user_string()} must belong to the group of {spec.key.to_user_string()}"
                )


def build_definitions(home: Path) -> Definitions:
    registry = DatasetRegistry(home)
    config_io = ConfigRunIOManager(home=str(home))
    config_definitions = configuration_assets(home)
    config_assets = AssetSelection.assets(*config_definitions, category_rules)
    assets = [
        definition.map_asset_specs(describe_stage)
        for definition in [
            category_rules,
            *(asset for branch in COLLECTION_BRANCHES for asset in declared_source_assets(branch)),
            filtered_commands,
            *annotation_assets(),
            *human_assets(),
            *finalization_assets(),
        ]
    ]
    _require_scoped_configuration_edges(
        [*config_definitions, *TRAIN_MAPPINGS], [*assets, *TRAIN_TABLES, bashguard_syntax_audit]
    )

    def partition_config(key: str) -> dict[str, JsonValue]:
        return TypeAdapter(dict[str, JsonValue]).validate_python(
            dataset_run_config(registry.get(key)).to_config_dict()
        )

    def release_config(key: str) -> dict[str, JsonValue]:
        return TypeAdapter(dict[str, JsonValue]).validate_python(
            release_run_config(registry.get(key)).to_config_dict()
        )

    def partition_tags(key: str) -> dict[str, str]:
        spec = registry.get(key)
        return {"dataset/category": spec.rules.category_id, "dataset/revision": spec.revision}

    build_config = PartitionedConfig(
        DATASET_PARTITIONS,
        run_config_for_partition_key_fn=partition_config,
        tags_for_partition_key_fn=partition_tags,
    )
    live_configuration = AssetSelection.assets(CONFIG_DIRECTORY)
    build_selection = (
        AssetSelection.assets(
            ["datasets", "human_review_queue"], ["datasets", "labeling_failures"], category_rules
        ).upstream()
        - live_configuration
    )
    automation_selection = build_selection - AssetSelection.assets(GLOBAL_CONFIG, category_rules)

    def automate(spec: AssetSpec) -> AssetSpec:
        if (
            spec.key in {CONFIG_DIRECTORY, GLOBAL_CONFIG, category_rules.key}
            or spec.key.path[-1] in RELEASE_STAGES
        ):
            return spec
        return automate_dataset_asset(spec, config_assets)

    job = UnresolvedAssetJobDefinition(
        name="build_dataset",
        description="Process eligible corpus and documentation inventories without source approval.",
        selection=build_selection,
        config=build_config,
    )

    collection_jobs = [
        UnresolvedAssetJobDefinition(
            name=branch.job,
            description=f"Acquire and extract {branch.namespace} candidates independently of labeling and review.",
            selection=AssetSelection.assets(AssetKey(["datasets", *branch.terminal.split("/")])).upstream()
            - live_configuration,
            config=build_config,
        )
        for branch in COLLECTION_BRANCHES
    ]
    preparation_job = UnresolvedAssetJobDefinition(
        name="prepare_dataset",
        description="Acquire and filter all three branches before labeling.",
        selection=AssetSelection.assets(filtered_commands, category_rules).upstream() - live_configuration,
        config=build_config,
    )
    release_job = UnresolvedAssetJobDefinition(
        name="release_dataset",
        description="Release eligible low-uncertainty rows automatically; uncertain rows require completed human review.",
        selection=AssetSelection.assets(
            GLOBAL_CONFIG,
            mapping_key("dataset_release"),
            *(AssetKey(["datasets", stage]) for stage in RELEASE_STAGES),
        ),
        config=PartitionedConfig(
            DATASET_PARTITIONS,
            run_config_for_partition_key_fn=release_config,
            tags_for_partition_key_fn=partition_tags,
        ),
    )
    configuration_job = UnresolvedAssetJobDefinition(
        name="refresh_dataset_configuration",
        description="Materialize registered settings and category rules without running processing stages.",
        selection=AssetSelection.assets(GLOBAL_CONFIG, category_rules),
    )
    configuration_observation_job = UnresolvedAssetJobDefinition(
        name="observe_dataset_configuration",
        description="Capture the live authored configuration for a dataset partition without approving it.",
        selection=live_configuration,
    )

    def release_requests(context: SensorEvaluationContext) -> SensorResult:
        from codebench_pipeline.release_runner import dataset_release_requests

        return dataset_release_requests(context, home)

    return Definitions(
        assets=[
            *(definition.map_asset_specs(automate) for definition in [*config_definitions, *assets]),
            bashguard_syntax_audit,
            *TRAIN_MAPPINGS,
            *TRAIN_TABLES,
        ],
        jobs=[
            job,
            *collection_jobs,
            preparation_job,
            release_job,
            configuration_job,
            configuration_observation_job,
            bashguard_audit_job(),
            *TRAIN_JOBS,
        ],
        sensors=[
            AutomationConditionSensorDefinition(
                name="dataset_build_requests",
                target=automation_selection,
                default_status=DefaultSensorStatus.STOPPED,
            ),
            SensorDefinition(
                name="dataset_release_requests",
                job=release_job,
                evaluation_fn=release_requests,
                default_status=DefaultSensorStatus.RUNNING,
                description="Continue release for a completed review snapshot that passes publication gates.",
            ),
        ],
        resources={
            FINALIZATION_TABLE_IO: finalization_table_io,
            "bashguard_analyzer": ResourceDefinition(
                resource_fn=BashGuardAnalyzer.from_environment, config_schema={}
            ),
            "keenable_credential": ResourceDefinition(
                resource_fn=KeenableDiscovery.from_environment, config_schema={}
            ),
            "prepared_capture_cache": ResourceDefinition(
                resource_fn=lambda: PreparedCaptureCache(home / "source-downloads"), config_schema={}
            ),
            "labeling_runtime": ResourceDefinition(
                resource_fn=lambda: LabelingRuntime.for_home(home), config_schema={}
            ),
            "labeling_checkpoints": labeling_checkpoint_resource(home),
            "config_run_io_manager": IOManagerDefinition(resource_fn=lambda: config_io, config_schema={}),
            AUDIT_IO_MANAGER: dataset_storage_io(home, review=False),
            TRAIN_TABLE_IO: dataset_storage_io(home, review=False, multipartition_dimension="dataset"),
            "train_partition_inputs": train_partition_inputs,
            "dataset_io_manager": dataset_storage_io(home),
            REVIEW_INPUT_MANAGER: review_input_manager,
        },
    )


defs = build_definitions(DEFAULT_HOME)
