import json
from collections.abc import Iterable
from pathlib import Path
from typing import Final

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    Config,
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    asset,
)
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, rules_input
from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    FINALIZATION_STAGES,
    Branch,
    collection_branches,
    stage_branch,
)
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_registry import DatasetRegistry, read_registered_spec
from codebench_pipeline.stage_configuration import collection_stage_configs, finalization_stage_configs
from codebench_pipeline.storage import IntegrityError

GLOBAL_CONFIG: Final = AssetKey(["datasets", "global_config"])
POLICY_SCOPES: Final = frozenset(
    {
        "existing_datasets",
        "documentation_crawl",
        "keenable_use_cases",
        "filtered_commands",
        "labeling_documentation",
        "labeling_attempts",
        "proposed_annotations",
        "category_eligible_annotations",
        "human_review_queue",
        "dataset_release",
    }
)
COLLECTION_SCOPES: Final = {branch.namespace: branch.group for branch in COLLECTION_BRANCHES}
CONFIG_JSON: Final = TypeAdapter(dict[str, JsonValue])


class GlobalConfig(Config):
    snapshot: str = ""


def mapping_key(scope: str) -> AssetKey:
    return AssetKey(["datasets", f"map_{scope}_config"])


def configuration_key(stage: str) -> AssetKey:
    if stage == "category_rules":
        return GLOBAL_CONFIG
    return mapping_key(stage if stage in FINALIZATION_STAGES else COLLECTION_SCOPES[stage_branch(stage)])


def configured_selection(stages: Iterable[AssetKey]) -> list[AssetKey]:
    selection = list(stages)
    return list(
        dict.fromkeys(
            [
                *selection,
                *(configuration_key("/".join(key.path[1:])) for key in selection),
                *([GLOBAL_CONFIG] if selection else []),
            ]
        )
    )


def policy_input(scope: str) -> AssetIn:
    return AssetIn(key=mapping_key(scope), dagster_type=str)


def mapped_rules(mapped_config: str) -> CategoryRules:
    return CategoryRules.model_validate(CONFIG_JSON.validate_json(mapped_config)["category_rules"])


def mapped_settings[Settings: Config](
    mapped_config: str, stage: str, settings_type: type[Settings]
) -> Settings:
    settings = CONFIG_JSON.validate_json(mapped_config)
    if settings.keys() != {stage}:
        raise IntegrityError(f"Expected same-run settings scoped to {stage}")
    return settings_type.model_validate(settings[stage])


class ConfigRunIOManager(ConfigurableIOManager):
    """Persist configuration JSON for cross-process use without cross-run fallback."""

    home: str

    def handle_output(self, context: OutputContext, obj: str) -> None:
        path = Path(self.home, "configuration-runs", context.run_id, *context.asset_key.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(obj, encoding="utf-8")

    def load_input(self, context: InputContext) -> str:
        path = Path(self.home, "configuration-runs", context.step_context.run_id, *context.asset_key.path)
        try:
            return path.read_text(encoding="utf-8")
        except FileNotFoundError as error:
            raise IntegrityError(
                f"Missing configuration {context.asset_key.to_user_string()} in this run; "
                "include configuration assets when selecting processing stages"
            ) from error


def configuration_assets(home: Path, *, declared: bool = False) -> list[AssetsDefinition]:
    @asset(
        key=GLOBAL_CONFIG,
        partitions_def=DATASET_PARTITIONS,
        group_name="dataset_configuration",
        io_manager_key="config_run_io_manager",
        description="**How it works:** Read the validated configuration for this dataset partition. "
        "Planned builds use their pinned snapshot; other runs read the registry.",
    )
    def global_config(context: AssetExecutionContext, config: GlobalConfig) -> str:
        registry = DatasetRegistry(home)
        spec = (
            read_registered_spec(config.snapshot.encode())
            if config.snapshot
            else registry.get(context.partition_key)
        )
        if spec.dataset_id != context.partition_key:
            raise IntegrityError("Configuration snapshot belongs to another dataset partition")
        if spec.has_declared_collection != declared:
            raise IntegrityError("Dataset registration changed configured source inputs; reload definitions")
        if declared and spec != registry.get_snapshot(context.partition_key):
            raise IntegrityError("Run configuration no longer matches the registered specification")
        return spec.model_dump_json()

    def mapping(scope: str) -> AssetsDefinition:
        needs_policy = scope in POLICY_SCOPES
        branch: Branch | None = next(
            (item.namespace for item in COLLECTION_BRANCHES if item.group == scope), None
        )

        def project(registered: str, category_rules: CategoryRules | None = None) -> str:
            spec = DatasetBuildSpec.model_validate_json(registered)
            if needs_policy and category_rules != spec.rules:
                raise IntegrityError("Category rules differ from the run configuration; replan the build")
            settings = (
                {scope: finalization_stage_configs(spec)[scope]}
                if branch is None
                else collection_stage_configs(spec, branch)
            )
            payload = {key: value.model_dump(mode="json") for key, value in settings.items()}
            if category_rules is not None:
                payload["category_rules"] = category_rules.model_dump(mode="json")
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        def settings_only(registered: str) -> str:
            return project(registered)

        return asset(
            key=mapping_key(scope),
            partitions_def=DATASET_PARTITIONS,
            group_name="dataset_configuration",
            io_manager_key="config_run_io_manager",
            ins={
                "registered": AssetIn(key=GLOBAL_CONFIG),
                **({"category_rules": rules_input()} if needs_policy else {}),
            },
            description=f"**How it works:** Project registered stage settings for {scope} into JSON "
            "keyed by consuming stage. Policy mappings also include the validated category rules.",
        )(project if needs_policy else settings_only)

    return [
        global_config,
        *(mapping(branch.group) for branch in collection_branches(declared=declared)),
        *(
            mapping(scope)
            for scope in FINALIZATION_STAGES
            if not declared or scope != "labeling_documentation"
        ),
    ]
