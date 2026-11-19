from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, Config, DagsterInstance, build_input_context
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.config_assets import configured_selection
from codebench_pipeline.dataframe_io import (
    REVIEW_PREPARATION,
    AssetReference,
    AssetSnapshot,
    DataFrameIOManager,
)
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import (
    FINALIZATION_STAGES,
    Branch,
    collection_branches,
    stage_branch,
    stage_name,
)
from codebench_pipeline.dataset_catalog import source_scope
from codebench_pipeline.dataset_config import DatasetBuildSpec, catalog_for_branch
from codebench_pipeline.dataset_dependencies import (
    DECLARED_INPUT_STAGES,
    INPUT_STAGES,
    KNOWN_STAGES,
    PREPARATION_STAGES,
    RELEASE_STAGES,
    collection_selection,
    preparation_stages,
)
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.documentation_authorization import (
    SourceInventoryAuthorization,
    frame_inventory_authorization,
)
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.stage_cache_compatibility import legacy_signature_matches
from codebench_pipeline.stage_configuration import collection_stage_configs, stage_configs
from codebench_pipeline.storage import IntegrityError, fingerprint

BranchUnavailable = Literal["waiting", "unconfigured", "failed", "stale"]

__all__ = [
    "INPUT_STAGES",
    "KNOWN_STAGES",
    "PREPARATION_STAGES",
    "RELEASE_STAGES",
    "DECLARED_INPUT_STAGES",
    "BuildPlan",
    "BranchUnavailable",
    "collection_selection",
    "graph_materialization_stale",
    "plan_dataset",
    "preparation_stages",
    "unavailable_candidates",
]


def unavailable_candidates(spec: DatasetBuildSpec, branch: Branch, state: BranchUnavailable) -> pd.DataFrame:
    empty = validated_frame([], CommandCandidatesTable)
    empty.attrs["branch_state"] = state
    empty.attrs["branch_configuration"] = fingerprint(
        {
            "stages": {
                stage: config.model_dump_json()
                for stage, config in collection_stage_configs(spec, branch).items()
            },
            "rules": source_scope(spec.rules),
        }
    )
    return empty


@dataclass(frozen=True, slots=True)
class BuildPlan:
    selected: tuple[str, ...]
    unchanged: tuple[str, ...]
    reasons: dict[str, str]
    waiting_for: Literal["sources"] | None = None
    blocked: tuple[str, ...] = ()
    source_revision: str | None = None
    unconfigured: tuple[Branch, ...] = ()
    references: dict[str, AssetReference] = field(default_factory=dict)
    source_authorizations: dict[str, SourceInventoryAuthorization] = field(default_factory=dict)
    declared: bool = False

    @property
    def asset_selection(self) -> list[AssetKey]:
        return configured_selection(
            (AssetKey(["datasets", *stage.split("/")]) for stage in self.selected), declared=self.declared
        )


def plan_dataset(
    spec: DatasetBuildSpec,
    configurations: Mapping[str, Config],
    instance: DagsterInstance,
    *,
    home: Path,
    through: str | None = None,
) -> BuildPlan:
    manager = DataFrameIOManager(storage=spec.storage)
    selected: dict[str, str] = {}
    values: dict[str, pd.DataFrame | CategoryRules | AssetSnapshot] = {"category_rules": spec.rules}
    unchanged: list[str] = []
    waiting_for: Literal["sources"] | None = None
    blocked: list[str] = []
    source_revision: str | None = None
    references: dict[str, AssetReference] = {}
    source_authorizations: dict[str, SourceInventoryAuthorization] = {}
    unconfigured: tuple[Branch, ...] = tuple(
        branch.namespace
        for branch in collection_branches(declared=spec.has_declared_collection)
        if not spec.has_declared_collection and not catalog_for_branch(spec, branch.namespace).configured
    )
    for branch in unconfigured:
        values[stage_name("command_candidates", branch)] = unavailable_candidates(
            spec, branch, "unconfigured"
        )
    dependencies_by_stage = DECLARED_INPUT_STAGES if spec.has_declared_collection else INPUT_STAGES
    needed: set[str] = set()
    if through is not None:
        pending = [through]
        while pending:
            stage = pending.pop()
            if stage not in needed:
                needed.add(stage)
                pending.extend(dependencies_by_stage[stage].values())
    for stage, dependencies in dependencies_by_stage.items():
        if stage in RELEASE_STAGES:
            continue
        if through is not None and stage not in needed:
            continue
        if stage not in ("category_rules", *FINALIZATION_STAGES) and stage_branch(stage) in unconfigured:
            continue
        if stage == "filtered_commands" and any(name not in FINALIZATION_STAGES for name in blocked):
            blocked.append(stage)
            continue
        if stage != "merged_command_candidates" and any(
            parent in blocked for parent in dependencies.values()
        ):
            blocked.append(stage)
            if stage.rsplit("/", 1)[-1] == "command_candidates":
                values[stage] = unavailable_candidates(spec, stage_branch(stage), "waiting")
            continue
        config = None if stage == "category_rules" else configurations[stage]
        if isinstance(config, DeclaredSourceStageConfig):
            try:
                require_source_rights(config)
            except HumanReviewError:
                blocked.append(stage)
                waiting_for = "sources"
                continue
        if (
            spec.has_declared_collection
            and stage == "merged_command_candidates"
            and any(parent in blocked for parent in dependencies.values())
        ):
            blocked.append(stage)
            continue
        key = AssetKey(["datasets", *stage.split("/")])
        records = instance.fetch_materializations(
            AssetRecordsFilter(asset_key=key, asset_partitions=[spec.dataset_id]), limit=1
        ).records
        if not records:
            selected[stage] = "never_materialized"
            continue
        if any(parent in selected and parent != "category_rules" for parent in dependencies.values()):
            selected[stage] = "upstream_changed"
            continue
        inputs = {name: values[parent] for name, parent in dependencies.items()}
        expected = (
            fingerprint(spec.rules.model_dump_json())
            if config is None
            else stage_signature(stage.rsplit("/", 1)[-1], config, inputs)
        )
        materialization = records[0].asset_materialization
        if materialization is None:
            raise RuntimeError("Materialization query returned an event without its materialization")
        metadata_key = "dagster/data_version" if stage == "category_rules" else "input_fingerprint"
        recorded = (
            materialization.tags.get("dagster/data_version")
            if stage == "category_rules" and materialization.tags is not None
            else materialization.metadata[metadata_key].value
        )
        if recorded != expected and not (
            not spec.has_declared_collection
            and config is not None
            and legacy_signature_matches(
                manager,
                stage.rsplit("/", 1)[-1],
                config,
                inputs,
                str(recorded),
                evidence=TypeAdapter(dict[str, JsonValue]).validate_python(
                    materialization.metadata["evidence"].value
                ),
            )
        ):
            selected[stage] = "inputs_or_configuration_changed"
            continue
        try:
            with build_input_context(
                asset_key=key,
                partition_key=spec.dataset_id,
                asset_partitions_def=DATASET_PARTITIONS,
                instance=instance,
            ) as context:
                persisted = manager.read_current(context)
        except FileNotFoundError:
            selected[stage] = "stored_output_missing"
            continue
        if materialization.metadata["codebench/content_digest"].value != persisted.reference.digest:
            raise IntegrityError("Current output differs from its recorded materialization")
        if isinstance(config, DeclaredSourceStageConfig) and isinstance(persisted.value, pd.DataFrame):
            try:
                validate_source_receipt(persisted.value, config, manager)
            except (FileNotFoundError, HumanReviewError):
                blocked.append(stage)
                waiting_for = "sources"
                continue
        if stage == "human_review_queue":
            try:
                with HumanReviewStore(home).reader() as transaction:
                    batch = transaction.get_current_batch(spec.dataset_id, "annotations", compact=True)
                if not isinstance(persisted.value, pd.DataFrame) or (
                    persisted.value.attrs.get(REVIEW_PREPARATION)
                    != {"batch_id": batch.batch_id, "snapshot_id": batch.snapshot.snapshot_id}
                ):
                    selected[stage] = "review_authority_changed"
                    continue
            except HumanReviewError as error:
                if error.code not in {"not_found", "stale"}:
                    raise
                selected[stage] = "review_authority_invalidated"
                continue
        if stage != "category_rules":
            values[stage] = persisted
        references[stage] = persisted.reference
        if stage.rsplit("/", 1)[-1] == "source_catalog":
            source_authorizations[stage] = SourceInventoryAuthorization(
                dataset_id=spec.dataset_id,
                branch=stage_branch(stage),
                catalog_digest=persisted.reference.digest,
                inventory_signature=str(recorded),
            )
        elif stage.rsplit("/", 1)[-1] == "command_candidates" and isinstance(persisted.value, pd.DataFrame):
            source_authorizations[stage] = frame_inventory_authorization(persisted.value, stage_branch(stage))
        unchanged.append(stage)
    return BuildPlan(
        tuple(selected),
        tuple(unchanged),
        selected,
        waiting_for,
        tuple(blocked),
        source_revision,
        unconfigured,
        references,
        source_authorizations,
        declared=spec.has_declared_collection,
    )


def graph_materialization_stale(
    spec: DatasetBuildSpec, instance: DagsterInstance, *, home: Path, stage: str
) -> bool | None:
    if not spec.has_declared_collection:
        return True if stage in KNOWN_STAGES and stage not in INPUT_STAGES else None
    if stage == "semantic_evidence" or (stage in KNOWN_STAGES and stage not in DECLARED_INPUT_STAGES):
        return True
    if stage not in DECLARED_INPUT_STAGES or stage in RELEASE_STAGES:
        return None
    plan = plan_dataset(spec, stage_configs(spec), instance, home=home, through=stage)
    return stage not in plan.unchanged
