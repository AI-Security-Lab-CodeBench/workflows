from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, Config, DagsterInstance, build_input_context

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import FINALIZATION_STAGES, Branch, stage_branch, stage_name
from codebench_pipeline.dataset_catalog import source_scope
from codebench_pipeline.dataset_config import DatasetBuildSpec, catalog_for_branch
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.storage import fingerprint

INPUT_STAGES: dict[str, dict[str, str]] = {
    "category_rules": {},
    "source_catalog": {"category_rules": "category_rules"},
    "source_documents": {"source_catalog": "source_catalog"},
    "document_adapters": {"source_documents": "source_documents"},
    "command_candidates": {"source_documents": "source_documents", "adapters": "document_adapters"},
}

INPUT_STAGES.update(
    {
        stage_name(stage, "documentation"): {
            name: stage_name(parent, "documentation")
            for name, parent in dependencies.items()
        }
        for stage, dependencies in tuple(INPUT_STAGES.items())
        if stage != "category_rules"
    }
)

INPUT_STAGES.update(
    {
        "merged_command_candidates": {
            "dataset_candidates": "command_candidates",
            "documentation_candidates": "documentation/command_candidates",
        },
        "filtered_commands": {
            "command_candidates": "merged_command_candidates",
            "category_rules": "category_rules",
        },
        "labeling_documentation": {"commands": "filtered_commands", "category_rules": "category_rules"},
        "proposed_annotations": {
            "commands": "filtered_commands",
            "documentation": "labeling_documentation",
            "category_rules": "category_rules",
        },
        "deduplicated_annotations": {"annotations": "proposed_annotations"},
        "human_review_queue": {
            "annotations": "deduplicated_annotations",
            "category_rules": "category_rules",
        },
        "human_review_decisions": {"human_review_queue": "human_review_queue"},
        "dataset_release": {
            "deduplicated_annotations": "deduplicated_annotations",
            "human_review_decisions": "human_review_decisions",
            "category_rules": "category_rules",
        },
    }
)
RELEASE_STAGES = ("human_review_decisions", "dataset_release")
PREPARATION_STAGES = tuple(stage for stage in INPUT_STAGES if stage not in RELEASE_STAGES)


def unavailable_candidates(
    spec: DatasetBuildSpec, branch: Branch, state: Literal["waiting", "unconfigured"]
) -> pd.DataFrame:
    empty = validated_frame([], CommandCandidatesTable)
    empty.attrs["branch_state"] = state
    empty.attrs["branch_configuration"] = fingerprint(
        {
            "catalog": catalog_for_branch(spec, branch).model_dump_json(),
            "extraction": spec.extraction.model_dump_json(),
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

    @property
    def asset_selection(self) -> list[AssetKey]:
        return [AssetKey(["datasets", *stage.split("/")]) for stage in self.selected]


def plan_dataset(
    spec: DatasetBuildSpec, configurations: Mapping[str, Config], instance: DagsterInstance, *, home: Path
) -> BuildPlan:
    manager = DataFrameIOManager(storage=spec.storage)
    selected: dict[str, str] = {}
    values: dict[str, pd.DataFrame | CategoryRules] = {"category_rules": spec.rules}
    unchanged: list[str] = []
    waiting_for: Literal["sources"] | None = None
    blocked: list[str] = []
    source_revision: str | None = None
    references: dict[str, AssetReference] = {}
    branches: tuple[Branch, ...] = ("dataset", "documentation")
    unconfigured: tuple[Branch, ...] = tuple(
        branch
        for branch in branches
        if not (
            catalog_for_branch(spec, branch).documents
            or catalog_for_branch(spec, branch).manifest is not None
        )
    )
    for branch in unconfigured:
        values[stage_name("command_candidates", branch)] = unavailable_candidates(
            spec, branch, "unconfigured"
        )
    for stage, dependencies in INPUT_STAGES.items():
        if stage not in PREPARATION_STAGES:
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
            if stage == "category_rules"
            else stage_signature(stage.rsplit("/", 1)[-1], configurations[stage], inputs)
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
        if recorded != expected:
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
        if stage != "category_rules":
            values[stage] = persisted.value
        references[stage] = persisted.reference
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
    )
