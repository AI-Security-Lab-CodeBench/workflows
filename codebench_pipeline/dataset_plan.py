from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, Config, DagsterInstance, build_input_context

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import ProposedAnnotationsTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import FINALIZATION_STAGES, Branch, stage_branch, stage_name
from codebench_pipeline.dataset_config import DatasetBuildSpec, catalog_for_branch
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_authorization
from codebench_pipeline.storage import fingerprint

INPUT_STAGES: dict[str, dict[str, str]] = {
    "category_rules": {},
    "source_catalog": {"category_rules": "category_rules"},
    "human_source_decisions": {"source_catalog": "source_catalog"},
    "source_documents": {
        "source_catalog": "source_catalog",
        "human_source_decisions": "human_source_decisions",
    },
    "document_adapters": {"source_documents": "source_documents"},
    "command_candidates": {"source_documents": "source_documents", "adapters": "document_adapters"},
    "filtered_commands": {
        "command_candidates": "command_candidates",
        "category_rules": "category_rules",
    },
    "labeling_documentation": {"commands": "filtered_commands", "category_rules": "category_rules"},
    "proposed_annotations": {
        "commands": "filtered_commands",
        "documentation": "labeling_documentation",
        "category_rules": "category_rules",
    },
}

INPUT_STAGES.update(
    {
        stage_name(stage, "documentation"): {
            name: stage_name(parent, "documentation")
            for name, parent in dependencies.items()
            if parent != "human_source_decisions"
        }
        for stage, dependencies in tuple(INPUT_STAGES.items())
        if stage not in {"category_rules", "human_source_decisions"}
    }
)

INPUT_STAGES.update(
    {
        "merged_proposed_annotations": {
            "dataset_annotations": "proposed_annotations",
            "documentation_annotations": "documentation/proposed_annotations",
        },
        "reviewed_annotations": {
            "annotations": "merged_proposed_annotations",
            "category_rules": "category_rules",
        },
        "deduplicated_annotations": {"annotations": "reviewed_annotations"},
        "human_review_queue": {
            "reviewed_annotations": "deduplicated_annotations",
            "category_rules": "category_rules",
        },
        "human_review_decisions": {"human_review_queue": "human_review_queue"},
        "dataset_release": {
            "reviewed_annotations": "deduplicated_annotations",
            "human_review_decisions": "human_review_decisions",
            "category_rules": "category_rules",
        },
    }
)
RELEASE_STAGES = ("human_review_decisions", "dataset_release")
PREPARATION_STAGES = tuple(stage for stage in INPUT_STAGES if stage not in RELEASE_STAGES)


def unavailable_annotations(
    spec: DatasetBuildSpec, branch: Branch, state: Literal["waiting", "unconfigured"]
) -> pd.DataFrame:
    empty = validated_frame([], ProposedAnnotationsTable)
    empty.attrs["branch_state"] = state
    empty.attrs["branch_configuration"] = fingerprint(
        {
            "catalog": catalog_for_branch(spec, branch).model_dump_json(),
            "annotation": spec.annotation.model_dump_json(),
            "documentation": spec.documentation.model_dump_json(),
            "rules": spec.rules.model_dump_json(),
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
    branches: tuple[Branch, ...] = ("dataset", "documentation")
    unconfigured: tuple[Branch, ...] = tuple(
        branch
        for branch in branches
        if not (
            catalog_for_branch(spec, branch).documents
            or catalog_for_branch(spec, branch).search_request is not None
        )
    )
    for branch in unconfigured:
        values[stage_name("proposed_annotations", branch)] = unavailable_annotations(
            spec, branch, "unconfigured"
        )
    for stage, dependencies in INPUT_STAGES.items():
        if stage not in PREPARATION_STAGES:
            continue
        if stage not in ("category_rules", *FINALIZATION_STAGES) and stage_branch(stage) in unconfigured:
            continue
        if stage == "deduplicated_annotations" and any(name not in FINALIZATION_STAGES for name in blocked):
            blocked.append(stage)
            continue
        if stage != "merged_proposed_annotations" and any(
            parent in blocked for parent in dependencies.values()
        ):
            blocked.append(stage)
            if stage.rsplit("/", 1)[-1] == "proposed_annotations":
                values[stage] = unavailable_annotations(spec, stage_branch(stage), "waiting")
            continue
        source_version: str | None = None
        if stage == "human_source_decisions":
            if "source_catalog" not in selected:
                try:
                    with HumanReviewStore(home).reader() as tx:
                        _, authorization = source_authorization(
                            SourceReviewSource(home, instance), spec.dataset_id, tx
                        )
                    source_version = fingerprint(authorization.model_dump(mode="json"))
                    source_revision = source_version
                except HumanReviewError as error:
                    if error.code not in {"not_found", "pending", "stale"}:
                        raise
            if source_version is None:
                waiting_for = "sources"
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
            source_version
            if source_version is not None
            else (
                fingerprint(spec.rules.model_dump_json())
                if stage == "category_rules"
                else stage_signature(stage.rsplit("/", 1)[-1], configurations[stage], inputs)
            )
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
                persisted = manager.load_input(context)
        except FileNotFoundError:
            selected[stage] = "stored_output_missing"
            continue
        if stage != "category_rules":
            values[stage] = persisted
        unchanged.append(stage)
    return BuildPlan(
        tuple(selected),
        tuple(unchanged),
        selected,
        waiting_for,
        tuple(blocked),
        source_revision,
        unconfigured,
    )
