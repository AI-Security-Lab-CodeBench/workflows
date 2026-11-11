import json
import re
from pathlib import Path
from typing import Final

import pandas as pd
from dagster import AssetExecutionContext, AssetsDefinition, DagsterInstance, Output, ResourceParam, asset
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataframe_schemas import (
    DocumentationTable,
    ProposedAnnotationsTable,
    ReviewedAnnotationsTable,
    ReviewQueueTable,
    frame_records,
    validated_frame,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, rules_input, table_input
from codebench_pipeline.dataset_branches import FINALIZATION_STAGES, Branch, stage_branch, stage_name
from codebench_pipeline.dataset_config import PublicationConfig, ReviewConfig
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.dataset_plan import plan_dataset, unavailable_annotations
from codebench_pipeline.dataset_tables import review_queue
from codebench_pipeline.dataset_tables import reviewed_annotations as review_table
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.object_store import sha256
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.typed_config import TypedConfig

FINALIZATION_BINDING: Final = "codebench_finalization"


def available_proposals(
    home: Path,
    data: DatasetDataService,
    dataset_id: str,
) -> tuple[dict[Branch, DataFrame[ProposedAnnotationsTable]], dict[str, JsonValue]]:
    spec = data.specification(dataset_id)
    plan = plan_dataset(spec, stage_configs(spec), data.instance, home=home)
    pending = tuple(stage for stage in (*plan.selected, *plan.blocked) if stage not in FINALIZATION_STAGES)
    binding: dict[str, JsonValue] = {}
    frames: dict[Branch, DataFrame[ProposedAnnotationsTable]] = {}
    branches: tuple[Branch, ...] = ("dataset", "documentation")
    for branch in branches:
        if branch in plan.unconfigured:
            binding[branch] = {"state": "unconfigured"}
            frames[branch] = ProposedAnnotationsTable.validate(
                unavailable_annotations(spec, branch, "unconfigured")
            )
        elif any(stage == "category_rules" or stage_branch(stage) == branch for stage in pending):
            binding[branch] = {"state": "waiting"}
            frames[branch] = ProposedAnnotationsTable.validate(
                unavailable_annotations(spec, branch, "waiting")
            )
        else:
            stored = data.read_current(dataset_id, stage_name("proposed_annotations", branch))
            if stored.publication_state == "stale":
                raise HumanReviewError("stale", "A configured collection branch is no longer authorized")
            if not isinstance(stored.value, pd.DataFrame):
                raise HumanReviewError("conflict", "Proposed annotations must be a table")
            frames[branch] = ProposedAnnotationsTable.validate(stored.value)
            binding[branch] = {"state": "configured", "digest": stored.reference.digest}
    return frames, binding


def finalization_binding(home: Path, data: DatasetDataService, dataset_id: str) -> dict[str, JsonValue]:
    return available_proposals(home, data, dataset_id)[1]


def authorize_finalization_output(
    home: Path,
    instance: DagsterInstance,
    dataset_id: str,
    frame: pd.DataFrame,
    *,
    complete: bool = False,
) -> None:
    expected = finalization_binding(home, DatasetDataService(home, instance), dataset_id)
    if frame.attrs.get(FINALIZATION_BINDING) != expected:
        raise HumanReviewError("stale", "Collection inputs changed during finalization")
    states = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(expected)
    if complete and any(value["state"] == "waiting" for value in states.values()):
        raise HumanReviewError("pending", "Final human review waits for every configured branch")


def merge_proposals(
    dataset: DataFrame[ProposedAnnotationsTable],
    documentation: DataFrame[ProposedAnnotationsTable],
) -> DataFrame[ProposedAnnotationsTable]:
    rows: list[dict[str, JsonValue]] = []
    origins: dict[str, JsonValue] = {}
    for branch, frame in (("dataset", dataset), ("documentation", documentation)):
        for row in frame_records(frame):
            original = str(row["candidate_id"])
            identifier = f"{branch}:{original}"
            origins[identifier] = {"branch": branch, "candidate_id": original, "source_id": row["source_id"]}
            rows.append({**row, "candidate_id": identifier})
    merged = validated_frame(rows, ProposedAnnotationsTable)
    merged.attrs["origins"] = origins
    return merged


def normalized_command(command: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_./:@%+=,\- \t]+", command) is None:
        return command
    return re.sub(r"[ \t]+", " ", command).strip(" ")


def deduplicate_annotations(
    annotations: DataFrame[ReviewedAnnotationsTable],
) -> DataFrame[ReviewedAnnotationsTable]:
    retained: list[dict[str, JsonValue]] = []
    survivors: dict[str, str] = {}
    duplicates: list[JsonValue] = []
    provenance = {"candidate_id", "source_id", "source_url", "source_position", "transformations", "command"}
    for row in frame_records(annotations):
        identifier = str(row["candidate_id"])
        normalized = normalized_command(str(row["command"]))
        key = json.dumps(
            {
                "command": normalized,
                "documentation": annotations.attrs.get("row_documentation", {}).get(identifier, []),
                **{name: value for name, value in row.items() if name not in provenance},
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        if key in survivors:
            duplicates.append(
                {
                    "duplicate_id": identifier,
                    "survivor_id": survivors[key],
                    "normalized_command": normalized,
                    "reason": "exact_normalized_command_and_annotation",
                }
            )
        else:
            retained.append(row)
            survivors[key] = identifier
    result = validated_frame(retained, ReviewedAnnotationsTable)
    result.attrs = {**annotations.attrs, "duplicates": duplicates}
    return result


def finalization_assets() -> list[AssetsDefinition]:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        ins={
            "dataset_annotations": table_input("proposed_annotations", ProposedAnnotationsTable),
            "documentation_annotations": table_input(
                "proposed_annotations", ProposedAnnotationsTable, "documentation"
            ),
        },
    )
    def merged_proposed_annotations(
        context: AssetExecutionContext,
        config: TypedConfig,
        dataset_annotations: DataFrame[ProposedAnnotationsTable],
        documentation_annotations: DataFrame[ProposedAnnotationsTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        if dataset_io_manager.review_home is None:
            raise HumanReviewError("pending", "Finalization requires a registered dataset")
        home = Path(dataset_io_manager.review_home)
        binding = finalization_binding(
            home, DatasetDataService(home, context.instance), context.partition_key
        )
        for branch, frame in (("dataset", dataset_annotations), ("documentation", documentation_annotations)):
            expected = (
                {"state": frame.attrs["branch_state"]}
                if "branch_state" in frame.attrs
                else {"state": "configured", "digest": sha256(frame_bytes(frame))}
            )
            if binding[branch] != expected:
                raise HumanReviewError("stale", "Merge inputs differ from current authorized branches")

        def compute(evidence: Path) -> DataFrame[ProposedAnnotationsTable]:
            merged = merge_proposals(dataset_annotations, documentation_annotations)
            merged.attrs[FINALIZATION_BINDING] = binding
            documents: dict[str, JsonValue] = {}
            branch_frames: tuple[tuple[Branch, DataFrame[ProposedAnnotationsTable]], ...] = (
                ("dataset", dataset_annotations),
                ("documentation", documentation_annotations),
            )
            for branch, frame in branch_frames:
                if "branch_state" in frame.attrs:
                    continue
                stored = DatasetDataService(home, context.instance).read_current(
                    context.partition_key,
                    stage_name("labeling_documentation", branch),
                )
                if not isinstance(stored.value, pd.DataFrame):
                    raise HumanReviewError("conflict", "Labeling documentation must be a table")
                for row in frame_records(frame):
                    citations = TypeAdapter(list[str]).validate_python(row["documentation_urls"])
                    documents[f"{branch}:{row['candidate_id']}"] = [
                        doc
                        for doc in frame_records(DocumentationTable.validate(stored.value))
                        if doc["utility"] == row["util"] or doc["url"] in citations
                    ]
            merged.attrs["row_documentation"] = documents
            return merged

        return cached_table(
            context,
            dataset_io_manager,
            config,
            {
                "dataset_annotations": dataset_annotations,
                "documentation_annotations": documentation_annotations,
            },
            ProposedAnnotationsTable,
            compute,
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ReviewedAnnotationsTable],
        ins={
            "merged_proposed_annotations": table_input(
                "merged_proposed_annotations", ProposedAnnotationsTable
            ),
            "category_rules": rules_input(),
        },
    )
    def reviewed_annotations(
        context: AssetExecutionContext,
        config: ReviewConfig,
        merged_proposed_annotations: DataFrame[ProposedAnnotationsTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ReviewedAnnotationsTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"annotations": merged_proposed_annotations, "category_rules": category_rules},
            ReviewedAnnotationsTable,
            lambda evidence: review_table(
                merged_proposed_annotations,
                validated_frame([], DocumentationTable),
                category_rules,
                config,
                evidence,
                cache=Path(dataset_io_manager.review_home) / "row-inference" / context.partition_key
                if dataset_io_manager.review_home is not None
                else None,
            ),
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ReviewedAnnotationsTable],
        ins={"reviewed_annotations": table_input("reviewed_annotations", ReviewedAnnotationsTable)},
    )
    def deduplicated_annotations(
        context: AssetExecutionContext,
        config: TypedConfig,
        reviewed_annotations: DataFrame[ReviewedAnnotationsTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ReviewedAnnotationsTable]]:
        def compute(evidence: Path) -> DataFrame[ReviewedAnnotationsTable]:
            frame = deduplicate_annotations(reviewed_annotations)
            (evidence / "duplicates.json").write_text(json.dumps(frame.attrs["duplicates"]), encoding="utf-8")
            return frame

        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"annotations": reviewed_annotations},
            ReviewedAnnotationsTable,
            compute,
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ReviewQueueTable],
        ins={
            "deduplicated_annotations": table_input("deduplicated_annotations", ReviewedAnnotationsTable),
            "category_rules": rules_input(),
        },
    )
    def human_review_queue(
        context: AssetExecutionContext,
        config: PublicationConfig,
        deduplicated_annotations: DataFrame[ReviewedAnnotationsTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ReviewQueueTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"reviewed_annotations": deduplicated_annotations, "category_rules": category_rules},
            ReviewQueueTable,
            lambda evidence: review_queue(deduplicated_annotations, category_rules, config),
        )

    return [merged_proposed_annotations, reviewed_annotations, deduplicated_annotations, human_review_queue]
