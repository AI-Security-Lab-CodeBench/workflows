import json
import re
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Output,
    ResourceParam,
    asset,
)
from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.category_eligibility import category_eligible_annotations as filter_category
from codebench_pipeline.collection_config import CollectionMergeConfig
from codebench_pipeline.config_assets import mapped_rules, mapped_settings, policy_input
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    ProposedAnnotationsTable,
    ReviewQueueTable,
    frame_records,
    validated_frame,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_collection_binding import (
    FINALIZATION_BINDING,
    authorize_finalization_output,
    available_candidates,
    finalization_binding,
)
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.dataset_tables import review_queue
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.object_store import sha256
from codebench_pipeline.repository_config import DEFAULT_SOURCE_WEIGHTS, SourceWeight
from codebench_pipeline.review_preparation import prepare_review_queue
from codebench_pipeline.typed_config import TypedConfig

__all__ = [
    "FINALIZATION_BINDING",
    "authorize_finalization_output",
    "available_candidates",
    "deduplicate_annotations",
    "finalization_assets",
    "finalization_binding",
    "merge_candidates",
    "merge_output",
    "normalized_command",
]


def merge_candidates(
    dataset: DataFrame[CommandCandidatesTable],
    documentation: DataFrame[CommandCandidatesTable],
    keenable: DataFrame[CommandCandidatesTable] | None = None,
    *,
    keenable_source_weight: SourceWeight = DEFAULT_SOURCE_WEIGHTS["keenable"],
) -> DataFrame[CommandCandidatesTable]:
    branches: dict[Branch, DataFrame[CommandCandidatesTable]] = {
        "dataset": dataset,
        "documentation": documentation,
    }
    if keenable is not None:
        branches["keenable"] = keenable
    rows: list[dict[str, JsonValue]] = []
    origins: dict[str, JsonValue] = {}
    for branch, frame in branches.items():
        for row in frame_records(frame):
            original = str(row["candidate_id"])
            identifier = f"{branch}:{original}"
            origins[identifier] = {"branch": branch, "candidate_id": original, "source_id": row["source_id"]}
            if branch == "keenable":
                row["source_weight"] = keenable_source_weight
            elif keenable is not None:
                row.setdefault("source_weight", DEFAULT_SOURCE_WEIGHTS[branch])
            rows.append({**row, "candidate_id": identifier, "source_id": f"{branch}:{row['source_id']}"})
    merged = validated_frame(rows, CommandCandidatesTable)
    merged.attrs["origins"] = origins
    merged.attrs["branch_metadata"] = {branch: dict(frame.attrs) for branch, frame in branches.items()}
    return merged


def normalized_command(command: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_./:@%+=,\- \t]+", command) is None:
        return command
    return re.sub(r"[ \t]+", " ", command).strip(" ")


def deduplicate_annotations(
    annotations: DataFrame[ProposedAnnotationsTable],
) -> DataFrame[ProposedAnnotationsTable]:
    survivors: dict[str, dict[str, JsonValue]] = {}
    identities: list[tuple[str, str, str]] = []
    provenance = {
        "candidate_id",
        "source_id",
        "source_url",
        "source_position",
        "source_weight",
        "transformations",
        "command",
    }
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
        identities.append((identifier, normalized, key))
        survivor = survivors.setdefault(key, row)
        weight, incumbent_weight = row.get("source_weight"), survivor.get("source_weight")
        if isinstance(weight, float) and isinstance(incumbent_weight, float) and weight > incumbent_weight:
            survivors[key] = row
    duplicates: list[JsonValue] = [
        {
            "duplicate_id": identifier,
            "survivor_id": survivors[key]["candidate_id"],
            "normalized_command": normalized,
            "reason": "exact_normalized_command_and_annotation",
        }
        for identifier, normalized, key in identities
        if identifier != survivors[key]["candidate_id"]
    ]
    result = validated_frame(list(survivors.values()), ProposedAnnotationsTable)
    result.attrs = {**annotations.attrs, "duplicates": duplicates}
    return result


def merge_output(
    context: AssetExecutionContext,
    inputs: dict[Branch, DataFrame[CommandCandidatesTable]],
    storage: DataFrameIOManager,
    settings: TypedConfig,
) -> Output[DataFrame[CommandCandidatesTable]]:
    if storage.review_home is None:
        raise HumanReviewError("pending", "Finalization requires a registered dataset")
    home = Path(storage.review_home)
    binding = finalization_binding(home, DatasetDataService(home, context.instance), context.partition_key)
    if set(binding) != set(inputs):
        raise HumanReviewError("stale", "Merge does not contain the registered collection branches")
    for branch, frame in inputs.items():
        expected = (
            {"state": frame.attrs["branch_state"]}
            if "branch_state" in frame.attrs
            else {"state": "configured", "digest": sha256(frame_bytes(frame))}
        )
        if binding[branch] != expected:
            raise HumanReviewError("stale", "Merge inputs differ from current authorized branches")
        if "keenable" in inputs and expected["state"] != "configured":
            raise HumanReviewError(
                "pending", "Declared collection merge requires current outputs from all three branches"
            )

    def compute(evidence: Path) -> DataFrame[CommandCandidatesTable]:
        merged = merge_candidates(
            inputs["dataset"],
            inputs["documentation"],
            inputs.get("keenable"),
            keenable_source_weight=(
                settings.keenable_source_weight
                if isinstance(settings, CollectionMergeConfig)
                else DEFAULT_SOURCE_WEIGHTS["keenable"]
            ),
        )
        merged.attrs[FINALIZATION_BINDING] = binding
        return merged

    return cached_table(
        context,
        storage,
        settings,
        {f"{branch}_candidates": frame for branch, frame in inputs.items()},
        CommandCandidatesTable,
        compute,
    )


def finalization_assets(*, declared: bool = False) -> list[AssetsDefinition]:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[CommandCandidatesTable],
        ins={
            "dataset_candidates": table_input("command_candidates", CommandCandidatesTable),
            "documentation_candidates": table_input(
                "command_candidates", CommandCandidatesTable, "documentation"
            ),
        },
    )
    def merged_command_candidates(
        context: AssetExecutionContext,
        dataset_candidates: DataFrame[CommandCandidatesTable],
        documentation_candidates: DataFrame[CommandCandidatesTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        return merge_output(
            context,
            {"dataset": dataset_candidates, "documentation": documentation_candidates},
            dataset_io_manager,
            TypedConfig(),
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        ins={
            "proposed_annotations": table_input("proposed_annotations", ProposedAnnotationsTable),
            "mapped_config": policy_input("deduplicated_annotations"),
        },
    )
    def deduplicated_annotations(
        context: AssetExecutionContext,
        proposed_annotations: DataFrame[ProposedAnnotationsTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        def compute(evidence: Path) -> DataFrame[ProposedAnnotationsTable]:
            frame = deduplicate_annotations(proposed_annotations)
            (evidence / "duplicates.json").write_text(json.dumps(frame.attrs["duplicates"]), encoding="utf-8")
            return frame

        return cached_table(
            context,
            dataset_io_manager,
            mapped_settings(mapped_config, "deduplicated_annotations", TypedConfig),
            {"annotations": proposed_annotations},
            ProposedAnnotationsTable,
            compute,
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        ins={
            "deduplicated_annotations": table_input("deduplicated_annotations", ProposedAnnotationsTable),
            "mapped_config": policy_input("category_eligible_annotations"),
        },
    )
    def category_eligible_annotations(
        context: AssetExecutionContext,
        config: TypedConfig,
        deduplicated_annotations: DataFrame[ProposedAnnotationsTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        category_rules = mapped_rules(mapped_config)

        def compute(evidence: Path) -> DataFrame[ProposedAnnotationsTable]:
            frame = filter_category(deduplicated_annotations, category_rules)
            (evidence / "category-exclusions.json").write_text(
                json.dumps(frame.attrs["category_exclusions"]), encoding="utf-8"
            )
            return frame

        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"annotations": deduplicated_annotations, "category_rules": category_rules},
            ProposedAnnotationsTable,
            compute,
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ReviewQueueTable],
        ins={
            "category_eligible_annotations": table_input(
                "category_eligible_annotations", ProposedAnnotationsTable
            ),
            "mapped_config": policy_input("human_review_queue"),
        },
    )
    def human_review_queue(
        context: AssetExecutionContext,
        config: PublicationConfig,
        category_eligible_annotations: DataFrame[ProposedAnnotationsTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ReviewQueueTable]]:
        category_rules = mapped_rules(mapped_config)
        result = cached_table(
            context,
            dataset_io_manager,
            config,
            {"annotations": category_eligible_annotations, "category_rules": category_rules},
            ReviewQueueTable,
            lambda evidence: review_queue(category_eligible_annotations, category_rules, config),
        )
        return Output(
            prepare_review_queue(context, dataset_io_manager, result.value),
            data_version=result.data_version,
            metadata=result.metadata,
        )

    merge_asset = merged_command_candidates
    if declared:
        from codebench_pipeline.collection_merge import collection_merge_asset

        merge_asset = collection_merge_asset()
    return [
        merge_asset,
        deduplicated_annotations,
        category_eligible_annotations,
        human_review_queue,
    ]
