import json
import re

from dagster import (
    AssetKey,
    AssetsDefinition,
    Output,
    asset,
)
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_eligibility import category_eligible_annotations as filter_category
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import (
    RegisteredConfig,
    mapped_input,
    registered_input,
    require_registered_rules,
)
from codebench_pipeline.dataframe_io import frame_bytes
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
from codebench_pipeline.dataset_dependencies import preparation_stages
from codebench_pipeline.dataset_tables import review_queue
from codebench_pipeline.finalization_table_asset import FinalizationTable, finalization_table_assets
from codebench_pipeline.object_store import sha256
from codebench_pipeline.repository_config import DEFAULT_SOURCE_WEIGHTS, SourceWeight
from codebench_pipeline.stage_configuration import ReviewQueueConfig
from codebench_pipeline.storage import IntegrityError

__all__ = [
    "FINALIZATION_BINDING",
    "authorize_finalization_output",
    "available_candidates",
    "deduplicate_annotations",
    "finalization_assets",
    "finalization_binding",
    "merge_candidates",
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


def deduplicated_annotations(proposed_annotations: DataFrame[ProposedAnnotationsTable]) -> FinalizationTable:
    frame = deduplicate_annotations(proposed_annotations)
    return FinalizationTable(frame, TypeAdapter(JsonValue).validate_python(frame.attrs["duplicates"]))


def category_eligible_annotations(
    deduplicated_annotations: DataFrame[ProposedAnnotationsTable], category_rules: CategoryRules
) -> FinalizationTable:
    frame = filter_category(deduplicated_annotations, category_rules)
    return FinalizationTable(
        frame, TypeAdapter(JsonValue).validate_python(frame.attrs["category_exclusions"])
    )


def review_evidence_keys(*excluded: str) -> tuple[AssetKey, ...]:
    return tuple(
        AssetKey(["datasets", *stage.split("/")]) for stage in preparation_stages() if stage not in excluded
    )


def finalization_assets() -> list[AssetsDefinition]:
    from codebench_pipeline.collection_merge import collection_merge_asset

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ReviewQueueTable],
        config_schema={},
        deps=review_evidence_keys("human_review_queue", "category_eligible_annotations"),
        ins={
            "category_eligible_annotations": table_input(
                "category_eligible_annotations", ProposedAnnotationsTable
            ),
            "mapped_config": mapped_input("human_review_queue", ReviewQueueConfig),
            "registered": registered_input(),
        },
    )
    def human_review_queue(
        category_eligible_annotations: DataFrame[ProposedAnnotationsTable],
        mapped_config: ReviewQueueConfig,
        registered: RegisteredConfig,
    ) -> Output[DataFrame[ReviewQueueTable]]:
        require_registered_rules(registered, mapped_config.category_rules)
        settings = mapped_config.human_review_queue
        if settings != registered.scopes.human_review_queue.human_review_queue:
            raise IntegrityError("Review settings differ from the registered configuration")
        return Output(
            review_queue(
                category_eligible_annotations, mapped_config.category_rules, settings.review_columns
            ),
            metadata={"review_input_digest": sha256(frame_bytes(category_eligible_annotations))},
        )

    return [
        collection_merge_asset(),
        *finalization_table_assets(deduplicated_annotations, category_eligible_annotations),
        human_review_queue,
    ]
