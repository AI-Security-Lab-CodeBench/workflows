from dagster import AssetIn, AssetKey, AssetsDefinition, DataVersion, Output, asset
from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.collection_config import CollectionMergeConfig
from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, frame_records, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, Branch, asset_path
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO, MERGE_INPUT, MergeInput
from codebench_pipeline.finalization_table_io import retain_failed_finalization
from codebench_pipeline.grounding_sources import REPOSITORY_CANDIDATES, RepositoryCandidate
from codebench_pipeline.repository_receipts import RECEIPT_ATTR, RepositoryReceipt, canonical, sha256
from codebench_pipeline.storage import IntegrityError


def merge_candidates(
    dataset: DataFrame[CommandCandidatesTable],
    documentation: DataFrame[CommandCandidatesTable],
    keenable: DataFrame[CommandCandidatesTable] | None = None,
    *,
    config: CollectionMergeConfig | None = None,
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
        records = frame_records(frame)
        evidence: dict[str, RepositoryCandidate] = {}
        if config is not None and records and branch != "keenable":
            serialized = frame.attrs.get(RECEIPT_ATTR)
            if not isinstance(serialized, str):
                raise IntegrityError(f"Missing repository candidate receipt for merge branch {branch}")
            receipt = RepositoryReceipt.model_validate_json(serialized)
            if (
                receipt.stage != "command_candidates"
                or receipt.branch != branch
                or receipt.table_digest != sha256(canonical(records))
            ):
                raise IntegrityError(f"Repository candidate receipt does not bind merge branch {branch}")
            evidence = REPOSITORY_CANDIDATES.validate_python(receipt.observations["candidates"])
        for row in records:
            original = str(row["candidate_id"])
            identifier = f"{branch}:{original}"
            origins[identifier] = {"branch": branch, "candidate_id": original, "source_id": row["source_id"]}
            row.pop("source_weight", None)
            if config is not None:
                origin_id = None
                if branch != "keenable":
                    source = evidence.get(original)
                    if source is None or source.source_id != row["source_id"]:
                        raise IntegrityError(f"Missing repository origin evidence for {identifier}")
                    origin_id = source.origin_id
                row["source_weight"] = config.weight(branch, origin_id)
            rows.append({**row, "candidate_id": identifier, "source_id": f"{branch}:{row['source_id']}"})
    merged = validated_frame(rows, CommandCandidatesTable)
    merged.attrs["origins"] = origins
    merged.attrs["branch_metadata"] = {branch: dict(frame.attrs) for branch, frame in branches.items()}
    return merged


def collection_merge_asset() -> AssetsDefinition:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_finalization",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key=FINALIZATION_TABLE_IO,
        dagster_type=TABLE_TYPES[CommandCandidatesTable],
        hooks={retain_failed_finalization},
        ins={
            "mapped_config": mapped_input(
                "merged_command_candidates",
                CollectionMergeConfig,
                input_manager_key=FINALIZATION_TABLE_IO,
            ),
            **{
                f"{branch.namespace}_candidates": AssetIn(
                    key=AssetKey(asset_path("command_candidates", branch.namespace)),
                    dagster_type=MERGE_INPUT
                    if branch.namespace == "dataset"
                    else TABLE_TYPES[CommandCandidatesTable],
                    input_manager_key=FINALIZATION_TABLE_IO,
                )
                for branch in COLLECTION_BRANCHES
            },
        },
    )
    def merged_command_candidates(
        dataset_candidates: MergeInput,
        documentation_candidates: DataFrame[CommandCandidatesTable],
        keenable_candidates: DataFrame[CommandCandidatesTable],
        mapped_config: CollectionMergeConfig,
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        if dataset_candidates.signature is None:
            raise IntegrityError("Merge input hooks did not prepare the table")
        if dataset_candidates.cached is None:
            table = merge_candidates(
                dataset_candidates.frames["dataset"],
                documentation_candidates,
                keenable_candidates,
                config=mapped_config,
            )
            table.attrs[FINALIZATION_BINDING] = dataset_candidates.binding
        else:
            table = CommandCandidatesTable.validate(dataset_candidates.cached)
        return Output(table, data_version=DataVersion(dataset_candidates.signature))

    return merged_command_candidates
