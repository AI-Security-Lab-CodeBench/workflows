from dagster import AssetIn, AssetKey, AssetsDefinition, DataVersion, Output, asset
from pandera.typing import DataFrame

from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, asset_path
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_finalization import merge_candidates
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO, MERGE_INPUT, MergeInput
from codebench_pipeline.finalization_table_io import retain_failed_finalization
from codebench_pipeline.stage_configuration import DeclaredMergeConfig
from codebench_pipeline.storage import IntegrityError


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
                DeclaredMergeConfig,
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
        mapped_config: DeclaredMergeConfig,
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        if dataset_candidates.signature is None:
            raise IntegrityError("Merge input hooks did not prepare the table")
        if dataset_candidates.cached is None:
            table = merge_candidates(
                dataset_candidates.frames["dataset"],
                documentation_candidates,
                keenable_candidates,
                keenable_source_weight=mapped_config.merged_command_candidates.keenable_source_weight,
            )
            table.attrs[FINALIZATION_BINDING] = dataset_candidates.binding
        else:
            table = CommandCandidatesTable.validate(dataset_candidates.cached)
        return Output(table, data_version=DataVersion(dataset_candidates.signature))

    return merged_command_candidates
