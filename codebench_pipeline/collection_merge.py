from dagster import AssetExecutionContext, AssetsDefinition, Output, ResourceParam, asset
from pandera.typing import DataFrame

from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES
from codebench_pipeline.dataset_finalization import merge_output
from codebench_pipeline.stage_configuration import DeclaredMergeConfig


def collection_merge_asset() -> AssetsDefinition:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_finalization",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[CommandCandidatesTable],
        ins={
            "mapped_config": mapped_input("merged_command_candidates", DeclaredMergeConfig),
            **{
                f"{branch.namespace}_candidates": table_input(
                    "command_candidates", CommandCandidatesTable, branch.namespace
                )
                for branch in COLLECTION_BRANCHES
            },
        },
    )
    def merged_command_candidates(
        context: AssetExecutionContext,
        dataset_candidates: DataFrame[CommandCandidatesTable],
        documentation_candidates: DataFrame[CommandCandidatesTable],
        keenable_candidates: DataFrame[CommandCandidatesTable],
        mapped_config: DeclaredMergeConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        return merge_output(
            context,
            {
                "dataset": dataset_candidates,
                "documentation": documentation_candidates,
                "keenable": keenable_candidates,
            },
            dataset_io_manager,
            mapped_config.merged_command_candidates,
        )

    return merged_command_candidates
