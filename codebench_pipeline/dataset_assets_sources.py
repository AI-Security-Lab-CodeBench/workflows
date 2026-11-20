from dagster import AssetIn, AssetKey, DataVersion, MetadataValue, Output, asset
from pandera.typing import DataFrame

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import (
    RegisteredConfig,
    mapped_input,
    registered_input,
)
from codebench_pipeline.dataframe_schemas import FilteredCommandsTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, RULES_TYPE, TABLE_TYPES
from codebench_pipeline.dataset_extract import filter_candidates
from codebench_pipeline.finalization_table_asset import FILTERING_INPUT, FINALIZATION_TABLE_IO, FilteringInput
from codebench_pipeline.finalization_table_io import retain_failed_finalization
from codebench_pipeline.stage_configuration import FilterConfig
from codebench_pipeline.storage import IntegrityError, fingerprint


@asset(
    key_prefix=["datasets"],
    group_name="configuration",
    partitions_def=DATASET_PARTITIONS,
    io_manager_key="dataset_io_manager",
    dagster_type=RULES_TYPE,
    ins={"registered": registered_input()},
)
def category_rules(registered: RegisteredConfig) -> Output[CategoryRules]:
    rules = registered.rules
    version = fingerprint(rules.model_dump_json())
    return Output(
        rules,
        data_version=DataVersion(version),
        metadata={
            "cache_hit": False,
            "category": rules.canonical,
            "version": rules.version,
            "rules": MetadataValue.json(rules.model_dump(mode="json")),
        },
    )


@asset(
    key_prefix=["datasets"],
    group_name="dataset_building",
    partitions_def=DATASET_PARTITIONS,
    io_manager_key=FINALIZATION_TABLE_IO,
    dagster_type=TABLE_TYPES[FilteredCommandsTable],
    config_schema={},
    hooks={retain_failed_finalization},
    ins={
        "command_candidates": AssetIn(
            key=AssetKey(["datasets", "merged_command_candidates"]),
            dagster_type=FILTERING_INPUT,
            input_manager_key=FINALIZATION_TABLE_IO,
        ),
        "mapped_config": mapped_input(
            "filtered_commands", FilterConfig, input_manager_key=FINALIZATION_TABLE_IO
        ),
    },
)
def filtered_commands(
    command_candidates: FilteringInput,
    mapped_config: FilterConfig,
) -> Output[DataFrame[FilteredCommandsTable]]:
    if command_candidates.signature is None:
        raise IntegrityError("Filtering input hooks did not prepare the table")
    if command_candidates.cached is None:
        candidates = command_candidates.candidates
        table = filter_candidates(candidates, mapped_config.category_rules, mapped_config.filtered_commands)
        table.attrs = {**candidates.attrs, **table.attrs}
    else:
        table = FilteredCommandsTable.validate(command_candidates.cached)
    return Output(table, data_version=DataVersion(command_candidates.signature))
