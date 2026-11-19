from pathlib import Path

from dagster import (
    AssetIn,
    AssetKey,
    AssetSelection,
    MetadataValue,
    Output,
    PartitionedConfig,
    RunConfig,
    asset,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.bashguard_audit import AUDIT_STAGE, BashGuardAuditTable, audit_candidates
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.stage_descriptions import DESCRIPTIONS

AUDIT_IO_MANAGER = "bashguard_audit_io_manager"


@asset(
    name=AUDIT_STAGE,
    key_prefix=["datasets"],
    group_name="dataset_audit",
    description=DESCRIPTIONS[AUDIT_STAGE].strip(),
    compute_kind="BashGuard",
    partitions_def=DATASET_PARTITIONS,
    io_manager_key=AUDIT_IO_MANAGER,
    dagster_type=TABLE_TYPES[BashGuardAuditTable],
    ins={
        "candidates": AssetIn(
            key=AssetKey(["datasets", "merged_command_candidates"]),
            input_manager_key=AUDIT_IO_MANAGER,
            dagster_type=TABLE_TYPES[CommandCandidatesTable],
        ),
    },
)
def bashguard_syntax_audit(
    candidates: DataFrame[CommandCandidatesTable],
) -> Output[DataFrame[BashGuardAuditTable]]:
    frame, metrics = audit_candidates(candidates)
    return Output(
        frame, metadata={"syntax_audit_metrics": MetadataValue.json(metrics.model_dump(mode="json"))}
    )


def bashguard_audit_job(home: Path) -> UnresolvedAssetJobDefinition:
    """Use the partition's persisted object storage without loading review or source configuration."""

    def audit_config(key: str) -> dict[str, JsonValue]:
        storage = DatasetRegistry(home).location(key).storage
        config = RunConfig(resources={AUDIT_IO_MANAGER: DataFrameIOManager(storage=storage)})
        return TypeAdapter(dict[str, JsonValue]).validate_python(config.to_config_dict())

    return UnresolvedAssetJobDefinition(
        name="audit_bashguard_syntax",
        description="Audit the syntax of persisted merged candidates without running collection or labeling.",
        selection=AssetSelection.assets(bashguard_syntax_audit),
        partitions_def=DATASET_PARTITIONS,
        config=PartitionedConfig(DATASET_PARTITIONS, run_config_for_partition_key_fn=audit_config),
    )
