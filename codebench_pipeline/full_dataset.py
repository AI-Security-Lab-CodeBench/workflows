"""Publish and load a reference-only bundle of the two complete CodeBench splits."""

from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetSelection,
    DataVersion,
    InputContext,
    Output,
    PythonObjectDagsterType,
    StaticPartitionsDefinition,
    TextMetadataValue,
    asset,
    input_manager,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from pandera.typing import DataFrame
from pydantic import Field, TypeAdapter, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.config_assets import GLOBAL_CONFIG, ConfigRunIOManager, current_configuration_record
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager, materialized_reference
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.finalization_publication import TABLE_REGISTRATION_METADATA
from codebench_pipeline.input_provenance import (
    current_materialization,
    input_materialization,
    producer_run_id,
)
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.split_schema import SplitRowsTable
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.train_assets import TRAIN_TABLE_IO, Split
from codebench_pipeline.train_config import SplitCategory, TrainSource
from codebench_pipeline.typed_config import ConfigArtifact

FULL_DATASET_KEY: Final = AssetKey(["codebench", "full_dataset"])
_CONFIG_IO: Final = "config_run_io_manager"
_MERGED_MEMBER_INPUT: Final = "full_dataset_member_input"
_SOURCES: Final = TypeAdapter(dict[SplitCategory, list[TrainSource]])


class ParquetMemberReference(ConfigArtifact):
    """A successful merged-table receipt and its original immutable storage location."""

    split: Split
    partition: Literal["codebench"] = CODEBENCH_DATASET_ID
    run_id: str = Field(min_length=1)
    materialization_id: int = Field(gt=0)
    registration_digest: Digest
    reference: AssetReference
    storage: StorageConfig
    row_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_location(self) -> "ParquetMemberReference":
        if self.reference.format != "parquet":
            raise PydanticCustomError("split_format", "Expected an immutable Parquet reference")
        if self.storage.backend == "local" and not Path(self.storage.local_root).is_absolute():
            raise PydanticCustomError("split_storage", "Expected an absolute local storage root")
        return self

    @property
    def asset_key(self) -> AssetKey:
        return AssetKey([self.split, "merged_rows"])

    def load(self) -> DataFrame[SplitRowsTable]:
        """Verify and load the original bytes without consulting mutable asset pointers."""
        value = DataFrameIOManager.from_environment(self.storage).read_version(self.reference)
        if not isinstance(value, pd.DataFrame) or len(value) != self.row_count:
            raise IntegrityError(f"Stored {self.split} table does not match the bundle row count")
        return SplitRowsTable.validate(value)


class FullDatasetBundle(ConfigArtifact):
    """One dataset with independent model-training and held-out evaluation members."""

    version: Literal[1] = 1
    train: ParquetMemberReference
    test: ParquetMemberReference

    @model_validator(mode="after")
    def validate_members(self) -> "FullDatasetBundle":
        if self.train.split != "train" or self.test.split != "test":
            raise PydanticCustomError("split_members", "Train and test must name their own merged assets")
        if self.train.registration_digest != self.test.registration_digest:
            raise PydanticCustomError("split_registration", "Both members must share one registration")
        return self


@dataclass(frozen=True, slots=True)
class _MergedInput:
    member: ParquetMemberReference
    sources: dict[SplitCategory, list[TrainSource]]
    pinned_producer: bool


_MERGED_INPUT_TYPE = PythonObjectDagsterType(_MergedInput, name="CodeBenchMergedInput")
FULL_DATASET_TYPE = PythonObjectDagsterType(FullDatasetBundle, name="FullDatasetBundle")


@input_manager(required_resource_keys={_CONFIG_IO, TRAIN_TABLE_IO})
def full_dataset_member_input(context: InputContext) -> _MergedInput:
    config_io = context.resources.config_run_io_manager
    manager = context.resources.train_table_io
    if (
        not isinstance(config_io, ConfigRunIOManager)
        or not isinstance(manager, DataFrameIOManager)
        or manager.registered_revision is None
    ):
        raise IntegrityError("Full dataset inputs require pinned split storage")
    if (
        DatasetRegistry(Path(config_io.home)).current_revision(CODEBENCH_DATASET_ID)
        != manager.registered_revision
    ):
        raise IntegrityError("CodeBench registration changed while loading the full dataset inputs")
    match context.asset_key.path:
        case ["train", "merged_rows"]:
            split = "train"
        case ["test", "merged_rows"]:
            split = "test"
        case _:
            raise IntegrityError("Expected a complete CodeBench split")
    if context.asset_partition_key != CODEBENCH_DATASET_ID or context.upstream_output is None:
        raise IntegrityError(f"Expected a Dagster input for the CodeBench {split} merge")
    record = input_materialization(context)
    if record is None or record.asset_materialization is None:
        raise IntegrityError(f"No materialization for the CodeBench {split} merge")
    pinned = producer_run_id(context.upstream_output) is not None
    reference = materialized_reference(
        context.instance, record, context.upstream_output.name if pinned else "result"
    )
    if reference.format != "parquet" or record.asset_materialization.metadata.get(
        TABLE_REGISTRATION_METADATA
    ) != TextMetadataValue(manager.registered_revision):
        raise IntegrityError(f"The {split} merge is not a Parquet output of the pinned registration")
    if not pinned and manager.current_reference(context) != reference:
        raise IntegrityError(f"The {split} merge pointer differs from its materialization")
    value = manager.read_version(reference)
    if not isinstance(value, pd.DataFrame):
        raise IntegrityError(f"Expected a {split} table")
    rows = SplitRowsTable.validate(value)
    storage = manager.storage
    if storage.backend == "local":
        storage = storage.model_copy(update={"local_root": str(Path(storage.local_root).absolute())})
    member = ParquetMemberReference(
        split=split,
        run_id=record.run_id,
        materialization_id=record.storage_id,
        registration_digest=manager.registered_revision,
        reference=reference,
        storage=storage,
        row_count=len(rows),
    )
    return _MergedInput(member, _SOURCES.validate_python(rows.attrs.get(f"{split}_sources")), pinned)


@asset(
    key=FULL_DATASET_KEY,
    group_name="dataset_merge",
    partitions_def=StaticPartitionsDefinition([CODEBENCH_DATASET_ID]),
    io_manager_key=_CONFIG_IO,
    dagster_type=FULL_DATASET_TYPE,
    required_resource_keys={_CONFIG_IO, TRAIN_TABLE_IO},
    ins={
        split: AssetIn(
            key=AssetKey([split, "merged_rows"]),
            dagster_type=_MERGED_INPUT_TYPE,
            input_manager_key=_MERGED_MEMBER_INPUT,
        )
        for split in ("train", "test")
    },
)
def full_dataset(
    context: AssetExecutionContext, train: _MergedInput, test: _MergedInput
) -> Output[FullDatasetBundle]:
    """Bundle both complete merged tables without copying their rows."""
    config_io = context.resources.config_run_io_manager
    tables = context.resources.train_table_io
    if not isinstance(config_io, ConfigRunIOManager) or not isinstance(tables, DataFrameIOManager):
        raise IntegrityError("Full dataset publication requires the configured storage managers")
    bundle = FullDatasetBundle(train=train.member, test=test.member)
    revision = bundle.train.registration_digest
    config_record = current_configuration_record(context.instance, GLOBAL_CONFIG, context.partition_key)
    registered = config_io.registered(config_record.run_id, materialization=config_record)
    if registered.dataset_id != context.partition_key or registered.registration_digest != revision:
        raise IntegrityError("The full dataset inputs differ from the materialized registration")
    for selection, config in ((train, registered.train), (test, registered.test)):
        member = selection.member
        if (
            config is None
            or not config.sources
            or selection.sources
            != {category: sources.sources for category, sources in config.sources_by_category().items()}
        ):
            raise IntegrityError(f"The {member.split} merge does not cover the registered source pins")
        if not selection.pinned_producer:
            current = current_materialization(context.instance, member.asset_key, member.partition)
            if (
                current is None
                or current.storage_id != member.materialization_id
                or tables.reference_at(member.asset_key.path, member.partition) != member.reference
            ):
                raise IntegrityError(f"The {member.split} merge changed while building the full dataset")
    return Output(bundle, data_version=DataVersion(fingerprint(bundle.model_dump_json())))


build_full_dataset_job = UnresolvedAssetJobDefinition(
    name="build_full_dataset",
    selection=AssetSelection.assets(full_dataset),
    description="Bundle the successful CodeBench training and held-out test merges.",
)
