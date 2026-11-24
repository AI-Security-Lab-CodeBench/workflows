from dataclasses import dataclass
from typing import Final, Literal, assert_never

import pandas as pd
from dagster import (
    AllPartitionMapping,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    DagsterInstance,
    DataVersion,
    Dict,
    DynamicPartitionsDefinition,
    InputContext,
    Output,
    PythonObjectDagsterType,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    input_manager,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import DataFrame

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    REGISTERED_CONFIG_TYPE,
    RegisteredConfig,
    mapping_key,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager, materialized_reference
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.input_provenance import current_materialization
from codebench_pipeline.split_schema import SplitRowsTable
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.train_config import (
    CategorySources,
    CountSettings,
    MergeSettings,
    SplitStats,
    TrainConfig,
)
from codebench_pipeline.train_tables import load_train_category, merge_train_categories, train_stats
from codebench_pipeline.typed_config import ConfigArtifact

Split = Literal["train", "test"]
_SPLITS: Final[tuple[Split, ...]] = ("train", "test")
_MERGE_PARTITIONS: Final = StaticPartitionsDefinition([CODEBENCH_DATASET_ID])
TRAIN_TABLE_IO: Final = "train_table_io"


@dataclass(frozen=True, slots=True)
class _SplitAssets:
    mappings: tuple[AssetsDefinition, ...]
    tables: tuple[AssetsDefinition, ...]
    jobs: tuple[UnresolvedAssetJobDefinition, ...]


def _split_config(config: RegisteredConfig | DatasetBuildSpec, split: Split) -> TrainConfig | None:
    match split:
        case "train":
            return config.train
        case "test":
            return config.test
    assert_never(split)


def _category_partitions_name(split: Split) -> str:
    return f"{CODEBENCH_DATASET_ID}_{split}_categories"


def register_dataset_partitions(instance: DagsterInstance, spec: DatasetBuildSpec) -> None:
    instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
    if spec.dataset_id != CODEBENCH_DATASET_ID:
        return
    for split in _SPLITS:
        config = _split_config(spec, split)
        name = _category_partitions_name(split)
        desired: set[str] = {source.category for source in config.sources} if config is not None else set()
        current = set(instance.get_dynamic_partitions(name))
        if additions := sorted(desired - current):
            instance.add_dynamic_partitions(name, additions)
        for category in sorted(current - desired):
            instance.delete_dynamic_partition(name, category)


def _output[T: ConfigArtifact](value: T) -> Output[T]:
    return Output(value, data_version=DataVersion(fingerprint(value.model_dump_json())))


_SOURCE_TYPE = PythonObjectDagsterType(CategorySources, name="TrainCategorySources")
_COUNT_TYPE = PythonObjectDagsterType(CountSettings, name="TrainCountSettings")
_MERGE_TYPE = PythonObjectDagsterType(MergeSettings, name="TrainMergeSettings")
_STATS_TYPE = PythonObjectDagsterType(SplitStats, name="SplitStats")
_ROWS_TYPE = pandera_schema_to_dagster_type(SplitRowsTable)
_CATEGORY_ROWS_TYPE = Dict[str, _ROWS_TYPE]
_SPLIT_CONFIG = AssetIn(
    key=GLOBAL_CONFIG,
    dagster_type=REGISTERED_CONFIG_TYPE,
    partition_mapping=SpecificPartitionsPartitionMapping([CODEBENCH_DATASET_ID]),
)


@input_manager(required_resource_keys={TRAIN_TABLE_IO})
def train_partition_inputs(context: InputContext) -> dict[str, DataFrame[SplitRowsTable]]:
    manager = context.resources.train_table_io
    if not isinstance(manager, DataFrameIOManager):
        raise IntegrityError("Category merge requires registered dataset storage")
    if not isinstance(context.asset_partitions_def, DynamicPartitionsDefinition):
        raise IntegrityError("Category merge requires dynamically partitioned category rows")
    frames: dict[str, DataFrame[SplitRowsTable]] = {}
    for key in sorted(context.asset_partition_keys):
        record = current_materialization(context.instance, context.asset_key, key)
        if record is not None:
            reference = materialized_reference(context.instance, record)
            snapshot = manager.read_planned_output(context.asset_key, key, reference.digest)
            if not isinstance(snapshot.value, pd.DataFrame):
                raise IntegrityError("Expected a category table")
            frames[key] = SplitRowsTable.validate(snapshot.value)
    return frames


def _split_assets(split: Split) -> _SplitAssets:
    partitions = DynamicPartitionsDefinition(name=_category_partitions_name(split))
    rows_key = AssetKey([split, "category_rows"])
    sources_attr = f"{split}_sources"
    digest_attr = f"{sources_attr}_digest"
    consumption = "model training" if split == "train" else "held-out model evaluation"

    def configured(registered: RegisteredConfig) -> TrainConfig:
        config = _split_config(registered, split)
        if config is None or not config.sources:
            raise IntegrityError(f"No {split} sources configured for this registration")
        return config

    def category_sources(context: AssetExecutionContext, config: TrainConfig) -> CategorySources:
        category = context.partition_key
        sources = config.sources_by_category().get(category)
        if sources is None:
            raise IntegrityError(f"No {split} sources configured for {category}")
        return sources

    def require_source_version(frame: pd.DataFrame, digest: str) -> None:
        if frame.attrs.get(digest_attr) != digest:
            raise IntegrityError(
                f"{split.capitalize()} category rows do not match configured sources; rematerialize the category"
            )

    @asset(
        key=mapping_key(f"{split}_load"),
        partitions_def=partitions,
        group_name=split,
        io_manager_key="config_run_io_manager",
        dagster_type=_SOURCE_TYPE,
        ins={"registered": _SPLIT_CONFIG},
    )
    def load_config(context: AssetExecutionContext, registered: RegisteredConfig) -> Output[CategorySources]:
        return _output(category_sources(context, configured(registered)))

    @asset(
        key=mapping_key(f"{split}_counts"),
        partitions_def=partitions,
        group_name=split,
        io_manager_key="config_run_io_manager",
        dagster_type=_COUNT_TYPE,
        ins={"registered": _SPLIT_CONFIG},
    )
    def count_config(context: AssetExecutionContext, registered: RegisteredConfig) -> Output[CountSettings]:
        config = configured(registered)
        sources = category_sources(context, config)
        return _output(CountSettings(group_by=list(config.group_by), sources_digest=sources.digest))

    @asset(
        key=mapping_key(f"{split}_merge"),
        partitions_def=_MERGE_PARTITIONS,
        group_name=split,
        io_manager_key="config_run_io_manager",
        dagster_type=_MERGE_TYPE,
        ins={"registered": _SPLIT_CONFIG},
    )
    def merge_config(registered: RegisteredConfig) -> Output[MergeSettings]:
        sources = configured(registered).sources_by_category()
        return _output(MergeSettings(category_digests={key: value.digest for key, value in sources.items()}))

    @asset(
        key=rows_key,
        partitions_def=partitions,
        group_name=split,
        io_manager_key=TRAIN_TABLE_IO,
        dagster_type=_ROWS_TYPE,
        ins={"settings": AssetIn(key=load_config.key, dagster_type=_SOURCE_TYPE)},
    )
    def category_rows(settings: CategorySources) -> DataFrame[SplitRowsTable]:
        frame = load_train_category(settings.sources)
        frame.attrs[sources_attr] = [source.model_dump(mode="json") for source in settings.sources]
        frame.attrs[digest_attr] = settings.digest
        return frame

    @asset(
        key=AssetKey([split, "stats"]),
        partitions_def=partitions,
        group_name=split,
        io_manager_key="config_run_io_manager",
        dagster_type=_STATS_TYPE,
        ins={
            "rows": AssetIn(key=rows_key, dagster_type=_ROWS_TYPE),
            "settings": AssetIn(key=count_config.key, dagster_type=_COUNT_TYPE),
        },
    )
    def stats(rows: DataFrame[SplitRowsTable], settings: CountSettings) -> Output[SplitStats]:
        require_source_version(rows, settings.sources_digest)
        return _output(train_stats(rows, settings.group_by))

    @asset(
        key=AssetKey([split, "merged_rows"]),
        partitions_def=_MERGE_PARTITIONS,
        group_name=split,
        description=f"Full {split} dataset for {consumption}, containing rows from all configured "
        "categories in the single CodeBench partition.",
        io_manager_key=TRAIN_TABLE_IO,
        dagster_type=_ROWS_TYPE,
        ins={
            "categories": AssetIn(
                key=rows_key,
                dagster_type=_CATEGORY_ROWS_TYPE,
                partition_mapping=AllPartitionMapping(),
                input_manager_key="train_partition_inputs",
            ),
            "settings": AssetIn(key=merge_config.key, dagster_type=_MERGE_TYPE),
        },
    )
    def merged_rows(
        categories: dict[str, DataFrame[SplitRowsTable]], settings: MergeSettings
    ) -> DataFrame[SplitRowsTable]:
        selected: dict[str, DataFrame[SplitRowsTable]] = {}
        for category, digest in sorted(settings.category_digests.items()):
            if category not in categories:
                raise IntegrityError(f"Missing required {split} category: {category}")
            require_source_version(categories[category], digest)
            selected[category] = categories[category]
        frame = merge_train_categories(selected)
        frame.attrs[sources_attr] = {key: value.attrs[sources_attr] for key, value in selected.items()}
        return frame

    return _SplitAssets(
        mappings=(load_config, count_config, merge_config),
        tables=(category_rows, stats, merged_rows),
        jobs=(
            UnresolvedAssetJobDefinition(
                name=f"load_{split}_category",
                selection=AssetSelection.assets(load_config, count_config, category_rows, stats),
            ),
            UnresolvedAssetJobDefinition(
                name=f"merge_{split}_data", selection=AssetSelection.assets(merge_config, merged_rows)
            ),
        ),
    )


_BRANCHES: Final = tuple(_split_assets(split) for split in _SPLITS)
TRAIN_MAPPINGS = [mapping for branch in _BRANCHES for mapping in branch.mappings]
TRAIN_TABLES = [table for branch in _BRANCHES for table in branch.tables]
TRAIN_JOBS = [job for branch in _BRANCHES for job in branch.jobs]
