from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal, assert_never

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    DagsterInstance,
    DataVersion,
    DynamicPartitionsDefinition,
    InputContext,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    Output,
    PythonObjectDagsterType,
    asset,
    input_manager,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from dagster._core.types.dagster_type import Any as DagsterAny

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    REGISTERED_CONFIG_TYPE,
    RegisteredConfig,
    mapping_key,
    registered_input,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager, materialized_reference
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.input_provenance import current_materialization
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.train_config import TrainConfig, TrainGroupColumn, TrainSource
from codebench_pipeline.train_tables import count_train_labels, load_train_category, merge_train_categories
from codebench_pipeline.typed_config import ConfigArtifact

Split = Literal["train", "test"]
_SPLITS: Final[tuple[Split, ...]] = ("train", "test")
DATASET_DIM: Final = MultiToSingleDimensionPartitionMapping(partition_dimension_name="dataset")
TRAIN_TABLE_IO: Final = "train_table_io"


class CategorySources(ConfigArtifact):
    sources: list[TrainSource]

    @property
    def digest(self) -> str:
        return fingerprint(self.model_dump_json())


class CountSettings(ConfigArtifact):
    group_by: list[TrainGroupColumn]
    sources_digest: str


class MergeSettings(ConfigArtifact):
    category_digests: dict[str, str]


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


def _category_dimension(split: Split) -> str:
    return f"{split}_categories"


def register_dataset_partitions(instance: DagsterInstance, spec: DatasetBuildSpec) -> None:
    instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
    for split in _SPLITS:
        config = _split_config(spec, split)
        if config is not None and config.sources:
            instance.add_dynamic_partitions(
                _category_dimension(split), sorted({source.category for source in config.sources})
            )


def _categories(config: TrainConfig) -> Mapping[str, CategorySources]:
    groups: defaultdict[str, list[TrainSource]] = defaultdict(list)
    for source in config.sources:
        groups[source.category].append(source)
    return {category: CategorySources(sources=sources) for category, sources in groups.items()}


def _output[T: ConfigArtifact](value: T) -> Output[T]:
    return Output(value, data_version=DataVersion(fingerprint(value.model_dump_json())))


_SOURCE_TYPE = PythonObjectDagsterType(CategorySources, name="TrainCategorySources")
_COUNT_TYPE = PythonObjectDagsterType(CountSettings, name="TrainCountSettings")
_MERGE_TYPE = PythonObjectDagsterType(MergeSettings, name="TrainMergeSettings")
_CATEGORY_CONFIG = AssetIn(
    key=GLOBAL_CONFIG, dagster_type=REGISTERED_CONFIG_TYPE, partition_mapping=DATASET_DIM
)


@input_manager(required_resource_keys={TRAIN_TABLE_IO})
def train_partition_inputs(context: InputContext) -> Mapping[str, pd.DataFrame]:
    manager = context.resources.train_table_io
    if not isinstance(manager, DataFrameIOManager):
        raise IntegrityError("Category merge requires registered dataset storage")
    partitions = context.asset_partitions_def
    if not isinstance(partitions, MultiPartitionsDefinition):
        raise IntegrityError("Category merge requires multipartitioned category rows")
    frames: dict[str, pd.DataFrame] = {}
    for key in sorted(context.asset_partition_keys):
        record = current_materialization(context.instance, context.asset_key, key)
        if record is not None:
            reference = materialized_reference(context.instance, record)
            snapshot = manager.read_planned_output(context.asset_key, key, reference.digest)
            if not isinstance(snapshot.value, pd.DataFrame):
                raise IntegrityError("Expected a category table")
            category = partitions.get_partition_key_from_str(key).keys_by_dimension["category"]
            frames[category] = snapshot.value
    return frames


def _split_assets(split: Split) -> _SplitAssets:
    partitions = MultiPartitionsDefinition(
        {
            "dataset": DATASET_PARTITIONS,
            "category": DynamicPartitionsDefinition(name=_category_dimension(split)),
        }
    )
    rows_key = AssetKey([split, "category_rows"])
    sources_attr = f"{split}_sources"
    digest_attr = f"{sources_attr}_digest"

    def configured(registered: RegisteredConfig) -> TrainConfig:
        config = _split_config(registered, split)
        if config is None or not config.sources:
            raise IntegrityError(f"No {split} sources configured for this registration")
        return config

    def category_sources(context: AssetExecutionContext, config: TrainConfig) -> CategorySources:
        category = partitions.get_partition_key_from_str(context.partition_key).keys_by_dimension["category"]
        sources = _categories(config).get(category)
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
        ins={"registered": _CATEGORY_CONFIG},
    )
    def load_config(context: AssetExecutionContext, registered: RegisteredConfig) -> Output[CategorySources]:
        return _output(category_sources(context, configured(registered)))

    @asset(
        key=mapping_key(f"{split}_counts"),
        partitions_def=partitions,
        group_name=split,
        io_manager_key="config_run_io_manager",
        dagster_type=_COUNT_TYPE,
        ins={"registered": _CATEGORY_CONFIG},
    )
    def count_config(context: AssetExecutionContext, registered: RegisteredConfig) -> Output[CountSettings]:
        config = configured(registered)
        sources = category_sources(context, config)
        return _output(CountSettings(group_by=list(config.group_by), sources_digest=sources.digest))

    @asset(
        key=mapping_key(f"{split}_merge"),
        partitions_def=DATASET_PARTITIONS,
        group_name=split,
        io_manager_key="config_run_io_manager",
        dagster_type=_MERGE_TYPE,
        ins={"registered": registered_input()},
    )
    def merge_config(registered: RegisteredConfig) -> Output[MergeSettings]:
        return _output(
            MergeSettings(
                category_digests={k: v.digest for k, v in _categories(configured(registered)).items()}
            )
        )

    @asset(
        key=rows_key,
        partitions_def=partitions,
        group_name=split,
        io_manager_key=TRAIN_TABLE_IO,
        ins={"settings": AssetIn(key=load_config.key, dagster_type=_SOURCE_TYPE)},
    )
    def category_rows(settings: CategorySources) -> pd.DataFrame:
        frame = load_train_category(settings.sources)
        frame.attrs[sources_attr] = [source.model_dump(mode="json") for source in settings.sources]
        frame.attrs[digest_attr] = settings.digest
        return frame

    @asset(
        key=AssetKey([split, "label_counts"]),
        partitions_def=partitions,
        group_name=split,
        io_manager_key=TRAIN_TABLE_IO,
        ins={
            "rows": AssetIn(key=rows_key),
            "settings": AssetIn(key=count_config.key, dagster_type=_COUNT_TYPE),
        },
    )
    def label_counts(rows: pd.DataFrame, settings: CountSettings) -> pd.DataFrame:
        require_source_version(rows, settings.sources_digest)
        return count_train_labels(rows, settings.group_by)

    @asset(
        key=AssetKey([split, "merged_rows"]),
        partitions_def=DATASET_PARTITIONS,
        group_name=split,
        io_manager_key=TRAIN_TABLE_IO,
        ins={
            "categories": AssetIn(
                key=rows_key,
                dagster_type=DagsterAny,
                partition_mapping=DATASET_DIM,
                input_manager_key="train_partition_inputs",
            ),
            "settings": AssetIn(key=merge_config.key, dagster_type=_MERGE_TYPE),
        },
    )
    def merged_rows(categories: Mapping[str, pd.DataFrame], settings: MergeSettings) -> pd.DataFrame:
        selected: dict[str, pd.DataFrame] = {}
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
        tables=(category_rows, label_counts, merged_rows),
        jobs=(
            UnresolvedAssetJobDefinition(
                name=f"load_{split}_category",
                selection=AssetSelection.assets(load_config, count_config, category_rows, label_counts),
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
