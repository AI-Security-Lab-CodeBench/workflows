from dataclasses import dataclass
from pathlib import Path
from typing import Literal, assert_never

import pandas as pd
from dagster import AssetKey, DagsterInstance, build_input_context

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import (
    DatasetDescriptor,
    DatasetList,
    DatasetQueryRequest,
    DatasetViewError,
    RulesResponse,
    TableResponse,
)
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_plan import INPUT_STAGES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.table_query import TableQueryError, query_table


@dataclass(frozen=True, slots=True)
class StoredDatasetAsset:
    spec: DatasetBuildSpec
    stage: str
    reference: AssetReference
    value: pd.DataFrame | CategoryRules
    publication_state: Literal["published", "legacy_unreviewed", "stale"] | None = None


class DatasetDataService:
    def __init__(self, home: Path, instance: DagsterInstance) -> None:
        self.registry = DatasetRegistry(home)
        self.home = home.resolve()
        self.instance = instance

    def datasets(self) -> DatasetList:
        specs = [self.registry.get(key) for key in self.registry.keys()]
        return DatasetList(
            datasets=tuple(
                DatasetDescriptor(
                    dataset_id=spec.dataset_id,
                    category_id=spec.rules.category_id,
                    category=spec.rules.canonical,
                )
                for spec in specs
            )
        )

    def specification(self, dataset_id: str) -> DatasetBuildSpec:
        try:
            return self.registry.get(dataset_id)
        except FileNotFoundError as error:
            raise DatasetViewError("not_found", "Dataset is not registered") from error

    def read_current(self, dataset_id: str, stage: str) -> StoredDatasetAsset:
        historical = {
            "reviewed_annotations",
            "merged_reviewed_annotations",
            "documentation/reviewed_annotations",
            "automatic_dataset_release",
            "documentation/automatic_dataset_release",
            "documentation/human_review_queue",
            "documentation/human_review_decisions",
            "documentation/dataset_release",
        }
        if stage not in INPUT_STAGES and stage not in historical:
            raise DatasetViewError("not_found", "Unknown dataset stage")
        spec = self.specification(dataset_id)
        manager = DataFrameIOManager(storage=spec.storage, review_home=str(self.home))
        with build_input_context(
            asset_key=AssetKey(["datasets", *stage.split("/")]),
            partition_key=dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as context:
            try:
                snapshot = manager.read_current(context)
            except FileNotFoundError as error:
                raise DatasetViewError("not_materialized", "This stage has no persisted output") from error
        return StoredDatasetAsset(spec, stage, snapshot.reference, snapshot.value, snapshot.publication_state)

    def query(self, stage: str, request: DatasetQueryRequest) -> TableResponse | RulesResponse:
        stored = self.read_current(request.dataset_id, stage)
        if request.snapshot_id is not None and request.snapshot_id != stored.reference.digest:
            raise HumanReviewError("stale", "The dataset snapshot changed; refresh before continuing")
        match stored.value:
            case pd.DataFrame():
                return TableResponse(
                    dataset_id=request.dataset_id,
                    stage=stage,
                    snapshot_id=stored.reference.digest,
                    page=query_table(stored.value, request.query),
                    publication_state=stored.publication_state,
                )
            case CategoryRules():
                query = request.query
                if query.search or query.filters or query.sort or query.offset:
                    raise TableQueryError("query", "Category rules are JSON, not a queryable table")
                return RulesResponse(
                    dataset_id=request.dataset_id,
                    snapshot_id=stored.reference.digest,
                    rules=stored.value,
                )
            case _:
                assert_never(stored.value)
