from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    DataVersion,
    MetadataValue,
    Output,
    ResourceParam,
    asset,
)
from pandera.typing import DataFrame

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import HumanReviewDecisionsTable, SourceCatalogTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_frames import decision_frame
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_authorization
from codebench_pipeline.storage import fingerprint


def source_review_assets() -> list[AssetsDefinition]:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[HumanReviewDecisionsTable],
        ins={"source_catalog": table_input("source_catalog", SourceCatalogTable)},
    )
    def human_source_decisions(
        context: AssetExecutionContext,
        source_catalog: DataFrame[SourceCatalogTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[HumanReviewDecisionsTable]]:
        if dataset_io_manager.review_home is None:
            raise HumanReviewError("pending", "Complete source selection before acquiring source documents")
        home = Path(dataset_io_manager.review_home)
        with HumanReviewStore(home).transaction(timeout=10) as tx:
            resolved, token = source_authorization(
                SourceReviewSource(home, context.instance), context.partition_key, tx
            )
            if not SourceCatalogTable.validate(source_catalog).equals(resolved.frame):
                raise HumanReviewError("stale", "Source inventory differs from the completed source review")
            frame = decision_frame(token, tx.list_decisions(token.batch_id))
        version = fingerprint(token.model_dump(mode="json"))
        return Output(
            frame,
            data_version=DataVersion(version),
            metadata={
                "input_fingerprint": version,
                "cache_hit": False,
                "schema": "HumanReviewDecisionsTable",
                "evidence": MetadataValue.json(token.model_dump(mode="json")),
            },
        )

    return [human_source_decisions]
