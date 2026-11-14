from pathlib import Path

from dagster import AssetExecutionContext, DataVersion, MetadataValue, Output, ResourceParam, asset
from pandera.typing import DataFrame

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    ProposedAnnotationsTable,
    ReviewQueueTable,
    ReviewSnapshotTable,
    validated_frame,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, rules_input, table_input
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.human_review import CompletionEffect, HumanReviewError
from codebench_pipeline.review_service import ReviewService


@asset(
    key_prefix=["datasets"],
    partitions_def=DATASET_PARTITIONS,
    io_manager_key="dataset_io_manager",
    dagster_type=TABLE_TYPES[ReviewSnapshotTable],
    ins={
        "human_review_queue": table_input("human_review_queue", ReviewQueueTable),
        "category_eligible_annotations": table_input(
            "category_eligible_annotations", ProposedAnnotationsTable
        ),
        "category_rules": rules_input(),
    },
)
def human_review_snapshot(
    context: AssetExecutionContext,
    config: PublicationConfig,
    human_review_queue: DataFrame[ReviewQueueTable],
    category_eligible_annotations: DataFrame[ProposedAnnotationsTable],
    category_rules: CategoryRules,
    dataset_io_manager: ResourceParam[DataFrameIOManager],
) -> Output[DataFrame[ReviewSnapshotTable]]:
    if dataset_io_manager.review_home is None:
        raise HumanReviewError("not_found", "Review preparation requires a dataset home")
    home = Path(dataset_io_manager.review_home)
    batch = ReviewService(
        home, DatasetDataService(home, context.instance), lambda _token: CompletionEffect()
    ).prepare(context.partition_key)
    if set(batch.snapshot.candidate_ids) != set(human_review_queue["candidate_id"]):
        raise HumanReviewError("stale", "Prepared membership differs from the materialized queue")
    frame = validated_frame(
        [{"batch_id": batch.batch_id, "snapshot_id": batch.snapshot.snapshot_id}],
        ReviewSnapshotTable,
    )
    frame.attrs = dict(category_eligible_annotations.attrs)
    signature = stage_signature(
        "human_review_snapshot",
        config,
        {
            "human_review_queue": human_review_queue,
            "category_eligible_annotations": category_eligible_annotations,
            "category_rules": category_rules,
        },
    )
    return Output(
        frame,
        data_version=DataVersion(signature),
        metadata={
            "input_fingerprint": signature,
            "cache_hit": False,
            "schema": "ReviewSnapshotTable",
            "evidence": MetadataValue.json({"batch_id": batch.batch_id}),
        },
    )
