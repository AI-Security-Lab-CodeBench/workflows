from pathlib import Path

from dagster import AssetExecutionContext
from pandera.typing import DataFrame

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import ReviewQueueTable
from codebench_pipeline.human_review import CompletionEffect, HumanReviewError


def prepare_review_queue(
    context: AssetExecutionContext,
    dataset_io_manager: DataFrameIOManager,
    queue: DataFrame[ReviewQueueTable],
) -> DataFrame[ReviewQueueTable]:
    from codebench_pipeline.review_service import ReviewService

    if dataset_io_manager.review_home is None:
        raise HumanReviewError("not_found", "Review preparation requires a dataset home")
    home = Path(dataset_io_manager.review_home)
    batch = ReviewService(
        home, DatasetDataService(home, context.instance), lambda _token: CompletionEffect()
    ).prepare(context.partition_key)
    if set(batch.snapshot.candidate_ids) != set(queue["candidate_id"]):
        raise HumanReviewError("stale", "Prepared membership differs from the materialized queue")
    prepared = queue.copy()
    prepared.attrs[REVIEW_PREPARATION] = {
        "batch_id": batch.batch_id,
        "snapshot_id": batch.snapshot.snapshot_id,
    }
    return prepared
