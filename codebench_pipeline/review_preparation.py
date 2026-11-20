from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    BoolMetadataValue,
    JsonMetadataValue,
    OpExecutionContext,
    OutputContext,
    TextMetadataValue,
    build_input_context,
)

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION, AssetReference, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import ProposedAnnotationsTable, ReviewQueueTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.object_store import sha256
from codebench_pipeline.review_snapshots import ResolvedReview, ReviewSnapshots
from codebench_pipeline.review_stages import annotation_stage
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.stage_configuration import ReviewQueueConfig


class QueueMetadataSink(Protocol):
    def add_output_metadata(
        self, metadata: Mapping[str, BoolMetadataValue | JsonMetadataValue | TextMetadataValue]
    ) -> None: ...


def record_queue_metadata(
    sink: QueueMetadataSink,
    cache_hit: BoolMetadataValue,
    evidence: JsonMetadataValue,
    fingerprint: TextMetadataValue,
) -> None:
    sink.add_output_metadata(
        {
            "cache_hit": cache_hit,
            "evidence": evidence,
            "input_fingerprint": fingerprint,
            "schema": TextMetadataValue(ReviewQueueTable.__name__),
        }
    )


def prepare_review_queue(
    context: OutputContext,
    dataset_io_manager: DataFrameIOManager,
    queue: pd.DataFrame,
) -> pd.DataFrame:
    from codebench_pipeline.review_service import AnnotationReviewSource

    if dataset_io_manager.review_home is None:
        raise HumanReviewError("not_found", "Review preparation requires a dataset home")
    home = Path(dataset_io_manager.review_home)
    context_run = context.step_context
    instance = context_run.instance
    dataset_id = context.asset_partition_key
    registered = dataset_io_manager.run_configuration(context.run_id, dataset_id)
    settings = registered.scopes.human_review_queue.human_review_queue
    policy = ReviewQueueConfig.project(registered, registered.rules)
    if context.output_metadata.get("review_policy_digest") != TextMetadataValue(
        sha256(policy.model_dump_json().encode())
    ):
        raise HumanReviewError("stale", "Mapped review policy differs from the registered policy")
    reference = dataset_io_manager.current_reference(
        build_input_context(
            instance=instance,
            asset_key=AssetKey(["datasets", "category_eligible_annotations"]),
            partition_key=dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
        )
    )
    digest = context.output_metadata.get("review_input_digest")
    if not isinstance(digest, TextMetadataValue) or digest.value != reference.digest:
        raise HumanReviewError("stale", "Review queue input changed before output commit")
    value = dataset_io_manager.read_version(reference)
    if not isinstance(value, pd.DataFrame):
        raise HumanReviewError("stale", "Review queue input is not an annotations table")
    annotations = ProposedAnnotationsTable.validate(value)
    execution = AssetExecutionContext(OpExecutionContext(context_run))
    cached = cached_table(
        execution,
        dataset_io_manager,
        settings,
        {"annotations": annotations, "category_rules": registered.rules},
        ReviewQueueTable,
        lambda _: ReviewQueueTable.validate(queue),
        registered=registered,
    )
    snapshots = ReviewSnapshots.for_manager(home, DatasetDataService(home, instance), dataset_io_manager)

    def capture(_: str) -> ResolvedReview:
        resolved = snapshots.capture_registered(
            registered, dataset_io_manager, tuple(settings.review_columns)
        )
        if resolved.manifest.references["category_eligible_annotations"] != reference or set(
            resolved.snapshot.candidate_ids
        ) != set(cached.value["candidate_id"]):
            raise HumanReviewError("stale", "Prepared membership differs from the materialized queue")
        return resolved

    batch = ReviewWorkflow(
        dataset_io_manager.review_store, annotation_stage(), AnnotationReviewSource(snapshots, capture)
    ).prepare(dataset_id, empty_queue_actor="automatic low-uncertainty policy")
    match cached.metadata:
        case {
            "cache_hit": BoolMetadataValue() as hit,
            "evidence": JsonMetadataValue() as evidence,
            "input_fingerprint": TextMetadataValue() as fingerprint,
        }:
            record_queue_metadata(context, hit, evidence, fingerprint)
        case _:
            raise HumanReviewError("conflict", "Review queue output is missing framework provenance")
    if cached.data_version is not None:
        execution.set_data_version(context.asset_key, cached.data_version)
    prepared = cached.value.copy()
    prepared.attrs[REVIEW_PREPARATION] = {
        "batch_id": batch.batch_id,
        "snapshot_id": batch.snapshot.snapshot_id,
    }
    return prepared


def write_review_queue(
    context: OutputContext, manager: DataFrameIOManager, queue: pd.DataFrame
) -> tuple[pd.DataFrame, AssetReference]:
    """Publish only if the prepared batch still owns the queue under the writer lock."""
    prepared = prepare_review_queue(context, manager, queue)
    reference = manager.persist_value(prepared)
    with manager.review_store.transaction(timeout=10, dataset_id=context.asset_partition_key) as tx:
        batch = tx.get_current_batch(context.asset_partition_key, "annotations", compact=True)
        if prepared.attrs.get(REVIEW_PREPARATION) != {
            "batch_id": batch.batch_id,
            "snapshot_id": batch.snapshot.snapshot_id,
        }:
            raise HumanReviewError("stale", "Review preparation changed before output commit")
        manager.publish_reference(context, reference)
    return prepared, reference
