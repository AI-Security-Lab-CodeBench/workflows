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

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    CommandsTable,
    ReviewedAnnotationsTable,
    ReviewQueueTable,
    frame_records,
    validated_frame,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, rules_input, table_input
from codebench_pipeline.dataset_branches import Branch, asset_path
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_tables import publication, review_queue
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.publication_authorization import preparation_authorization
from codebench_pipeline.review_frames import REVIEW_BINDING
from codebench_pipeline.review_snapshots import ReviewSnapshots


def automatic_release_asset(branch: Branch = "dataset") -> AssetsDefinition:
    @asset(
        key_prefix=asset_path("automatic_dataset_release", branch)[:-1],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[CommandsTable],
        ins={
            "human_review_queue": table_input("human_review_queue", ReviewQueueTable, branch),
            "reviewed_annotations": table_input("reviewed_annotations", ReviewedAnnotationsTable, branch),
            "category_rules": rules_input(),
        },
    )
    def automatic_dataset_release(
        context: AssetExecutionContext,
        config: PublicationConfig,
        human_review_queue: DataFrame[ReviewQueueTable],
        reviewed_annotations: DataFrame[ReviewedAnnotationsTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandsTable]]:
        if dataset_io_manager.review_home is None:
            raise HumanReviewError("pending", "A registered review authority is required")
        home = Path(dataset_io_manager.review_home)
        with HumanReviewStore(home).transaction(timeout=10) as transaction:
            resolved = ReviewSnapshots(home, DatasetDataService(home, context.instance), branch).current(
                context.partition_key
            )
            if (
                frame_records(reviewed_annotations) != frame_records(resolved.reviewed)
                or category_rules != resolved.rules
                or frame_records(human_review_queue)
                != frame_records(review_queue(resolved.reviewed, resolved.rules, config))
            ):
                raise HumanReviewError("stale", "Preparation inputs differ from the current snapshot")
            token = preparation_authorization(resolved, transaction)
            approvals = frozenset(
                row.candidate_id
                for row in transaction.list_decisions(token.batch_id)
                if row.decision == "approve"
            )
        prepared = publication(reviewed_annotations, category_rules, config, approved_ids=approvals)
        frame = validated_frame(
            [{key: value for key, value in row.items()} for row in prepared.commands], CommandsTable
        )
        frame.attrs[REVIEW_BINDING] = token.model_dump(mode="json")
        signature = stage_signature(
            "automatic_dataset_release",
            config,
            {
                "human_review_queue": human_review_queue,
                "reviewed_annotations": reviewed_annotations,
                "category_rules": category_rules,
            },
        )
        return Output(
            frame,
            data_version=DataVersion(signature),
            metadata={
                "input_fingerprint": signature,
                "cache_hit": False,
                "schema": "CommandsTable",
                "evidence": MetadataValue.json(token.model_dump(mode="json")),
            },
        )

    return automatic_dataset_release
