from dataclasses import dataclass
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
from pydantic import ValidationError

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    CommandsTable,
    HumanReviewDecisionsTable,
    ReviewedAnnotationsTable,
    ReviewQueueTable,
    validated_frame,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, rules_input, table_input
from codebench_pipeline.dataset_branches import Branch, asset_branch, asset_path
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.human_review import CompletionToken, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.review_frames import decision_frame, require_decision_frame
from codebench_pipeline.review_snapshots import ResolvedReview, ReviewSnapshots
from codebench_pipeline.storage import fingerprint


@dataclass(frozen=True, slots=True)
class AuthorizedReview:
    resolved: ResolvedReview
    token: CompletionToken
    decisions: DataFrame[HumanReviewDecisionsTable]


def authorized_review(
    context: AssetExecutionContext, config: HumanReviewConfig, manager: DataFrameIOManager
) -> AuthorizedReview:
    if manager.review_home is None or not config.authorization:
        raise HumanReviewError("pending", "Complete human review before materializing release assets")
    try:
        token = CompletionToken.model_validate_json(config.authorization)
    except ValidationError as error:
        raise HumanReviewError("stale", "Invalid completion authorization") from error
    home = Path(manager.review_home)
    snapshots = ReviewSnapshots(
        home, DatasetDataService(home, context.instance), asset_branch(context.asset_key.path)
    )
    with HumanReviewStore(home).reader() as tx:
        resolved = snapshots.current(context.partition_key)
        tx.authorize_completion(resolved.snapshot, token)
        decisions = decision_frame(token, tx.list_decisions(token.batch_id))
    return AuthorizedReview(resolved, token, decisions)


def human_assets(branch: Branch = "dataset") -> list[AssetsDefinition]:
    @asset(
        key_prefix=asset_path("human_review_decisions", branch)[:-1],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[HumanReviewDecisionsTable],
        ins={"human_review_queue": table_input("human_review_queue", ReviewQueueTable, branch)},
    )
    def human_review_decisions(
        context: AssetExecutionContext,
        config: HumanReviewConfig,
        human_review_queue: DataFrame[ReviewQueueTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[HumanReviewDecisionsTable]]:
        approved = authorized_review(context, config, dataset_io_manager)
        reviewed = approved.resolved.reviewed.loc[
            approved.resolved.reviewed["candidate_id"].isin(approved.resolved.snapshot.candidate_ids)
        ].reset_index(drop=True)
        queue = ReviewQueueTable.validate(human_review_queue)
        if not queue.loc[:, reviewed.columns].equals(reviewed):
            raise HumanReviewError("stale", "Review queue differs from the completed snapshot")
        version = fingerprint(approved.token.model_dump(mode="json"))
        return Output(
            approved.decisions,
            data_version=DataVersion(version),
            metadata={
                "input_fingerprint": version,
                "cache_hit": False,
                "schema": "HumanReviewDecisionsTable",
                "evidence": MetadataValue.json(approved.token.model_dump(mode="json")),
            },
        )

    @asset(
        key_prefix=asset_path("dataset_release", branch)[:-1],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[CommandsTable],
        ins={
            "reviewed_annotations": table_input("deduplicated_annotations", ReviewedAnnotationsTable),
            "human_review_decisions": table_input(
                "human_review_decisions", HumanReviewDecisionsTable, branch
            ),
            "category_rules": rules_input(),
        },
    )
    def dataset_release(
        context: AssetExecutionContext,
        config: HumanReviewConfig,
        reviewed_annotations: DataFrame[ReviewedAnnotationsTable],
        human_review_decisions: DataFrame[HumanReviewDecisionsTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandsTable]]:
        approved = authorized_review(context, config, dataset_io_manager)
        require_decision_frame(human_review_decisions, approved.decisions)
        if (
            not ReviewedAnnotationsTable.validate(reviewed_annotations).equals(approved.resolved.reviewed)
            or category_rules != approved.resolved.rules
        ):
            raise HumanReviewError("stale", "Release inputs differ from the completed snapshot")
        identities = frozenset(
            str(value)
            for value in approved.decisions.loc[
                approved.decisions["human_decision"] == "approve", "candidate_id"
            ]
        )
        prepared = publication(
            approved.resolved.reviewed,
            approved.resolved.rules,
            approved.resolved.manifest.publication,
            approved_ids=identities,
        )
        frame = validated_frame(
            [{key: value for key, value in row.items()} for row in prepared.commands], CommandsTable
        )
        version = fingerprint(approved.token.model_dump(mode="json"))
        return Output(
            frame,
            data_version=DataVersion(version),
            metadata={
                "input_fingerprint": version,
                "cache_hit": False,
                "schema": "CommandsTable",
                "evidence": MetadataValue.json(approved.token.model_dump(mode="json")),
            },
        )

    return [human_review_decisions, dataset_release]
