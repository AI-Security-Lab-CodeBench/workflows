from dagster import (
    AssetIn,
    AssetKey,
    AssetsDefinition,
    DataVersion,
    MetadataValue,
    Output,
    asset,
)
from pandera.typing import DataFrame

from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION
from codebench_pipeline.dataframe_schemas import (
    CommandsTable,
    HumanReviewDecisionsTable,
    ProposedAnnotationsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.guarded_publication import build_release_proposal
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.review_frames import require_decision_frame
from codebench_pipeline.review_inputs import (
    DECISION_INPUT,
    RELEASE_INPUT,
    REVIEW_INPUT_MANAGER,
    DecisionInput,
    ReleaseInput,
)
from codebench_pipeline.stage_configuration import DatasetReleaseConfig, ReviewDecisionConfig
from codebench_pipeline.storage import fingerprint


def human_assets() -> list[AssetsDefinition]:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[HumanReviewDecisionsTable],
        ins={
            "approved": AssetIn(
                key=AssetKey(["datasets", "human_review_queue"]),
                dagster_type=DECISION_INPUT,
                input_manager_key=REVIEW_INPUT_MANAGER,
            ),
            "mapped_config": mapped_input("human_review_decisions", ReviewDecisionConfig),
        },
    )
    def human_review_decisions(
        approved: DecisionInput,
        mapped_config: ReviewDecisionConfig,
    ) -> Output[DataFrame[HumanReviewDecisionsTable]]:
        if approved.token.dataset_id != mapped_config.dataset_id or approved.queue.attrs.get(
            REVIEW_PREPARATION
        ) != {
            "batch_id": approved.token.batch_id,
            "snapshot_id": approved.token.snapshot_id,
        }:
            raise HumanReviewError("stale", "Prepared snapshot differs from completion authorization")
        reviewed = approved.reviewed.loc[
            approved.reviewed["candidate_id"].isin(approved.candidate_ids)
        ].reset_index(drop=True)
        if not approved.queue.drop(columns="exclusion_reason").equals(reviewed):
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
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[CommandsTable],
        ins={
            "category_eligible_annotations": table_input(
                "category_eligible_annotations", ProposedAnnotationsTable
            ),
            "approved": AssetIn(
                key=AssetKey(["datasets", "human_review_decisions"]),
                dagster_type=RELEASE_INPUT,
                input_manager_key=REVIEW_INPUT_MANAGER,
            ),
            "mapped_config": mapped_input("dataset_release", DatasetReleaseConfig),
        },
    )
    def dataset_release(
        category_eligible_annotations: DataFrame[ProposedAnnotationsTable],
        approved: ReleaseInput,
        mapped_config: DatasetReleaseConfig,
    ) -> Output[DataFrame[CommandsTable]]:
        require_decision_frame(approved.persisted_decisions, approved.decisions)
        token = approved.evidence.token
        if (
            token.dataset_id != mapped_config.dataset_id
            or not ProposedAnnotationsTable.validate(category_eligible_annotations).equals(
                approved.evidence.resolved.reviewed
            )
            or mapped_config.digest != approved.execution.mapping_digest
        ):
            raise HumanReviewError("stale", "Release inputs differ from the completed snapshot")
        proposal = build_release_proposal(approved.evidence, mapped_config, approved.execution)
        version = fingerprint(token.model_dump(mode="json"))
        return Output(
            proposal.output,
            data_version=DataVersion(version),
            metadata={
                "input_fingerprint": version,
                "cache_hit": False,
                "schema": "CommandsTable",
                "evidence": MetadataValue.json(token.model_dump(mode="json")),
            },
        )

    return [human_review_decisions, dataset_release]
