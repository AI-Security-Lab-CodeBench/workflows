from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from dagster import InputContext, PythonObjectDagsterType, input_manager
from pandera.typing import DataFrame
from pydantic import ValidationError

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    HumanReviewDecisionsTable,
    ProposedAnnotationsTable,
    ReviewQueueTable,
)
from codebench_pipeline.guarded_publication import ReleaseExecution
from codebench_pipeline.guarded_publication_evidence import ReleaseInputs
from codebench_pipeline.human_review import CompletionToken, HumanReviewError
from codebench_pipeline.review_frames import decision_frame
from codebench_pipeline.review_snapshots import ReviewSnapshots, release_configuration_digest
from codebench_pipeline.stage_configuration import DatasetReleaseConfig


@dataclass(frozen=True, slots=True)
class DecisionInput:
    queue: DataFrame[ReviewQueueTable]
    reviewed: DataFrame[ProposedAnnotationsTable]
    candidate_ids: tuple[str, ...]
    token: CompletionToken
    decisions: DataFrame[HumanReviewDecisionsTable]


@dataclass(frozen=True, slots=True)
class ReleaseInput:
    persisted_decisions: DataFrame[HumanReviewDecisionsTable]
    decisions: DataFrame[HumanReviewDecisionsTable]
    evidence: ReleaseInputs
    execution: ReleaseExecution


REVIEW_INPUT_MANAGER = "review_inputs"
DECISION_INPUT = PythonObjectDagsterType(DecisionInput, name="AuthorizedDecisionInput")
RELEASE_INPUT = PythonObjectDagsterType(ReleaseInput, name="AuthorizedReleaseInput")


@input_manager(required_resource_keys={"dataset_io_manager"})
def review_input_manager(context: InputContext) -> DecisionInput | ReleaseInput:
    manager = context.resources.dataset_io_manager
    if not isinstance(manager, DataFrameIOManager):
        raise HumanReviewError("conflict", "Review inputs require a bound dataset IO manager")
    if manager.review_home is None or not manager.review_token:
        raise HumanReviewError("pending", "Complete human review before materializing release assets")
    try:
        token = CompletionToken.model_validate_json(manager.review_token)
    except ValidationError as error:
        raise HumanReviewError("stale", "Invalid completion authorization") from error
    dataset_id = context.asset_partition_key
    run_id = context.step_context.run_id
    manager.bind_configuration_producer(context)
    registered = manager.run_configuration(run_id, dataset_id)
    home = Path(manager.review_home)
    snapshots = ReviewSnapshots.for_manager(home, DatasetDataService(home, context.instance), manager)
    table = manager.load_input(context)
    if not isinstance(table, pd.DataFrame):
        raise HumanReviewError("conflict", "Review input is not a persisted table")
    with manager.review_store.reader() as tx:
        resolved = snapshots.authoritative_registered(registered, manager)
        tx.authorize_completion(resolved.snapshot, token)
        records = tx.list_decisions(token.batch_id)
    decisions = decision_frame(token, records)
    reviewed = ProposedAnnotationsTable.validate(resolved.reviewed)
    match context.asset_key.path[-1]:
        case "human_review_queue":
            return DecisionInput(
                ReviewQueueTable.validate(table), reviewed, resolved.snapshot.candidate_ids, token, decisions
            )
        case "human_review_decisions":
            evidence = ReleaseInputs(snapshots, resolved, token, records)
            policy = DatasetReleaseConfig.project(registered, registered.rules)
            return ReleaseInput(
                HumanReviewDecisionsTable.validate(table),
                decisions,
                evidence,
                ReleaseExecution.bind(
                    manager.repository_run(dataset_id, run_id),
                    evidence,
                    policy,
                    release_configuration_digest(registered),
                    manager.review_store.publication_versions(),
                ),
            )
        case _:
            raise HumanReviewError("conflict", "Unsupported review input asset")
