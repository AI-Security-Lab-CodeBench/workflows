from pathlib import Path

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.human_review import DecisionChange
from codebench_pipeline.review_api_models import CompletionRequest, DecisionRequest
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_stage


def approve_fixture_sources(home: Path, dataset_id: str) -> None:
    with dataset_instance(home) as instance:
        workflow = ReviewWorkflow(home, source_stage(), SourceReviewSource(home, instance))
        view = workflow.view(DatasetQueryRequest(dataset_id=dataset_id))
        if view.batch.state == "completed":
            return
        for row in view.decisions:
            if row.decision != "pending":
                continue
            workflow.decide(
                DecisionRequest(
                    dataset_id=dataset_id,
                    snapshot_id=view.batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=view.batch.batch_id,
                        generation=view.batch.generation,
                        candidate_id=row.candidate_id,
                        expected_row_revision=row.row_revision,
                        decision="approve"
                        if row.candidate_id in view.batch.snapshot.eligible_candidate_ids
                        else "reject",
                        reviewer="fixture source reviewer",
                    ),
                )
            )
        saved = workflow.view(DatasetQueryRequest(dataset_id=dataset_id))
        workflow.complete(
            CompletionRequest(
                dataset_id=dataset_id,
                snapshot_id=saved.batch.snapshot.snapshot_id,
                expected=saved.batch,
                reviewer="fixture source reviewer",
            )
        )
