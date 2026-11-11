import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.human_review import (
    CompletionEffect,
    CompletionToken,
    DecisionChange,
    HumanReviewError,
    ReviewBatch,
    ReviewDecision,
    ReviewHistoryEntry,
    ReviewSnapshot,
)
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    EvidenceResponse,
    RetractionRequest,
)
from codebench_pipeline.review_stages import ReviewInput, ReviewStage
from codebench_pipeline.review_workflow import ReviewWorkflow


class LifecycleSource:
    def __init__(self, stage_id: str) -> None:
        self.snapshot = ReviewSnapshot(
            dataset_id="CAT-3.1",
            review_stage=stage_id,
            snapshot_id="a" * 64,
            candidate_ids=("first", "second"),
            eligible_candidate_ids=("first", "second"),
        )

    def current(self, dataset_id: str) -> ReviewInput:
        return ReviewInput(self.snapshot, pd.DataFrame({"id": ["first", "second"]}), "id")

    def evidence(self, snapshot: ReviewSnapshot, candidate_id: str) -> EvidenceResponse:
        return EvidenceResponse(fields={})


class ReviewLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.stage = ReviewStage(
            stage_id="selection", title="Selection", asset_keys=(("input",),), can_continue=True
        )
        self.source = LifecycleSource(self.stage.stage_id)
        self.workflow = ReviewWorkflow(self.home, self.stage, self.source)
        self.query = DatasetQueryRequest(dataset_id="CAT-3.1")
        batch = self.workflow.view(self.query).batch
        for candidate_id in ("first", "second"):
            self.workflow.decide(
                DecisionRequest(
                    dataset_id="CAT-3.1",
                    snapshot_id=batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=batch.batch_id,
                        generation=batch.generation,
                        candidate_id=candidate_id,
                        expected_row_revision=0,
                        decision="approve",
                        notes=f"retain {candidate_id}",
                        reviewer="original actor",
                    ),
                )
            )

    def completion(self) -> CompletionRequest:
        batch = self.workflow.view(self.query).batch
        return CompletionRequest(
            dataset_id="CAT-3.1",
            snapshot_id=batch.snapshot.snapshot_id,
            expected=batch,
            reviewer="completion actor",
        )

    def retraction(self) -> RetractionRequest:
        return RetractionRequest(
            **self.completion().model_dump(),
            candidate_id="first",
            expected_row_revision=1,
        )

    def persisted(self) -> tuple[ReviewBatch, tuple[ReviewDecision, ...], tuple[ReviewHistoryEntry, ...]]:
        with self.workflow.store.transaction() as tx:
            batch = tx.get_current_batch("CAT-3.1", self.stage.stage_id)
            return batch, tx.list_decisions(batch.batch_id), tx.history("CAT-3.1")

    def test_completed_row_retraction_preserves_other_rows_and_invalidates_token(self) -> None:
        completed = self.workflow.complete(self.completion())
        before = self.workflow.view(self.query)
        retracted = self.workflow.retract(self.retraction())
        view = self.workflow.view(self.query)
        self.assertEqual(retracted.decision.decision, "pending")
        self.assertEqual(retracted.decision.notes, "retain first")
        self.assertEqual(retracted.decision.reviewer, "completion actor")
        self.assertEqual(retracted.decision.row_revision, 2)
        self.assertEqual(view.decisions[1], before.decisions[1])
        self.assertEqual(view.batch.generation, before.batch.generation + 1)
        self.assertEqual(view.batch.state, "open")
        self.assertIsNone(view.batch.completion)
        assert completed.completion is not None
        with self.assertRaises(HumanReviewError) as stale:
            self.workflow.approved_records("CAT-3.1", completed.completion)
        self.assertEqual(stale.exception.code, "stale")
        with self.workflow.store.transaction() as tx:
            history = tx.history("CAT-3.1")
        self.assertEqual(tuple(entry.event for entry in history[-2:]), ("reopened", "decision"))
        self.assertEqual(history[-1].decision, retracted.decision)

    def test_open_row_retraction_does_not_reopen_or_change_other_stage(self) -> None:
        other_stage = self.stage.model_copy(update={"stage_id": "other"})
        other = ReviewWorkflow(self.home, other_stage, LifecycleSource("other"))
        original_other = other.view(self.query)
        before = self.workflow.view(self.query)
        with self.assertRaises(HumanReviewError) as foreign:
            other.retract(self.retraction())
        self.assertEqual(foreign.exception.code, "stale")
        self.workflow.retract(self.retraction())
        after = self.workflow.view(self.query)
        self.assertEqual(after.batch.generation, before.batch.generation)
        self.assertEqual(after.decisions[1], before.decisions[1])
        self.assertEqual(other.view(self.query), original_other)
        unchanged = self.persisted()
        with self.assertRaises(HumanReviewError) as pending:
            self.workflow.retract(self.retraction().model_copy(update={"expected_row_revision": 2}))
        self.assertEqual(pending.exception.code, "conflict")
        self.assertEqual(self.persisted(), unchanged)

    def test_invalid_retractions_leave_completed_batch_rows_and_audit_unchanged(self) -> None:
        self.workflow.complete(self.completion())
        request = self.retraction()
        before = self.persisted()
        invalid = (
            (request.model_copy(update={"expected_row_revision": 0}), "stale"),
            (request.model_copy(update={"candidate_id": "foreign"}), "not_found"),
            (request.model_copy(update={"snapshot_id": "b" * 64}), "stale"),
            (
                request.model_copy(
                    update={"expected": request.expected.model_copy(update={"batch_revision": 0})}
                ),
                "stale",
            ),
            (
                request.model_copy(
                    update={"expected": request.expected.model_copy(update={"generation": 99})}
                ),
                "stale",
            ),
            (
                request.model_copy(
                    update={"expected": request.expected.model_copy(update={"batch_id": "foreign"})}
                ),
                "stale",
            ),
        )
        for attempted, code in invalid:
            with self.subTest(code=code, request=attempted):
                with self.assertRaises(HumanReviewError) as error:
                    self.workflow.retract(attempted)
                self.assertEqual(error.exception.code, code)
                self.assertEqual(self.persisted(), before)

    def test_retraction_rolls_back_reopen_when_row_save_fails(self) -> None:
        self.workflow.complete(self.completion())
        request = self.retraction()
        before = self.persisted()
        with patch.object(HumanReviewTransaction, "save_decision", side_effect=RuntimeError("save failed")):
            with self.assertRaisesRegex(RuntimeError, "save failed"):
                self.workflow.retract(request)
        self.assertEqual(self.persisted(), before)

    def test_continuation_persists_run_without_changing_token_and_can_retry(self) -> None:
        def launch(token: CompletionToken) -> CompletionEffect:
            with self.workflow.store.transaction() as tx:
                tx.authorize_completion(self.source.snapshot, token)
            return CompletionEffect(run_id="actual-run")

        self.workflow.on_complete = launch
        completed = self.workflow.complete(self.completion())
        assert completed.completion is not None
        self.assertEqual(completed.run_id, "actual-run")
        self.assertEqual(completed.batch.continuation_run_id, "actual-run")
        request = self.completion()
        rows = self.workflow.view(self.query).decisions

        def fail(token: CompletionToken) -> CompletionEffect:
            raise HumanReviewError("continuation_failed", "Launch failed")

        self.workflow.on_complete = fail
        before = self.persisted()
        with self.assertRaises(HumanReviewError) as failed:
            self.workflow.continue_review(request)
        self.assertEqual(failed.exception.code, "continuation_failed")
        self.assertEqual(self.persisted(), before)

        def retry_launch(token: CompletionToken) -> CompletionEffect:
            with self.workflow.store.transaction() as tx:
                tx.authorize_completion(self.source.snapshot, token)
            return CompletionEffect(run_id="retry-run")

        restarted = ReviewWorkflow(self.home, self.stage, self.source, on_complete=retry_launch)
        result = restarted.continue_review(request)
        self.assertEqual(result.completion, completed.completion)
        self.assertEqual(result.batch.batch_revision, completed.batch.batch_revision)
        self.assertEqual(restarted.view(self.query).decisions, rows)
        self.assertEqual(result.batch.continuation_run_id, "retry-run")
        self.assertEqual(restarted.view(self.query).batch.continuation_run_id, "retry-run")

        def unchanged_launch(token: CompletionToken) -> CompletionEffect:
            return CompletionEffect()

        restarted.on_complete = unchanged_launch
        unchanged = restarted.continue_review(request)
        self.assertEqual(unchanged.batch.continuation_run_id, "retry-run")
        with self.workflow.store.transaction() as tx:
            continued = tuple(row for row in tx.history("CAT-3.1") if row.event == "continued")
        self.assertEqual(
            tuple(row.batch.completion for row in continued), (completed.completion, completed.completion)
        )
        self.assertEqual(
            tuple(row.batch.continuation_run_id for row in continued), ("actual-run", "retry-run")
        )
        self.workflow.retract(self.retraction())
        self.assertIsNone(self.workflow.view(self.query).batch.continuation_run_id)
        with self.assertRaises(HumanReviewError) as stale:
            restarted.continue_review(request)
        self.assertEqual(stale.exception.code, "stale")

    def test_continuation_requires_callback_and_rejects_stale_batch_without_changes(self) -> None:
        self.workflow.complete(self.completion())
        request = self.completion()
        before = self.persisted()
        with self.assertRaises(HumanReviewError) as missing:
            self.workflow.continue_review(request)
        self.assertEqual(missing.exception.code, "continuation_failed")
        self.assertEqual(self.persisted(), before)

    def test_disabled_continuation_capability_never_calls_the_callback(self) -> None:
        self.workflow.complete(self.completion())
        self.workflow.stage = self.stage.model_copy(update={"can_continue": False})

        def unexpected(token: CompletionToken) -> CompletionEffect:
            raise AssertionError("Disabled continuation was invoked")

        self.workflow.on_complete = unexpected
        before = self.persisted()
        with self.assertRaises(HumanReviewError) as disabled:
            self.workflow.continue_review(self.completion())
        self.assertEqual(disabled.exception.code, "continuation_failed")
        self.assertEqual(self.persisted(), before)

    def test_continuation_rechecks_authorization_after_callback(self) -> None:
        def reopen_during_launch(token: CompletionToken) -> CompletionEffect:
            self.workflow.reopen(self.completion())
            return CompletionEffect(run_id="obsolete-run")

        self.workflow.on_complete = reopen_during_launch
        with self.assertRaises(HumanReviewError) as stale:
            self.workflow.complete(self.completion())
        self.assertEqual(stale.exception.code, "stale")
        view = self.workflow.view(self.query)
        self.assertEqual(view.batch.state, "open")
        self.assertIsNone(view.batch.continuation_run_id)
