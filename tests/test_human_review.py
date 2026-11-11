from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from codebench_pipeline.human_review import (
    DecisionChange,
    HumanReviewError,
    ReviewSnapshot,
)
from codebench_pipeline.human_review_store import HumanReviewStore


class HumanReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.store = HumanReviewStore(self.home)
        self.snapshot = ReviewSnapshot(
            dataset_id="CAT-3.1",
            snapshot_id="a" * 64,
            candidate_ids=("first", "second"),
            eligible_candidate_ids=("first",),
        )

    def test_strict_snapshot_rejects_ambiguous_membership_and_coercion(self) -> None:
        invalid = (
            {"candidate_ids": ("first", "first")},
            {"eligible_candidate_ids": ("missing",)},
            {"eligible_candidate_ids": ("first", "first")},
            {"snapshot_id": "A" * 64},
            {"candidate_ids": ["first", "second"]},
            {"extra": True},
        )
        for fields in invalid:
            with self.subTest(fields=fields):
                with self.assertRaises(ValidationError):
                    ReviewSnapshot.model_validate(self.snapshot.model_dump() | fields)
        with self.assertRaises(ValidationError):
            self.snapshot.dataset_id = "changed"

    def test_default_pending_and_ineligible_approval_cannot_complete_batch(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(self.snapshot)
            self.assertEqual(tx.counts(batch.batch_id).pending, 2)
            with self.assertRaises(HumanReviewError) as pending:
                tx.complete(self.snapshot, batch, "reviewer")
            self.assertEqual(pending.exception.code, "pending")
            with self.assertRaises(HumanReviewError) as ineligible:
                tx.save_decision(
                    self.snapshot,
                    DecisionChange(
                        batch_id=batch.batch_id,
                        generation=1,
                        candidate_id="second",
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="reviewer",
                    ),
                )
            self.assertEqual(ineligible.exception.code, "ineligible")
            self.assertEqual(tx.counts(batch.batch_id).pending, 2)

    def test_decisions_complete_with_persisted_notes_and_self_reported_reviewer(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(self.snapshot)
            first = tx.save_decision(
                self.snapshot,
                DecisionChange(
                    batch_id=batch.batch_id,
                    generation=1,
                    candidate_id="first",
                    expected_row_revision=0,
                    decision="approve",
                    notes="evidence checked",
                    reviewer="self-reported person",
                ),
            )
            tx.save_decision(
                self.snapshot,
                DecisionChange(
                    batch_id=batch.batch_id,
                    generation=1,
                    candidate_id="second",
                    expected_row_revision=0,
                    decision="reject",
                    notes="not eligible",
                    reviewer="another person",
                ),
            )
            current = tx.get_current_batch(self.snapshot.dataset_id)
            self.assertEqual(current.batch_revision, 2)
            token = tx.complete(self.snapshot, current, "completion person")
        with HumanReviewStore(self.home).transaction() as tx:
            current = tx.get_current_batch(self.snapshot.dataset_id)
            self.assertEqual(current.state, "completed")
            self.assertEqual(current.reviewer, "completion person")
            self.assertEqual(tx.authorize_completion(self.snapshot, token), token)
            self.assertEqual(tx.list_decisions(batch.batch_id)[0], first)
            self.assertEqual(first.notes, "evidence checked")
            self.assertEqual(first.reviewer, "self-reported person")
            self.assertEqual(first.row_revision, 1)
            self.assertEqual(
                tx.counts(batch.batch_id).model_dump(), {"pending": 0, "approve": 1, "reject": 1}
            )

    def test_stale_same_row_fails_but_different_rows_share_original_generation(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(self.snapshot)
            change = DecisionChange(
                batch_id=batch.batch_id,
                generation=1,
                candidate_id="first",
                expected_row_revision=0,
                decision="approve",
                reviewer="first writer",
            )
            tx.save_decision(self.snapshot, change)
        with self.store.transaction() as tx:
            with self.assertRaises(HumanReviewError) as stale:
                tx.save_decision(self.snapshot, change.model_copy(update={"notes": "lost update"}))
            self.assertEqual(stale.exception.code, "stale")
            second = tx.save_decision(
                self.snapshot,
                DecisionChange(
                    batch_id=batch.batch_id,
                    generation=1,
                    candidate_id="second",
                    expected_row_revision=0,
                    decision="reject",
                    reviewer="second writer",
                ),
            )
            self.assertEqual(second.row_revision, 1)
            with self.assertRaises(HumanReviewError) as revision:
                tx.complete(self.snapshot, batch, "reviewer")
            self.assertEqual(revision.exception.code, "stale")
            self.assertEqual(tx.list_decisions(batch.batch_id)[0].notes, "")

    def test_completed_batch_requires_reopen_and_old_generation_stays_invalid(self) -> None:
        snapshot = ReviewSnapshot(
            dataset_id="empty",
            snapshot_id="b" * 64,
            candidate_ids=(),
            eligible_candidate_ids=(),
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(snapshot)
            with self.assertRaises(ValidationError):
                tx.complete(snapshot, batch, " \t ")
            token = tx.complete(snapshot, batch, "human")
            current = tx.get_current_batch("empty")
            with self.assertRaises(HumanReviewError) as completed:
                tx.save_decision(
                    snapshot,
                    DecisionChange(
                        batch_id=batch.batch_id,
                        generation=1,
                        candidate_id="anything",
                        expected_row_revision=0,
                        decision="reject",
                        reviewer="human",
                    ),
                )
            self.assertEqual(completed.exception.code, "completed")
            reopened = tx.reopen(snapshot, current, "human reopening")
            self.assertEqual(reopened.generation, 2)
            self.assertIsNone(reopened.completion)
            with self.assertRaises(HumanReviewError) as stale:
                tx.authorize_completion(snapshot, token)
            self.assertEqual(stale.exception.code, "stale")
            next_token = tx.complete(snapshot, reopened, "human")
            self.assertNotEqual(next_token.token, token.token)
            with self.assertRaises(HumanReviewError):
                tx.authorize_completion(snapshot, token)

    def test_reopen_keeps_decisions_and_history_across_process_style_restart(self) -> None:
        snapshot = self.snapshot.model_copy(
            update={
                "candidate_ids": ("first",),
                "eligible_candidate_ids": ("first",),
            }
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(snapshot)
            row = tx.save_decision(
                snapshot,
                DecisionChange(
                    batch_id=batch.batch_id,
                    generation=1,
                    candidate_id="first",
                    expected_row_revision=0,
                    decision="approve",
                    notes="retain this",
                    reviewer="human",
                ),
            )
            tx.complete(snapshot, tx.get_current_batch(snapshot.dataset_id), "human")
        with HumanReviewStore(self.home).transaction() as tx:
            tx.reopen(snapshot, tx.get_current_batch(snapshot.dataset_id), "reopener")
        with HumanReviewStore(self.home).transaction() as tx:
            self.assertEqual(tx.list_decisions(batch.batch_id), (row,))
            self.assertEqual(
                tuple(item.event for item in tx.history(snapshot.dataset_id)),
                ("created", "decision", "completed", "reopened"),
            )
            with self.assertRaises(HumanReviewError) as generation:
                tx.save_decision(
                    snapshot,
                    DecisionChange(
                        batch_id=batch.batch_id,
                        generation=1,
                        candidate_id="first",
                        expected_row_revision=1,
                        decision="reject",
                        reviewer="stale human",
                    ),
                )
            self.assertEqual(generation.exception.code, "stale")


if __name__ == "__main__":
    unittest.main()
