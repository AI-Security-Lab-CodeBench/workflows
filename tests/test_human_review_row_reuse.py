import tempfile
import unittest
from pathlib import Path

from pydantic import JsonValue, ValidationError

from codebench_pipeline.human_review import DecisionChange, DecisionValue, HumanReviewError, ReviewSnapshot
from codebench_pipeline.human_review_store import HumanReviewStore


class HumanReviewRowReuseTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = HumanReviewStore(Path(temporary.name))
        self.snapshot = ReviewSnapshot(
            dataset_id="rows",
            snapshot_id="0" * 64,
            candidate_ids=("first", "second"),
            eligible_candidate_ids=("first", "second"),
            record_fingerprints={"first": "a" * 64, "second": "b" * 64},
        )
        self.complete_snapshot()

    def complete_snapshot(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(self.snapshot)
            choices: tuple[tuple[str, DecisionValue], ...] = (("first", "approve"), ("second", "reject"))
            for candidate_id, decision in choices:
                tx.save_decision(
                    self.snapshot,
                    DecisionChange(
                        batch_id=batch.batch_id,
                        generation=batch.generation,
                        candidate_id=candidate_id,
                        expected_row_revision=0,
                        decision=decision,
                        reviewer="original reviewer",
                        notes=f"checked {candidate_id}",
                    ),
                )
            self.original = tx.get_current_batch(self.snapshot.dataset_id, self.snapshot.review_stage)
            self.decisions = tx.list_decisions(batch.batch_id)
            self.token = tx.complete(self.snapshot, self.original, "completion reviewer")

    def test_new_neighbor_preserves_decisions_and_requires_new_completion(self) -> None:
        updated = self.snapshot.model_copy(
            update={
                "snapshot_id": "1" * 64,
                "candidate_ids": ("first", "second", "third"),
                "eligible_candidate_ids": ("first", "second", "third"),
                "record_fingerprints": {**self.snapshot.record_fingerprints, "third": "c" * 64},
            }
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(updated)
            rows = tx.list_decisions(batch.batch_id)
            self.assertEqual([row.decision for row in rows], ["approve", "reject", "pending"])
            for actual, previous in zip(rows[:2], self.decisions, strict=True):
                self.assertEqual(
                    actual,
                    previous.model_copy(
                        update={
                            "batch_id": batch.batch_id,
                            "carried_from_batch_id": self.original.batch_id,
                        }
                    ),
                )
            self.assertEqual(batch.state, "open")
            self.assertIsNone(batch.completion)
            self.assertEqual(tx.get_batch(self.original.batch_id).state, "superseded")
            self.assertEqual(tx.list_decisions(self.original.batch_id), self.decisions)
            self.assertEqual(
                [
                    entry.decision.candidate_id
                    for entry in tx.history("rows")
                    if entry.event == "carried" and entry.decision is not None
                ],
                ["first", "second"],
            )
            with self.assertRaises(HumanReviewError):
                tx.authorize_completion(updated, self.token)
            with self.assertRaises(HumanReviewError) as pending:
                tx.complete(updated, batch, "completion reviewer")
            self.assertEqual(pending.exception.code, "pending")

    def test_changed_row_invalidates_only_its_own_decision(self) -> None:
        updated = self.snapshot.model_copy(
            update={"snapshot_id": "1" * 64, "record_fingerprints": {"first": "a" * 64, "second": "d" * 64}}
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(updated)
            rows = tx.list_decisions(batch.batch_id)
            self.assertEqual([row.decision for row in rows], ["approve", "pending"])
            self.assertIsNone(rows[1].carried_from_batch_id)

    def test_reordering_reuses_rows_but_never_the_old_completion(self) -> None:
        updated = self.snapshot.model_copy(
            update={"snapshot_id": "1" * 64, "candidate_ids": ("second", "first")}
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(updated)
            self.assertEqual(tx.counts(batch.batch_id).pending, 0)
            self.assertIsNone(batch.completion)
            self.assertEqual(tx.ensure_current_batch(updated), batch)
            token = tx.complete(updated, batch, "new completion reviewer")
            self.assertNotEqual(token.token, self.token.token)
            self.assertEqual(tx.authorize_completion(updated, token), token)

    def test_changed_eligibility_does_not_carry_an_obsolete_approval(self) -> None:
        updated = self.snapshot.model_copy(
            update={"snapshot_id": "1" * 64, "eligible_candidate_ids": ("second",)}
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(updated)
            self.assertEqual(
                [row.decision for row in tx.list_decisions(batch.batch_id)], ["pending", "reject"]
            )

    def test_changed_bounds_and_legacy_fingerprints_require_fresh_decisions(self) -> None:
        updates: tuple[dict[str, JsonValue], ...] = (
            {"min_approvals": 1},
            {"max_approvals": 1},
            {"record_fingerprints": {}},
            {"review_stage": "another_stage"},
            {"dataset_id": "another_dataset"},
        )
        for index, changes in enumerate(updates, start=1):
            self.snapshot = self.snapshot.model_copy(update={"dataset_id": f"case-{index}"})
            self.complete_snapshot()
            with self.subTest(changes=changes), self.store.transaction() as tx:
                snapshot = self.snapshot.model_copy(update={**changes, "snapshot_id": str(index) * 64})
                batch = tx.ensure_current_batch(snapshot)
                self.assertEqual(
                    [row.decision for row in tx.list_decisions(batch.batch_id)], ["pending", "pending"]
                )

    def test_retracted_decision_is_not_resurrected_from_completed_history(self) -> None:
        with self.store.transaction() as tx:
            current = tx.get_current_batch("rows")
            reopened = tx.reopen(self.snapshot, current, "retracting reviewer")
            tx.save_decision(
                self.snapshot,
                DecisionChange(
                    batch_id=reopened.batch_id,
                    generation=reopened.generation,
                    candidate_id="first",
                    expected_row_revision=1,
                    decision="pending",
                    reviewer="retracting reviewer",
                ),
            )
            updated = self.snapshot.model_copy(update={"snapshot_id": "1" * 64})
            batch = tx.ensure_current_batch(updated)
            self.assertEqual(
                [row.decision for row in tx.list_decisions(batch.batch_id)], ["pending", "reject"]
            )

    def test_partial_or_foreign_fingerprint_coverage_is_rejected(self) -> None:
        for values in ({"first": "a" * 64}, {**self.snapshot.record_fingerprints, "foreign": "c" * 64}):
            with self.subTest(values=values), self.assertRaises(ValidationError):
                ReviewSnapshot.model_validate({**self.snapshot.model_dump(), "record_fingerprints": values})


if __name__ == "__main__":
    unittest.main()
