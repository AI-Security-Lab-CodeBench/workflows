from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from codebench_pipeline.human_review import (
    DecisionChange,
    HumanReviewError,
    PublicationPayload,
    ReviewSnapshot,
)
from codebench_pipeline.human_review_store import HumanReviewStore


class HumanReviewStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.store = HumanReviewStore(self.home)
        self.snapshot = ReviewSnapshot(
            dataset_id="CAT-3.1",
            snapshot_id="a" * 64,
            candidate_ids=("candidate'; DROP TABLE inert; --",),
            eligible_candidate_ids=("candidate'; DROP TABLE inert; --",),
        )

    def test_changed_snapshot_supersedes_without_carrying_decisions_or_losing_audit(self) -> None:
        with self.store.transaction() as tx:
            original = tx.ensure_current_batch(self.snapshot)
            row = tx.save_decision(
                self.snapshot,
                DecisionChange(
                    batch_id=original.batch_id,
                    generation=1,
                    candidate_id=self.snapshot.candidate_ids[0],
                    expected_row_revision=0,
                    decision="approve",
                    notes="old evidence",
                    reviewer="human",
                ),
            )
            token = tx.complete(self.snapshot, tx.get_current_batch(self.snapshot.dataset_id), "human")
        changed = self.snapshot.model_copy(update={"snapshot_id": "b" * 64})
        with HumanReviewStore(self.home).transaction() as tx:
            fresh = tx.ensure_current_batch(changed)
            self.assertNotEqual(fresh.batch_id, original.batch_id)
            self.assertEqual(fresh.state, "open")
            self.assertEqual(tx.list_decisions(fresh.batch_id)[0].decision, "pending")
            self.assertEqual(tx.list_decisions(original.batch_id), (row,))
            self.assertEqual(tx.get_batch(original.batch_id).state, "superseded")
            self.assertEqual(
                tuple(item.event for item in tx.history(changed.dataset_id)),
                ("created", "decision", "completed", "superseded", "created"),
            )
            for snapshot in (self.snapshot, changed):
                with self.subTest(snapshot=snapshot.snapshot_id):
                    with self.assertRaises(HumanReviewError) as stale:
                        tx.authorize_completion(snapshot, token)
                    self.assertEqual(stale.exception.code, "stale")

    def test_publication_checks_persisted_token_and_current_server_binding(self) -> None:
        snapshot = self.snapshot.model_copy(
            update={
                "candidate_ids": (),
                "eligible_candidate_ids": (),
            }
        )
        payload = PublicationPayload(
            payload_key="payload-key",
            payload_hash="c" * 64,
            csv_reference="releases/content/commands.csv",
            bundle_reference="releases/content/bundle",
            dagster_run_id="run-id",
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(snapshot)
            token = tx.complete(snapshot, batch, "human")
            forged = token.model_copy(update={"token": "client-made"})
            with self.assertRaises(HumanReviewError) as rejected:
                tx.record_publication(snapshot, forged, payload)
            self.assertEqual(rejected.exception.code, "stale")
            self.assertEqual(tx.list_publications(snapshot.dataset_id), ())
            reference = tx.record_publication(snapshot, token, payload)
            changed = snapshot.model_copy(update={"snapshot_id": "d" * 64})
            with self.assertRaises(HumanReviewError):
                tx.record_publication(changed, token, payload)
            tx.reopen(snapshot, tx.get_current_batch(snapshot.dataset_id), "reopener")
            with self.assertRaises(HumanReviewError):
                tx.record_publication(snapshot, token, payload)
            tx.ensure_current_batch(changed)
        with HumanReviewStore(self.home).transaction() as tx:
            self.assertEqual(tx.get_publication(reference.publication_id), reference)
            self.assertEqual(tx.list_publications(snapshot.dataset_id), (reference,))
            self.assertEqual(reference.payload_key, "payload-key")
            self.assertEqual(reference.payload_hash, "c" * 64)
            self.assertEqual(reference.dagster_run_id, "run-id")
            published = tuple(
                entry for entry in tx.history(snapshot.dataset_id) if entry.event == "published"
            )
            self.assertEqual(tuple(entry.publication for entry in published), (reference,))

    def test_transaction_rollback_removes_decision_and_audit_changes(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(self.snapshot)
        with self.assertRaisesRegex(RuntimeError, "caller failed"):
            with self.store.transaction() as tx:
                tx.save_decision(
                    self.snapshot,
                    DecisionChange(
                        batch_id=batch.batch_id,
                        generation=1,
                        candidate_id=self.snapshot.candidate_ids[0],
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="human",
                    ),
                )
                token = tx.complete(self.snapshot, tx.get_current_batch(self.snapshot.dataset_id), "human")
                tx.record_publication(
                    self.snapshot,
                    token,
                    PublicationPayload(
                        payload_key="rolled-back",
                        payload_hash="e" * 64,
                        csv_reference="commands.csv",
                        bundle_reference="bundle",
                        dagster_run_id="rolled-back-run",
                    ),
                )
                raise RuntimeError("caller failed")
        with HumanReviewStore(self.home).transaction() as tx:
            self.assertEqual(tx.counts(batch.batch_id).pending, 1)
            self.assertEqual(tx.get_current_batch(self.snapshot.dataset_id).batch_revision, 0)
            self.assertEqual(tx.get_current_batch(self.snapshot.dataset_id).state, "open")
            self.assertEqual(tx.list_publications(self.snapshot.dataset_id), ())
            self.assertEqual(
                tuple(entry.event for entry in tx.history(self.snapshot.dataset_id)), ("created",)
            )

    def test_immediate_lock_is_held_and_connection_is_closed_after_context(self) -> None:
        with self.store.transaction() as tx:
            tx.ensure_current_batch(self.snapshot)
            with self.assertRaises(HumanReviewError) as locked:
                with HumanReviewStore(self.home).transaction():
                    self.fail("Competing write transaction acquired the lock")
            self.assertEqual(locked.exception.code, "conflict")
        with self.assertRaises(sqlite3.ProgrammingError):
            tx.get_current_batch(self.snapshot.dataset_id)
        with self.store.transaction() as next_tx:
            self.assertEqual(next_tx.get_current_batch(self.snapshot.dataset_id).state, "open")

    def test_category_isolation_preserves_identical_candidate_ids(self) -> None:
        other = self.snapshot.model_copy(update={"dataset_id": "CAT-3.2"})
        with self.store.transaction() as tx:
            first = tx.ensure_current_batch(self.snapshot)
            second = tx.ensure_current_batch(other)
            tx.save_decision(
                self.snapshot,
                DecisionChange(
                    batch_id=first.batch_id,
                    generation=1,
                    candidate_id=self.snapshot.candidate_ids[0],
                    expected_row_revision=0,
                    decision="approve",
                    reviewer="human",
                ),
            )
            tx.ensure_current_batch(self.snapshot.model_copy(update={"snapshot_id": "f" * 64}))
        with HumanReviewStore(self.home).transaction() as tx:
            self.assertEqual(tx.get_current_batch(other.dataset_id), second)
            self.assertEqual(tx.counts(second.batch_id).pending, 1)
            self.assertEqual(tuple(entry.event for entry in tx.history(other.dataset_id)), ("created",))

    def test_same_digest_cannot_rebind_membership_and_missing_identity_is_explicit(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(self.snapshot)
            rebound = self.snapshot.model_copy(update={"eligible_candidate_ids": ()})
            with self.assertRaises(HumanReviewError) as conflict:
                tx.ensure_current_batch(rebound)
            self.assertEqual(conflict.exception.code, "conflict")
            self.assertEqual(tx.ensure_current_batch(self.snapshot), batch)
            with self.assertRaises(HumanReviewError) as missing:
                tx.get_current_batch("unknown-category")
            self.assertEqual(missing.exception.code, "not_found")

    def test_completion_rejects_missing_rows_even_when_remaining_rows_are_terminal(self) -> None:
        snapshot = self.snapshot.model_copy(
            update={"candidate_ids": ("first", "second"), "eligible_candidate_ids": ("first", "second")}
        )
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(snapshot)
            tx.save_decision(
                snapshot,
                DecisionChange(
                    batch_id=batch.batch_id,
                    generation=1,
                    candidate_id="first",
                    expected_row_revision=0,
                    decision="approve",
                    reviewer="human",
                ),
            )
        with sqlite3.connect(self.store.path) as connection:
            connection.execute(
                "DELETE FROM human_review_decisions WHERE batch_id = ? AND candidate_id = ?",
                (batch.batch_id, "second"),
            )
        with self.store.transaction() as tx:
            with self.assertRaises(HumanReviewError) as incomplete:
                tx.complete(snapshot, tx.get_current_batch(snapshot.dataset_id), "human")
            self.assertEqual(incomplete.exception.code, "conflict")
            self.assertEqual(tx.get_current_batch(snapshot.dataset_id).state, "open")

    def test_publication_rechecks_decisions_instead_of_trusting_completed_flag(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(self.snapshot)
            row = tx.save_decision(
                self.snapshot,
                DecisionChange(
                    batch_id=batch.batch_id,
                    generation=1,
                    candidate_id=self.snapshot.candidate_ids[0],
                    expected_row_revision=0,
                    decision="reject",
                    reviewer="human",
                ),
            )
            token = tx.complete(self.snapshot, tx.get_current_batch(self.snapshot.dataset_id), "human")
        for changed, code in (("pending", "pending"), ("approve", "stale")):
            with self.subTest(decision=changed):
                with sqlite3.connect(self.store.path) as connection:
                    connection.execute(
                        "UPDATE human_review_decisions SET data = ? WHERE batch_id = ? AND candidate_id = ?",
                        (
                            row.model_copy(update={"decision": changed}).model_dump_json(),
                            batch.batch_id,
                            row.candidate_id,
                        ),
                    )
                with self.store.transaction() as tx:
                    with self.assertRaises(HumanReviewError) as stale:
                        tx.authorize_completion(self.snapshot, token)
                    self.assertEqual(stale.exception.code, code)
                    self.assertEqual(tx.list_publications(self.snapshot.dataset_id), ())


if __name__ == "__main__":
    unittest.main()
