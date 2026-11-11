from __future__ import annotations

from typing import assert_never
from uuid import uuid4

from pydantic import TypeAdapter

from codebench_pipeline.human_review import (
    BatchVersion,
    CompletionToken,
    DecisionChange,
    HumanReviewError,
    PublicationPayload,
    PublicationReference,
    ReviewBatch,
    ReviewDecision,
    ReviewHistoryEntry,
    ReviewSnapshot,
    Text,
    decisions_digest,
    require_decided_batch,
)
from codebench_pipeline.human_review_schema import ReviewRepository, timestamp


class HumanReviewTransaction(ReviewRepository):
    def ensure_current_batch(self, snapshot: ReviewSnapshot) -> ReviewBatch:
        reusable: dict[str, ReviewDecision] = {}
        try:
            current = self.get_current_batch(snapshot.dataset_id, snapshot.review_stage)
        except HumanReviewError as error:
            if error.code != "not_found":
                raise
        else:
            if current.snapshot == snapshot:
                return current
            if current.snapshot.snapshot_id == snapshot.snapshot_id:
                raise HumanReviewError("conflict", "Snapshot digest has a different identity binding")
            if (
                current.snapshot.min_approvals == snapshot.min_approvals
                and current.snapshot.max_approvals == snapshot.max_approvals
            ):
                reusable = {
                    row.candidate_id: row
                    for row in self.list_decisions(current.batch_id)
                    if row.decision != "pending"
                    and row.candidate_id in snapshot.record_fingerprints
                    and snapshot.record_fingerprints[row.candidate_id]
                    == current.snapshot.record_fingerprints.get(row.candidate_id)
                    and (row.candidate_id in snapshot.eligible_candidate_ids)
                    == (row.candidate_id in current.snapshot.eligible_candidate_ids)
                }
            superseded = current.model_copy(update={"state": "superseded", "updated_at": timestamp()})
            self._write_batch(superseded, current=False)
            self._audit(
                ReviewHistoryEntry(event="superseded", batch=superseded, recorded_at=superseded.updated_at)
            )
        now = timestamp()
        batch = ReviewBatch(
            batch_id=uuid4().hex,
            snapshot=snapshot,
            generation=1,
            batch_revision=len(reusable),
            state="open",
            created_at=now,
            updated_at=now,
        )
        self._connection.execute(
            """INSERT INTO human_review_batches(batch_id, dataset_id, review_stage, current, data)
               VALUES (?, ?, ?, 1, ?)""",
            (batch.batch_id, snapshot.dataset_id, snapshot.review_stage, batch.model_dump_json()),
        )
        for candidate_id in snapshot.candidate_ids:
            previous = reusable.get(candidate_id)
            row = (
                previous.model_copy(
                    update={"batch_id": batch.batch_id, "carried_from_batch_id": previous.batch_id}
                )
                if previous is not None
                else ReviewDecision(
                    batch_id=batch.batch_id, candidate_id=candidate_id, created_at=now, updated_at=now
                )
            )
            self._connection.execute(
                "INSERT INTO human_review_decisions(batch_id, candidate_id, data) VALUES (?, ?, ?)",
                (batch.batch_id, candidate_id, row.model_dump_json()),
            )
        self._audit(ReviewHistoryEntry(event="created", batch=batch, recorded_at=now))
        for row in self.list_decisions(batch.batch_id):
            if row.carried_from_batch_id is not None:
                self._audit(ReviewHistoryEntry(event="carried", batch=batch, decision=row, recorded_at=now))
        return batch

    def _bound(self, snapshot: ReviewSnapshot, batch_id: str, generation: int) -> ReviewBatch:
        batch = self.get_current_batch(snapshot.dataset_id, snapshot.review_stage)
        if batch.snapshot != snapshot or batch.batch_id != batch_id or batch.generation != generation:
            raise HumanReviewError("stale", "Snapshot, batch or generation is no longer current")
        return batch

    def save_decision(self, snapshot: ReviewSnapshot, change: DecisionChange) -> ReviewDecision:
        batch = self._bound(snapshot, change.batch_id, change.generation)
        batch.require_open()
        row = self._connection.execute(
            "SELECT data FROM human_review_decisions WHERE batch_id = ? AND candidate_id = ?",
            (batch.batch_id, change.candidate_id),
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Candidate is not in this batch")
        previous = ReviewDecision.model_validate_json(TypeAdapter(str).validate_python(row[0]))
        if previous.row_revision != change.expected_row_revision:
            raise HumanReviewError("stale", "Decision row revision changed")
        match change.decision:
            case "approve":
                if change.candidate_id not in snapshot.eligible_candidate_ids:
                    raise HumanReviewError("ineligible", "Human approval cannot override eligibility")
            case "pending" | "reject":
                pass
            case _:
                assert_never(change.decision)
        now = timestamp()
        decision = ReviewDecision(
            batch_id=batch.batch_id,
            candidate_id=change.candidate_id,
            decision=change.decision,
            notes=change.notes,
            reviewer=change.reviewer,
            row_revision=previous.row_revision + 1,
            created_at=previous.created_at,
            updated_at=now,
        )
        self._connection.execute(
            "UPDATE human_review_decisions SET data = ? WHERE batch_id = ? AND candidate_id = ?",
            (decision.model_dump_json(), batch.batch_id, change.candidate_id),
        )
        updated = batch.model_copy(
            update={
                "batch_revision": batch.batch_revision + 1,
                "updated_at": now,
            }
        )
        self._write_batch(updated)
        self._audit(ReviewHistoryEntry(event="decision", batch=updated, decision=decision, recorded_at=now))
        return decision

    def expected_batch(self, snapshot: ReviewSnapshot, expected: BatchVersion) -> ReviewBatch:
        batch = self._bound(snapshot, expected.batch_id, expected.generation)
        if batch.batch_revision != expected.batch_revision:
            raise HumanReviewError("stale", "Batch revision changed")
        return batch

    def complete(
        self,
        snapshot: ReviewSnapshot,
        expected: BatchVersion,
        reviewer: str,
    ) -> CompletionToken:
        actor = TypeAdapter(Text).validate_python(reviewer)
        batch = self.expected_batch(snapshot, expected)
        batch.require_open()
        decisions = self.list_decisions(batch.batch_id)
        require_decided_batch(batch, decisions)
        token = CompletionToken(
            dataset_id=snapshot.dataset_id,
            review_stage=snapshot.review_stage,
            snapshot_id=snapshot.snapshot_id,
            decision_digest=decisions_digest(decisions),
            batch_id=batch.batch_id,
            generation=batch.generation,
            batch_revision=batch.batch_revision + 1,
            token=uuid4().hex,
        )
        now = timestamp()
        updated = batch.model_copy(
            update={
                "state": "completed",
                "completion": token,
                "reviewer": actor,
                "batch_revision": token.batch_revision,
                "updated_at": now,
            }
        )
        self._write_batch(updated)
        self._audit(ReviewHistoryEntry(event="completed", batch=updated, recorded_at=now))
        return token

    def reopen(self, snapshot: ReviewSnapshot, expected: BatchVersion, reviewer: str) -> ReviewBatch:
        actor = TypeAdapter(Text).validate_python(reviewer)
        batch = self.expected_batch(snapshot, expected)
        match batch.state:
            case "completed":
                pass
            case "open":
                raise HumanReviewError("conflict", "Only a completed batch can be reopened")
            case "superseded":
                raise HumanReviewError("stale", "Batch was superseded")
            case _:
                assert_never(batch.state)
        now = timestamp()
        updated = batch.model_copy(
            update={
                "state": "open",
                "generation": batch.generation + 1,
                "batch_revision": batch.batch_revision + 1,
                "completion": None,
                "continuation_run_id": None,
                "reviewer": actor,
                "updated_at": now,
            }
        )
        self._write_batch(updated)
        self._audit(ReviewHistoryEntry(event="reopened", batch=updated, recorded_at=now))
        return updated

    def record_continuation(
        self,
        snapshot: ReviewSnapshot,
        authorization: CompletionToken,
        run_id: str | None,
    ) -> ReviewBatch:
        run_id = TypeAdapter(Text | None).validate_python(run_id)
        self.authorize_completion(snapshot, authorization)
        batch = self.get_current_batch(snapshot.dataset_id, snapshot.review_stage)
        if run_id is None:
            return batch
        updated = batch.model_copy(update={"continuation_run_id": run_id, "updated_at": timestamp()})
        self._write_batch(updated)
        self._audit(ReviewHistoryEntry(event="continued", batch=updated, recorded_at=updated.updated_at))
        return updated

    def authorize_completion(
        self,
        snapshot: ReviewSnapshot,
        token: CompletionToken,
    ) -> CompletionToken:
        batch = self._bound(snapshot, token.batch_id, token.generation)
        if (
            batch.completion != token
            or token.dataset_id != snapshot.dataset_id
            or token.review_stage != snapshot.review_stage
        ):
            raise HumanReviewError("stale", "Completion token does not match persisted authorization")
        match batch.state:
            case "completed":
                decisions = self.list_decisions(batch.batch_id)
                require_decided_batch(batch, decisions)
                if not batch.reviewer.strip() or decisions_digest(decisions) != token.decision_digest:
                    raise HumanReviewError("stale", "Completed decisions or completion provenance changed")
                return token
            case "open" | "superseded":
                raise HumanReviewError("conflict", "Current batch is not completed")
            case _:
                assert_never(batch.state)

    def record_publication(
        self,
        snapshot: ReviewSnapshot,
        authorization: CompletionToken,
        payload: PublicationPayload,
    ) -> PublicationReference:
        token = self.authorize_completion(snapshot, authorization)
        reference = PublicationReference(
            **payload.model_dump(),
            publication_id=uuid4().hex,
            authorization=token,
            created_at=timestamp(),
        )
        self._connection.execute(
            """INSERT INTO human_review_publications(publication_id, dataset_id, batch_id, data)
                VALUES (?, ?, ?, ?)""",
            (reference.publication_id, snapshot.dataset_id, token.batch_id, reference.model_dump_json()),
        )
        self._audit(
            ReviewHistoryEntry(
                event="published",
                batch=self.get_current_batch(snapshot.dataset_id, snapshot.review_stage),
                publication=reference,
                recorded_at=reference.created_at,
            )
        )
        return reference
