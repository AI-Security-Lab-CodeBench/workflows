from __future__ import annotations

from uuid import uuid4

from psycopg.types.json import Jsonb
from pydantic import TypeAdapter

from codebench_pipeline.human_review import (
    BatchVersion,
    CompletionToken,
    HumanReviewError,
    PublicationPayload,
    PublicationReference,
    ReviewBatch,
    ReviewHistoryEntry,
    ReviewSnapshot,
    Text,
    compact_batch,
    decisions_digest,
    require_decided_batch,
)
from codebench_pipeline.human_review_schema import timestamp
from codebench_pipeline.review_postgres_queries import ReviewQueries


class ReviewLifecycle(ReviewQueries):
    def lock_dataset(self, dataset_id: str) -> None:
        if dataset_id in self._locked:
            return
        key = f"{self._namespace}:dataset:{dataset_id}"
        if self._timeout == 0:
            row = self.connection.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0)) AS locked", (key,)
            ).fetchone()
            if row is None or not row["locked"]:
                raise HumanReviewError("conflict", "Another review transaction holds this dataset")
        else:
            self.connection.execute(
                "SELECT set_config('lock_timeout', %s, true)", (f"{max(1, int(self._timeout * 1000))}ms",)
            )
            self.connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (key,))
        self._locked.add(dataset_id)

    def _bound(self, snapshot: ReviewSnapshot, batch_id: str, generation: int) -> ReviewBatch:
        if not self.connection.read_only:
            self.lock_dataset(snapshot.dataset_id)
        try:
            batch = self.get_current_batch(snapshot.dataset_id, snapshot.review_stage)
        except HumanReviewError as error:
            if error.code == "not_found":
                raise HumanReviewError("stale", "No current authorization exists for this snapshot") from error
            raise
        full = self._full_batch(batch.batch_id)
        if (snapshot != full.snapshot and snapshot != compact_batch(full).snapshot) or (
            batch.batch_id != batch_id or batch.generation != generation
        ):
            raise HumanReviewError("stale", "Snapshot, batch or generation is no longer current")
        return batch

    def expected_batch(self, snapshot: ReviewSnapshot, expected: BatchVersion) -> ReviewBatch:
        batch = self._bound(snapshot, expected.batch_id, expected.generation)
        if batch.batch_revision != expected.batch_revision:
            raise HumanReviewError("stale", "Batch revision changed")
        return batch

    def complete(self, snapshot: ReviewSnapshot, expected: BatchVersion, reviewer: str) -> CompletionToken:
        actor = TypeAdapter(Text).validate_python(reviewer)
        batch = self.expected_batch(snapshot, expected)
        batch.require_open()
        decisions = self.list_decisions(batch.batch_id)
        if self.counts(batch.batch_id).blocked:
            raise HumanReviewError("ineligible", "Cached approvals are blocked by current validation")
        require_decided_batch(self._full_batch(batch.batch_id), decisions)
        token = CompletionToken(
            dataset_id=snapshot.dataset_id, review_stage=snapshot.review_stage, snapshot_id=snapshot.snapshot_id,
            decision_digest=decisions_digest(decisions), batch_id=batch.batch_id, generation=batch.generation,
            batch_revision=batch.batch_revision + 1, token=uuid4().hex,
        )
        now = timestamp()
        updated = batch.model_copy(update={"state": "completed", "completion": token, "reviewer": actor,
                                          "batch_revision": token.batch_revision, "updated_at": now})
        self._write_batch(updated)
        self.audit(ReviewHistoryEntry(event="completed", batch=updated, recorded_at=now))
        return token

    def reopen(self, snapshot: ReviewSnapshot, expected: BatchVersion, reviewer: str) -> ReviewBatch:
        actor = TypeAdapter(Text).validate_python(reviewer)
        batch = self.expected_batch(snapshot, expected)
        if batch.state != "completed":
            raise HumanReviewError("conflict", "Only a completed batch can be reopened")
        now = timestamp()
        updated = batch.model_copy(update={
            "state": "open", "generation": batch.generation + 1, "batch_revision": batch.batch_revision + 1,
            "completion": None, "continuation_run_id": None, "reviewer": actor, "updated_at": now,
        })
        self._write_batch(updated)
        self.audit(ReviewHistoryEntry(event="reopened", batch=updated, recorded_at=now))
        return updated

    def authorize_completion(self, snapshot: ReviewSnapshot, token: CompletionToken) -> CompletionToken:
        batch = self._bound(snapshot, token.batch_id, token.generation)
        if batch.completion != token or token.dataset_id != snapshot.dataset_id or token.review_stage != snapshot.review_stage:
            raise HumanReviewError("stale", "Completion token does not match persisted authorization")
        if batch.state != "completed":
            raise HumanReviewError("conflict", "Current batch is not completed")
        decisions = self.list_decisions(batch.batch_id)
        require_decided_batch(self._full_batch(batch.batch_id), decisions)
        if self.counts(batch.batch_id).blocked:
            raise HumanReviewError("ineligible", "Cached approvals are blocked by current validation")
        if not batch.reviewer.strip() or decisions_digest(decisions) != token.decision_digest:
            raise HumanReviewError("stale", "Completed decisions or completion provenance changed")
        return token

    def record_continuation(
        self, snapshot: ReviewSnapshot, authorization: CompletionToken, run_id: str | None,
    ) -> ReviewBatch:
        run_id = TypeAdapter(Text | None).validate_python(run_id)
        self.authorize_completion(snapshot, authorization)
        batch = self.get_current_batch(snapshot.dataset_id, snapshot.review_stage)
        if run_id is None:
            return batch
        updated = batch.model_copy(update={"continuation_run_id": run_id, "updated_at": timestamp()})
        self._write_batch(updated)
        self.audit(ReviewHistoryEntry(event="continued", batch=updated, recorded_at=updated.updated_at))
        return updated

    def record_publication(
        self, snapshot: ReviewSnapshot, authorization: CompletionToken, payload: PublicationPayload,
    ) -> PublicationReference:
        token = self.authorize_completion(snapshot, authorization)
        reference = PublicationReference(**payload.model_dump(), publication_id=uuid4().hex, authorization=token,
                                         created_at=timestamp())
        self.connection.execute(
            "INSERT INTO review_publications(publication_id,dataset_id,batch_id,data) VALUES(%s,%s,%s,%s)",
            (reference.publication_id, snapshot.dataset_id, token.batch_id, Jsonb(reference.model_dump(mode="json"))),
        )
        self.audit(ReviewHistoryEntry(event="published", batch=self.get_current_batch(snapshot.dataset_id, snapshot.review_stage),
                                       publication=reference, recorded_at=reference.created_at))
        return reference
