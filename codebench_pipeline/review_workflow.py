from collections.abc import Callable
from pathlib import Path

import pandas as pd
from pydantic import JsonValue

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.human_review import (
    CompletionEffect,
    CompletionToken,
    DecisionChange,
    HumanReviewError,
    PreparedReview,
    ReviewBatch,
    compact_batch,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    DecisionResponse,
    EvidenceResponse,
    EvidenceTarget,
    NextReviewRequest,
    PublicationResponse,
    PublicationTarget,
    RetractionRequest,
    ReviewItemResponse,
    ReviewStatus,
    ReviewViewResponse,
)
from codebench_pipeline.review_stages import ReviewSource, ReviewStage


class ReviewWorkflow:
    """Prepare once, review persisted rows, and authorize immutable decision versions."""

    def __init__(
        self,
        home: Path,
        stage: ReviewStage,
        source: ReviewSource,
        *,
        on_complete: Callable[[CompletionToken], CompletionEffect] | None = None,
    ) -> None:
        self.store = HumanReviewStore(home)
        self.stage = stage
        self.source = source
        self.on_complete = on_complete

    def prepare(self, dataset_id: str) -> ReviewBatch:
        with self.store.transaction(timeout=10, dataset_id=dataset_id) as tx:
            resolved = self.source.current(dataset_id)
            if resolved.snapshot.review_stage != self.stage.stage_id:
                raise HumanReviewError("conflict", "Prepared input belongs to another review stage")
            prepared = PreparedReview(
                snapshot=resolved.snapshot,
                id_column=resolved.id_column,
                columns=tuple(str(column) for column in resolved.frame.columns),
                rows=tuple(frame_records(resolved.frame)),
                validation=resolved.validation,
                evidence=resolved.evidence,
            )
            return tx.prepare(prepared)

    def _batch(
        self, tx: HumanReviewTransaction, dataset_id: str, snapshot_id: str | None
    ) -> ReviewBatch:
        batch = tx.get_current_batch(dataset_id, self.stage.stage_id)
        if snapshot_id is not None and batch.snapshot.snapshot_id != snapshot_id:
            raise HumanReviewError("stale", "The prepared input changed; refresh before continuing")
        return batch

    def view(self, request: DatasetQueryRequest) -> ReviewViewResponse:
        with self.store.reader() as tx:
            return tx.review_page(request, self.stage.stage_id)

    def item(self, target: EvidenceTarget) -> ReviewItemResponse:
        with self.store.reader() as tx:
            batch = tx.get_batch(target.batch_id)
            if batch.snapshot.review_stage != self.stage.stage_id:
                raise HumanReviewError("not_found", "Item belongs to another review stage")
            return tx.item(target)

    def next_pending(self, request: NextReviewRequest) -> ReviewItemResponse | None:
        with self.store.reader() as tx:
            return tx.next_pending(request, self.stage.stage_id)

    def validate_edits(
        self,
        rows: tuple[dict[str, JsonValue], ...],
        edits: dict[str, JsonValue],
        validation: dict[str, JsonValue],
    ) -> None:
        return None

    def decide(self, request: DecisionRequest) -> DecisionResponse:
        with self.store.transaction(timeout=10, dataset_id=request.dataset_id) as tx:
            batch = self._batch(tx, request.dataset_id, request.snapshot_id)
            if request.change.edits:
                self.validate_edits(
                    tx.rows_for_hash(batch.batch_id, request.change.candidate_id),
                    request.change.edits,
                    tx.validation(batch.batch_id),
                )
            decision = tx.save_decision(batch.snapshot, request.change)
            return DecisionResponse(
                batch=compact_batch(tx.get_current_batch(request.dataset_id, self.stage.stage_id)),
                decision=decision,
                counts=tx.counts(batch.batch_id),
            )

    def complete(self, request: CompletionRequest) -> PublicationResponse:
        with self.store.transaction(timeout=10, dataset_id=request.dataset_id) as tx:
            batch = self._batch(tx, request.dataset_id, request.snapshot_id)
            if (
                batch.completion is not None
                and request.expected.batch_id == batch.batch_id
                and request.expected.generation == batch.generation
                and request.expected.batch_revision in {
                    batch.batch_revision, batch.completion.batch_revision - 1
                }
            ):
                token = tx.authorize_completion(batch.snapshot, batch.completion)
            else:
                token = tx.complete(batch.snapshot, request.expected, request.reviewer)
        return self._completion_response(token)

    def continue_review(self, request: CompletionRequest) -> PublicationResponse:
        with self.store.transaction(timeout=10, dataset_id=request.dataset_id) as tx:
            batch = self._batch(tx, request.dataset_id, request.snapshot_id)
            tx.expected_batch(batch.snapshot, request.expected)
            if batch.completion is None:
                raise HumanReviewError("conflict", "Current review is not completed")
            token = tx.authorize_completion(batch.snapshot, batch.completion)
            if self.on_complete is None or not self.stage.can_continue:
                raise HumanReviewError("continuation_failed", "This review stage has no continuation")
        return self._completion_response(token)

    def _completion_response(self, token: CompletionToken) -> PublicationResponse:
        effect = self.on_complete(token) if self.on_complete is not None else CompletionEffect()
        with self.store.transaction(timeout=10, dataset_id=token.dataset_id) as tx:
            batch = self._batch(tx, token.dataset_id, token.snapshot_id)
            tx.authorize_completion(batch.snapshot, token)
            if effect.publication is not None and tx.get_publication(
                effect.publication.publication_id
            ) != effect.publication:
                raise HumanReviewError("publication_failed", "Publication reference does not match")
            batch = tx.record_continuation(batch.snapshot, token, effect.run_id)
            return PublicationResponse(
                batch=compact_batch(batch),
                counts=tx.counts(batch.batch_id),
                completion=token,
                publication=effect.publication,
                run_id=effect.run_id,
            )

    def retract(self, request: RetractionRequest) -> DecisionResponse:
        with self.store.transaction(timeout=10, dataset_id=request.dataset_id) as tx:
            batch = self._batch(tx, request.dataset_id, request.snapshot_id)
            tx.expected_batch(batch.snapshot, request.expected)
            previous = tx.item(
                EvidenceTarget(
                    dataset_id=request.dataset_id,
                    batch_id=batch.batch_id,
                    candidate_id=request.candidate_id,
                )
            ).decision
            if previous.decision == "pending":
                raise HumanReviewError("conflict", "Only a terminal decision can be retracted")
            if batch.state == "completed":
                batch = tx.reopen(batch.snapshot, request.expected, request.reviewer)
            decision = tx.save_decision(
                batch.snapshot,
                DecisionChange(
                    batch_id=batch.batch_id,
                    generation=batch.generation,
                    candidate_id=request.candidate_id,
                    expected_row_revision=request.expected_row_revision,
                    decision="pending",
                    notes=previous.notes,
                    reviewer=request.reviewer,
                ),
            )
            return DecisionResponse(
                batch=compact_batch(tx.get_current_batch(request.dataset_id, self.stage.stage_id)),
                counts=tx.counts(batch.batch_id),
                decision=decision,
            )

    def reopen(self, request: CompletionRequest) -> ReviewStatus:
        with self.store.transaction(timeout=10, dataset_id=request.dataset_id) as tx:
            batch = self._batch(tx, request.dataset_id, request.snapshot_id)
            reopened = tx.reopen(batch.snapshot, request.expected, request.reviewer)
            return ReviewStatus(batch=compact_batch(reopened), counts=tx.counts(batch.batch_id))

    def evidence(self, target: EvidenceTarget) -> EvidenceResponse:
        with self.store.reader() as tx:
            item = tx.item(target)
            batch = tx.get_batch(target.batch_id)
            if batch.snapshot.review_stage != self.stage.stage_id:
                raise HumanReviewError("not_found", "Evidence belongs to another stage")
            return EvidenceResponse(
                fields={
                    "record": item.row.values,
                    "occurrences": list(tx.rows_for_hash(target.batch_id, target.candidate_id)),
                    "validation": tx.validation(target.batch_id),
                    "review_columns": list(item.review_columns),
                    "edits": item.decision.edits,
                }
            )

    def approved_records(self, dataset_id: str, token: CompletionToken) -> pd.DataFrame:
        with self.store.reader() as tx:
            batch = self._batch(tx, dataset_id, token.snapshot_id)
            tx.authorize_completion(batch.snapshot, token)
            prepared = tx.prepared(batch.batch_id)
            decisions = {row.candidate_id: row for row in tx.list_decisions(batch.batch_id)}
            rows: list[dict[str, JsonValue]] = []
            for row in prepared.rows:
                decision = decisions[str(row[prepared.id_column])]
                if decision.decision == "approve":
                    rows.append({**row, **decision.edits})
            return pd.DataFrame(rows, columns=prepared.columns)

    def download(self, target: PublicationTarget) -> bytes:
        raise HumanReviewError("not_found", "This review stage does not publish downloadable files")
