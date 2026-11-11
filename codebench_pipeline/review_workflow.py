from collections.abc import Callable
from pathlib import Path
from typing import assert_never

import pandas as pd
from pydantic import TypeAdapter

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.human_review import (
    CompletionEffect,
    CompletionToken,
    DecisionChange,
    HumanReviewError,
    PublicationReference,
    ReviewBatch,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    DecisionResponse,
    EvidenceResponse,
    EvidenceTarget,
    PublicationResponse,
    PublicationTarget,
    RetractionRequest,
    ReviewPage,
    ReviewStatus,
    ReviewTableRow,
    ReviewViewResponse,
)
from codebench_pipeline.review_stages import ReviewInput, ReviewSource, ReviewStage
from codebench_pipeline.table_query import query_table


class ReviewWorkflow:
    """Stage-isolated human decisions over any immutable, server-resolved table."""

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

    def _current(self, dataset_id: str, snapshot_id: str | None) -> ReviewInput:
        resolved = self.source.current(dataset_id)
        snapshot = resolved.snapshot
        if (
            snapshot.dataset_id != dataset_id
            or snapshot.review_stage != self.stage.stage_id
            or snapshot.min_approvals != self.stage.min_approvals
            or snapshot.max_approvals != self.stage.max_approvals
        ):
            raise HumanReviewError("conflict", "Review source has a different stage or policy binding")
        identities = TypeAdapter(list[str]).validate_python(
            resolved.frame[resolved.id_column].tolist(), strict=True
        )
        if tuple(identities) != snapshot.candidate_ids:
            raise HumanReviewError("conflict", "Review table identities differ from its snapshot")
        if snapshot_id is not None and snapshot_id != snapshot.snapshot_id:
            raise HumanReviewError("stale", "The review snapshot changed; refresh before continuing")
        return resolved

    def current_publication(
        self, transaction: HumanReviewTransaction, batch: ReviewBatch
    ) -> PublicationReference | None:
        publication = transaction.last_publication(batch.snapshot.dataset_id, self.stage.stage_id)
        if publication is not None and publication.authorization == batch.completion:
            return publication
        return None

    def view(self, request: DatasetQueryRequest) -> ReviewViewResponse:
        try:
            with self.store.reader() as tx:
                resolved = self._current(request.dataset_id, request.snapshot_id)
                if (
                    tx.get_current_batch(request.dataset_id, self.stage.stage_id).snapshot
                    == resolved.snapshot
                ):
                    return self._view_response(request, resolved, tx)
        except HumanReviewError as error:
            if error.code != "not_found":
                raise
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(request.dataset_id, request.snapshot_id)
            tx.ensure_current_batch(resolved.snapshot)
            return self._view_response(request, resolved, tx)

    def _view_response(
        self, request: DatasetQueryRequest, resolved: ReviewInput, tx: HumanReviewTransaction
    ) -> ReviewViewResponse:
        batch = tx.get_current_batch(request.dataset_id, self.stage.stage_id)
        decisions = tx.list_decisions(batch.batch_id)
        publication = self.current_publication(tx, batch)
        indexed = {row.candidate_id: row for row in decisions}
        frame = resolved.frame.copy(deep=True)
        identifiers = resolved.snapshot.candidate_ids
        frame["human_decision"] = [indexed[key].decision for key in identifiers]
        frame["human_reviewer"] = [indexed[key].reviewer for key in identifiers]
        frame["human_notes"] = [indexed[key].notes for key in identifiers]
        page = query_table(frame, request.query)
        reviewed_page = ReviewPage(
            columns=page.columns,
            total_rows=page.total_rows,
            matched_rows=page.matched_rows,
            offset=page.offset,
            limit=page.limit,
            rows=tuple(
                ReviewTableRow(
                    row_index=row.row_index,
                    values=row.values,
                    review_record_id=identifiers[row.row_index],
                )
                for row in page.rows
            ),
        )
        return ReviewViewResponse(
            batch=batch,
            counts=tx.counts(batch.batch_id),
            decisions=decisions,
            page=reviewed_page,
            publication=publication,
        )

    def decide(self, request: DecisionRequest) -> DecisionResponse:
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(request.dataset_id, request.snapshot_id)
            decision = tx.save_decision(resolved.snapshot, request.change)
            return DecisionResponse(
                batch=tx.get_current_batch(request.dataset_id, self.stage.stage_id),
                decision=decision,
                counts=tx.counts(decision.batch_id),
            )

    def complete(self, request: CompletionRequest) -> PublicationResponse:
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(request.dataset_id, request.snapshot_id)
            batch = tx.get_current_batch(request.dataset_id, self.stage.stage_id)
            if (
                self.stage.stage_id == "annotations"
                and batch.snapshot == resolved.snapshot
                and batch.completion is not None
                and request.expected.batch_id == batch.batch_id
                and request.expected.generation == batch.generation
                and request.expected.batch_revision
                in {
                    batch.batch_revision,
                    batch.completion.batch_revision - 1,
                }
            ):
                token = tx.authorize_completion(resolved.snapshot, batch.completion)
            else:
                token = tx.complete(resolved.snapshot, request.expected, request.reviewer)
        return self._completion_response(token)

    def continue_review(self, request: CompletionRequest) -> PublicationResponse:
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(request.dataset_id, request.snapshot_id)
            batch = tx.expected_batch(resolved.snapshot, request.expected)
            if batch.completion is None:
                raise HumanReviewError("conflict", "Current batch is not completed")
            token = tx.authorize_completion(resolved.snapshot, batch.completion)
            if self.on_complete is None or not self.stage.can_continue:
                raise HumanReviewError("continuation_failed", "This review stage has no continuation")
        return self._completion_response(token)

    def _completion_response(self, token: CompletionToken) -> PublicationResponse:
        effect = self.on_complete(token) if self.on_complete is not None else CompletionEffect()
        reference = effect.publication
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(token.dataset_id, token.snapshot_id)
            tx.authorize_completion(resolved.snapshot, token)
            if reference is not None:
                persisted = tx.get_publication(reference.publication_id)
                if persisted != reference or persisted.authorization != token:
                    raise HumanReviewError(
                        "publication_failed", "Publisher returned a different authorization"
                    )
            batch = (
                tx.record_continuation(resolved.snapshot, token, effect.run_id)
                if self.on_complete is not None
                else tx.get_batch(token.batch_id)
            )
            return PublicationResponse(
                batch=batch,
                counts=tx.counts(token.batch_id),
                completion=token,
                publication=reference,
                run_id=effect.run_id,
            )

    def retract(self, request: RetractionRequest) -> DecisionResponse:
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(request.dataset_id, request.snapshot_id)
            batch = tx.expected_batch(resolved.snapshot, request.expected)
            previous = next(
                (
                    row
                    for row in tx.list_decisions(batch.batch_id)
                    if row.candidate_id == request.candidate_id
                ),
                None,
            )
            if previous is None:
                raise HumanReviewError("not_found", "Candidate is not in this batch")
            if previous.row_revision != request.expected_row_revision:
                raise HumanReviewError("stale", "Decision row revision changed")
            match previous.decision:
                case "pending":
                    raise HumanReviewError("conflict", "Only a terminal decision can be retracted")
                case "approve" | "reject":
                    pass
                case _:
                    assert_never(previous.decision)
            match batch.state:
                case "completed":
                    batch = tx.reopen(resolved.snapshot, request.expected, request.reviewer)
                case "open":
                    pass
                case "superseded":
                    raise HumanReviewError("stale", "Batch was superseded")
                case _:
                    assert_never(batch.state)
            decision = tx.save_decision(
                resolved.snapshot,
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
                batch=tx.get_current_batch(request.dataset_id, self.stage.stage_id),
                counts=tx.counts(decision.batch_id),
                decision=decision,
            )

    def reopen(self, request: CompletionRequest) -> ReviewStatus:
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(request.dataset_id, request.snapshot_id)
            batch = tx.reopen(resolved.snapshot, request.expected, request.reviewer)
            return ReviewStatus(batch=batch, counts=tx.counts(batch.batch_id))

    def evidence(self, target: EvidenceTarget) -> EvidenceResponse:
        with self.store.reader() as tx:
            batch = tx.get_batch(target.batch_id)
            if (
                batch.snapshot.dataset_id != target.dataset_id
                or batch.snapshot.review_stage != self.stage.stage_id
            ):
                raise HumanReviewError("not_found", "Review batch does not belong to this dataset and stage")
            if target.candidate_id not in batch.snapshot.candidate_ids:
                raise HumanReviewError("not_found", "Record is not in this batch")
        return self.source.evidence(batch.snapshot, target.candidate_id)

    def approved_records(self, dataset_id: str, token: CompletionToken) -> pd.DataFrame:
        """Downstream stages must resolve and authorize again at their own commit boundary."""
        with self.store.transaction(timeout=10) as tx:
            resolved = self._current(dataset_id, token.snapshot_id)
            tx.authorize_completion(resolved.snapshot, token)
            approved = {
                row.candidate_id for row in tx.list_decisions(token.batch_id) if row.decision == "approve"
            }
            return resolved.frame.loc[resolved.frame[resolved.id_column].isin(approved)].copy(deep=True)

    def download(self, target: PublicationTarget) -> bytes:
        raise HumanReviewError("not_found", "This review stage does not publish downloadable files")
