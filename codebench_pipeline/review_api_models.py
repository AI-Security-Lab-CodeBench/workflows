from typing import Protocol

from pydantic import JsonValue

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.human_review import (
    BatchVersion,
    CompletionToken,
    DecisionChange,
    PublicationReference,
    ReviewBatch,
    ReviewCounts,
    ReviewDecision,
    Revision,
    Text,
)
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.table_query import QueryModel, TableRow


class DecisionRequest(QueryModel):
    dataset_id: Identifier
    snapshot_id: Digest
    change: DecisionChange


class CompletionRequest(QueryModel):
    dataset_id: Identifier
    snapshot_id: Digest
    expected: BatchVersion
    reviewer: Text


class RetractionRequest(CompletionRequest):
    candidate_id: Text
    expected_row_revision: Revision


class EvidenceTarget(QueryModel):
    dataset_id: Identifier
    batch_id: Text
    candidate_id: Text


class PublicationTarget(QueryModel):
    dataset_id: Identifier
    publication_id: Text


class ReviewStatus(QueryModel):
    batch: ReviewBatch
    counts: ReviewCounts


class DecisionResponse(ReviewStatus):
    decision: ReviewDecision


class ReviewTableRow(TableRow):
    review_record_id: Text


class ReviewPage(QueryModel):
    columns: tuple[str, ...]
    total_rows: int
    matched_rows: int
    offset: int
    limit: int
    rows: tuple[ReviewTableRow, ...]


class ReviewViewResponse(ReviewStatus):
    decisions: tuple[ReviewDecision, ...]
    page: ReviewPage
    publication: PublicationReference | None


class PublicationResponse(ReviewStatus):
    completion: CompletionToken | None = None
    publication: PublicationReference | None = None
    run_id: Text | None = None


class EvidenceResponse(QueryModel):
    fields: dict[str, JsonValue]


class ReviewBackend(Protocol):
    def view(self, request: DatasetQueryRequest) -> ReviewViewResponse: ...

    def decide(self, request: DecisionRequest) -> DecisionResponse: ...

    def retract(self, request: RetractionRequest) -> DecisionResponse: ...

    def complete(self, request: CompletionRequest) -> PublicationResponse: ...

    def continue_review(self, request: CompletionRequest) -> PublicationResponse: ...

    def reopen(self, request: CompletionRequest) -> ReviewStatus: ...

    def evidence(self, target: EvidenceTarget) -> EvidenceResponse: ...

    def download(self, target: PublicationTarget) -> bytes: ...
