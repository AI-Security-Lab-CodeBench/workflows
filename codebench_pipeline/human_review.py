from __future__ import annotations

from typing import Annotated, Literal, Self, assert_never

from pydantic import BaseModel, ConfigDict, Field, model_validator

from codebench_pipeline.storage import fingerprint

Text = Annotated[str, Field(min_length=1, pattern=r"\S")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Revision = Annotated[int, Field(ge=0)]
Generation = Annotated[int, Field(ge=1)]
DecisionValue = Literal["pending", "approve", "reject"]
ErrorCode = Literal[
    "not_found",
    "conflict",
    "stale",
    "pending",
    "ineligible",
    "completed",
    "publication_failed",
    "continuation_failed",
    "selection",
]


class HumanReviewError(RuntimeError):
    code: ErrorCode
    detail: str

    def __init__(self, code: ErrorCode, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class ReviewModel(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class ReviewSnapshot(ReviewModel):
    """Server-resolved immutable content and evidence, never a client assertion."""

    dataset_id: Text
    review_stage: Text = "annotations"
    snapshot_id: Digest
    candidate_ids: tuple[Text, ...]
    eligible_candidate_ids: tuple[Text, ...]
    record_fingerprints: dict[Text, Digest] = Field(default_factory=dict)
    min_approvals: Revision = 0
    max_approvals: Revision | None = None

    @model_validator(mode="after")
    def validate_membership(self) -> Self:
        for identifiers in (self.candidate_ids, self.eligible_candidate_ids):
            if len(set(identifiers)) != len(identifiers):
                raise ValueError("Candidate identities must be unique")
        if not set(self.eligible_candidate_ids).issubset(self.candidate_ids):
            raise ValueError("Eligible identities must belong to the complete snapshot")
        if self.record_fingerprints and set(self.record_fingerprints) != set(self.candidate_ids):
            raise ValueError("Record fingerprints must cover exactly the snapshot identities")
        if self.max_approvals is not None and self.max_approvals < self.min_approvals:
            raise ValueError("Approval bounds are inconsistent")
        return self


class BatchVersion(ReviewModel):
    batch_id: Text
    generation: Generation
    batch_revision: Revision


class CompletionToken(BatchVersion):
    dataset_id: Text
    review_stage: Text = "annotations"
    snapshot_id: Digest
    decision_digest: Digest
    token: Text


class ReviewBatch(BatchVersion):
    snapshot: ReviewSnapshot
    state: Literal["open", "completed", "superseded"]
    created_at: Text
    updated_at: Text
    reviewer: str = ""
    completion: CompletionToken | None = None
    continuation_run_id: Text | None = None

    def require_open(self) -> None:
        match self.state:
            case "open":
                return
            case "completed":
                raise HumanReviewError("completed", "Explicit reopen is required before editing")
            case "superseded":
                raise HumanReviewError("stale", "Batch was superseded")
            case _:
                assert_never(self.state)


class ReviewDecision(ReviewModel):
    batch_id: Text
    candidate_id: Text
    decision: DecisionValue = "pending"
    notes: str = ""
    reviewer: str = ""
    row_revision: Revision = 0
    created_at: Text
    updated_at: Text
    carried_from_batch_id: Text | None = None


class DecisionChange(ReviewModel):
    batch_id: Text
    generation: Generation
    candidate_id: Text
    expected_row_revision: Revision
    decision: DecisionValue
    notes: str = ""
    reviewer: Text


class ReviewCounts(ReviewModel):
    pending: Revision
    approve: Revision
    reject: Revision


class PublicationPayload(ReviewModel):
    payload_key: Text
    payload_hash: Digest
    csv_reference: Text
    bundle_reference: Text
    dagster_run_id: Text


class PublicationReference(PublicationPayload):
    publication_id: Text
    authorization: CompletionToken
    created_at: Text


class CompletionEffect(ReviewModel):
    publication: PublicationReference | None = None
    run_id: Text | None = None


class ReviewHistoryEntry(ReviewModel):
    event: Literal[
        "created", "decision", "completed", "reopened", "superseded", "published", "continued", "carried"
    ]
    batch: ReviewBatch
    decision: ReviewDecision | None = None
    publication: PublicationReference | None = None
    recorded_at: Text


def require_decided_batch(batch: ReviewBatch, decisions: tuple[ReviewDecision, ...]) -> None:
    """Check the persisted record set at completion and publication boundaries."""
    expected = set(batch.snapshot.candidate_ids)
    observed = {row.candidate_id for row in decisions}
    if (
        observed != expected
        or len(decisions) != len(expected)
        or any(row.batch_id != batch.batch_id for row in decisions)
    ):
        raise HumanReviewError("conflict", "Decision identities do not match the complete batch")
    eligible = set(batch.snapshot.eligible_candidate_ids)
    for row in decisions:
        match row.decision:
            case "pending":
                raise HumanReviewError("pending", "Every row must be decided before completion")
            case "approve":
                if row.candidate_id not in eligible:
                    raise HumanReviewError("ineligible", "Human approval cannot override eligibility")
            case "reject":
                pass
            case _:
                assert_never(row.decision)
        if not row.reviewer.strip() or row.row_revision < 1:
            raise HumanReviewError("conflict", "A terminal decision lacks review provenance")
    approvals = sum(row.decision == "approve" for row in decisions)
    if approvals < batch.snapshot.min_approvals or (
        batch.snapshot.max_approvals is not None and approvals > batch.snapshot.max_approvals
    ):
        raise HumanReviewError("selection", "Approved records do not satisfy this stage's selection bounds")


def decisions_digest(decisions: tuple[ReviewDecision, ...]) -> str:
    return fingerprint([row.model_dump(mode="json", exclude_none=True) for row in decisions])
