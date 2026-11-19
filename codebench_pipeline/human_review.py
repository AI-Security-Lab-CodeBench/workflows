from __future__ import annotations

import json
from typing import Annotated, Literal, Self, assert_never

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_serializer, model_validator

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


def review_partition_id(partition_key: str | None) -> str:
    return "partition-" + fingerprint(partition_key)


def review_artifact_id(asset_key: tuple[str, ...]) -> str:
    return json.dumps(asset_key, ensure_ascii=False, separators=(",", ":"))


class ReviewPartition(ReviewModel):
    dataset_id: Text
    artifact_id: str = ""
    scope_id: str = ""
    partition_key: str | None
    invalidated: bool = False


class LabelCorrections(ReviewModel):
    model_config = ConfigDict(
        frozen=True, strict=True, extra="forbid", populate_by_name=True, serialize_by_alias=True
    )

    category_id: Text | None = None
    risk_id: Text | None = None
    severity: Literal["low", "medium", "high", "critical"] | None = None
    class_: Literal["allow", "deny", "ask"] | None = Field(default=None, alias="class")

    @model_validator(mode="after")
    def require_values(self) -> Self:
        if not self.model_fields_set or any(getattr(self, field) is None for field in self.model_fields_set):
            raise ValueError("Label corrections require nonnull values for supplied dimensions")
        return self

    @model_serializer
    def supplied_values(self) -> dict[str, str]:
        values = {
            "category_id": self.category_id,
            "risk_id": self.risk_id,
            "severity": self.severity,
            "class": self.class_,
        }
        return {key: value for key, value in values.items() if value is not None}


class ReviewSnapshot(ReviewModel):
    """Server-resolved immutable content and evidence, never a client assertion."""

    dataset_id: Text
    artifact_id: str = ""
    scope_id: str = ""
    partition_key: str | None = None
    review_stage: Text = "annotations"
    snapshot_id: Digest
    candidate_ids: tuple[Text, ...]
    eligible_candidate_ids: tuple[Text, ...]
    record_fingerprints: dict[Text, Digest] = Field(default_factory=dict)
    review_columns: tuple[Text, ...] = ()
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
    artifact_id: str = ""
    scope_id: str = ""
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
    label_corrections: LabelCorrections | None = None
    edits: dict[Text, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_correction_approval(self) -> Self:
        if (self.label_corrections is not None or self.edits) and self.decision != "approve":
            raise ValueError("Label corrections require an approval")
        return self


def compact_batch(batch: ReviewBatch) -> ReviewBatch:
    return batch.model_copy(
        update={
            "snapshot": batch.snapshot.model_copy(
                update={"candidate_ids": (), "eligible_candidate_ids": (), "record_fingerprints": {}}
            )
        }
    )


class DecisionChange(ReviewModel):
    batch_id: Text
    generation: Generation
    candidate_id: Text
    expected_row_revision: Revision
    decision: DecisionValue
    notes: str = ""
    reviewer: Text
    edits: dict[Text, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_correction_approval(self) -> Self:
        if self.edits and self.decision != "approve":
            raise ValueError("Edits require an approval")
        return self


class ReviewCounts(ReviewModel):
    pending: Revision
    approve: Revision
    reject: Revision
    blocked: Revision = 0
    approved_occurrences: Revision = 0


class PreparedReview(ReviewModel):
    snapshot: ReviewSnapshot
    id_column: Text
    columns: tuple[Text, ...]
    rows: tuple[dict[str, JsonValue], ...]
    validation: dict[str, JsonValue] = Field(default_factory=dict)
    evidence: dict[str, JsonValue] = Field(default_factory=dict)
    column_schema: dict[str, dict[str, JsonValue]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def capture_column_schema(self) -> Self:
        from codebench_pipeline.review_postgres_values import json_type

        if set(self.column_schema) - set(self.columns):
            raise ValueError("Declared column schema contains unknown columns")
        captured: dict[str, dict[str, JsonValue]] = {}
        for column in self.columns:
            declared = self.column_schema.get(column, {})
            selected = column in self.snapshot.review_columns
            editable = declared.get("editable", selected)
            if type(editable) is not bool or (editable and not selected):
                raise ValueError("Only selected review columns may be editable")
            types: list[JsonValue] = [
                name for name in sorted({json_type(row[column]) for row in self.rows if column in row})
            ]
            captured[column] = {
                **({"type": types} if types else {}),
                **declared,
                "editable": editable,
                "readOnly": not editable,
            }
        object.__setattr__(self, "column_schema", captured)
        return self


class PublicationPayload(ReviewModel):
    payload_key: Text
    payload_hash: Digest
    csv_reference: Text
    bundle_reference: Text
    dagster_run_id: Text
    proposal_digest: Digest | None = Field(default=None, exclude_if=lambda value: value is None)


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
