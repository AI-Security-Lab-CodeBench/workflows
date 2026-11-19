from collections.abc import Sequence
from typing import Literal

from pydantic import Field, JsonValue, TypeAdapter, model_validator

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, index_records, validate_label
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.storage import fingerprint


class LabelingAttempt(CandidateRecord):
    status: Literal["success", "failure"]
    label: LabelDecision | None
    failure_code: str = ""
    failure_detail: str = ""
    row_input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    documentation_manifest_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    documentation: list[SemanticSnippet]
    attempt_evidence_reference: str = Field(min_length=1)

    @model_validator(mode="after")
    def valid_outcome(self) -> "LabelingAttempt":
        if (
            fingerprint([doc.model_dump(mode="json") for doc in self.documentation])
            != self.documentation_manifest_digest
        ):
            raise ValueError("Labeling documentation manifest digest mismatch")
        match self.status:
            case "success":
                if self.label is None:
                    raise ValueError("Successful attempts require a label")
                if self.label.candidate_id != self.candidate_id or self.label.util != self.util:
                    raise ValueError("Successful label identity differs from its attempt")
                if self.failure_code or self.failure_detail:
                    raise ValueError("Successful attempts cannot carry a failure")
            case "failure":
                if self.label is not None or not self.failure_code.strip() or not self.failure_detail.strip():
                    raise ValueError("Failed attempts require a reason and cannot carry a label")
        return self


def historical_attempt_cells(row: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Decode cells from the historical labeling table without rewriting evidence."""
    payload = dict(row)
    label = TypeAdapter(str).validate_python(payload.pop("label_json"), strict=True)
    docs = TypeAdapter(str).validate_python(payload.pop("documentation_json"), strict=True)
    payload["label"] = LabelDecision.model_validate_json(label).model_dump(mode="json") if label else None
    payload["documentation"] = [
        doc.model_dump(mode="json")
        for doc in TypeAdapter(list[SemanticSnippet]).validate_json(docs, strict=True)
    ]
    return payload


def validate_attempts(
    candidates: Sequence[CandidateRecord],
    attempts: Sequence[LabelingAttempt],
    rules: CategoryRules,
) -> None:
    expected = index_records(candidates)
    actual = index_records(attempts)
    if expected.keys() != actual.keys():
        raise ValueError("Labeling outcomes do not cover the exact eligible candidate identities")
    for identity, attempt in actual.items():
        original = CandidateRecord.model_validate(
            attempt.model_dump(include=set(CandidateRecord.model_fields))
        )
        if original != expected[identity]:
            raise ValueError("Labeling attempt changed original candidate evidence")
        if attempt.label is not None:
            validate_label(
                original,
                attempt.label,
                rules,
                {doc.url for doc in attempt.documentation},
            )


def label_attempt(
    candidate: CandidateRecord,
    label: LabelDecision,
    docs: list[SemanticSnippet],
    row_fingerprint: str,
    evidence_reference: str,
) -> LabelingAttempt:
    return LabelingAttempt(
        **candidate.model_dump(),
        status="success",
        label=label,
        row_input_fingerprint=row_fingerprint,
        documentation=docs,
        documentation_manifest_digest=fingerprint([doc.model_dump(mode="json") for doc in docs]),
        attempt_evidence_reference=evidence_reference,
    )


def failed_attempt(
    candidate: CandidateRecord,
    code: str,
    detail: str,
    docs: list[SemanticSnippet],
    row_fingerprint: str,
    evidence_reference: str,
) -> LabelingAttempt:
    return LabelingAttempt(
        **candidate.model_dump(),
        status="failure",
        label=None,
        failure_code=code,
        failure_detail=detail,
        row_input_fingerprint=row_fingerprint,
        documentation=docs,
        documentation_manifest_digest=fingerprint([doc.model_dump(mode="json") for doc in docs]),
        attempt_evidence_reference=evidence_reference,
    )
