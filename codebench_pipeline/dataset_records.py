from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Literal, Protocol, TypeVar

from pydantic import ConfigDict, Field, model_validator

from codebench_pipeline.category_rules import CategoryRules, ClassName, SeverityName, StrictModel, Text
from codebench_pipeline.models import SemanticSnippet

DocumentationSnippet = SemanticSnippet
HttpURL = Annotated[str, Field(pattern=r"^https?://[^\s/]+(?:/[^\s]*)?$")]
Confidence = Literal["low", "medium", "high"]


class CandidateRecord(StrictModel):
    candidate_id: Text
    command: Text
    util: Text
    context: str
    source_id: Text
    source_url: HttpURL
    source_position: Text
    transformations: list[Text]
    risk_candidates: list[Text]

    @model_validator(mode="after")
    def unique_routing(self) -> CandidateRecord:
        if len(self.risk_candidates) != len(set(self.risk_candidates)):
            raise ValueError("Duplicate risk candidates")
        return self


class LabelDecision(StrictModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        populate_by_name=True,
        serialize_by_alias=True,
    )

    candidate_id: Text
    decision: Literal["accept", "needs_review", "reject"]
    category_id: Text
    risk_id: Text
    severity: SeverityName
    class_: ClassName = Field(alias="class")
    description: Text
    util: Text
    category_confidence: Confidence
    risk_confidence: Confidence
    severity_confidence: Confidence
    class_confidence: Confidence
    rationale: Annotated[Text, Field(max_length=400)]
    documentation_urls: list[HttpURL]


class ReviewDecision(StrictModel):
    candidate_id: Text
    verdict: Literal["approve", "needs_review", "reject"]
    confidence: Confidence
    rationale: Annotated[Text, Field(max_length=400)]


class Identified(Protocol):
    @property
    def candidate_id(self) -> str: ...


Record = TypeVar("Record", bound=Identified)


def index_records(records: Sequence[Record]) -> dict[str, Record]:
    indexed = {record.candidate_id: record for record in records}
    if len(indexed) != len(records):
        raise ValueError("Duplicate candidate identities")
    return indexed


def routing(candidate: CandidateRecord, rules: CategoryRules) -> list[str]:
    known = {risk.id for risk in rules.risks}
    if not set(candidate.risk_candidates) <= known:
        raise ValueError(f"Unknown routing risk for {candidate.candidate_id}")
    matching = set(rules.matching_risks(candidate.command))
    allowed = [risk for risk in candidate.risk_candidates if risk in matching]
    if not allowed:
        raise ValueError(f"No supplied command pattern supports {candidate.candidate_id}")
    return allowed


def validate_label(
    candidate: CandidateRecord,
    label: LabelDecision,
    rules: CategoryRules,
    documentation_urls: set[str] | None = None,
) -> None:
    if label.candidate_id != candidate.candidate_id:
        raise ValueError("Label identity differs from candidate")
    if label.category_id != rules.category_id or label.risk_id not in routing(candidate, rules):
        raise ValueError("Label taxonomy differs from supplied routing rules")
    if label.class_ not in {rule.name for rule in rules.classes}:
        raise ValueError("Class is absent from supplied rules")
    if label.severity not in {rule.name for rule in rules.severities}:
        raise ValueError("Severity is absent from supplied rules")
    if label.util != candidate.util:
        raise ValueError("Label utility differs from source utility")
    if len(label.documentation_urls) != len(set(label.documentation_urls)):
        raise ValueError("Duplicate documentation URLs")
    if documentation_urls is not None and not set(label.documentation_urls) <= documentation_urls:
        raise ValueError("Label cites documentation absent from supplied evidence")
    if label.decision == "accept" and not label.documentation_urls:
        raise ValueError("Accepted label requires supporting documentation")
