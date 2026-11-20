from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .keenable_policy import Case, Digest, Identity, Media, Permission, Record


class _Hit(BaseModel):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True)
    url: str


class SearchEnvelope(BaseModel):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True)
    results: list[_Hit]


class Lead(Record):
    case: Case
    rank: int
    url_sha256: Digest
    url: str | None
    excluded: Literal["invalid_url", "outside_case", "duplicate", "selection_budget"] | None = None
    origin: Literal["search", "declared"] = Field(
        default="search", exclude_if=lambda value: value == "search"
    )
    search_slot: str | None = Field(default=None, exclude_if=lambda slot: slot is None)


class SelectedPage(Record):
    lead: Lead
    permission: Permission | None
    state: Literal["pending", "withheld"]


class CallReceipt(Record):
    slot: str
    kind: Literal["search", "origin", "plan"]
    target: str
    request_sha256: Digest
    reserved_at: str
    reserved_bytes: int
    state: Literal["dispatched", "succeeded", "failed", "withheld"]
    observed_at: str | None = None
    status: int | None = None
    response_sha256: Digest | None = None
    response_bytes: int | None = None
    retained_sha256: Digest | None = None
    peer: str | None = None
    reason: str | None = None
    retry_after: int | None = Field(default=None, ge=0, exclude_if=lambda seconds: seconds is None)


class SearchDecision(Record):
    option: Identity | None


class SearchFeedback(Record):
    option: Identity
    returned: int
    eligible: int
    new_urls: int


class SearchPlanReceipt(Record):
    mode: Literal["fixed", "adaptive"]
    policy_sha256: Digest
    planner_sha256: Digest | None
    feedback: tuple[SearchFeedback, ...]
    decisions: tuple[CallReceipt, ...]
    stop: Literal["budget", "options_exhausted", "planner_stop"]


class CatalogReceipt(Record):
    declaration_sha256: Digest
    leads: tuple[Lead, ...]
    selected: tuple[SelectedPage, ...]
    searches: tuple[CallReceipt, ...]
    plan: SearchPlanReceipt | None = Field(default=None, exclude_if=lambda plan: plan is None)


class VerifiedPage(Record):
    url: str
    origin_id: str
    body_sha256: Digest
    size: int
    media_type: Media
    rights_evidence_sha256: Digest
    call: CallReceipt
    case: Case | None = Field(default=None, exclude_if=lambda case: case is None)


class CaptureLock(Record):
    declaration_sha256: Digest
    catalog_sha256: Digest
    pages: tuple[VerifiedPage, ...]
    withheld: tuple[str, ...]
    origin_calls: tuple[CallReceipt, ...]
    rights_calls: tuple[CallReceipt, ...] = Field(default=(), exclude_if=lambda calls: not calls)


class AdapterReceipt(Record):
    url: str
    body_sha256: Digest
    permission_sha256: Digest
    parser: Literal["reviewed-byte-spans-v1", "literal-code-blocks-v2"] = "reviewed-byte-spans-v1"


class CandidateReceipt(Record):
    candidate_id: Digest
    origin_id: str
    url: str
    body_sha256: Digest
    start: int
    end: int
    span_sha256: Digest
    command: str
    case: Case
    attribution: str
    rights_evidence_sha256: Digest
