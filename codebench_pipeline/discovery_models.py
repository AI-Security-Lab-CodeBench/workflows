from __future__ import annotations

from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

Risk = Annotated[str, Field(pattern=r"^CAT-[1-9][0-9]*\.[1-9][0-9]*$")]
Origin = Literal["human_authored", "human_collected", "agent", "sft", "synthetic", "unknown"]


class DiscoveryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RiskSearch(DiscoveryModel):
    risk: Risk
    queries: list[str] = Field(min_length=1, max_length=4)
    inspection_urls: list[str] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def valid_inspection_urls(self) -> Self:
        if any(not is_http_url(url) for url in self.inspection_urls):
            raise ValueError("Inspection references must be HTTP(S) URLs without credentials")
        return self


class SearchRequest(DiscoveryModel):
    """Explicit bounded queries, with licensing and provenance queries as needed."""

    searches: list[RiskSearch] = Field(min_length=1, max_length=100)
    model: str = Field(min_length=1)
    results_per_query: int = Field(default=5, ge=1, le=10)
    pages_per_risk: int = Field(default=5, ge=1, le=20)
    max_chars: int = Field(default=20000, ge=1000, le=50000)
    timeout_seconds: int = Field(default=60, ge=1, le=300)


class SourceProposal(DiscoveryModel):
    """A model's inspected-source proposal; never a shared SourceDocument."""

    source_id: str = Field(min_length=1)
    risk: Risk
    url: str
    source_type: str = Field(min_length=1)
    origin: Origin
    origin_evidence: str
    commands_evidence: str
    license: str
    license_url: str
    license_evidence: str
    reuse_decision: Literal["permitted", "unknown", "prohibited"] = "unknown"
    revision: str
    decision: Literal["proposed", "needs_review", "exclude"]
    analysis: str = Field(min_length=1)
    confidence: Literal["low", "medium", "high"]
    rationale: str = Field(min_length=1)
    adapter_requested: bool = False


def is_http_url(url: str) -> bool:
    parts = urlsplit(url)
    return (
        parts.scheme in ("http", "https")
        and bool(parts.hostname)
        and not parts.username
        and not parts.password
    )
