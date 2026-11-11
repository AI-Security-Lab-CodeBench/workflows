from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import AdapterSpec

Risk = Annotated[str, Field(pattern=r"^CAT-[1-9][0-9]*\.[1-9][0-9]*$")]
Origin = Literal["human_authored", "human_collected", "agent", "sft", "synthetic", "unknown"]
DiscoveryPurpose = Literal["dataset", "documentation", "documentation_commands"]


class DiscoveryError(RuntimeError):
    """Discovery input, evidence or model selection violated its contract."""


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


class InspectedPage(DiscoveryModel):
    url: str
    risk: Risk
    retrieved_at: str
    evidence_path: Path
    text: str


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


class InspectedSelection(DiscoveryModel):
    sources: list[SourceProposal]


class AdapterRequest(DiscoveryModel):
    source_id: str
    source_url: str
    inspected_evidence: Path
    proposed_adapter: AdapterSpec | None = None


class DiscoveryFailure(DiscoveryModel):
    operation: str
    target: str
    error: str


class DiscoveryOutput(DiscoveryModel):
    purpose: DiscoveryPurpose = "dataset"
    sources: list[SourceProposal]
    adapter_requests: list[AdapterRequest]
    inspected: list[InspectedPage]
    failures: list[DiscoveryFailure]
    downloaded_source_ready: Literal[False] = False


class AcquisitionReceipt(DiscoveryModel):
    source_id: str
    requested_url: str
    final_url: str
    retrieved_at: str
    sha256: str
    bytes: int
    content_type: str


class SemanticsDocument(DiscoveryModel):
    utility: str = Field(min_length=1)
    subcommands: list[str] = Field(min_length=1)
    url: str = Field(pattern=r"^https?://")
    retrieved_at: str = Field(min_length=1)
    snippet: str = Field(min_length=1)
    version_limits: str = Field(min_length=1)


def save_model(path: Path, value: BaseModel) -> None:
    with path.open("x", encoding="utf-8") as stream:
        stream.write(value.model_dump_json(indent=2) + "\n")


def is_http_url(url: str) -> bool:
    parts = urlsplit(url)
    return (
        parts.scheme in ("http", "https")
        and bool(parts.hostname)
        and not parts.username
        and not parts.password
    )
