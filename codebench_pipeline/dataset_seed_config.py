from __future__ import annotations

import re
from pathlib import Path
from typing import Final, Literal

from pydantic import Field, JsonValue, TypeAdapter, model_validator

from codebench_pipeline.category_rules import Text
from codebench_pipeline.discovery_models import Risk, SearchRequest, is_http_url
from codebench_pipeline.models import Digest, SemanticSnippet, SourceDocument
from codebench_pipeline.source_transform_contract import SourceTransformConfig
from codebench_pipeline.storage import IntegrityError, digest
from codebench_pipeline.typed_config import TypedConfig
from scripts.dataset_schema import SECRET_PATTERNS

PROVENANCE: Final = TypeAdapter(dict[str, JsonValue])


class SourceSeed(TypedConfig):
    """An inspected source starting point, not a synthetic command or RNG seed."""

    source_id: Text
    url: str = Field(pattern=r"^https?://")
    local_path: Text | None = None
    license: Text
    license_url: str = Field(pattern=r"^https?://")
    source_type: Text
    accessibility: Literal["accessible"]
    reuse_decision: Literal["permitted"]
    commands_present: bool = Field(strict=True)
    revision: Text
    retrieved_at: Text
    provenance_type: str | None = None
    format: Literal["text", "markdown", "json", "jsonl", "csv"] = "text"
    text_field: str = "$"
    context_field: str = ""
    source_locator: str = ""
    sha256: Digest | None = None
    git_blob_sha1: str | None = Field(default=None, pattern=r"^[a-f0-9]{40}$")
    upstream_sha256: Digest | None = None
    transformations: list[str] = Field(default_factory=list)
    inventory_id: str = ""
    attribution: str = ""
    untrusted_text: bool = True
    provenance_evidence_json: str = Field(
        default="{}",
        description="JSON object preserving arbitrary inspected provenance in Dagster-compatible form.",
    )
    raw_format: str = ""
    capture_stage: str = ""
    preserve_all_context: bool = False
    content_format: str = ""
    transform: SourceTransformConfig | None = None

    def _source_document(self) -> SourceDocument:
        """Convert metadata through the existing source eligibility boundary."""
        if self.local_path is None:
            raise IntegrityError("A remote source has no local SourceDocument")
        metadata = self.model_dump(exclude={
            "provenance_evidence_json", "local_path", "git_blob_sha1", "transform"
        })
        document = SourceDocument.model_validate(
            {
                **metadata,
                "local_path": Path(self.local_path),
                "provenance_evidence": PROVENANCE.validate_json(
                    self.provenance_evidence_json, strict=True
                ),
            }
        )
        if any(pattern.search(document.model_dump_json()) for pattern in SECRET_PATTERNS):
            raise ValueError("Sensitive source metadata cannot be persisted")
        return document

    @model_validator(mode="after")
    def eligible_metadata(self) -> SourceSeed:
        if self.commands_present is not True:
            raise ValueError("Source inputs must establish that commands are present")
        lower = self.source_type.lower()
        if any(word in lower for word in ("agent", "synthetic", "generated", "sft")):
            raise ValueError("Agent results and synthetic/SFT sources are excluded")
        if any(word in lower for word in ("dataset", "corpus", "benchmark")) and self.provenance_type not in {
            "human_authored", "human_collected"
        }:
            raise ValueError("Dataset origin requires inspected human provenance")
        if not is_http_url(self.url) or not is_http_url(self.license_url):
            raise ValueError("Source and license URLs must be HTTP(S) without credentials")
        if self.local_path is None and self.sha256 is None and self.git_blob_sha1 is None:
            raise ValueError("Remote sources require an expected SHA-256 or Git blob SHA-1")
        for field in (self.text_field, self.context_field):
            if field and not re.fullmatch(r"\$|[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)*", field):
                raise ValueError("Parsing fields must be declarative dictionary paths")
        PROVENANCE.validate_json(self.provenance_evidence_json, strict=True)
        metadata = self.model_dump_json()
        if any(pattern.search(metadata) for pattern in SECRET_PATTERNS):
            raise ValueError("Sensitive source metadata cannot be persisted")
        return self

    def to_source_document(self) -> SourceDocument:
        """Validate referenced raw bytes and return the complete source provenance."""
        document = self._source_document()
        local = document.local_path.resolve()
        actual = digest(local)
        if document.sha256 is not None and actual != document.sha256:
            raise IntegrityError(f"Source checksum changed: {document.source_id}")
        return document.model_copy(update={"local_path": local, "sha256": actual})


class RiskSearchSeeds(TypedConfig):
    risk: Risk
    queries: list[Text] = Field(min_length=1, max_length=4)
    inspection_urls: list[str] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def valid_inspection_urls(self) -> RiskSearchSeeds:
        if any(not is_http_url(url) for url in self.inspection_urls):
            raise ValueError("Inspection references must be HTTP(S) URLs without credentials")
        return self


class SearchSeeds(TypedConfig):
    """Bounded source-search starting points, never command generation or RNG seeds."""

    searches: list[RiskSearchSeeds] = Field(min_length=1, max_length=100)
    results_per_query: int = Field(default=5, ge=1, le=10)
    pages_per_risk: int = Field(default=5, ge=1, le=20)
    max_chars: int = Field(default=20000, ge=1000, le=50000)
    timeout_seconds: int = Field(default=60, ge=1, le=300)

    @model_validator(mode="after")
    def unique_risks(self) -> SearchSeeds:
        if len(self.searches) != len({group.risk for group in self.searches}):
            raise ValueError("Search risk groups must be unique")
        return self

    def to_search_request(self, model: str) -> SearchRequest:
        return SearchRequest.model_validate({**self.model_dump(), "model": model})


class DocumentationSeed(TypedConfig):
    """Inline inspected semantics evidence, with the original URL and limitations."""

    utility: Text
    subcommands: list[Text] = Field(min_length=1)
    url: str = Field(pattern=r"^https?://")
    retrieved_at: Text
    snippet: Text
    version_limits: Text

    @model_validator(mode="after")
    def nonsensitive_evidence(self) -> DocumentationSeed:
        metadata = self.model_dump_json()
        if any(pattern.search(metadata) for pattern in SECRET_PATTERNS):
            raise ValueError("Sensitive documentation cannot enter inference")
        return self

    def to_semantic_snippet(self) -> SemanticSnippet:
        return SemanticSnippet.model_validate(self.model_dump())


def unique_sources(documents: list[SourceSeed]) -> list[SourceSeed]:
    if len(documents) != len({document.source_id for document in documents}):
        raise ValueError("Duplicate source identifiers")
    return documents
