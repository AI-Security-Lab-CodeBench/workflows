from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

Identifier = Annotated[str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,119}$")]
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
Stage = Literal[
    "source_snapshot",
    "semantic_evidence",
    "adapter",
    "extraction",
    "filtering",
    "candidates",
    "label_packets",
    "labels",
    "review_packets",
    "reviews",
    "export",
]


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AdapterSpec(FrozenModel):
    format: Literal["text", "markdown", "json", "jsonl", "csv"]
    text_field: str = "$"
    context_field: str = ""
    rationale: str = Field(min_length=1)


class AdapterPlan(FrozenModel):
    documents: dict[str, AdapterSpec]


class SemanticSnippet(FrozenModel):
    utility: str = Field(min_length=1)
    subcommands: list[str] = Field(min_length=1)
    url: str = Field(pattern=r"^https?://")
    retrieved_at: str = Field(min_length=1)
    snippet: str = Field(min_length=1)
    version_limits: str = Field(min_length=1)


class ReplaySpec(FrozenModel):
    candidates: Path
    sources: Path
    semantics: Path
    packets: Path
    responses: Path
    review_packets: Path
    reviews: Path


class DatasetSpec(FrozenModel):
    dataset_id: Identifier
    revision: str = Field(min_length=1)
    documents: Path
    semantics: Path
    adapter: AdapterSpec | None = None
    use_document_adapters: bool = False
    replay: ReplaySpec | None = None
    label_model: str = "chatgpt-subscription/gpt-6-luna"
    review_model: str = "chatgpt-subscription/gpt-6-luna"
    batch_size: int = Field(default=8, ge=1, le=32)
    limit_per_risk: int = Field(default=250, ge=1, le=250)


class Artifact(FrozenModel):
    dataset_id: Identifier
    stage: Stage
    fingerprint: Digest
    path: Path
    files: dict[str, Digest]
    inputs: dict[str, Digest]
    created_at: str
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class SourceDocument(FrozenModel):
    source_id: str = Field(min_length=1)
    url: str = Field(pattern=r"^https?://")
    local_path: Path
    license: str = Field(min_length=1)
    license_url: str = Field(pattern=r"^https?://")
    source_type: str = Field(min_length=1)
    accessibility: Literal["accessible"]
    reuse_decision: Literal["permitted"]
    commands_present: Literal[True]
    revision: str = Field(min_length=1)
    retrieved_at: str = Field(min_length=1)
    provenance_type: str | None = None
    format: Literal["text", "markdown", "json", "jsonl", "csv"] = "text"
    text_field: str = "$"
    context_field: str = ""
    source_locator: str = ""
    sha256: Digest | None = None
    upstream_sha256: Digest | None = None
    transformations: list[str] = Field(default_factory=list)
    inventory_id: str = ""
    attribution: str = ""
    untrusted_text: bool = True
    provenance_evidence: dict[str, JsonValue] = Field(default_factory=dict)
    raw_format: str = ""
    capture_stage: str = ""
    preserve_all_context: bool = False
    content_format: str = ""

    @model_validator(mode="after")
    def corpus_requires_human_origin(self) -> SourceDocument:
        lower = self.source_type.lower()
        if any(word in lower for word in ("agent", "synthetic", "generated", "sft")):
            raise ValueError("Agent results and synthetic/SFT sources are excluded")
        if any(word in lower for word in ("dataset", "corpus", "benchmark")) and self.provenance_type not in {
            "human_authored",
            "human_collected",
        }:
            raise ValueError("Dataset origin requires inspected human provenance")
        return self
