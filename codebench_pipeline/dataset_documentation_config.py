from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.dataset_seed_config import DocumentationSeed, SearchSeeds
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.source_manifests import load_documentation_manifest
from codebench_pipeline.typed_config import TypedConfig


class DocumentationConfig(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    mode: Literal["provided_snippets"] = "provided_snippets"
    manifest: str | None = None
    manifest_header_json: str | None = Field(default=None, exclude_if=lambda value: value is None)
    snippets: list[DocumentationSeed] = Field(default_factory=list)
    search_request: SearchSeeds | None = None
    revision: str = Field(default="1", min_length=1)
    maximum_bytes: int = Field(default=1_048_576, gt=0, le=10_485_760)
    model: ModelConfig | None = None

    @model_validator(mode="after")
    def matching_inputs(self) -> DocumentationConfig:
        if self.search_request is not None:
            raise ValueError("Documentation search belongs to the local agent; supply a manifest")
        return self


class BundleDocumentation(DocumentationConfig):
    @model_validator(mode="after")
    def one_snippet_input(self) -> BundleDocumentation:
        if self.manifest is not None and self.snippets:
            raise PydanticCustomError("documentation_input", "Choose a manifest or inline snippets")
        if self.manifest_header_json is not None:
            raise PydanticCustomError("documentation_input", "Manifest headers are resolved at registration")
        return self

    def resolve(self, parent: Path, category_id: str) -> DocumentationConfig:
        manifest = (
            load_documentation_manifest((parent / self.manifest).resolve(), category_id)
            if self.manifest is not None
            else None
        )
        return DocumentationConfig(
            mode=self.mode,
            snippets=manifest.snippets if manifest is not None else self.snippets,
            revision=manifest.revision if manifest is not None else self.revision,
            maximum_bytes=self.maximum_bytes,
            model=self.model,
            manifest_header_json=manifest.model_dump_json(exclude={"snippets"})
            if manifest is not None
            else None,
        )
