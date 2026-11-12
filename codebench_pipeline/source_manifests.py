from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from codebench_pipeline.dataset_seed_config import DocumentationSeed, SourceSeed, unique_sources
from codebench_pipeline.models import Identifier
from codebench_pipeline.source_transforms import transformation_bytes
from codebench_pipeline.storage import IntegrityError


class SourceManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    version: Literal[1] = 1
    category_id: Identifier
    revision: str = Field(min_length=1)
    sources: list[SourceSeed]
    evidence: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def distinct_sources(self) -> "SourceManifest":
        unique_sources(self.sources)
        return self


class DocumentationManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    version: Literal[1] = 1
    category_id: Identifier
    revision: str = Field(min_length=1)
    snippets: list[DocumentationSeed]
    evidence: dict[str, JsonValue] = Field(default_factory=dict)


def manifest_sources(path: Path, category_id: str) -> list[SourceSeed]:
    manifest = SourceManifest.model_validate_json(path.read_bytes())
    if manifest.category_id != category_id:
        raise IntegrityError("Source manifest belongs to another category")
    return [resolve_source_paths(source, path.parent) for source in manifest.sources]


def resolve_source_paths(source: SourceSeed, base: Path) -> SourceSeed:
    resolved = source.model_copy(
        update={"local_path": str((base / source.local_path).resolve())}
        if source.local_path is not None else {}
    )
    if source.transform is not None:
        transform = source.transform.model_copy(
            update={"module_path": str((base / source.transform.module_path).resolve())}
        )
        resolved = resolved.model_copy(update={"transform": transform})
    return resolved


def validate_transformations(sources: list[SourceSeed]) -> None:
    modules = {
        (source.transform.module_path, source.transform.sha256): source.transform
        for source in sources if source.transform is not None
    }
    for transform in modules.values():
        transformation_bytes(transform)


def manifest_documentation(path: Path, category_id: str) -> list[DocumentationSeed]:
    manifest = DocumentationManifest.model_validate_json(path.read_bytes())
    if manifest.category_id != category_id:
        raise IntegrityError("Documentation manifest belongs to another category")
    return manifest.snippets
