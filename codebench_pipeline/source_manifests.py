from collections.abc import Mapping
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from codebench_pipeline.dataset_seed_config import DocumentationSeed, SourceSeed, unique_sources
from codebench_pipeline.models import Identifier
from codebench_pipeline.source_transforms import transformation_bytes
from codebench_pipeline.storage import IntegrityError


class ManifestHeader(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    version: Literal[1] = 1
    category_id: Identifier
    revision: str = Field(min_length=1)
    evidence: dict[str, JsonValue] = Field(default_factory=dict)


class SourceManifest(ManifestHeader):
    sources: list[SourceSeed]

    @model_validator(mode="after")
    def distinct_sources(self) -> "SourceManifest":
        unique_sources(self.sources)
        return self


class DocumentationManifest(ManifestHeader):
    snippets: list[DocumentationSeed]


@lru_cache(maxsize=4)
def _source_manifest(payload: bytes) -> SourceManifest:
    return SourceManifest.model_validate_json(payload)


@lru_cache(maxsize=4)
def _documentation_manifest(payload: bytes) -> DocumentationManifest:
    return DocumentationManifest.model_validate_json(payload)


def load_source_manifest(path: Path, category_id: str) -> SourceManifest:
    manifest = _source_manifest(path.read_bytes())
    if manifest.category_id != category_id:
        raise IntegrityError("Source manifest belongs to another category")
    declared_paths = {
        value
        for source in manifest.sources
        for value in (
            source.local_path,
            source.transform.module_path if source.transform is not None else None,
        )
        if value is not None
    }
    resolved_paths = {value: str((path.parent / value).resolve()) for value in declared_paths}
    return manifest.model_copy(
        update={
            "evidence": deepcopy(manifest.evidence),
            "sources": [
                resolve_source_paths(source, path.parent, resolved_paths=resolved_paths)
                for source in manifest.sources
            ],
        },
    )


def manifest_sources(path: Path, category_id: str) -> list[SourceSeed]:
    return load_source_manifest(path, category_id).sources


def resolve_source_paths(
    source: SourceSeed, base: Path, *, resolved_paths: Mapping[str, str] | None = None
) -> SourceSeed:
    paths = resolved_paths
    if paths is None:
        paths = {
            value: str((base / value).resolve())
            for value in (
                source.local_path,
                source.transform.module_path if source.transform is not None else None,
            )
            if value is not None
        }
    updates: dict[str, str | list[str]] = {"transformations": list(source.transformations)}
    if source.local_path is not None:
        updates["local_path"] = paths[source.local_path]
    resolved = source.model_copy(update=updates)
    if source.transform is not None:
        transform = source.transform.model_copy(update={"module_path": paths[source.transform.module_path]})
        resolved = resolved.model_copy(update={"transform": transform})
    return resolved


def validate_transformations(sources: list[SourceSeed]) -> None:
    modules = {
        (source.transform.module_path, source.transform.sha256): source.transform
        for source in sources
        if source.transform is not None
    }
    for transform in modules.values():
        transformation_bytes(transform)


def load_documentation_manifest(path: Path, category_id: str) -> DocumentationManifest:
    manifest = _documentation_manifest(path.read_bytes())
    if manifest.category_id != category_id:
        raise IntegrityError("Documentation manifest belongs to another category")
    return manifest.model_copy(
        update={
            "evidence": deepcopy(manifest.evidence),
            "snippets": [
                snippet.model_copy(update={"subcommands": list(snippet.subcommands)})
                for snippet in manifest.snippets
            ],
        }
    )


def manifest_documentation(path: Path, category_id: str) -> list[DocumentationSeed]:
    return load_documentation_manifest(path, category_id).snippets
