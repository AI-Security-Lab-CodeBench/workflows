from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, model_validator

from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.models import Digest
from codebench_pipeline.source_transform_contract import SourceTransformConfig
from codebench_pipeline.storage import IntegrityError, fingerprint

RepositoryBranch = Literal["dataset", "documentation"]
RepositoryId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_-]*$")]
Commit = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{40}$")]
RepositoryName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]+/[A-Za-z0-9_][A-Za-z0-9_.-]*$")]
RepositoryScope = Literal["cheatsheet", "tldr", "aws_examples", "readme", "compose", "markdown_files"]
RepositoryFormat = Literal["text", "markdown"]
SourceWeight = Annotated[float, Field(strict=True, ge=0, le=1, allow_inf_nan=False)]
DEFAULT_SOURCE_WEIGHTS: Final[dict[Branch, float]] = {
    "documentation": 0.9,
    "dataset": 0.6,
    "keenable": 0.3,
}


def check_path(path: str) -> None:
    if (
        not path
        or "\\" in path
        or any(ord(char) < 32 or ord(char) == 127 for char in path)
        or any(part in ("", ".", "..", ".git") for part in path.split("/"))
    ):
        raise IntegrityError(f"Unsafe repository path: {path!r}")


class RepositoryModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", revalidate_instances="always")


class RightsFile(RepositoryModel):
    path: str
    sha256: Digest


class RepositoryOrigin(RepositoryModel):
    origin_id: RepositoryId
    repository: RepositoryName
    commit: Commit
    tree_digest: Digest
    scope: RepositoryScope
    included_paths: tuple[str, ...] = ()
    excluded_paths: tuple[str, ...] = ()
    license_id: Literal["Apache-2.0", "CC0-1.0", "CC-BY-4.0"]
    license: RightsFile
    notices: tuple[RightsFile, ...] = ()
    attribution: str = Field(min_length=1)
    source_type: Literal["community_command_corpus", "maintainer_documentation", "official_documentation"]
    provenance_type: Literal[
        "human_collected", "human_authored", "community_documentation", "vendor_documentation"
    ]
    format: RepositoryFormat
    use: Literal["retain_and_quote_attributed_examples"]
    transform: SourceTransformConfig

    @model_validator(mode="after")
    def check_scope(self) -> RepositoryOrigin:
        paths = (
            *self.included_paths,
            *self.excluded_paths,
            self.license.path,
            *(item.path for item in self.notices),
        )
        for path in paths:
            check_path(path)
        if len(set(paths)) != len(paths):
            raise ValueError("Repository policy paths must be distinct")
        if self.scope == "markdown_files":
            if (
                not self.included_paths
                or self.excluded_paths
                or self.format != "markdown"
                or any(not path.endswith(".md") for path in self.included_paths)
            ):
                raise ValueError("Markdown file scope requires only explicit Markdown paths")
        elif self.included_paths:
            raise ValueError("Explicit repository paths require the Markdown file scope")
        return self


class RepositoryConfig(RepositoryModel):
    version: Literal[1]
    category_id: Literal["CAT-3"]
    revision: RepositoryId
    refresh: Literal["reviewed_declaration_and_pin_only"]
    failure: Literal["block_branch_on_incomplete_scope_or_integrity_error"]
    dataset: tuple[RepositoryOrigin, ...] = Field(min_length=1)
    documentation: tuple[RepositoryOrigin, ...] = Field(min_length=1)
    source_weights: dict[RepositoryId, SourceWeight] = Field(default_factory=dict)
    keenable_source_weight: SourceWeight = DEFAULT_SOURCE_WEIGHTS["keenable"]
    maximum_file_bytes: int = Field(default=512 * 1024, ge=1, le=8 * 1024 * 1024)
    workers: int = Field(default=12, ge=1, le=24)

    @model_validator(mode="after")
    def check_branches(self) -> RepositoryConfig:
        origins = (*self.dataset, *self.documentation)
        if len({item.origin_id for item in origins}) != len(origins):
            raise ValueError("Repository origin IDs must be unique across branches")
        if self.source_weights.keys() - {item.origin_id for item in origins}:
            raise ValueError("Source weight overrides must name declared repository origins")
        dataset_weights = [self.source_weight("dataset", origin.origin_id) for origin in self.dataset]
        doc_weights = [self.source_weight("documentation", origin.origin_id) for origin in self.documentation]
        if not self.keenable_source_weight < min(dataset_weights) <= max(dataset_weights) < min(doc_weights):
            raise ValueError("Source weights must keep documentation above datasets above Keenable")
        if len({item.repository.casefold() for item in origins}) != len(origins):
            raise ValueError("Repository origins must be independent")
        if any(
            origin.scope not in {"cheatsheet", "readme", "compose", "markdown_files"}
            or origin.source_type != "community_command_corpus"
            or origin.provenance_type not in {"human_authored", "human_collected"}
            for origin in self.dataset
        ) or any(
            origin.scope not in {"tldr", "aws_examples", "markdown_files"}
            or origin.source_type == "community_command_corpus"
            or origin.provenance_type not in {"vendor_documentation", "community_documentation"}
            for origin in self.documentation
        ):
            raise ValueError("Repository origin provenance or scope differs from the branch")
        return self

    def origins(self, branch: RepositoryBranch) -> tuple[RepositoryOrigin, ...]:
        return self.dataset if branch == "dataset" else self.documentation

    def source_weight(self, branch: RepositoryBranch, origin_id: str) -> float:
        return self.source_weights.get(origin_id, DEFAULT_SOURCE_WEIGHTS[branch])

    @property
    def digest(self) -> str:
        return fingerprint(self.model_dump(mode="json"))

    def branch_digest(self, branch: RepositoryBranch) -> str:
        """Bind the selected branch's declarations and transitive parser files."""
        from codebench_pipeline.repository_etl import parser_revisions

        origins = self.origins(branch)
        declarations: list[JsonValue] = [origin.model_dump(mode="json") for origin in origins]
        return fingerprint(
            {
                "branch": branch,
                "settings": self.model_dump(
                    mode="json",
                    exclude={
                        "dataset",
                        "documentation",
                        "revision",
                        "source_weights",
                        "keenable_source_weight",
                    },
                ),
                "source_weights": {
                    origin.origin_id: self.source_weight(branch, origin.origin_id) for origin in origins
                },
                "origins": declarations,
                "parsers": parser_revisions(tuple(origin.transform for origin in origins)),
            }
        )


def load_repository_config(path: Path) -> RepositoryConfig:
    return RepositoryConfig.model_validate_json(path.read_bytes())


def repository_component(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value) is None:
        raise IntegrityError(f"Unsafe repository run identity: {value!r}")
    return value
