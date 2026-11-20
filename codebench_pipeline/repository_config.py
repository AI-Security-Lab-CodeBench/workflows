from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Final, Literal, assert_never

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.merge_config import DEFAULT_KEENABLE_SOURCE_WEIGHT, SourceWeight
from codebench_pipeline.models import Digest
from codebench_pipeline.source_transform_contract import SourceTransformConfig
from codebench_pipeline.storage import IntegrityError, fingerprint

RepositoryBranch = Literal["dataset", "documentation"]
RepositoryId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_-]*$")]
Commit = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{40}$")]
RepositoryName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]+/[A-Za-z0-9_][A-Za-z0-9_.-]*$")]
RepositoryScope = Literal["cheatsheet", "tldr", "aws_examples", "readme", "compose", "markdown_files"]
RepositoryFormat = Literal["text", "markdown"]
RepositoryLicense = Literal["Apache-2.0", "CC0-1.0", "CC-BY-4.0"]
RepositoryUse = Literal["retain_and_quote_attributed_examples"]
REPOSITORY_USE: Final[RepositoryUse] = "retain_and_quote_attributed_examples"
MaximumFileBytes = Annotated[int, Field(ge=1, le=8 * 1024 * 1024)]
CaptureWorkers = Annotated[int, Field(ge=1, le=24)]
DEFAULT_MAXIMUM_FILE_BYTES: Final = 512 * 1024
DEFAULT_CAPTURE_WORKERS: Final = 12


def check_path(path: str) -> None:
    if (
        not path
        or "\\" in path
        or any(ord(char) < 32 or ord(char) == 127 for char in path)
        or any(part in ("", ".", "..", ".git") for part in path.split("/"))
    ):
        raise IntegrityError(f"Unsafe repository path: {path!r}")


def selector_format(selector: RepositoryScope) -> RepositoryFormat:
    match selector:
        case "cheatsheet" | "aws_examples":
            return "text"
        case "tldr" | "readme" | "compose" | "markdown_files":
            return "markdown"
    assert_never(selector)


class RepositoryModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", revalidate_instances="always")


class RightsFile(RepositoryModel):
    path: str
    sha256: Digest


class RepositoryOriginIdentity(RepositoryModel):
    origin_id: RepositoryId
    repository: RepositoryName
    commit: Commit
    tree_digest: Digest
    included_paths: tuple[str, ...] = ()
    excluded_paths: tuple[str, ...] = ()
    source_type: Literal["community_command_corpus", "maintainer_documentation", "official_documentation"]
    provenance_type: Literal[
        "human_collected", "human_authored", "community_documentation", "vendor_documentation"
    ]

    def check_scope_paths(self, selector: RepositoryScope, rights_files: tuple[RightsFile, ...]) -> None:
        paths = (*self.included_paths, *self.excluded_paths, *(item.path for item in rights_files))
        for path in paths:
            check_path(path)
        if len(set(paths)) != len(paths):
            raise PydanticCustomError("repository_paths", "Repository policy paths must be distinct")
        if selector == "markdown_files":
            if (
                not self.included_paths
                or self.excluded_paths
                or any(not path.endswith(".md") for path in self.included_paths)
            ):
                raise PydanticCustomError(
                    "repository_markdown_paths", "Markdown file scope requires only explicit Markdown paths"
                )
        elif self.included_paths:
            raise PydanticCustomError(
                "repository_included_paths", "Explicit repository paths require the Markdown file scope"
            )


class RepositoryOriginScope(RepositoryOriginIdentity):
    scope: RepositoryScope
    license_id: RepositoryLicense
    license: RightsFile
    notices: tuple[RightsFile, ...] = ()
    attribution: str = Field(min_length=1)
    format: RepositoryFormat
    use: RepositoryUse

    @model_validator(mode="after")
    def check_scope(self) -> RepositoryOriginScope:
        self.check_scope_paths(self.scope, (self.license, *self.notices))
        if self.scope == "markdown_files" and self.format != "markdown":
            raise PydanticCustomError(
                "repository_markdown_format", "Markdown file scope requires Markdown format"
            )
        return self


class RepositoryOrigin(RepositoryOriginScope):
    transform: SourceTransformConfig


def repository_origin_matches(origin: RepositoryOriginScope, branch: RepositoryBranch) -> bool:
    if branch == "dataset":
        return (
            origin.scope in {"cheatsheet", "readme", "compose", "markdown_files"}
            and origin.source_type == "community_command_corpus"
            and origin.provenance_type in {"human_authored", "human_collected"}
        )
    return (
        origin.scope in {"tldr", "aws_examples", "markdown_files"}
        and origin.source_type != "community_command_corpus"
        and origin.provenance_type in {"vendor_documentation", "community_documentation"}
    )


class RepositorySettings(RepositoryModel):
    maximum_file_bytes: MaximumFileBytes = DEFAULT_MAXIMUM_FILE_BYTES
    workers: CaptureWorkers = DEFAULT_CAPTURE_WORKERS

    def digest_for_origins(self, branch: RepositoryBranch, origins: tuple[RepositoryOrigin, ...]) -> str:
        from codebench_pipeline.repository_etl import parser_revisions

        declarations: list[JsonValue] = [origin.model_dump(mode="json") for origin in origins]
        return fingerprint(
            {
                "branch": branch,
                "settings": self.model_dump(
                    mode="json",
                    include={
                        "version",
                        "category_id",
                        "refresh",
                        "failure",
                        "declaration_revision",
                        "maximum_file_bytes",
                        "workers",
                    },
                ),
                "origins": declarations,
                "parsers": parser_revisions(tuple(origin.transform for origin in origins)),
            }
        )


class RepositoryConfig(RepositorySettings):
    """Read the combined repository declaration stored in historical registrations."""

    version: Literal[1]
    category_id: Literal["CAT-3"]
    revision: RepositoryId
    refresh: Literal["reviewed_declaration_and_pin_only"]
    failure: Literal["block_branch_on_incomplete_scope_or_integrity_error"]
    dataset: tuple[RepositoryOrigin, ...] = Field(min_length=1)
    documentation: tuple[RepositoryOrigin, ...] = Field(min_length=1)
    source_weights: dict[RepositoryId, SourceWeight] = Field(default_factory=dict)
    keenable_source_weight: SourceWeight = DEFAULT_KEENABLE_SOURCE_WEIGHT

    @model_validator(mode="after")
    def check_branches(self) -> RepositoryConfig:
        origins = (*self.dataset, *self.documentation)
        if len({item.origin_id for item in origins}) != len(origins):
            raise ValueError("Repository origin IDs must be unique across branches")
        if len({item.repository.casefold() for item in origins}) != len(origins):
            raise ValueError("Repository origins must be independent")
        if any(not repository_origin_matches(origin, "dataset") for origin in self.dataset) or any(
            not repository_origin_matches(origin, "documentation") for origin in self.documentation
        ):
            raise ValueError("Repository origin provenance or scope differs from the branch")
        return self

    def origins(self, branch: RepositoryBranch) -> tuple[RepositoryOrigin, ...]:
        return self.dataset if branch == "dataset" else self.documentation

    @property
    def digest(self) -> str:
        return fingerprint(self.model_dump(mode="json"))

    def branch_digest(self, branch: RepositoryBranch) -> str:
        """Bind the selected branch's declarations and transitive parser files."""
        return self.digest_for_origins(branch, self.origins(branch))


def load_repository_config(path: Path) -> RepositoryConfig:
    return RepositoryConfig.model_validate_json(path.read_bytes())


def repository_component(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value) is None:
        raise IntegrityError(f"Unsafe repository run identity: {value!r}")
    return value
