from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Final, Literal, override

from pydantic import Field, JsonValue, TypeAdapter

from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import RepositoryModel
from codebench_pipeline.source_transform_contract import (
    CanonicalSourceRow,
    SourceTransformConfig,
    TransformContext,
)
from codebench_pipeline.source_transforms import source_rows, transformation_bytes, transformed_rows
from codebench_pipeline.storage import IntegrityError, digest

_DEPLOYMENT_ROOT: Final = Path(__file__).resolve().parents[1]
_RUNTIME_DIGEST: Final = "a41476c5723d996fc5863b653c52efb8c1d4c0fc9fd57392d16cae02e69a7b60"
_LEGACY_CHEATSHEETS: Final = (
    "transforms/cheatsheets.py",
    "7580c09e876750b9f3d2affd042559f2c0e9744c8c8278f109f54dc3d90ad371",
    "Cheatsheet",
    "class",
)
_CHEATSHEETS_FRONT_MATTER: Final = "business-configs/categories/cat3/collection/cheatsheets_front_matter.py"
_HISTORICAL_CHEATSHEETS_PATH: Final = "business-configs/categories/cat3/collection/cheatsheets_v2.py"
RepositoryParserId = Literal["cheatsheets_front_matter", "tldr", "aws_cli_examples", "markdown_shell"]
_REVIEWED_PARSERS: Final[dict[RepositoryParserId, tuple[str, str, str, Literal["function", "class"]]]] = {
    "cheatsheets_front_matter": (
        _CHEATSHEETS_FRONT_MATTER,
        "c72984484228cb6366f7747b242e08118cd7093895ae41c654c082d56a4d1055",
        "transform",
        "function",
    ),
    "tldr": (
        "transforms/tldr.py",
        "ed3f9f60bcde3de149e4e7a4f4df296f44484519203a2d50016cd57c4e7da01e",
        "transform",
        "function",
    ),
    "aws_cli_examples": (
        "transforms/aws_rst.py",
        "3d47be03dc33e8568447dfad2d872e84020ae76b70dd30bd9fbc27f124409bbd",
        "AwsExamples",
        "class",
    ),
    "markdown_shell": (
        "business-configs/categories/cat3/collection/markdown_shell.py",
        "fa11020e7de0fc178fe59e2e99e2b1b01e3d00148b8d2f871a1032535a5dd991",
        "transform",
        "function",
    ),
}
_APPROVED_TRANSFORMS: Final = frozenset((*_REVIEWED_PARSERS.values(), _LEGACY_CHEATSHEETS))
_POSITION: Final = re.compile(r"document; (characters|lines) (\d+):(\d+)")
_PARSER_DEPENDENCIES: Final = {_CHEATSHEETS_FRONT_MATTER: (_LEGACY_CHEATSHEETS[:2],)}


class ContextOptions(RepositoryModel):
    context_characters: int = Field(default=100, ge=0, le=1000, strict=True)

    def pinned_options(self) -> dict[str, JsonValue]:
        return {"context_characters": self.context_characters}


class FencedLinesOptions(ContextOptions):
    mode: Literal["fenced_lines"]
    allowed_utilities: tuple[
        Literal["aws", "docker", "docker-compose", "gcloud", "helm", "kubectl", "terraform"], ...
    ] = Field(min_length=1)

    @override
    def pinned_options(self) -> dict[str, JsonValue]:
        return {"utilities": list(self.allowed_utilities), "context_characters": self.context_characters}


class LiteralOptions(ContextOptions):
    mode: Literal["literal"]
    utility: Literal["podman", "pulumi"]
    inline_usage: bool = Field(default=False, strict=True)

    @override
    def pinned_options(self) -> dict[str, JsonValue]:
        options: dict[str, JsonValue] = {
            "utility": self.utility,
            "context_characters": self.context_characters,
        }
        if self.inline_usage:
            options["inline_usage"] = True
        return options


class ContextRepositoryParser(RepositoryModel):
    parser_id: Literal["cheatsheets_front_matter", "tldr", "aws_cli_examples"]
    sha256: Digest
    options: ContextOptions = ContextOptions()


class MarkdownRepositoryParser(RepositoryModel):
    parser_id: Literal["markdown_shell"]
    sha256: Digest
    options: Annotated[FencedLinesOptions | LiteralOptions, Field(discriminator="mode")]


type RepositoryParser = Annotated[
    ContextRepositoryParser | MarkdownRepositoryParser, Field(discriminator="parser_id")
]


def repository_parser_transform(parser: RepositoryParser) -> SourceTransformConfig:
    return reviewed_parser(parser.parser_id).model_copy(
        update={"options_json": json.dumps(parser.options.pinned_options(), separators=(",", ":"))}
    )


def reviewed_parser(parser_id: RepositoryParserId) -> SourceTransformConfig:
    """Bind an authored parser ID to the independently reviewed implementation."""
    module_path, sha256, entrypoint, kind = _REVIEWED_PARSERS[parser_id]
    return SourceTransformConfig(
        module_path=module_path,
        sha256=sha256,
        entrypoint=entrypoint,
        kind=kind,
        trusted=True,
    )


def _is_approved(transform: SourceTransformConfig) -> bool:
    return (
        transform.trusted
        and (_parser_path(transform.module_path), transform.sha256, transform.entrypoint, transform.kind)
        in _APPROVED_TRANSFORMS
    )


def _parser_path(relative: str) -> str:
    return _CHEATSHEETS_FRONT_MATTER if relative == _HISTORICAL_CHEATSHEETS_PATH else relative


def _deployment_path(relative: str) -> Path:
    expected = _DEPLOYMENT_ROOT / _parser_path(relative)
    if expected.resolve(strict=True) != expected:
        raise IntegrityError(f"Repository ETL path is not its reviewed deployment location: {relative}")
    return expected


def parser_revisions(transforms: tuple[SourceTransformConfig, ...]) -> dict[str, JsonValue]:
    """Fingerprint local deployment files used by the selected parser set."""
    paths = {
        "uv.lock",
        "codebench_pipeline/dataset_records.py",
        "codebench_pipeline/source_transform_contract.py",
        "codebench_pipeline/source_transforms.py",
        "codebench_pipeline/repository.py",
        "codebench_pipeline/repository_acquisition.py",
        "codebench_pipeline/repository_branch_config.py",
        "codebench_pipeline/repository_config.py",
        "codebench_pipeline/repository_etl.py",
        "codebench_pipeline/repository_legacy.py",
        "codebench_pipeline/repository_tree.py",
        "scripts/dataset_schema.py",
    }
    for transform in transforms:
        if _is_approved(transform):
            parser = _parser_path(transform.module_path)
            paths.add(parser)
            paths.update(path for path, _ in _PARSER_DEPENDENCIES.get(parser, ()))
    return {path: digest(_deployment_path(path)) for path in sorted(paths)}


class RepositoryAdapter(RepositoryModel):
    deployment_digest: Digest
    transform: SourceTransformConfig


def authorize_adapter(transform: SourceTransformConfig) -> RepositoryAdapter:
    if not _is_approved(transform):
        raise IntegrityError(
            f"Repository ETL is absent from the deployment allowlist: {transform.module_path}"
        )
    module = _deployment_path(transform.module_path)
    if digest(_DEPLOYMENT_ROOT / "uv.lock") != _RUNTIME_DIGEST:
        raise IntegrityError("Repository ETL dependency lock differs from the reviewed deployment")
    for path, expected in _PARSER_DEPENDENCIES.get(_parser_path(transform.module_path), ()):
        if digest(_deployment_path(path)) != expected:
            raise IntegrityError(f"Repository ETL dependency differs from its approved pin: {path}")
    transformation_bytes(transform.model_copy(update={"module_path": str(module)}))
    return RepositoryAdapter(deployment_digest=_RUNTIME_DIGEST, transform=transform)


def repository_rows(
    text: str, adapter: RepositoryAdapter, context: TransformContext
) -> Iterator[CanonicalSourceRow]:
    transform = adapter.transform
    pinned = transform.model_copy(update={"module_path": str(_deployment_path(transform.module_path))})
    for row in transformed_rows(pinned, source_rows(text, "text"), context):
        yield row.model_copy(
            update={
                "transformations": [
                    f"trusted_python:{transform.sha256}:{transform.entrypoint}",
                    *row.transformations,
                ]
            }
        )


class SourceSpan(RepositoryModel):
    byte_start: int
    byte_end: int


def source_span(text: str, position: str) -> SourceSpan:
    location = _POSITION.fullmatch(position)
    if location is None:
        raise IntegrityError("Repository ETL did not return an exact source position")
    start, end = int(location[2]), int(location[3])
    if location[1] == "lines":
        lines = text.splitlines(keepends=True)
        if not 1 <= start <= end <= len(lines):
            raise IntegrityError("Repository ETL line span exceeds the captured source")
        start, end = sum(map(len, lines[: start - 1])), sum(map(len, lines[:end]))
    if not 0 <= start < end <= len(text):
        raise IntegrityError("Repository ETL character span exceeds the captured source")
    return SourceSpan(byte_start=len(text[:start].encode()), byte_end=len(text[:end].encode()))


ADAPTER_BINDINGS = TypeAdapter(dict[str, RepositoryAdapter])
