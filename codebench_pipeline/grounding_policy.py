import re
import shlex
from datetime import datetime
from typing import Annotated, Final, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.keenable_policy import Media, canonical_url
from codebench_pipeline.models import Digest

_INLINE_MARKUP: Final = re.compile(r"(`+|\*\*|__)([a-zA-Z0-9][a-zA-Z0-9.\s-]*?)\1")
_VERSION_CLAUSE: Final = re.compile(r"(?<=[.!?])\s+|;")
_VERSION_LABEL: Final = re.compile(r"\b(versions?|client|cli)\b", re.IGNORECASE)
_VERSION_VALUE: Final = re.compile(r"\s*[:(=-]?\s*(?:v?\d|[<>~^`*_$?]|(?:latest|lts)\b)", re.IGNORECASE)
_RESTRICTION: Final = re.compile(
    r"\b(?:requires?|required|only|must|supported|unsupported|compatible|incompatible|minimum|maximum)\b",
    re.IGNORECASE,
)
_NON_EXACT: Final = re.compile(
    r"\b(?:at least|at most|up to|minimum|maximum|before|after|not|except|excluding)\b", re.IGNORECASE
)
_EXACT_VERSION: Final = re.compile(
    r"\s+v?(\d+(?:\.\d+)+(?:-[a-z0-9.-]+)?)(?:\s+|-)only\s*(?:[.!?]|:\s+.*)?",
    re.IGNORECASE | re.ASCII,
)


def version_constraints(text: str) -> tuple[str, ...] | None:
    """Record detected versions, an empty tuple for unknown, or None for recognized unresolved prose."""
    versions: set[str] = set()
    prose = _INLINE_MARKUP.sub(r"\2", text)
    for clause in _VERSION_CLAUSE.split(prose):
        for label in _VERSION_LABEL.finditer(clause):
            expression = clause[label.end() :]
            if not _VERSION_VALUE.match(expression) and not (
                label[0].lower().startswith("version") and _RESTRICTION.search(clause)
            ):
                continue
            exact = _EXACT_VERSION.fullmatch(expression.rstrip())
            if exact is None or _NON_EXACT.search(clause[: label.start()]):
                return None
            versions.add(exact[1])
    return tuple(versions) if len(versions) <= 1 else None


class GroundingRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceVersion(GroundingRecord):
    kind: Literal["source_version"] = "source_version"
    version: str = Field(min_length=1, max_length=128, pattern=r"\S")
    source_url: str
    source_capture_sha256: Digest
    source_position: str = Field(min_length=1)
    source_revision: str = Field(min_length=1, max_length=128)


class VersionIndependent(GroundingRecord):
    kind: Literal["version_independent"] = "version_independent"


class OperationGrant(GroundingRecord):
    url: str
    capture_sha256: Digest
    rights_sha256: Digest
    attribution: str = Field(min_length=1, max_length=256)
    valid_until: AwareDatetime
    media_type: Media
    operation: str = Field(pattern=r"^[a-zA-Z0-9_.-]+(?: [a-zA-Z0-9_.-]+)*$")
    flags: tuple[str, ...] = ()
    argument_patterns: tuple[str, ...] = ()
    applicability: str = Field(min_length=1, max_length=400)
    version_scope: Annotated[SourceVersion | VersionIndependent, Field(discriminator="kind")] | None = None
    byte_start: int = Field(ge=0)
    byte_end: int = Field(gt=0, le=524288)
    library_id: str | None = None
    requested_version: str | None = Field(default=None, min_length=1, max_length=128, pattern=r"\S")
    source_revision: str | None = Field(default=None, min_length=1, max_length=128)
    permitted_use: Literal["fetch_retain_quote"]
    retain_origin_bytes: bool = False

    @model_validator(mode="after")
    def check_grant(self) -> "OperationGrant":
        if (
            canonical_url(self.url) != self.url
            or not 0 < self.byte_end - self.byte_start <= 1200
            or self.flags != tuple(sorted(set(self.flags)))
            or any(re.fullmatch(r"--?[a-zA-Z0-9][a-zA-Z0-9_-]*", flag) is None for flag in self.flags)
        ):
            raise ValueError("Expected a pinned HTTPS origin, bounded quote, and canonical flags")
        if len(self.argument_patterns) > 32 or any(len(rule) > 160 for rule in self.argument_patterns):
            raise ValueError("Expected a bounded exact argument or operand policy")
        if self.source_revision is not None and self.library_id is not None:
            raise ValueError("A captured-source grant is not a Context7 lookup grant")
        if (self.library_id is None) != (self.requested_version is None):
            raise ValueError("Context7 operation grants require an explicit reviewed library version")
        if isinstance(self.version_scope, SourceVersion) and (
            canonical_url(self.version_scope.source_url) != self.version_scope.source_url
            or (self.requested_version is not None and self.requested_version != self.version_scope.version)
        ):
            raise ValueError(
                "Version scope must pin the candidate origin and agree with the provider version"
            )
        try:
            for rule in self.argument_patterns:
                re.compile(rule)
        except re.error as error:
            raise ValueError("Invalid operation argument policy") from error
        return self

    def applies(self, candidate: CandidateRecord, now: datetime) -> bool:
        tokens = command_tokens(candidate)
        operation = self.operation.split()
        if tokens[: len(operation)] != operation or now >= self.valid_until:
            return False
        arguments = tokens[len(operation) :]
        flags = tuple(sorted({word.partition("=")[0] for word in arguments if word.startswith("-")}))
        return (
            flags == self.flags
            and len(arguments) == len(self.argument_patterns)
            and all(re.fullmatch(rule, argument) for rule, argument in zip(self.argument_patterns, arguments))
        )


def command_tokens(candidate: CandidateRecord) -> list[str]:
    """Only literal, single-operation candidates have a deterministic lookup route."""
    if "\n" in candidate.command or "\r" in candidate.command:
        return []
    try:
        words = shlex.split(candidate.command)
    except ValueError:
        return []
    if not words or words[0] != candidate.util or any(re.search(r"[;&|<>`$\n]", word) for word in words):
        return []
    return words
