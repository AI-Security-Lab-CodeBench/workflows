import json
import os
import subprocess
from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import batched
from typing import Final, Literal, get_args

import pandera.pandas as pa
from pandera.typing import DataFrame, Series
from pydantic import ConfigDict, TypeAdapter

from codebench_pipeline.category_rules import StrictModel, Text
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    StrictTable,
    model_records,
    validated_frame,
)

AUDIT_STAGE: Final = "bashguard_syntax_audit"
AuditStatus = Literal["parsed", "empty", "size", "utf8", "nul", "parse", "complexity"]
SyntaxFlag = Literal[
    "arithmetic_expansion",
    "assignment",
    "background",
    "command_substitution",
    "conditional",
    "function_definition",
    "multiple_top_level_statements",
    "parameter_expansion",
    "pipeline",
    "process_substitution",
    "redirection",
]
OriginBranch = Literal["dataset", "documentation", "keenable"]


class BashGuardAuditTable(StrictTable):
    candidate_id: Series[str] = pa.Field(unique=True, description="Candidate ID in the merged table.")
    branch: Series[str] = pa.Field(isin=get_args(OriginBranch))
    original_candidate_id: Series[str]
    original_source_id: Series[str]
    source_position: Series[str]
    status: Series[str] = pa.Field(isin=get_args(AuditStatus), description="Parser outcome.")
    flags: Series[list[str]] = pa.Field(description="Distinct Bash syntax features present in the row.")


class AuditInput(StrictModel):
    candidate_id: Text
    command: str
    source_position: Text


class AuditOrigin(StrictModel):
    branch: OriginBranch
    candidate_id: Text
    source_id: Text


class SyntaxAudit(StrictModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    status: AuditStatus
    flags: list[SyntaxFlag]


class AuditRow(SyntaxAudit):
    candidate_id: Text
    branch: OriginBranch
    original_candidate_id: Text
    original_source_id: Text
    source_position: Text


class AuditMetrics(StrictModel):
    candidates: int
    status_counts: dict[AuditStatus, int]
    flag_counts: dict[SyntaxFlag, int]


@dataclass(frozen=True, slots=True)
class AuditProtocolError(Exception):
    expected: int
    received: int

    def __str__(self) -> str:
        return f"BashGuard audit returned {self.received} responses for {self.expected} inputs"


def syntax_audits(inputs: Sequence[AuditInput], binary: str) -> Iterator[SyntaxAudit]:
    """Send inert JSON strings to the syntax-only executable in bounded batches."""
    for batch in batched(inputs, 64):
        result = subprocess.run(
            [binary],
            input="".join(f"{json.dumps(row.command)}\n" for row in batch),
            text=True,
            encoding="utf-8",
            capture_output=True,
            check=True,
            timeout=30,
        )
        lines = result.stdout.splitlines()
        if len(lines) != len(batch):
            raise AuditProtocolError(expected=len(batch), received=len(lines))
        yield from (SyntaxAudit.model_validate_json(line) for line in lines)


@dataclass(frozen=True)
class BashGuardAnalyzer:
    binary: str

    @classmethod
    def from_environment(cls) -> "BashGuardAnalyzer":
        return cls(os.environ.get("CODEBENCH_BASHGUARD_AUDIT_BINARY", "/opt/codebench/bin/bashguard-audit"))


def audit_candidates(
    candidates: DataFrame[CommandCandidatesTable],
    analyzer: BashGuardAnalyzer,
) -> tuple[DataFrame[BashGuardAuditTable], AuditMetrics]:
    """Join parser results to untouched merge provenance and count rows per status and flag."""
    inputs = model_records(candidates, AuditInput)
    origins = TypeAdapter(dict[str, AuditOrigin]).validate_python(candidates.attrs["origins"])
    rows = [
        AuditRow(
            candidate_id=item.candidate_id,
            branch=origin.branch,
            original_candidate_id=origin.candidate_id,
            original_source_id=origin.source_id,
            source_position=item.source_position,
            status=syntax.status,
            flags=syntax.flags,
        )
        for item, syntax in zip(inputs, syntax_audits(inputs, analyzer.binary), strict=True)
        for origin in (origins[item.candidate_id],)
    ]
    metrics = AuditMetrics(
        candidates=len(rows),
        status_counts=dict(Counter(row.status for row in rows)),
        flag_counts=dict(Counter(flag for row in rows for flag in row.flags)),
    )
    frame = validated_frame([row.model_dump(mode="json") for row in rows], BashGuardAuditTable)
    frame.attrs["bashguard_syntax_audit"] = metrics.model_dump(mode="json")
    return frame, metrics
