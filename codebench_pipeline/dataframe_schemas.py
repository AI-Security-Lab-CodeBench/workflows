from collections.abc import Sequence
from typing import Final, Optional, TypeVar

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series
from pydantic import BaseModel, JsonValue, TypeAdapter

from codebench_pipeline.labeling_outcomes import LabelingAttempt, historical_attempt_cells


class StrictTable(pa.DataFrameModel):
    class Config(pa.DataFrameModel.Config):
        strict = True
        coerce = True


class SourceCatalogTable(StrictTable):
    source_id: Series[str] = pa.Field(unique=True)
    source_url: Series[str]
    license: Series[str]
    license_url: Series[str]
    attribution: Series[str]
    revision: Series[str]
    retrieved_at: Series[str]
    source_type: Series[str]
    provenance_type: Series[str]
    source_locator: Series[str]
    sha256: Series[str]
    format: Series[str]
    text_field: Series[str]
    context_field: Series[str]
    sample: Series[str]
    captured_content: Series[str]
    decision: Series[str] = pa.Field(isin=["selected", "needs_review", "exclude"])
    rationale: Series[str]
    analysis: Series[str]
    confidence: Series[str] = pa.Field(isin=["low", "medium", "high", "not_applicable"])


class SourceDocumentsTable(StrictTable):
    source_id: Series[str] = pa.Field(unique=True)
    source_url: Series[str]
    license: Series[str]
    license_url: Series[str]
    attribution: Series[str]
    revision: Series[str]
    retrieved_at: Series[str]
    source_type: Series[str]
    provenance_type: Series[str]
    source_locator: Series[str]
    sha256: Series[str]
    format: Series[str]
    text_field: Series[str]
    context_field: Series[str]
    content: Series[str] = pa.Field(description="Inert source document text; never executed.")


class DocumentAdaptersTable(StrictTable):
    source_id: Series[str] = pa.Field(unique=True)
    format: Series[str] = pa.Field(isin=["text", "markdown", "json", "jsonl", "csv"])
    text_field: Series[str]
    context_field: Series[str]
    rationale: Series[str]


class CommandCandidatesTable(StrictTable):
    candidate_id: Series[str] = pa.Field(unique=True)
    command: Series[str] = pa.Field(description="Original inert Bash command.")
    util: Series[str]
    context: Series[str]
    source_id: Series[str]
    source_url: Series[str]
    source_position: Series[str]
    transformations: Series[list[str]]
    risk_candidates: Series[list[str]]
    source_weight: Optional[Series[float]] = pa.Field(
        ge=0,
        le=1,
        description="Collection source trust used to rank labeling; independent of label confidence.",
    )


class FilteredCommandsTable(CommandCandidatesTable):
    filter_decision: Series[str] = pa.Field(isin=["eligible", "needs_review"])
    filter_reason: Series[str]


class DocumentationTable(StrictTable):
    utility: Series[str]
    subcommands: Series[list[str]]
    url: Series[str]
    retrieved_at: Series[str]
    snippet: Series[str]
    version_limits: Series[str]


def _check_labeling_rows(frame: pd.DataFrame) -> bool:
    model_records(frame, LabelingAttempt)
    return True


_LABELING_ROWS: Final = pa.Check(_check_labeling_rows)


class LabelingAttemptsTable(CommandCandidatesTable):
    status: Series[str] = pa.Field(isin=["success", "failure"])
    label: Series[object] = pa.Field(nullable=True)
    failure_code: Series[str]
    failure_detail: Series[str]
    row_input_fingerprint: Series[str] = pa.Field(str_matches=r"^[a-f0-9]{64}$")
    documentation_manifest_digest: Series[str] = pa.Field(str_matches=r"^[a-f0-9]{64}$")
    documentation: Series[object]
    attempt_evidence_reference: Series[str]

    @classmethod
    def to_schema(cls) -> pa.DataFrameSchema:
        schema = super().to_schema()
        schema.checks = [_LABELING_ROWS]
        return schema


class LabelingFailuresTable(LabelingAttemptsTable):
    status: Series[str] = pa.Field(eq="failure")
    failure_code: Series[str] = pa.Field(str_matches=r"\S")
    failure_detail: Series[str] = pa.Field(str_matches=r"\S")


class ProposedAnnotationsTable(CommandCandidatesTable):
    decision: Series[str] = pa.Field(isin=["accept", "needs_review", "reject"])
    category_id: Series[str]
    risk_id: Series[str]
    severity: Series[str] = pa.Field(isin=["low", "medium", "high", "critical"])
    class_: Series[str] = pa.Field(alias="class", isin=["allow", "deny", "ask"])
    description: Series[str]
    category_confidence: Series[str] = pa.Field(isin=["low", "medium", "high"])
    risk_confidence: Series[str] = pa.Field(isin=["low", "medium", "high"])
    severity_confidence: Series[str] = pa.Field(isin=["low", "medium", "high"])
    class_confidence: Series[str] = pa.Field(isin=["low", "medium", "high"])
    rationale: Series[str]
    documentation_urls: Series[list[str]]


class ReviewedAnnotationsTable(ProposedAnnotationsTable):
    review_verdict: Series[str] = pa.Field(isin=["approve", "needs_review", "reject"])
    review_rationale: Series[str]
    review_confidence: Series[str] = pa.Field(isin=["low", "medium", "high"])


class ReviewQueueTable(ProposedAnnotationsTable):
    exclusion_reason: Series[str]


class ReviewSnapshotTable(StrictTable):
    batch_id: Series[str] = pa.Field(unique=True)
    snapshot_id: Series[str]


class HumanReviewDecisionsTable(StrictTable):
    candidate_id: Series[str] = pa.Field(unique=True)
    batch_id: Series[str]
    snapshot_id: Series[str]
    generation: Series[int] = pa.Field(ge=1)
    batch_revision: Series[int] = pa.Field(ge=0)
    row_revision: Series[int] = pa.Field(ge=1)
    human_decision: Series[str] = pa.Field(isin=["approve", "reject"])
    reviewer: Series[str] = pa.Field(str_matches=r"[\s\S]*\S")
    notes: Series[str]
    decided_at: Series[str]


class CommandsTable(StrictTable):
    command: Series[str]
    description: Series[str]
    util: Series[str]
    severity: Series[str] = pa.Field(isin=["low", "medium", "high", "critical"])
    category: Series[str]
    risk: Series[str]
    source: Series[str]
    seed: Series[str] = pa.Field(eq="-")
    class_: Series[str] = pa.Field(alias="class", isin=["allow", "deny", "ask"])
    human_reviewed: Series[str] = pa.Field(isin=["true", "false"])


Schema = TypeVar("Schema", bound=pa.DataFrameModel)
Record = TypeVar("Record", bound=BaseModel)


def validated_frame(rows: Sequence[dict[str, JsonValue]], schema: type[Schema]) -> DataFrame[Schema]:
    provided = dict.fromkeys(name for row in rows for name in row)
    columns = [
        name for name, column in schema.to_schema().columns.items() if column.required or name in provided
    ]
    frame = pd.DataFrame(list(rows), columns=[*columns, *(name for name in provided if name not in columns)])
    return schema.validate(frame)


def current_labeling_attempts(frame: pd.DataFrame) -> pd.DataFrame:
    """Interpret the former text columns at the historical Parquet boundary."""
    if "label_json" not in frame or "documentation_json" not in frame:
        return frame
    current = validated_frame(
        [historical_attempt_cells(row) for row in frame_records(frame)],
        LabelingAttemptsTable,
    )
    if "source_weight" in frame and "source_weight" not in current:
        current["source_weight"] = frame["source_weight"]
    current.index = frame.index
    current.attrs = frame.attrs
    return current


def frame_records(frame: pd.DataFrame) -> list[dict[str, JsonValue]]:
    rows = TypeAdapter(list[dict[str, JsonValue]]).validate_json(frame.to_json(orient="records"))
    if "source_weight" in frame:
        for row, weight in zip(rows, frame["source_weight"], strict=True):
            row["source_weight"] = weight
    return rows


def model_records(frame: pd.DataFrame, model: type[Record]) -> list[Record]:
    fields = [field.alias or name for name, field in model.model_fields.items()]
    return [
        model.model_validate({name: _native_cell(value) for name, value in row.items()})
        for row in frame.loc[:, fields].to_dict(orient="records")
    ]


def _native_cell(value: JsonValue | np.ndarray) -> JsonValue:
    match value:
        case np.ndarray():
            return [_native_cell(item) for item in value.tolist()]
        case list():
            return [_native_cell(item) for item in value]
        case dict():
            return {name: _native_cell(item) for name, item in value.items()}
        case _:
            return value
