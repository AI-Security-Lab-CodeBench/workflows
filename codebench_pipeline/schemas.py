from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Annotated, Final, Literal, TypeVar

import pandera.polars as pa
import polars as pl
from pandera.polars import PolarsData
from pydantic import ConfigDict, Field, JsonValue, TypeAdapter, model_validator

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.models import Digest, FrozenModel, Stage

FIELDS: Final = ("command", "description", "util", "severity", "category", "risk", "source", "seed", "class")
DEFAULT_RULES: Final = CategoryRules.model_validate(
    json.loads((Path(__file__).resolve().parents[1] / "configs/datasets/cat3.json").read_bytes())["rules"]
)
CATEGORY: Final = DEFAULT_RULES.canonical
RISKS: Final = tuple(risk.canonical for risk in DEFAULT_RULES.risks)
CONFIDENCES: Final = tuple(f"{field}_confidence" for field in ("category", "risk", "severity", "class"))
REVIEW_FIELDS: Final = (
    *FIELDS,
    *"candidate_id csv_record context source_urls source_locator documentation_urls".split(),
    *CONFIDENCES,
    *"rationale review_rationale".split(),
)
PENDING_FIELDS: Final = (
    *(
        "candidate_id command risk class severity llm_rationale exclusion_reason label_decision "
        "review_verdict rationale_source description util"
    ).split(),
    *CONFIDENCES,
    *(
        "context source_urls documentation_urls source_locator label_rationale review_rationale "
        "category source seed required_decision evidence_jsonl evidence_record"
    ).split(),
)
ALL_REVIEW_FIELDS: Final = (
    *REVIEW_FIELDS,
    *"label_decision review_verdict export_status exclusion_reason proposed_util".split(),
)
Text = Annotated[str, Field(min_length=1)]
HttpURL = Annotated[str, Field(pattern=r"^https?://[^/\s]+")]
RiskCode = Text
Model = TypeVar("Model", bound=FrozenModel)
JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])


class BridgeError(RuntimeError):
    def __init__(self, message: str, path: Path | None = None) -> None:
        self.message = message
        self.path = path
        super().__init__(f"{message}: {path}" if path is not None else message)


class BoundaryModel(FrozenModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CandidateRow(BoundaryModel):
    command: Text
    description: str
    util: Text
    severity: str
    category: str
    risk: str
    source: Text
    seed: Text
    class_: str = Field(alias="class")

    @model_validator(mode="after")
    def valid_seed(self) -> CandidateRow:
        if (self.source == "synthetic" and self.seed.strip() == "-") or (
            self.source != "synthetic" and self.seed != "-"
        ):
            raise BridgeError("Invalid source/seed contract")
        return self


class Candidate(BoundaryModel):
    candidate_id: Text
    row: CandidateRow
    source_ids: Annotated[list[Text], Field(min_length=1)]
    source_locator: Text
    evidence_context: Text
    transformations: list[Text]
    documentation_urls: list[HttpURL]
    tool_version: str | None
    label_rationale: str | None
    scenario_id: Text
    role: Literal["dangerous", "counterexample", "unpaired"]
    paired_candidate_ids: list[Text]
    synthetic_model: str | None
    status: Literal["candidate", "accepted", "rejected", "needs_review"]


class SourceEvidence(BoundaryModel):
    source_id: Text
    url: HttpURL
    locator: Text
    revision: Text
    retrieved_at: Text
    source_type: Text
    risks: Annotated[list[Text], Field(min_length=1)]
    accessibility: Literal["accessible"]
    relevance: Text
    commands_present: Literal[True]
    license: Text
    license_url: HttpURL
    reuse_decision: Literal["permitted"]
    notes: str


class Semantics(BoundaryModel):
    utility: Text
    subcommands: Annotated[list[Text], Field(min_length=1)]
    url: HttpURL
    retrieved_at: Text
    snippet: Text
    version_limits: Text


class RawCommand(BoundaryModel):
    raw_id: Digest
    source_id: Text
    doc_full_url: HttpURL
    doc_pos: Text
    local_path: Text
    format: Literal["text", "markdown", "json", "jsonl", "csv"]
    raw_command: Text
    command: Text
    context: str
    extraction_transformations: list[Text]


class EligibleCommand(RawCommand):
    transformations: list[Text]
    risk_candidates: Annotated[list[RiskCode], Field(min_length=1, max_length=1)]
    filter_status: Literal["eligible"]
    filter_reason: Text
    duplicate_of: Literal[""]


class StageMetadata(BoundaryModel):
    stage: Stage
    status: Literal["completed", "empty"]
    replay: bool
    annotation_origin: Literal["fresh_invocations", "historical_replay", "not_invoked"]
    statistics: dict[str, JsonValue]
    evidence_sha256: dict[str, Digest] = Field(default_factory=dict)


def read_jsonl(path: Path, model: type[Model]) -> list[Model]:
    result: list[Model] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            raise BridgeError(f"Blank JSONL record {number}", path)
        result.append(model.model_validate_json(line))
    return result


def write_json(path: Path, value: dict[str, JsonValue]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(JSON_OBJECT.dump_json(value, indent=2) + b"\n")


def seed_contract(data: PolarsData) -> bool:
    frame = data.lazyframe.collect()
    sources = TypeAdapter(list[str]).validate_python(frame.get_column("source").to_list())
    seeds = TypeAdapter(list[str]).validate_python(frame.get_column("seed").to_list())
    return all(
        seed.strip() != "-" if source == "synthetic" else seed == "-"
        for source, seed in zip(sources, seeds, strict=True)
    )


def nonblank(value: str) -> bool:
    return bool(value.strip())


def one_of(values: tuple[str, ...]) -> pa.Check:
    def included(value: str) -> bool:
        return value in values

    return pa.Check(included, element_wise=True)


RAW_SCHEMA: Final = pa.DataFrameSchema(
    {
        **{
            field: pa.Column(str, unique=field == "raw_id")
            for field in RawCommand.model_fields
            if field != "extraction_transformations"
        },
        "extraction_transformations": pa.Column(pl.List(pl.String)),
    },
    strict=True,
)
ELIGIBLE_SCHEMA: Final = pa.DataFrameSchema(
    {
        **RAW_SCHEMA.columns,
        "transformations": pa.Column(pl.List(pl.String)),
        "risk_candidates": pa.Column(pl.List(pl.String)),
        "filter_status": pa.Column(str, one_of(("eligible",))),
        "filter_reason": pa.Column(str),
        "duplicate_of": pa.Column(str, one_of(("",))),
    },
    strict=True,
)


def export_schema(rules: CategoryRules) -> pa.DataFrameSchema:
    values = {
        "severity": tuple(rule.name for rule in rules.severities),
        "category": (rules.canonical,),
        "risk": tuple(risk.canonical for risk in rules.risks),
        "class": tuple(rule.name for rule in rules.classes),
    }
    return pa.DataFrameSchema(
        {
            field: pa.Column(
                str,
                [
                    pa.Check(nonblank, element_wise=True),
                    *([one_of(values[field])] if field in values else []),
                ],
            )
            for field in FIELDS
        },
        checks=pa.Check(seed_contract),
        strict=True,
        ordered=True,
    )


EXPORT_SCHEMA: Final = export_schema(DEFAULT_RULES)


def validate_commands(
    path: Path, *, eligible: bool = False, rules: CategoryRules = DEFAULT_RULES
) -> list[RawCommand] | list[EligibleCommand]:
    records = read_jsonl(path, EligibleCommand) if eligible else read_jsonl(path, RawCommand)
    if eligible:
        known = {risk.id for risk in rules.risks}
        for record in records:
            if isinstance(record, EligibleCommand) and not set(record.risk_candidates) <= known:
                raise BridgeError("Risk candidates are outside the supplied taxonomy", path)
    schema = ELIGIBLE_SCHEMA if eligible else RAW_SCHEMA
    lists = {"extraction_transformations": pl.List(pl.String)}
    if eligible:
        lists.update(transformations=pl.List(pl.String), risk_candidates=pl.List(pl.String))
    if records:
        frame = pl.DataFrame(
            [item.model_dump(mode="json", by_alias=True) for item in records],
            schema_overrides=lists,
        )
    else:
        fields = EligibleCommand.model_fields if eligible else RawCommand.model_fields
        frame = pl.DataFrame(schema={**{field: pl.String for field in fields}, **lists})
    schema.validate(frame)
    return records


def validate_export(path: Path, *, rules: CategoryRules = DEFAULT_RULES) -> pl.DataFrame:
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(stream, strict=True))
    if not rows or rows[0] != list(FIELDS) or any(len(row) != len(FIELDS) for row in rows[1:]):
        raise BridgeError("Expected exactly nine canonical CSV fields in order", path)
    frame = pl.DataFrame(rows[1:], schema={field: pl.String for field in FIELDS}, orient="row")
    export_schema(rules).validate(frame)
    return frame
