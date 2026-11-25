import re
from dataclasses import dataclass
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from codebench_pipeline.batch_labeling_rows import RowEvidence
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, LabelPolicy, label_risk_ids
from codebench_pipeline.grounding_tools import GroundingOrigins
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.live_grounding import LivePage, trusted_live_url
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.storage import IntegrityError, fingerprint

ROW_LABELING_CONTRACT: Final = "independent-row-react-v1"
ROW_LABELING_POLICY: Final = "advisory-risk-hints-v1"


class _Strict(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class SubmittedOutcome(_Strict):
    candidate_id: str
    row_input_fingerprint: str
    label: LabelDecision | None
    evidence_ids: list[str] = Field(max_length=8)
    grounding_quotes: dict[str, str]
    grounding_rationale: str = Field(max_length=800)
    failure_code: str = Field(max_length=100)
    failure_detail: str = Field(max_length=1200)


class SubmittedLabel(_Strict):
    outcome: SubmittedOutcome


class Function(_Strict):
    name: str
    arguments: str


class ToolCall(_Strict):
    id: str = Field(min_length=1)
    index: int | None = None
    type: Literal["function"]
    function: Function


class _Message(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    role: Literal["assistant"]
    content: str | None = None
    reasoning_content: str | None = None
    tool_calls: list[ToolCall] = Field(default_factory=list)


class _Choice(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    index: Literal[0]
    finish_reason: str
    message: _Message


class Completion(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    id: str
    model: str
    choices: list[_Choice] = Field(min_length=1, max_length=1)
    usage: dict[str, JsonValue] = Field(default_factory=dict)


_OBJECT = TypeAdapter(dict[str, JsonValue])
_OUTCOMES = TypeAdapter(list[JsonValue])


def tool_schema(name: str, description: str, schema: dict[str, JsonValue]) -> dict[str, JsonValue]:
    raw = dict(schema)
    definitions = _OBJECT.validate_python(raw.pop("$defs", {}))

    def inline(value: JsonValue) -> JsonValue:
        if isinstance(value, list):
            return [inline(item) for item in value]
        if not isinstance(value, dict):
            return value
        reference = value.get("$ref")
        target = (
            _OBJECT.validate_python(definitions[reference.removeprefix("#/$defs/")])
            if isinstance(reference, str)
            else {}
        )
        return {key: inline(item) for key, item in {**target, **value}.items() if key != "$ref"}

    return {
        "type": "function",
        "function": {"name": name, "description": description, "parameters": inline(raw)},
    }


def submitted_value(arguments: str) -> JsonValue:
    envelope = _OBJECT.validate_json(arguments)
    if envelope.keys() != {"outcome"}:
        raise ValueError("Expected a single outcome object")
    return envelope["outcome"]


def page_evidence_id(candidate: CandidateRecord, page: LivePage) -> str:
    return fingerprint(
        [candidate.model_dump(mode="json", include=set(CandidateRecord.model_fields)), page.identity]
    )


def page_observation(content: str, command: str) -> tuple[str, ...]:
    tokens = re.compile(r"--?[\w-]+|[\w-]{2,}")
    terms = set(tokens.findall(command.lower()))
    passages = dict.fromkeys(
        span.group().rstrip()
        for line in re.split(r"\n+|(?<=[.!?])\s+", content)
        for span in re.finditer(r"\S.{0,259}(?=\s|$)|\S{260}", line.strip())
    )

    def relevance(passage: str) -> int:
        matched = terms.intersection(tokens.findall(passage.lower()))
        return sum((3 if term.startswith("-") else 1) for term in matched)

    return tuple(sorted(passages, key=relevance, reverse=True)[:6])


@dataclass(frozen=True, slots=True)
class OfferedPassages:
    url: str
    quotes: tuple[str, ...]


class LiveSupport(_Strict):
    row_input_fingerprint: str
    pages: dict[str, LivePage]
    quotes: dict[str, str]
    rationale: str

    def documentation(self, candidate: CandidateRecord, grounding: GroundingOrigins) -> list[SemanticSnippet]:
        if not self.pages or self.pages.keys() != self.quotes.keys() or not self.rationale.strip():
            raise IntegrityError("Missing candidate-bound fetched documentation support")
        docs: list[SemanticSnippet] = []
        for evidence_id, page in sorted(self.pages.items()):
            quote = self.quotes[evidence_id]
            if (
                evidence_id != page_evidence_id(candidate, page)
                or candidate.candidate_id not in page.candidate_ids
                or not trusted_live_url(grounding, page.request_url)
                or not trusted_live_url(grounding, page.url)
                or not quote.strip()
                or len(quote) > 1200
                or quote not in page.content
            ):
                raise IntegrityError(
                    f"Citation for {page.url} must exactly match a verbatim passage from that fetched page."
                )
            docs.append(
                SemanticSnippet(
                    utility=candidate.util,
                    subcommands=[candidate.command],
                    url=page.url,
                    retrieved_at=page.fetched_at,
                    snippet=quote,
                    version_limits="Verify applicability from the cited documentation and candidate context.",
                )
            )
        return docs

    def verify(
        self, candidate: CandidateRecord, attempt: LabelingAttempt, grounding: GroundingOrigins
    ) -> None:
        docs = self.documentation(candidate, grounding)
        if (
            self.row_input_fingerprint != attempt.row_input_fingerprint
            or attempt.documentation != docs
            or attempt.label is None
            or set(attempt.label.documentation_urls) != {doc.url for doc in docs}
        ):
            raise IntegrityError("Persisted labeling support does not match its candidate-bound attempt")


@dataclass(frozen=True, slots=True)
class RowInput:
    candidate: CandidateRecord
    fingerprint: str

    def finalize_tool(self, rules: LabelPolicy, evidence: dict[str, OfferedPassages]) -> dict[str, JsonValue]:
        schema = _OBJECT.validate_python(SubmittedLabel.model_json_schema())
        definitions = _OBJECT.validate_python(schema["$defs"])
        label = _OBJECT.validate_python(definitions[LabelDecision.__name__])
        properties = _OBJECT.validate_python(label["properties"])
        for field in ("candidate_id", "util"):
            del properties[field]
        for field in ("description", "rationale"):
            prose = _OBJECT.validate_python(properties[field])
            prose.pop("pattern", None)
            properties[field] = prose
        properties["category_id"] = {"type": "string", "enum": [rules.category_id]}
        properties["risk_id"] = {
            "type": "string",
            "enum": list[JsonValue](label_risk_ids(self.candidate, rules)),
        }
        urls = sorted({page.url for page in evidence.values()})
        properties["documentation_urls"] = (
            {"type": "array", "items": {"type": "string", "enum": list[JsonValue](urls)}}
            if urls
            else {"type": "array", "items": {"type": "string"}, "maxItems": 0}
        )
        label["properties"] = properties
        label["required"] = [
            field for field in _OUTCOMES.validate_python(label["required"]) if field in properties
        ]
        definitions[LabelDecision.__name__] = label

        outcome = _OBJECT.validate_python(definitions[SubmittedOutcome.__name__])
        fields = _OBJECT.validate_python(outcome["properties"])
        fields["candidate_id"] = {"type": "string", "enum": [self.candidate.candidate_id]}
        fields["row_input_fingerprint"] = {"type": "string", "enum": [self.fingerprint]}
        fields["evidence_ids"] = (
            {"type": "array", "items": {"type": "string", "enum": list[JsonValue](evidence)}, "maxItems": 8}
            if evidence
            else {"type": "array", "items": {"type": "string"}, "maxItems": 0}
        )
        fields["grounding_quotes"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                identity: {"type": "string", "enum": list[JsonValue](page.quotes)}
                for identity, page in evidence.items()
            },
        }
        outcome["properties"] = fields
        definitions[SubmittedOutcome.__name__] = outcome
        schema["$defs"] = definitions
        return tool_schema("finalize", "Submit the candidate judgment or explicit failure.", schema)

    def parse(self, value: JsonValue) -> SubmittedOutcome:
        fields = _OBJECT.validate_python(value)
        label = fields.get("label")
        if isinstance(label, dict):
            fields = {
                **fields,
                "label": {"candidate_id": self.candidate.candidate_id, "util": self.candidate.util, **label},
            }
        outcome = SubmittedOutcome.model_validate(fields)
        if outcome.row_input_fingerprint != self.fingerprint:
            raise ValueError(f"Copy the unchanged row_input_fingerprint: {self.fingerprint}")
        if outcome.label is None and (
            not outcome.failure_code.strip()
            or not outcome.failure_detail.strip()
            or outcome.evidence_ids
            or outcome.grounding_quotes
        ):
            raise ValueError("Failure requires a code and detail, and cannot cite supporting evidence")
        return outcome

    def support(
        self, outcome: SubmittedOutcome, pages: dict[str, LivePage], grounding: GroundingOrigins
    ) -> tuple[LiveSupport, list[SemanticSnippet]]:
        ids = set(outcome.evidence_ids)
        if outcome.failure_code or outcome.failure_detail:
            raise ValueError("Set failure_code and failure_detail to empty strings when submitting a label.")
        if not ids or not ids <= pages.keys():
            raise ValueError(
                "Select at least one evidence_id from a page fetched for this row; query hits do not count."
            )
        if len(ids) != len(outcome.evidence_ids) or ids != outcome.grounding_quotes.keys():
            raise ValueError(
                "Use distinct evidence_ids and exactly the same IDs as keys in grounding_quotes."
            )
        if not outcome.grounding_rationale.strip():
            raise ValueError(
                "Explain how the selected verbatim passages support this row in grounding_rationale."
            )
        support = LiveSupport(
            row_input_fingerprint=self.fingerprint,
            pages={identity: pages[identity] for identity in outcome.evidence_ids},
            quotes=outcome.grounding_quotes,
            rationale=outcome.grounding_rationale,
        )
        return support, support.documentation(self.candidate, grounding)


def row_evidence(support: LiveSupport, docs: list[SemanticSnippet]) -> list[RowEvidence]:
    return [
        RowEvidence(evidence_id=identity, snippet=doc, provider_metadata=support.pages[identity].identity)
        for identity, doc in zip(sorted(support.pages), docs, strict=True)
    ]
