from collections.abc import Sequence
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.models import Digest
from codebench_pipeline.typed_config import ConfigArtifact

Category = Annotated[str, Field(pattern=r"^CAT-[1-9][0-9]*$")]
Risk = Annotated[str, Field(pattern=r"^CAT-[1-9][0-9]*\.[1-9][0-9]*$")]
Severity = Literal["critical", "high", "medium", "low"]
Action = Literal["allow", "ask", "deny"]
Split = Literal["train", "test"]


class _LabelAxes(ConfigArtifact):
    category: Category
    risk: Risk
    severity: Severity

    @model_validator(mode="after")
    def risk_belongs_to_category(self) -> Self:
        if not self.risk.startswith(f"{self.category}."):
            raise PydanticCustomError("risk_category", "Risk must belong to its category")
        return self


class Labels(_LabelAxes):
    action: Action


class ModifyGold(_LabelAxes):
    action: Literal["modify"]


type Gold = Labels | ModifyGold


class Other(ConfigArtifact):
    category: Literal["other"] = "other"


type Decision = Labels | Other


class Context(ConfigArtifact):
    environment: Literal["", "unknown", "production", "nonproduction"] = ""
    approval: Literal["", "unknown", "granted", "not_granted"] = ""


class Fixture(ConfigArtifact):
    command: str = Field(min_length=1)
    expected: Decision
    context: Context = Context()


class NativeRule(ConfigArtifact):
    id: str = Field(min_length=1)
    scope: Literal["", "simple", "ast"] = "simple"
    when: str = Field(min_length=1)
    labels: Labels
    rationale: str = Field(min_length=1)
    sources: tuple[str, ...] = Field(min_length=1)
    capture_allow_shapes: tuple[tuple[str, ...], ...] = Field(default=(), exclude_if=lambda shapes: not shapes)
    capture_guarded: bool = Field(default=False, exclude_if=lambda guarded: not guarded)

    @field_validator("sources")
    @classmethod
    def https_sources(cls, sources: tuple[str, ...]) -> tuple[str, ...]:
        for source in sources:
            try:
                url = urlsplit(source)
            except ValueError as exc:
                raise PydanticCustomError("source_url", "Expected a valid HTTPS source") from exc
            if url.scheme != "https" or not url.hostname or url.username is not None:
                raise PydanticCustomError("source_url", "Expected an HTTPS source without credentials")
        return sources


class Package(ConfigArtifact):
    rule: NativeRule
    tests: tuple[Fixture, ...] = Field(min_length=1)


class Bundle(ConfigArtifact):
    version: Literal[1] = 1
    rules: tuple[NativeRule, ...] = Field(min_length=1, max_length=128)

    @field_validator("rules")
    @classmethod
    def unique_rule_ids(cls, rules: tuple[NativeRule, ...]) -> tuple[NativeRule, ...]:
        if len({rule.id for rule in rules}) != len(rules):
            raise PydanticCustomError("rule_ids", "Bundle rule IDs must be distinct")
        return rules


class EmptyLearnedBundle(ConfigArtifact):
    version: Literal[1] = 1
    kind: Literal["empty_learned"]
    rules: tuple[()]


type LearnedBundle = Bundle | EmptyLearnedBundle


class EvaluationInput(ConfigArtifact):
    id: str
    command: str
    context: Context = Context()


class Diagnostic(ConfigArtifact):
    code: str
    message: str
    phase: str | None = None
    rule_id: str | None = None
    field: str | None = None
    line: int | None = None
    column: int | None = None


class NativeResult(ConfigArtifact):
    status: Literal["matched", "abstain"]
    basis: Literal["literal_cli_form", "bash_ast"]
    labels: Labels | None = None
    rule_ids: tuple[str, ...]
    rationales: tuple[str, ...]
    sources: tuple[str, ...]
    reasons: tuple[str, ...]
    bundle_sha256: Digest

    @model_validator(mode="after")
    def consistent_decision(self) -> Self:
        if self.status == "matched":
            if self.labels is None or not self.rule_ids or self.reasons:
                raise PydanticCustomError("native_match", "A match must carry labels and matching IDs")
        elif self.labels is not None or self.rule_ids:
            raise PydanticCustomError(
                "native_abstention", "An abstention cannot carry labels or matching IDs"
            )
        return self


class NativeRow(ConfigArtifact):
    id: str
    result: NativeResult
    error: Diagnostic | None = None


class NativeEvaluation(ConfigArtifact):
    runtime_version: str
    bundle_sha256: Digest
    results: tuple[NativeRow, ...]


class NativeFailure(ConfigArtifact):
    runtime_version: str
    error: Diagnostic


class EvaluatedRow(ConfigArtifact):
    input: EvaluationInput
    expected: Gold | Other
    actual: Decision | None
    reasons: tuple[str, ...]
    rule_ids: tuple[str, ...]
    basis: Literal["literal_cli_form", "bash_ast"] | None = None
    rationales: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()
    error: Diagnostic | None = None

    @classmethod
    def from_native(cls, pair: tuple[EvaluationInput, Gold | Other], native: NativeRow) -> Self:
        input_row, expected = pair
        result = native.result
        error = native.error
        actual: Decision | None = result.labels
        if error is None and actual is None:
            if result.reasons[:1] == ("no_proven_rule",):
                actual = Other()
            else:
                reason = result.reasons[0] if result.reasons else "unexplained_abstention"
                error = Diagnostic(code=reason, message="; ".join(result.reasons) or reason, phase="evaluate")
        return cls(
            input=input_row,
            expected=expected,
            actual=actual if error is None else None,
            reasons=result.reasons,
            rule_ids=result.rule_ids,
            basis=result.basis,
            rationales=result.rationales,
            sources=result.sources,
            error=error,
        )

    @property
    def id(self) -> str:
        return self.input.id

    @property
    def passed(self) -> bool:
        return self.error is None and self.actual == self.expected


class HitSet(ConfigArtifact):
    rule_id: str
    row_ids: tuple[str, ...]


class EvaluationReceipt(ConfigArtifact):
    """Private evidence. Commands, expected decisions and diagnostics are not public metadata."""

    runtime_version: str
    bundle_sha256: Digest
    split: Split
    corpus_digest: Digest
    fixtures: tuple[EvaluatedRow, ...] = ()
    rows: tuple[EvaluatedRow, ...]
    hit_sets: tuple[HitSet, ...]

    @property
    def fixtures_passed(self) -> bool:
        return all(row.passed for row in self.fixtures)

    def gate_failure(self) -> Diagnostic | None:
        """Check the submitted fixtures and train safety without imposing a coverage threshold."""
        if not self.fixtures_passed:
            return Diagnostic(code="fixture_mismatch", message="One or more fixtures failed")
        if any(row.error is not None or (row.rule_ids and not row.passed) for row in self.rows):
            return Diagnostic(
                code="train_regression", message="Training contains wrong matches or evaluator errors"
            )
        return None

    def page(
        self, *, offset: int = 0, limit: int = 25, kind: Literal["fixtures", "rows"] = "rows"
    ) -> tuple[EvaluatedRow, ...]:
        """Select a bounded page without serializing or copying the complete receipt."""
        if offset < 0 or not 1 <= limit <= 100:
            raise EvaluationError(
                Diagnostic(code="page_bounds", message="Expected offset >= 0 and limit 1..100")
            )
        rows = self.rows if kind == "rows" else self.fixtures
        return rows[offset : offset + limit]


class EvaluationError(Exception):
    def __init__(self, diagnostic: Diagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(f"{diagnostic.code}: {diagnostic.message}")


class FinalizeSubmission(ConfigArtifact):
    drop: tuple[str, ...] = ()
    add: tuple[Package, ...] = ()
    justification: str = Field(pattern=r"\S")


class FinalizeReceipt(ConfigArtifact):
    submission: FinalizeSubmission
    packages: tuple[Package, ...]
    bundle: LearnedBundle
    additions: tuple[EvaluationReceipt, ...]
    combined: EvaluationReceipt


class FinalizeError(EvaluationError):
    def __init__(self, diagnostic: Diagnostic, evidence: Sequence[EvaluationReceipt] = ()) -> None:
        self.evidence = tuple(evidence)
        super().__init__(diagnostic)
