import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from codebench_pipeline.batch_labeling import annotate_batches
from codebench_pipeline.dataframe_io import AssetReference
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.dataset_registry import read_registered_spec
from codebench_pipeline.labeling_outcomes import LabelingAttempt, validate_attempts
from codebench_pipeline.labeling_stage_settings import LabelingRules
from codebench_pipeline.live_grounding import LiveReceipt
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.review_policy import requires_human_review
from codebench_pipeline.storage import IntegrityError

type EvaluationSplit = Literal["development", "heldout", "all"]


class CohortRecord(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    row: dict[str, JsonValue]
    seed_documentation: list[SemanticSnippet] = Field(default_factory=list)
    selection_reason: str
    split: Literal["development", "heldout"]
    documentation_gap_candidate: bool = False
    category_certain_other_uncertain: bool = False

    def candidate(self) -> CandidateRecord:
        return CandidateRecord.model_validate({name: self.row[name] for name in CandidateRecord.model_fields})

    def baseline(self) -> LabelDecision | None:
        if not {field.alias or name for name, field in LabelDecision.model_fields.items()} <= self.row.keys():
            return None
        return LabelDecision.model_validate(
            {
                field.alias or name: self.row[field.alias or name]
                for name, field in LabelDecision.model_fields.items()
            }
        )


class EvaluationCohort(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    dataset: str
    selection_version: str
    baseline_model: ModelConfig | None = None
    snapshots: dict[str, AssetReference]
    records: list[CohortRecord] = Field(min_length=1, max_length=100)


class _PacketPlan(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    grouping_version: str
    batch_size: int
    packets: list[list[str]]


class _ConversationMessage(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    role: str
    content: str | None = None


class _PromptCandidate(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    candidate_id: str


class _BatchInputRow(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    candidate: _PromptCandidate


class _BatchInput(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    records: list[_BatchInputRow] = Field(min_length=1)


class _BatchTranscript(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    logical_batch_id: str
    provider_calls: int = Field(ge=0)
    grounding_call_budget: int = Field(ge=0)
    messages: list[_ConversationMessage]


def _batch_evidence(evidence: Path, candidate_ids: set[str]) -> dict[str, JsonValue]:
    """Count admitted retrieval operations and separate completed conversations from the packet plan."""
    plan_path = evidence / "label-packets.json"
    plan = _PacketPlan.model_validate_json(plan_path.read_bytes()) if plan_path.exists() else None
    batches: list[JsonValue] = []
    represented: set[str] = set()
    tool_calls: Counter[str] = Counter()
    for path in sorted(evidence.glob("batches/*/transcript.json")):
        transcript = _BatchTranscript.model_validate_json(path.read_bytes())
        if transcript.logical_batch_id != path.parent.name:
            raise IntegrityError(f"Batch transcript ID differs from its evidence path: {path.parent.name}")
        initial_request = next(
            (message.content for message in transcript.messages if message.role == "user"), None
        )
        if initial_request is None:
            raise IntegrityError(f"Batch has no initial request: {transcript.logical_batch_id}")
        batch_input = _BatchInput.model_validate_json(initial_request)
        members: list[JsonValue] = [row.candidate.candidate_id for row in batch_input.records]
        member_ids = {row.candidate.candidate_id for row in batch_input.records}
        if len(member_ids) != len(members) or member_ids & represented or not member_ids <= candidate_ids:
            raise IntegrityError(f"Invalid batch membership: {transcript.logical_batch_id}")
        represented.update(member_ids)
        operations = [
            LiveReceipt.model_validate_json(operation.read_bytes())
            for operation in path.parent.glob("grounding/operation-*/receipt.json")
        ]
        if (
            len(operations) != transcript.provider_calls
            or sorted(operation.index for operation in operations) != list(range(len(operations)))
            or transcript.provider_calls > transcript.grounding_call_budget
        ):
            raise IntegrityError(
                f"Grounding receipts do not match the batch budget: {transcript.logical_batch_id}"
            )
        calls = Counter(operation.tool for operation in operations)
        tool_calls.update(calls)
        batches.append(
            {
                "batch_id": transcript.logical_batch_id,
                "transcript": path.relative_to(evidence).as_posix(),
                "candidate_ids": members,
                "rows": len(members),
                "grounding_call_budget": transcript.grounding_call_budget,
                "documentation_provider_calls": transcript.provider_calls,
                "query_calls": calls["query"],
                "page_fetch_calls": calls["page_fetch"],
            }
        )
    untranscribed: list[JsonValue] = list(sorted(candidate_ids - represented))
    return {
        "packet_plan": plan.model_dump(mode="json") if plan is not None else None,
        "batches": batches,
        "untranscribed_candidate_ids": untranscribed,
        "documentation_provider_calls": tool_calls.total(),
        "query_calls": tool_calls["query"],
        "page_fetch_calls": tool_calls["page_fetch"],
    }


def _write(path: Path, value: JsonValue) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, allow_nan=False, indent=2)


def _credentials() -> None:
    values = TypeAdapter(dict[str, str]).validate_json(sys.stdin.read())
    if set(values) != {"KEENABLE_API_KEY"}:
        raise ValueError("Credential input must contain exactly the Keenable grounding-provider key")
    if any(not value.strip() or "\n" in value or "\r" in value for value in values.values()):
        raise ValueError("Grounding credential input is invalid")
    os.environ.update(values)


def evaluation_summary(
    records: list[CohortRecord], attempts: list[LabelingAttempt], evidence: Path, elapsed: float
) -> dict[str, JsonValue]:
    """Summarize validated outcomes and observed batch receipts without assigning correctness."""
    originals = {record.candidate().candidate_id: record for record in records}
    changes: list[JsonValue] = []
    outcomes: Counter[str] = Counter()
    failures: Counter[str] = Counter()
    baseline_review = 0
    baseline_rows = 0
    proposed_review = 0
    labels: list[LabelDecision] = []
    for attempt in attempts:
        original = originals[attempt.candidate_id]
        before = original.baseline()
        if before is not None:
            baseline_rows += 1
            baseline_review += requires_human_review(before)
        outcomes[attempt.status] += 1
        after = attempt.label
        if after is None:
            failures[attempt.failure_code] += 1
            changes.append(
                {
                    "candidate_id": attempt.candidate_id,
                    "status": "failure",
                    "failure_code": attempt.failure_code,
                    "failure_detail": attempt.failure_detail,
                }
            )
            continue
        labels.append(after)
        proposed_review += requires_human_review(after)
        fields = (
            "decision",
            "category_id",
            "risk_id",
            "severity",
            "class",
            "category_confidence",
            "risk_confidence",
            "severity_confidence",
            "class_confidence",
        )
        previous = before.model_dump(mode="json", by_alias=True) if before is not None else {}
        current = after.model_dump(mode="json", by_alias=True)
        delta: dict[str, JsonValue] = {
            field: {"before": previous[field], "after": current[field]}
            for field in fields
            if field in previous and previous[field] != current[field]
        }
        changes.append(
            {
                "candidate_id": attempt.candidate_id,
                "status": "success",
                "changes": delta,
                "baseline_non_allow_to_allow": (
                    before.class_ != "allow" and after.class_ == "allow" if before is not None else None
                ),
                "requires_grounded_adjudication": True,
            }
        )
    model_calls = 0
    prompt_tokens = 0
    completion_tokens = 0
    for path in evidence.glob("batches/*/turn-*/raw_response.json"):
        payload = TypeAdapter(dict[str, JsonValue]).validate_json(path.read_bytes())
        usage = payload.get("usage")
        model_calls += 1
        if isinstance(usage, dict):
            for name, target in (("prompt_tokens", "prompt"), ("completion_tokens", "completion")):
                value = usage.get(name, 0)
                if isinstance(value, int) and not isinstance(value, bool):
                    if target == "prompt":
                        prompt_tokens += value
                    else:
                        completion_tokens += value
    model_seconds = 0.0
    capacity_wait_seconds = 0.0
    for directory in evidence.glob("batches/*/turn-*/capacity/capacity-*"):
        times: dict[str, list[int]] = {}
        for path in directory.glob("*.json"):
            event = TypeAdapter(dict[str, JsonValue]).validate_json(path.read_bytes()).get("event")
            if isinstance(event, str):
                times.setdefault(event, []).append(int(path.name.split("-", 1)[0]))
        if "response" in times and "submitted" in times:
            model_seconds += (max(times["response"]) - min(times["submitted"])) / 1_000_000_000
        if "wait" in times and "admission" in times:
            capacity_wait_seconds += (min(times["admission"]) - min(times["wait"])) / 1_000_000_000
    return {
        "status": "completed",
        "rows": len(records),
        "outcomes": dict(outcomes),
        "failure_reasons": dict(failures),
        "rows_with_baseline": baseline_rows,
        "baseline_uncertainty_rows_before_category_filter": baseline_review,
        "enhanced_uncertainty_rows_before_category_filter": proposed_review,
        "decisions": dict(Counter(label.decision for label in labels)),
        "risks": dict(Counter(label.risk_id for label in labels)),
        "classes": dict(Counter(label.class_ for label in labels)),
        "model_calls": model_calls,
        **_batch_evidence(evidence, set(originals)),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "elapsed_seconds": elapsed,
        "active_model_seconds": model_seconds,
        "capacity_wait_seconds": capacity_wait_seconds,
        "quality_gate": "pending_grounded_adjudication" if labels else "no_successful_outcomes",
        "changes": changes,
    }


def run_evaluation(
    cohort_path: Path,
    annotation_path: Path,
    out: Path,
    split: EvaluationSplit,
    batch_size: int | None,
) -> dict[str, JsonValue]:
    cohort_bytes = cohort_path.read_bytes()
    cohort = EvaluationCohort.model_validate_json(cohort_bytes)
    config = AnnotationConfig.model_validate_json(annotation_path.read_bytes())
    batch = config.batch
    if batch is None:
        raise ValueError("Evaluation requires an enhanced batch-labeling configuration")
    if batch_size is not None:
        batch = batch.model_copy(update={"batch_size": batch_size})
        config = config.model_copy(update={"batch": batch})
    spec = read_registered_spec((cohort_path.parent / "spec.json").read_bytes())
    rules = LabelingRules.from_rules(spec.rules)
    if any(record.seed_documentation for record in cohort.records):
        raise ValueError("Live-grounding evaluation cannot use seed documentation")
    records = [record for record in cohort.records if split == "all" or record.split == split]
    if not records:
        raise ValueError("Evaluation split contains no cohort rows")
    candidates = [record.candidate() for record in records]
    if len({candidate.candidate_id for candidate in candidates}) != len(candidates):
        raise ValueError("Evaluation cohort contains duplicate identities")
    out.mkdir(parents=True, exist_ok=False)
    _write(
        out / "run-input.json",
        {
            "cohort_sha256": hashlib.sha256(cohort_bytes).hexdigest(),
            "split": split,
            "annotation": config.model_dump(mode="json"),
            "rules": rules.model_dump(mode="json"),
        },
    )
    started = time.monotonic()
    attempts = annotate_batches(
        candidates,
        rules,
        config.model,
        batch,
        out / "inference",
        maximum_context_characters=config.max_request_bytes,
    )
    elapsed = time.monotonic() - started
    _write(out / "attempts.json", [attempt.model_dump(mode="json") for attempt in attempts])
    try:
        validate_attempts(candidates, attempts, rules)
    except ValueError as error:
        expected = {candidate.candidate_id for candidate in candidates}
        actual = Counter(attempt.candidate_id for attempt in attempts)
        missing: list[JsonValue] = list(sorted(expected - actual.keys()))
        unexpected: list[JsonValue] = list(sorted(actual.keys() - expected))
        duplicates: dict[str, JsonValue] = {
            identity: count for identity, count in sorted(actual.items()) if count > 1
        }
        _write(
            out / "summary.json",
            {
                "status": "failed",
                "rows": len(records),
                "attempt_rows": len(attempts),
                "failure_reasons": {"invalid_outcomes": 1},
                "outcome_validation": {
                    "detail": str(error),
                    "missing_candidate_ids": missing,
                    "unexpected_candidate_ids": unexpected,
                    "duplicate_candidate_ids": duplicates,
                },
                "elapsed_seconds": elapsed,
                "quality_gate": "not_evaluable",
            },
        )
        raise
    summary = evaluation_summary(records, attempts, out / "inference", elapsed)
    _write(out / "summary.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate grounded labeling on an immutable <=100-row cohort."
    )
    parser.add_argument("--cohort", type=Path, required=True)
    parser.add_argument("--annotation-config", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--split", choices=("development", "heldout", "all"), default="development")
    parser.add_argument("--batch-size", type=int, choices=range(16, 51))
    parser.add_argument("--credentials-stdin", action="store_true")
    args = parser.parse_args()
    if args.credentials_stdin:
        _credentials()
    match args.split:
        case "development" | "heldout" | "all" as split:
            summary = run_evaluation(args.cohort, args.annotation_config, args.out, split, args.batch_size)
        case _:
            parser.error("Invalid evaluation split")
    print(
        json.dumps({key: value for key, value in summary.items() if key != "changes"}, indent=2), flush=True
    )


if __name__ == "__main__":
    main()
