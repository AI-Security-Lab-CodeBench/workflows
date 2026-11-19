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
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.review_policy import requires_human_review


class CohortRecord(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    row: dict[str, JsonValue]
    seed_documentation: list[SemanticSnippet]
    selection_reason: str
    split: Literal["development", "heldout"]
    documentation_gap_candidate: bool
    category_certain_other_uncertain: bool

    def candidate(self) -> CandidateRecord:
        return CandidateRecord.model_validate({name: self.row[name] for name in CandidateRecord.model_fields})

    def baseline(self) -> LabelDecision:
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
    baseline_model: ModelConfig
    snapshots: dict[str, AssetReference]
    records: list[CohortRecord] = Field(min_length=1, max_length=100)


def _write(path: Path, value: JsonValue) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, allow_nan=False, indent=2)


def _credentials() -> None:
    values = TypeAdapter(dict[str, str]).validate_json(sys.stdin.read())
    if set(values) != {"KEENABLE_API_KEY", "CONTEXT7_API_KEY"}:
        raise ValueError("Credential input must contain exactly the two grounding-provider keys")
    if any(not value.strip() or "\n" in value or "\r" in value for value in values.values()):
        raise ValueError("Grounding credential input is invalid")
    os.environ.update(values)


def evaluation_summary(
    records: list[CohortRecord], attempts: list[LabelingAttempt], evidence: Path, elapsed: float
) -> dict[str, JsonValue]:
    originals = {record.candidate().candidate_id: record for record in records}
    changes: list[JsonValue] = []
    outcomes: Counter[str] = Counter()
    failures: Counter[str] = Counter()
    baseline_review = 0
    proposed_review = 0
    labels: list[LabelDecision] = []
    for attempt in attempts:
        original = originals[attempt.candidate_id]
        before = original.baseline()
        baseline_review += requires_human_review(before)
        outcomes[attempt.status] += 1
        if attempt.status == "failure":
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
        after = LabelDecision.model_validate_json(attempt.label_json)
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
        previous = before.model_dump(mode="json", by_alias=True)
        current = after.model_dump(mode="json", by_alias=True)
        delta: dict[str, JsonValue] = {
            field: {"before": previous[field], "after": current[field]}
            for field in fields
            if previous[field] != current[field]
        }
        changes.append(
            {
                "candidate_id": attempt.candidate_id,
                "status": "success",
                "changes": delta,
                "potential_false_allow": before.class_ != "allow" and after.class_ == "allow",
                "requires_grounded_adjudication": bool(delta),
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
    provider_calls = 0
    for path in evidence.glob("batches/*/grounding/receipts/*.json"):
        receipt = TypeAdapter(dict[str, JsonValue]).validate_json(path.read_bytes())
        count = receipt.get("provider_calls", 0)
        if isinstance(count, int):
            provider_calls += count
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
        "rows": len(records),
        "outcomes": dict(outcomes),
        "failure_reasons": dict(failures),
        "baseline_uncertainty_rows_before_category_filter": baseline_review,
        "enhanced_uncertainty_rows_before_category_filter": proposed_review,
        "decisions": dict(Counter(label.decision for label in labels)),
        "risks": dict(Counter(label.risk_id for label in labels)),
        "classes": dict(Counter(label.class_ for label in labels)),
        "model_calls": model_calls,
        "documentation_provider_calls": provider_calls,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "elapsed_seconds": elapsed,
        "active_model_seconds": model_seconds,
        "capacity_wait_seconds": capacity_wait_seconds,
        "quality_gate": "pending_grounded_adjudication",
        "changes": changes,
    }


def run_evaluation(
    cohort_path: Path,
    annotation_path: Path,
    out: Path,
    split: Literal["development", "heldout", "all"],
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
    rules = spec.rules
    records = [record for record in cohort.records if split == "all" or record.split == split]
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
        [],
        rules,
        config.model,
        batch,
        out / "inference",
        cache=out / "cache",
        documentation_by_candidate={
            record.candidate().candidate_id: record.seed_documentation for record in records
        },
        maximum_context_characters=config.maximum_context_characters,
    )
    elapsed = time.monotonic() - started
    _write(out / "attempts.json", [attempt.model_dump(mode="json") for attempt in attempts])
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
    parser.add_argument("--batch-size", type=int, choices=(25, 50))
    parser.add_argument("--credentials-stdin", action="store_true")
    args = parser.parse_args()
    if args.credentials_stdin:
        _credentials()
    split = TypeAdapter(Literal["development", "heldout", "all"]).validate_python(args.split)
    summary = run_evaluation(args.cohort, args.annotation_config, args.out, split, args.batch_size)
    print(
        json.dumps({key: value for key, value in summary.items() if key != "changes"}, indent=2), flush=True
    )


if __name__ == "__main__":
    main()
