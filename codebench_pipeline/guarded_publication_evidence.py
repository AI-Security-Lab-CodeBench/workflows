from dataclasses import dataclass
from typing import Literal

import pandas as pd
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.dataframe_schemas import LabelingAttemptsTable
from codebench_pipeline.human_review import CompletionToken, HumanReviewError, ReviewDecision
from codebench_pipeline.labeling_tables import failures_from_attempts
from codebench_pipeline.review_snapshots import ResolvedReview, ReviewSnapshots


@dataclass(frozen=True, slots=True)
class ReleaseInputs:
    """Read only the evidence named by the completed review's immutable manifest."""

    snapshots: ReviewSnapshots
    resolved: ResolvedReview
    token: CompletionToken
    decisions: tuple[ReviewDecision, ...]

    def frame(
        self,
        stage: Literal[
            "merged_command_candidates",
            "filtered_commands",
            "labeling_attempts",
            "proposed_annotations",
            "deduplicated_annotations",
            "documentation/capture_documentation",
        ],
    ) -> pd.DataFrame:
        return self.snapshots.frame(self.resolved.manifest, stage)

    def attempts(self) -> DataFrame[LabelingAttemptsTable]:
        return LabelingAttemptsTable.validate(self.frame("labeling_attempts"))


def _identity_set(frame: pd.DataFrame) -> set[str]:
    identities = set(frame["candidate_id"])
    if len(identities) != len(frame):
        raise HumanReviewError("stale", "Release reconciliation found duplicate candidate identities")
    return identities


def reconciled_counts(
    inputs: ReleaseInputs, attempts: DataFrame[LabelingAttemptsTable]
) -> dict[str, JsonValue]:
    merged = inputs.frame("merged_command_candidates")
    filtered = inputs.frame("filtered_commands")
    proposed = inputs.frame("proposed_annotations")
    failures = failures_from_attempts(attempts)
    dedup = inputs.frame("deduplicated_annotations")
    eligible = inputs.resolved.reviewed
    duplicates = TypeAdapter(list[dict[str, JsonValue]]).validate_python(dedup.attrs.get("duplicates"))
    category_exclusions = TypeAdapter(list[dict[str, JsonValue]]).validate_python(
        eligible.attrs.get("category_exclusions")
    )
    success_ids, failure_ids = _identity_set(proposed), _identity_set(failures)
    duplicate_ids = {str(row["duplicate_id"]) for row in duplicates}
    excluded_ids = {str(row["candidate_id"]) for row in category_exclusions}
    attempts_ids = _identity_set(attempts)
    dedup_ids, eligible_ids = _identity_set(dedup), _identity_set(eligible)
    if (
        _identity_set(merged) != _identity_set(filtered)
        or _identity_set(filtered.loc[filtered["filter_decision"] == "eligible"]) != attempts_ids
        or attempts_ids != success_ids | failure_ids
        or success_ids & failure_ids
        or _identity_set(attempts.loc[attempts["status"] == "success"]) != success_ids
        or _identity_set(attempts.loc[attempts["status"] == "failure"]) != failure_ids
        or len(duplicate_ids) != len(duplicates)
        or not {str(row["survivor_id"]) for row in duplicates} <= dedup_ids
        or dedup_ids & duplicate_ids
        or success_ids != dedup_ids | duplicate_ids
        or len(excluded_ids) != len(category_exclusions)
        or eligible_ids & excluded_ids
        or dedup_ids != eligible_ids | excluded_ids
        or {row.candidate_id for row in inputs.decisions} != set(inputs.resolved.snapshot.candidate_ids)
    ):
        raise HumanReviewError(
            "stale", "Collection source, filter, labeling, deduplication or review counts do not reconcile"
        )
    deferred = int((failures["failure_code"] == "quota_deferred").sum())
    return {
        "candidates": len(merged),
        "eligible_to_label": len(attempts),
        "filter_decisions": {str(k): int(v) for k, v in filtered["filter_decision"].value_counts().items()},
        "supported": len(proposed),
        "unsupported": len(failures) - deferred,
        "quota_deferred": deferred,
        "exact_duplicates": len(duplicates),
        "category_excluded": len(category_exclusions),
        "category_eligible": len(eligible),
        "reviewed": len(inputs.decisions),
    }
