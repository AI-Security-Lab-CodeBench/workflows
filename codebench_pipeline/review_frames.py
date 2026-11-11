from typing import Final

import pandas as pd
from pandera.typing import DataFrame
from pydantic import JsonValue, ValidationError

from codebench_pipeline.dataframe_schemas import HumanReviewDecisionsTable, validated_frame
from codebench_pipeline.human_review import CompletionToken, HumanReviewError, ReviewDecision

REVIEW_BINDING: Final = "codebench_human_review"


def decision_frame(
    token: CompletionToken, decisions: tuple[ReviewDecision, ...]
) -> DataFrame[HumanReviewDecisionsTable]:
    rows: list[dict[str, JsonValue]] = [
        {
            "candidate_id": row.candidate_id,
            "batch_id": token.batch_id,
            "snapshot_id": token.snapshot_id,
            "generation": token.generation,
            "batch_revision": token.batch_revision,
            "row_revision": row.row_revision,
            "human_decision": row.decision,
            "reviewer": row.reviewer,
            "notes": row.notes,
            "decided_at": row.updated_at,
        }
        for row in decisions
    ]
    frame = validated_frame(rows, HumanReviewDecisionsTable)
    frame.attrs[REVIEW_BINDING] = token.model_dump(mode="json")
    return frame


def require_decision_frame(frame: pd.DataFrame, expected: pd.DataFrame) -> None:
    try:
        actual_token = CompletionToken.model_validate(frame.attrs.get(REVIEW_BINDING))
        expected_token = CompletionToken.model_validate(expected.attrs[REVIEW_BINDING])
    except ValidationError as error:
        raise HumanReviewError("stale", "Decision table has no valid completion binding") from error
    if actual_token != expected_token or not frame.equals(expected):
        raise HumanReviewError("stale", "Decision table differs from the completed review")
