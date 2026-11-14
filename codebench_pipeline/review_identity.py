import hashlib
import json

from pydantic import JsonValue

from codebench_pipeline.human_review import HumanReviewError


def review_identity(row: dict[str, JsonValue], columns: tuple[str, ...]) -> str:
    if not columns or len(set(columns)) != len(columns):
        raise HumanReviewError("ineligible", "Review columns must be nonempty and distinct")
    if any(column not in row for column in columns):
        raise HumanReviewError("ineligible", "A review column is absent from the input row")
    try:
        payload = json.dumps(
            {column: row[column] for column in sorted(columns)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise HumanReviewError("ineligible", "Review values must be finite JSON values") from error
    return hashlib.sha256(payload).hexdigest()
