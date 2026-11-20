from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.dataframe_schemas import ProposedAnnotationsTable, frame_records, validated_frame


def category_eligible_annotations(
    annotations: DataFrame[ProposedAnnotationsTable], category_id: str
) -> DataFrame[ProposedAnnotationsTable]:
    retained: list[dict[str, JsonValue]] = []
    exclusions: list[JsonValue] = []
    for row in frame_records(annotations):
        reasons: list[str] = []
        if row["category_id"] != category_id:
            reasons.append("different_category")
        if row["decision"] == "reject":
            reasons.append("label_rejected")
        if row["category_confidence"] not in ("medium", "high"):
            reasons.append("low_category_confidence")
        if reasons:
            exclusions.append({"candidate_id": row["candidate_id"], "reasons": [*reasons]})
        else:
            retained.append(row)
    result = validated_frame(retained, ProposedAnnotationsTable)
    result.attrs = {**annotations.attrs, "category_exclusions": exclusions}
    return result
