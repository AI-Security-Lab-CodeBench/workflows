import pandas as pd
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.dataframe_schemas import (
    ProposedAnnotationsTable,
    model_records,
)
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, validate_label
from codebench_pipeline.human_review import HumanReviewError, ReviewDecision
from codebench_pipeline.labeling_stage_settings import ProposalRules


def annotation_review_validation(rules: ProposalRules) -> dict[str, JsonValue]:
    """Compile annotation policy into the generic persisted JSON Schema contract."""
    candidate = CandidateRecord.model_json_schema()
    label = LabelDecision.model_json_schema(by_alias=True)
    properties = {**candidate["properties"], **label["properties"]}
    for field in ("command", "category_id", "risk_id", "severity", "class"):
        properties[field] = {**properties[field], "minLength": 1}
    properties["category_id"] = {"const": rules.category_id}
    properties["class"] = {"enum": list(rules.classes)}
    properties["severity"] = {"enum": list(rules.severities)}
    properties["decision"] = {"enum": ["accept", "needs_review"]}
    properties["documentation_urls"] = {
        **properties["documentation_urls"],
        "minItems": 1,
        "uniqueItems": True,
    }
    properties["risk_candidates"] = {
        "type": "array",
        "items": {"enum": [risk.id for risk in rules.risks]},
        "uniqueItems": True,
    }
    routes: list[dict[str, JsonValue]] = [
        {
            "properties": {
                "risk_id": {"const": risk.id},
                "risk_candidates": {"contains": {"const": risk.id}},
                "command": {"anyOf": [{"pattern": pattern} for pattern in risk.command_patterns]},
            },
        }
        for risk in rules.risks
    ]
    return TypeAdapter(dict[str, JsonValue]).validate_python(
        {
            "taxonomy": rules.model_dump(mode="json"),
            "original_row_schema": {
                "type": "object",
                "properties": {
                    "category_id": {"const": rules.category_id},
                    "decision": {"enum": ["accept", "needs_review"]},
                    "documentation_urls": {"type": "array", "minItems": 1},
                },
                "required": ["category_id", "decision", "documentation_urls"],
            },
            "row_schema": {
                "type": "object",
                "properties": properties,
                "required": sorted(set(candidate["required"]) | set(label["required"])),
                "anyOf": routes,
                "$defs": {**candidate.get("$defs", {}), **label.get("$defs", {})},
            },
        }
    )


def validate_review_edits(
    row: dict[str, JsonValue], edits: dict[str, JsonValue], rules: ProposalRules
) -> None:
    original = pd.DataFrame([row])
    label = model_records(original, LabelDecision)[0]
    if label.decision == "reject" or not label.documentation_urls or label.category_id != rules.category_id:
        raise HumanReviewError("ineligible", "Edits cannot override machine eligibility")
    try:
        changed = pd.DataFrame([{**row, **edits}])
        candidate = model_records(changed, CandidateRecord)[0]
        corrected = model_records(changed, LabelDecision)[0]
        validate_label(candidate, corrected, rules)
    except ValueError as error:
        raise HumanReviewError("ineligible", f"Invalid reviewed values: {error}") from error


def apply_label_corrections(
    annotations: pd.DataFrame,
    decisions: tuple[ReviewDecision, ...],
    rules: ProposalRules,
) -> DataFrame[ProposedAnnotationsTable]:
    """Overlay approved corrections on a copy without changing machine eligibility or evidence."""
    from codebench_pipeline.dataframe_schemas import frame_records

    originals = {str(row["candidate_id"]): row for row in frame_records(annotations)}
    for decision in decisions:
        edits = decision.edits or (
            decision.label_corrections.model_dump(mode="json", by_alias=True)
            if decision.label_corrections is not None
            else {}
        )
        if not edits:
            continue
        if decision.decision != "approve" or decision.candidate_id not in originals:
            raise HumanReviewError("ineligible", "Corrections require an approved annotation identity")
        validate_review_edits(originals[decision.candidate_id], edits, rules)
        originals[decision.candidate_id] = {**originals[decision.candidate_id], **edits}
    corrected = ProposedAnnotationsTable.validate(
        pd.DataFrame(list(originals.values()), columns=annotations.columns)
    )
    corrected.attrs = dict(annotations.attrs)
    return corrected
