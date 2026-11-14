from dataclasses import dataclass

import pandas as pd
from pandera.typing import DataFrame
from pydantic import JsonValue, ValidationError

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import (
    ProposedAnnotationsTable,
    model_records,
)
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, validate_label
from codebench_pipeline.human_review import HumanReviewError, LabelCorrections, ReviewDecision


@dataclass(frozen=True, slots=True)
class LabelCorrectionContext:
    candidate: CandidateRecord
    label: LabelDecision
    rules: CategoryRules


def corrected_label(context: LabelCorrectionContext, corrections: LabelCorrections) -> LabelDecision:
    """Validate corrected labels against the captured taxonomy."""
    updates = corrections.model_dump(mode="json", by_alias=True, exclude_none=True)
    if context.label.category_id != context.rules.category_id:
        raise HumanReviewError("ineligible", "Human corrections cannot override target category eligibility")
    if context.label.decision == "reject" or not context.label.documentation_urls:
        raise HumanReviewError("ineligible", "Corrections cannot override rejected or undocumented labels")
    label = LabelDecision.model_validate({**context.label.model_dump(by_alias=True), **updates})
    try:
        validate_label(context.candidate, label, context.rules)
    except ValueError as error:
        raise HumanReviewError("ineligible", f"Invalid label corrections: {error}") from error
    return label


def validate_review_edits(
    row: dict[str, JsonValue], edits: dict[str, JsonValue], rules: CategoryRules
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
    except (ValueError, ValidationError) as error:
        raise HumanReviewError("ineligible", f"Invalid reviewed values: {error}") from error


def apply_label_corrections(
    annotations: pd.DataFrame,
    decisions: tuple[ReviewDecision, ...],
    rules: CategoryRules,
) -> DataFrame[ProposedAnnotationsTable]:
    """Overlay approved corrections on a copy without changing machine eligibility or evidence."""
    from codebench_pipeline.dataframe_schemas import frame_records

    originals = {str(row["candidate_id"]): row for row in frame_records(annotations)}
    for decision in decisions:
        edits = decision.edits or (
            decision.label_corrections.model_dump(mode="json", by_alias=True)
            if decision.label_corrections is not None else {}
        )
        if not edits:
            continue
        if decision.decision != "approve" or decision.candidate_id not in originals:
            raise HumanReviewError("ineligible", "Corrections require an approved annotation identity")
        validate_review_edits(originals[decision.candidate_id], edits, rules)
        originals[decision.candidate_id] = {**originals[decision.candidate_id], **edits}
    corrected = ProposedAnnotationsTable.validate(pd.DataFrame(list(originals.values()), columns=annotations.columns))
    corrected.attrs = dict(annotations.attrs)
    return corrected
