from codebench_pipeline.dataset_records import OTHER_RISK_ID, LabelDecision


def requires_human_review(label: LabelDecision) -> bool:
    if label.risk_id == OTHER_RISK_ID:
        return False
    return label.decision == "needs_review" or any(
        confidence != "high"
        for confidence in (
            label.category_confidence,
            label.risk_confidence,
            label.severity_confidence,
            label.class_confidence,
        )
    )
