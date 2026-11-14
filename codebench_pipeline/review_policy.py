from codebench_pipeline.dataset_records import LabelDecision


def requires_human_review(label: LabelDecision) -> bool:
    return label.decision == "needs_review" or any(
        confidence != "high"
        for confidence in (
            label.category_confidence,
            label.risk_confidence,
            label.severity_confidence,
            label.class_confidence,
        )
    )
