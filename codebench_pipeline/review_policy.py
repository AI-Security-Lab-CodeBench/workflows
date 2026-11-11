from codebench_pipeline.dataset_records import LabelDecision, ReviewDecision


def requires_human_review(label: LabelDecision, review: ReviewDecision) -> bool:
    return any(
        confidence != "high"
        for confidence in (
            label.category_confidence,
            label.risk_confidence,
            label.severity_confidence,
            label.class_confidence,
            review.confidence,
        )
    )
