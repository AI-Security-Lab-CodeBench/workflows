from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    FINALIZATION_STAGES,
    stage_name,
)

CACHE_INPUTS: dict[str, dict[str, str]] = {
    **{
        stage_name(stage, branch.namespace): (
            {"source_catalog": stage_name(branch.index, branch.namespace)}
            if stage == branch.capture and branch.index is not None
            else {"source_documents": stage_name(branch.capture, branch.namespace)}
            if stage == "command_candidates" and branch.capture is not None
            else {}
        )
        for branch in COLLECTION_BRANCHES
        for stage in branch.declared_stages
    },
    "merged_command_candidates": {
        f"{branch.namespace}_candidates": branch.terminal for branch in COLLECTION_BRANCHES
    },
    "filtered_commands": {
        "command_candidates": "merged_command_candidates",
    },
    "labeling_attempts": {
        "commands": "filtered_commands",
    },
    "proposed_annotations": {"attempts": "labeling_attempts"},
    "labeling_failures": {"attempts": "labeling_attempts"},
    "deduplicated_annotations": {"annotations": "proposed_annotations"},
    "category_eligible_annotations": {"annotations": "deduplicated_annotations"},
    "human_review_queue": {
        "annotations": "category_eligible_annotations",
    },
}
RELEASE_STAGES = ("human_review_decisions", "dataset_release")


def review_evidence_stages() -> frozenset[str]:
    stages = {"category_rules"}
    pending = ["category_eligible_annotations"]
    while pending:
        stage = pending.pop()
        if stage not in stages:
            stages.add(stage)
            pending.extend(CACHE_INPUTS.get(stage, {}).values())
    return frozenset(stages)


def preparation_stages() -> tuple[str, ...]:
    """Return the established preparation stage inventory in reporting order."""
    return (
        "category_rules",
        *(
            stage_name(stage, branch.namespace)
            for branch in COLLECTION_BRANCHES
            for stage in branch.declared_stages
        ),
        *(stage for stage in FINALIZATION_STAGES if stage not in RELEASE_STAGES),
    )


DECLARED_INPUT_STAGES = (*preparation_stages(), *RELEASE_STAGES)
KNOWN_STAGES = (
    frozenset(DECLARED_INPUT_STAGES)
    | {"labeling_documentation", "human_source_decisions"}
    | {
        stage_name(stage, branch.namespace)
        for branch in COLLECTION_BRANCHES
        for stage in ("source_catalog", "source_documents", "document_adapters", "command_candidates")
    }
)
