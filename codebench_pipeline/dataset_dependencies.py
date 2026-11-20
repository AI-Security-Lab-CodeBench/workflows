from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    FINALIZATION_STAGES,
    stage_name,
)

CACHE_INPUTS: dict[str, dict[str, str]] = {
    **{
        stage_name(stage, branch.namespace): dependencies
        for branch in COLLECTION_BRANCHES
        for stage, dependencies in (
            (
                (
                    (branch.index, {"category_rules": "category_rules"}),
                    (branch.capture, {"source_catalog": stage_name(branch.index, branch.namespace)}),
                )
                if branch.index is not None
                else ((branch.capture, {"category_rules": "category_rules"}),)
            )
            + (("command_candidates", {"source_documents": stage_name(branch.capture, branch.namespace)}),)
        )
    },
    "merged_command_candidates": {
        f"{branch.namespace}_candidates": branch.terminal for branch in COLLECTION_BRANCHES
    },
    "filtered_commands": {
        "command_candidates": "merged_command_candidates",
        "category_rules": "category_rules",
    },
    "labeling_attempts": {
        "commands": "filtered_commands",
        "category_rules": "category_rules",
        "documentation_capture": "documentation/capture_documentation",
    },
    "proposed_annotations": {"attempts": "labeling_attempts", "category_rules": "category_rules"},
    "labeling_failures": {"attempts": "labeling_attempts"},
    "deduplicated_annotations": {"annotations": "proposed_annotations"},
    "category_eligible_annotations": {"annotations": "deduplicated_annotations"},
    "human_review_queue": {
        "annotations": "category_eligible_annotations",
        "category_rules": "category_rules",
    },
}
RELEASE_STAGES = ("human_review_decisions", "dataset_release")


def review_evidence_stages() -> frozenset[str]:
    stages: set[str] = set()
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
