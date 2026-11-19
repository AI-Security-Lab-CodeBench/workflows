from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, FINALIZATION_STAGES, Branch, stage_name

INPUT_STAGES: dict[str, dict[str, str]] = {
    "category_rules": {},
    "source_catalog": {"category_rules": "category_rules"},
    "source_documents": {"source_catalog": "source_catalog"},
    "document_adapters": {"source_documents": "source_documents"},
    "command_candidates": {"source_documents": "source_documents", "adapters": "document_adapters"},
}

INPUT_STAGES.update(
    {
        stage_name(stage, "documentation"): {
            name: stage_name(parent, "documentation") for name, parent in dependencies.items()
        }
        for stage, dependencies in tuple(INPUT_STAGES.items())
        if stage != "category_rules"
    }
)

INPUT_STAGES.update(
    {
        "merged_command_candidates": {
            "dataset_candidates": "command_candidates",
            "documentation_candidates": "documentation/command_candidates",
        },
        "filtered_commands": {
            "command_candidates": "merged_command_candidates",
            "category_rules": "category_rules",
        },
        "labeling_documentation": {"commands": "filtered_commands", "category_rules": "category_rules"},
        "labeling_attempts": {
            "commands": "filtered_commands",
            "documentation": "labeling_documentation",
            "category_rules": "category_rules",
        },
        "proposed_annotations": {"attempts": "labeling_attempts", "category_rules": "category_rules"},
        "labeling_failures": {"attempts": "labeling_attempts"},
        "deduplicated_annotations": {"annotations": "proposed_annotations"},
        "category_eligible_annotations": {
            "annotations": "deduplicated_annotations",
            "category_rules": "category_rules",
        },
        "human_review_queue": {
            "annotations": "category_eligible_annotations",
            "category_rules": "category_rules",
        },
        "human_review_decisions": {
            "human_review_queue": "human_review_queue",
        },
        "dataset_release": {
            "category_eligible_annotations": "category_eligible_annotations",
            "human_review_decisions": "human_review_decisions",
            "category_rules": "category_rules",
        },
    }
)
RELEASE_STAGES = ("human_review_decisions", "dataset_release")
PREPARATION_STAGES = tuple(stage for stage in INPUT_STAGES if stage not in RELEASE_STAGES)
COLLECTION_STAGE_NAMES = ("source_catalog", "source_documents", "document_adapters", "command_candidates")
DECLARED_INPUT_STAGES = {
    "category_rules": {},
    **{
        stage_name(stage, branch.namespace): {
            name: stage_name(parent, branch.namespace) for name, parent in INPUT_STAGES[stage].items()
        }
        for branch in COLLECTION_BRANCHES
        for stage in COLLECTION_STAGE_NAMES
    },
    **{
        stage: dependencies
        for stage, dependencies in INPUT_STAGES.items()
        if stage in FINALIZATION_STAGES and stage != "labeling_documentation"
    },
    "merged_command_candidates": {
        f"{branch.namespace}_candidates": branch.terminal for branch in COLLECTION_BRANCHES
    },
    "labeling_attempts": {
        name: stage for name, stage in INPUT_STAGES["labeling_attempts"].items() if name != "documentation"
    },
}
KNOWN_STAGES = frozenset(INPUT_STAGES) | frozenset(DECLARED_INPUT_STAGES)


def preparation_stages(*, declared: bool) -> tuple[str, ...]:
    return tuple(
        stage
        for stage in (DECLARED_INPUT_STAGES if declared else INPUT_STAGES)
        if stage not in RELEASE_STAGES
    )


def collection_selection(branch: Branch) -> tuple[str, ...]:
    return ("category_rules", *(stage_name(stage, branch) for stage in COLLECTION_STAGE_NAMES))
