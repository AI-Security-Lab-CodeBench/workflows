from typing import Final

from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, Branch
from codebench_pipeline.dataset_dependencies import CACHE_INPUTS
from codebench_pipeline.dataset_engine import implementation_signature
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, stage_configs
from codebench_pipeline.storage import fingerprint

REUSABLE_PROCESSING_STAGES: Final = frozenset(
    {
        "merged_command_candidates",
        "filtered_commands",
        "labeling_attempts",
        "proposed_annotations",
        "labeling_failures",
        "deduplicated_annotations",
        "category_eligible_annotations",
    }
)


def effective_approval(
    registered: DeclaredGlobalConfig, stage: str, source_branch: Branch | None
) -> tuple[str, ...]:
    """Project approval onto the settings that can affect one persisted stage."""
    rules = registered.rules
    if source_branch is not None:
        return (registered.source_config(source_branch).declaration_digest,)
    if stage == "category_rules":
        return (fingerprint(rules.model_dump_json()),)
    if stage not in REUSABLE_PROCESSING_STAGES:
        return (registered.registration_digest,)

    configurations = stage_configs(registered)
    stages: set[str] = set()
    pending = [stage]
    while pending:
        current = pending.pop()
        if current in REUSABLE_PROCESSING_STAGES and current not in stages:
            stages.add(current)
            pending.extend(CACHE_INPUTS[current].values())
    return (
        *(registered.source_config(branch.namespace).declaration_digest for branch in COLLECTION_BRANCHES),
        *(
            implementation_signature(
                scoped_stage,
                configurations[scoped_stage],
                {},
                {},
            )
            for scoped_stage in sorted(stages)
        ),
    )
