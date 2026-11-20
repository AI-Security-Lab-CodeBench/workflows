from typing import Final

from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, Branch
from codebench_pipeline.dataset_catalog import source_scope
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
        return (
            registered.source_config(source_branch).declaration_digest,
            fingerprint(source_scope(rules)),
        )
    if stage == "category_rules":
        return (fingerprint(rules.model_dump_json()),)
    if stage not in REUSABLE_PROCESSING_STAGES:
        return (registered.registration_digest,)

    configurations = stage_configs(registered)
    stages = ["merged_command_candidates"]
    if stage != "merged_command_candidates":
        stages.append("filtered_commands")
    if stage not in stages:
        stages.append("labeling_attempts")
    if stage not in stages:
        stages.append(stage)
    return (
        fingerprint(source_scope(rules)),
        *(registered.source_config(branch.namespace).declaration_digest for branch in COLLECTION_BRANCHES),
        *(
            implementation_signature(
                scoped_stage,
                configurations[scoped_stage],
                {"category_rules": rules}
                if scoped_stage in {"filtered_commands", "labeling_attempts"}
                else {},
                {},
            )
            for scoped_stage in stages
        ),
    )
