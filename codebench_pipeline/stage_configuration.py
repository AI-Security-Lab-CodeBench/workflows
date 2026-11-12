from dagster import Config

from codebench_pipeline.category_rules import CategoryRulesConfig
from codebench_pipeline.dataset_branches import Branch, stage_name
from codebench_pipeline.dataset_config import DatasetBuildSpec, catalog_for_branch, snapshots_for_branch
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.typed_config import TypedConfig


def stage_configs(spec: DatasetBuildSpec) -> dict[str, Config]:
    configurations: dict[str, Config] = {
        "category_rules": CategoryRulesConfig.model_validate_json(spec.rules.model_dump_json()),
        "merged_command_candidates": TypedConfig(),
        "filtered_commands": spec.filtering,
        "labeling_documentation": spec.documentation,
        "proposed_annotations": spec.annotation,
        "deduplicated_annotations": TypedConfig(),
        "human_review_queue": spec.publication,
        "human_review_decisions": HumanReviewConfig(),
        "dataset_release": HumanReviewConfig(),
    }
    branches: tuple[Branch, ...] = ("dataset", "documentation")
    for branch in branches:
        configurations.update(
            {
                stage_name(name, branch): config
                for name, config in {
                    "source_catalog": catalog_for_branch(spec, branch),
                    "source_documents": snapshots_for_branch(spec, branch),
                    "document_adapters": spec.adapters,
                    "command_candidates": spec.extraction,
                }.items()
            }
        )
    return configurations
