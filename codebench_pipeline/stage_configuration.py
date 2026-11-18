from dagster import Config

from codebench_pipeline.category_rules import CategoryRulesConfig
from codebench_pipeline.dataset_branches import Branch, stage_name
from codebench_pipeline.dataset_config import (
    DatasetBuildSpec,
    catalog_for_branch,
    snapshots_for_branch,
)
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.typed_config import TypedConfig


def finalization_stage_configs(spec: DatasetBuildSpec) -> dict[str, Config]:
    return {
        "merged_command_candidates": TypedConfig(),
        "filtered_commands": spec.filtering,
        "labeling_documentation": spec.documentation,
        "labeling_attempts": spec.annotation,
        "proposed_annotations": TypedConfig(),
        "labeling_failures": TypedConfig(),
        "deduplicated_annotations": TypedConfig(),
        "category_eligible_annotations": TypedConfig(),
        "human_review_queue": spec.publication,
        "human_review_decisions": HumanReviewConfig(),
        "dataset_release": HumanReviewConfig(),
    }


def collection_stage_configs(spec: DatasetBuildSpec, branch: Branch) -> dict[str, Config]:
    adapters, extraction = {
        "dataset": (spec.adapters, spec.extraction),
        "documentation": (
            spec.documentation_adapters or spec.adapters,
            spec.documentation_extraction or spec.extraction,
        ),
    }[branch]
    return {
        stage_name(name, branch): config
        for name, config in {
            "source_catalog": catalog_for_branch(spec, branch),
            "source_documents": snapshots_for_branch(spec, branch),
            "document_adapters": adapters,
            "command_candidates": extraction,
        }.items()
    }


def stage_configs(spec: DatasetBuildSpec) -> dict[str, Config]:
    return {
        "category_rules": CategoryRulesConfig.model_validate_json(spec.rules.model_dump_json()),
        **collection_stage_configs(spec, "dataset"),
        **collection_stage_configs(spec, "documentation"),
        **finalization_stage_configs(spec),
    }
