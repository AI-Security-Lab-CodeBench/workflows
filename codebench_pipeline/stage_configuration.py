import json

from dagster import Config
from pydantic import JsonValue

from codebench_pipeline.category_rules import CategoryRulesConfig
from codebench_pipeline.collection_config import CollectionMergeConfig, DeclaredSourceStageConfig
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, Branch, collection_branches, stage_name
from codebench_pipeline.dataset_config import (
    DatasetBuildSpec,
    catalog_for_branch,
    snapshots_for_branch,
)
from codebench_pipeline.keenable_policy import KeenableDeclaration
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.storage import IntegrityError, fingerprint
from codebench_pipeline.typed_config import TypedConfig


def finalization_stage_configs(spec: DatasetBuildSpec) -> dict[str, Config]:
    merge: TypedConfig = TypedConfig()
    if spec.declared_collection is not None:
        merge = CollectionMergeConfig(keenable_source_weight=spec.declared_collection.keenable_source_weight)
    return {
        "merged_command_candidates": merge,
        "filtered_commands": spec.filtering,
        **({"labeling_documentation": spec.documentation} if spec.documentation is not None else {}),
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
    if spec.declared_collection is not None:
        config = declared_source_config(spec, branch)
        return {stage_name(name, branch): config for name in BRANCH_BY_NAMESPACE[branch].declared_stages}
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


def collection_asset_settings(spec: DatasetBuildSpec, branch: Branch) -> dict[str, JsonValue]:
    """Serialize one scoped declaration for the branch's same-run config mapping."""
    return {
        "collection": declared_source_config(spec, branch).model_dump(
            mode="json", exclude={"source": {"source_list"}}, exclude_none=True
        )
    }


def stage_configs(spec: DatasetBuildSpec) -> dict[str, Config]:
    return {
        "category_rules": CategoryRulesConfig.model_validate_json(spec.rules.model_dump_json()),
        **{
            stage: config
            for branch in collection_branches(declared=spec.has_declared_collection)
            for stage, config in collection_stage_configs(spec, branch.namespace).items()
        },
        **finalization_stage_configs(spec),
    }


def declared_source_config(spec: DatasetBuildSpec, branch: Branch) -> DeclaredSourceStageConfig:
    if spec.declared_collection is None:
        raise IntegrityError("Registered dataset has no collection declarations")
    source = spec.declared_collection.for_branch(branch)
    if branch == "keenable" and source.declaration.get("search_plan") is not None:
        declaration = KeenableDeclaration.model_validate_json(json.dumps(source.declaration))
        if declaration.search_plan is not None and declaration.search_plan.mode == "adaptive":
            model, batch = spec.annotation.model, spec.annotation.batch
            if model.provider != "openai_compatible" or model.base_url is None or batch is None:
                raise IntegrityError(
                    "Adaptive discovery requires a registered remote model and capacity settings"
                )
            return DeclaredSourceStageConfig(
                branch=branch,
                source=source,
                planner_binding=fingerprint(
                    [model.base_url, model.model, batch.capacity.model_dump_json(), batch.backend_revision]
                ),
            )
    return DeclaredSourceStageConfig(branch=branch, source=source)
