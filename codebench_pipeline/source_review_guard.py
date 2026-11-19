from collections.abc import Mapping
from pathlib import Path
from typing import Final

import pandas as pd
from dagster import AssetExecutionContext, OutputContext
from pydantic import ValidationError

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, asset_branch
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.documentation_authorization import (
    SourceInventoryAuthorization,
    frame_inventory_authorization,
    inventory_authorization,
)
from codebench_pipeline.human_review import CompletionToken, HumanReviewError
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.stage_configuration import declared_source_config

SOURCE_BINDING: Final = "codebench_source_review"
SOURCE_GATED_STAGES: Final = frozenset(
    {
        "source_documents",
        "document_adapters",
        "command_candidates",
    }
)


def frame_authorization(frame: pd.DataFrame, field: str = SOURCE_BINDING) -> CompletionToken:
    try:
        return CompletionToken.model_validate(frame.attrs.get(field))
    except ValidationError as error:
        raise HumanReviewError(
            "stale", "Source-dependent output lacks a valid source-review binding"
        ) from error


def authorize_compute(
    context: AssetExecutionContext,
    manager: DataFrameIOManager,
    inputs: Mapping[str, pd.DataFrame | CategoryRules],
) -> SourceInventoryAuthorization:
    if manager.review_home is None:
        raise HumanReviewError("pending", "A registered source inventory is required")
    home = Path(manager.review_home)
    branch = asset_branch(context.asset_key.path)
    catalog, binding = inventory_authorization(home, context.instance, context.partition_key, branch)
    for name, value in inputs.items():
        if isinstance(value, pd.DataFrame):
            if name == "source_catalog" or "documentation_scope_receipt_json" in catalog.attrs:
                if not value.equals(catalog) or value.attrs != catalog.attrs:
                    raise HumanReviewError("stale", "Source catalog differs from the current inventory")
            elif frame_inventory_authorization(value, branch) != binding:
                raise HumanReviewError("stale", "Input belongs to an earlier source inventory")
    return binding


def authorize_output(
    context: OutputContext,
    manager: DataFrameIOManager,
    frame: pd.DataFrame,
) -> None:
    if manager.review_home is None:
        raise HumanReviewError("pending", "A registered source inventory is required")
    branch = asset_branch(context.asset_key.path)
    home = Path(manager.review_home)
    spec = DatasetRegistry(home).get_snapshot(context.asset_partition_key)
    if spec.has_declared_collection:
        config = declared_source_config(spec, branch)
        require_source_rights(config)
        validate_source_receipt(frame, config, manager)
    root = (
        BRANCH_BY_NAMESPACE[branch].declared_stages[0] if spec.has_declared_collection else "source_catalog"
    )
    if context.asset_key.path[-1] == root:
        return
    _, binding = inventory_authorization(
        home, context.step_context.instance, context.asset_partition_key, branch
    )
    if frame_inventory_authorization(frame, branch) != binding:
        raise HumanReviewError("stale", "Source inventory changed during computation")
