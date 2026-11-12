from collections.abc import Mapping
from pathlib import Path
from typing import Final

import pandas as pd
from dagster import AssetExecutionContext, OutputContext
from pydantic import ValidationError

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_branches import asset_branch
from codebench_pipeline.documentation_authorization import (
    SourceInventoryAuthorization,
    frame_inventory_authorization,
    inventory_authorization,
)
from codebench_pipeline.human_review import CompletionToken, HumanReviewError

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
            if name == "source_catalog":
                if not value.equals(catalog):
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
    _, binding = inventory_authorization(
        Path(manager.review_home), context.step_context.instance, context.asset_partition_key, branch
    )
    if frame_inventory_authorization(frame, branch) != binding:
        raise HumanReviewError("stale", "Source inventory changed during computation")
