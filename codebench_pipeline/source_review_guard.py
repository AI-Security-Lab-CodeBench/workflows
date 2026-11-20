from typing import Final

import pandas as pd
from dagster import DagsterInstance, OutputContext
from pydantic import ValidationError

from codebench_pipeline.dataframe_io import AssetSnapshot, DataFrameIOManager
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, Branch, asset_branch
from codebench_pipeline.documentation_authorization import (
    SourceInventoryAuthorization,
    frame_inventory_authorization,
    inventory_authorization,
    inventory_materialization,
)
from codebench_pipeline.human_review import CompletionToken, HumanReviewError
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig

SOURCE_BINDING: Final = "codebench_source_review"


def frame_authorization(frame: pd.DataFrame, field: str = SOURCE_BINDING) -> CompletionToken:
    try:
        return CompletionToken.model_validate(frame.attrs.get(field))
    except ValidationError as error:
        raise HumanReviewError(
            "stale", "Source-dependent output lacks a valid source-review binding"
        ) from error


def authorize_inventory_input(
    manager: DataFrameIOManager,
    instance: DagsterInstance,
    registered: DeclaredGlobalConfig,
    branch: Branch,
    snapshot: AssetSnapshot,
    *,
    inventory_input: bool,
) -> SourceInventoryAuthorization:
    binding = inventory_materialization(manager, instance, registered, branch)
    if not isinstance(snapshot.value, pd.DataFrame):
        raise HumanReviewError("conflict", "Source input is not a table")
    if inventory_input:
        if snapshot.reference.digest != binding.catalog_digest:
            raise HumanReviewError("stale", "Source catalog differs from the current inventory")
    elif frame_inventory_authorization(snapshot.value, branch) != binding:
        raise HumanReviewError("stale", "Input belongs to an earlier source inventory")
    return binding


def authorize_output(
    context: OutputContext,
    manager: DataFrameIOManager,
    frame: pd.DataFrame,
) -> None:
    branch = asset_branch(context.asset_key.path)
    registered = manager.run_configuration(context.run_id, context.asset_partition_key)
    config = registered.source_config(branch)
    stages = BRANCH_BY_NAMESPACE[branch].declared_stages
    if context.asset_key.path[-1] not in stages:
        raise HumanReviewError("conflict", "Only declared source stages can publish current outputs")
    require_source_rights(config)
    validate_source_receipt(frame, config, manager)
    if context.asset_key.path[-1] == stages[0]:
        return
    _, binding = inventory_authorization(manager, context.step_context.instance, registered, branch)
    if frame_inventory_authorization(frame, branch) != binding:
        raise HumanReviewError("stale", "Source inventory changed during computation")
