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
    DocumentationAuthorization,
    documentation_authorization,
    frame_documentation_authorization,
)
from codebench_pipeline.human_review import CompletionToken, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.review_frames import decision_frame, require_decision_frame
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_authorization

SOURCE_BINDING: Final = "codebench_source_review"
SOURCE_GATED_STAGES: Final = frozenset(
    {
        "source_documents",
        "document_adapters",
        "command_candidates",
        "filtered_commands",
        "labeling_documentation",
        "proposed_annotations",
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
) -> CompletionToken | DocumentationAuthorization:
    if manager.review_home is None:
        raise HumanReviewError("pending", "A registered source-review authority is required")
    home = Path(manager.review_home)
    with HumanReviewStore(home).transaction(timeout=10) as tx:
        if asset_branch(context.asset_key.path) == "documentation":
            catalog, binding = documentation_authorization(home, context.instance, context.partition_key)
            for name, value in inputs.items():
                if isinstance(value, pd.DataFrame):
                    if name == "source_catalog":
                        if not value.equals(catalog):
                            raise HumanReviewError(
                                "stale", "Documentation catalog differs from inspected sources"
                            )
                    elif frame_documentation_authorization(value) != binding:
                        raise HumanReviewError("stale", "Input belongs to an earlier documentation inventory")
            return binding
        source = SourceReviewSource(home, context.instance)
        resolved, token = source_authorization(source, context.partition_key, tx)
        for name, value in inputs.items():
            if isinstance(value, pd.DataFrame):
                if name == "human_source_decisions":
                    require_decision_frame(value, decision_frame(token, tx.list_decisions(token.batch_id)))
                elif name == "source_catalog":
                    if not value.equals(resolved.frame):
                        raise HumanReviewError("stale", "Source catalog differs from the reviewed inventory")
                elif frame_authorization(value) != token:
                    raise HumanReviewError("stale", "Input belongs to an earlier source selection")
        return token


def authorize_output(
    context: OutputContext,
    manager: DataFrameIOManager,
    frame: pd.DataFrame,
    transaction: HumanReviewTransaction,
) -> None:
    if manager.review_home is None:
        raise HumanReviewError("pending", "A registered source-review authority is required")
    if asset_branch(context.asset_key.path) == "documentation":
        _, binding = documentation_authorization(
            Path(manager.review_home), context.step_context.instance, context.asset_partition_key
        )
        if frame_documentation_authorization(frame) != binding:
            raise HumanReviewError("stale", "Documentation inventory changed during computation")
        return
    source = SourceReviewSource(Path(manager.review_home), context.step_context.instance)
    _, token = source_authorization(source, context.asset_partition_key, transaction)
    if context.asset_key.path[-1] == "human_source_decisions":
        require_decision_frame(frame, decision_frame(token, transaction.list_decisions(token.batch_id)))
    elif frame_authorization(frame) != token:
        raise HumanReviewError("stale", "Source selection changed while this output was being computed")
