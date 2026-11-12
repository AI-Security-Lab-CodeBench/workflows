from pathlib import Path

import pandas as pd
from dagster import InputContext

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataset_branches import asset_branch
from codebench_pipeline.documentation_authorization import (
    frame_inventory_authorization,
    inventory_authorization,
)
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_frames import REVIEW_BINDING
from codebench_pipeline.source_review_guard import SOURCE_GATED_STAGES, frame_authorization


def materialization_stale(home: Path, context: InputContext, value: pd.DataFrame | CategoryRules) -> bool:
    stage = context.asset_key.path[-1]
    if not isinstance(value, pd.DataFrame):
        return False
    try:
        from codebench_pipeline.dataset_branches import FINALIZATION_STAGES

        if stage in FINALIZATION_STAGES[:-2]:
            from codebench_pipeline.dataset_finalization import authorize_finalization_output

            authorize_finalization_output(
                home,
                context.instance,
                context.asset_partition_key,
                value,
                complete=stage != "merged_command_candidates",
            )
            return False
        if stage in SOURCE_GATED_STAGES:
            branch = asset_branch(context.asset_key.path)
            actual = frame_inventory_authorization(value, branch)
            _, current = inventory_authorization(
                home, context.instance, context.asset_partition_key, branch
            )
            return actual != current
        if stage in {"human_review_decisions", "automatic_dataset_release"}:
            from codebench_pipeline.data_views import DatasetDataService
            from codebench_pipeline.publication_authorization import authorize_publication
            from codebench_pipeline.review_snapshots import ReviewSnapshots

            actual = frame_authorization(value, REVIEW_BINDING)
            with HumanReviewStore(home).reader() as tx:
                resolved = ReviewSnapshots(
                    home, DatasetDataService(home, context.instance), asset_branch(context.asset_key.path)
                ).current(context.asset_partition_key)
                authorize_publication(resolved, actual, tx)
    except (HumanReviewError, DatasetViewError):
        return True
    return False
