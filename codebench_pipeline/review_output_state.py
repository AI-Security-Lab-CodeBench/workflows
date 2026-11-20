from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd
from dagster import InputContext

from codebench_pipeline.artifact_currentness import ArtifactCurrentness
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION, AssetSnapshot, DataFrameIOManager
from codebench_pipeline.dataset_branches import DECLARED_SOURCE_STAGES, asset_branch
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_frames import REVIEW_BINDING
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.source_review_guard import frame_authorization
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig


def read_current_snapshot(home: Path, manager: DataFrameIOManager, context: InputContext) -> AssetSnapshot:
    state: Literal["legacy_unreviewed", "stale"] | None = None
    if context.asset_key.path[-1] == "dataset_release":
        from codebench_pipeline.publication_inspection import read_inspected_publication

        published = read_inspected_publication(
            home, context.asset_partition_key, context.instance, asset_branch(context.asset_key.path)
        )
        if published is not None:
            return published
        if not DatasetRegistry(home).location(context.asset_partition_key).has_declared_collection:
            reference = manager.current_reference(context)
            return AssetSnapshot(reference, manager.read_version(reference), "legacy_unreviewed")
    reference = manager.current_reference(context)
    value = manager.read_version(reference)
    inspection = ArtifactCurrentness(home, context.instance)
    record = inspection.latest(context.asset_key, context.asset_partition_key)
    if record is None or inspection.stale(record, reference, value):
        state = "stale"
    return AssetSnapshot(reference, value, state)


def registered_materialization_stale(
    manager: DataFrameIOManager,
    context: InputContext,
    value: pd.DataFrame | CategoryRules,
    registered: DeclaredGlobalConfig,
) -> bool:
    if not isinstance(value, pd.DataFrame):
        return False
    if manager.review_home is None:
        raise HumanReviewError("pending", "Currentness checks require a registered dataset")
    home = Path(manager.review_home)
    key = context.asset_key.path
    if key[0] != "datasets":
        return False
    if key[-1] in DECLARED_SOURCE_STAGES:
        config = registered.source_config(asset_branch(key))
        require_source_rights(config)
        validate_source_receipt(value, config, manager)
        return False
    if key == ["datasets", "human_review_queue"]:
        with manager.review_store.reader() as tx:
            batch = tx.get_current_batch(registered.dataset_id, "annotations", compact=True)
        return value.attrs.get(REVIEW_PREPARATION) != {
            "batch_id": batch.batch_id,
            "snapshot_id": batch.snapshot.snapshot_id,
        }
    return review_output_stale(home, context, value, manager.review_store, manager=manager)


def review_output_stale(
    home: Path,
    context: InputContext,
    value: pd.DataFrame,
    review_store: HumanReviewStore,
    *,
    manager: DataFrameIOManager | None = None,
) -> bool:
    stage = context.asset_key.path[-1]
    try:
        if stage == "automatic_dataset_release":
            return True
        if stage == "human_review_decisions":
            from codebench_pipeline.data_views import DatasetDataService
            from codebench_pipeline.publication_authorization import authorize_publication
            from codebench_pipeline.review_snapshots import ReviewSnapshots

            actual = frame_authorization(value, REVIEW_BINDING)
            with review_store.reader() as tx:
                data = DatasetDataService(home, context.instance)
                branch = asset_branch(context.asset_key.path)
                snapshots = (
                    ReviewSnapshots.for_manager(home, data, manager, branch)
                    if manager is not None
                    else ReviewSnapshots.from_environment(home, data, branch)
                )
                resolved = snapshots.current_with_store(context.asset_partition_key, review_store)
                authorize_publication(resolved, actual, tx)
    except (HumanReviewError, DatasetViewError):
        return True
    return False
