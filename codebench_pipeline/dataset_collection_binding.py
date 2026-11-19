from pathlib import Path
from typing import Final

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, DagsterEventType, DagsterInstance
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable
from codebench_pipeline.dataset_branches import FINALIZATION_STAGES, Branch, collection_branches, stage_branch
from codebench_pipeline.dataset_plan import BranchUnavailable, plan_dataset, unavailable_candidates
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.stage_configuration import stage_configs

FINALIZATION_BINDING: Final = "codebench_finalization"


def pending_branch_state(
    instance: DagsterInstance, dataset_id: str, pending: tuple[str, ...]
) -> BranchUnavailable:
    stale = False
    for stage in pending:
        key = AssetKey(["datasets", *stage.split("/")])
        selected = instance.get_latest_planned_materialization_info(key, dataset_id)
        prior = instance.fetch_materializations(
            AssetRecordsFilter(asset_key=key, asset_partitions=[dataset_id]), limit=1
        ).records
        if selected is not None and (not prior or selected.storage_id > prior[0].storage_id):
            if any(
                event.step_key == key.to_python_identifier()
                for event in instance.all_logs(selected.run_id, of_type=DagsterEventType.STEP_FAILURE)
            ):
                return "failed"
        stale |= bool(prior)
    return "stale" if stale else "waiting"


def available_candidates(
    home: Path,
    data: DatasetDataService,
    dataset_id: str,
) -> tuple[dict[Branch, DataFrame[CommandCandidatesTable]], dict[str, JsonValue]]:
    spec = data.specification(dataset_id)
    plan = plan_dataset(
        spec, stage_configs(spec), data.instance, home=home, through="merged_command_candidates"
    )
    pending = tuple(stage for stage in (*plan.selected, *plan.blocked) if stage not in FINALIZATION_STAGES)
    binding: dict[str, JsonValue] = {}
    frames: dict[Branch, DataFrame[CommandCandidatesTable]] = {}
    for descriptor in collection_branches(declared=spec.has_declared_collection):
        branch = descriptor.namespace
        branch_pending = tuple(stage for stage in pending if stage_branch(stage) == branch)
        if branch in plan.unconfigured:
            binding[branch] = {"state": "unconfigured"}
            frames[branch] = CommandCandidatesTable.validate(
                unavailable_candidates(spec, branch, "unconfigured")
            )
        elif branch_pending or "category_rules" in pending:
            state = (
                pending_branch_state(data.instance, dataset_id, branch_pending)
                if spec.has_declared_collection
                else "waiting"
            )
            binding[branch] = {"state": state}
            frames[branch] = CommandCandidatesTable.validate(unavailable_candidates(spec, branch, state))
        else:
            reference = plan.references[descriptor.terminal]
            stored = DataFrameIOManager(storage=spec.storage).read_version(reference)
            if not isinstance(stored, pd.DataFrame):
                raise HumanReviewError("conflict", "Command candidates must be a table")
            frames[branch] = CommandCandidatesTable.validate(stored)
            binding[branch] = {"state": "configured", "digest": reference.digest}
    return frames, binding


def finalization_binding(home: Path, data: DatasetDataService, dataset_id: str) -> dict[str, JsonValue]:
    return available_candidates(home, data, dataset_id)[1]


def authorize_finalization_output(
    home: Path,
    instance: DagsterInstance,
    dataset_id: str,
    frame: pd.DataFrame,
    *,
    complete: bool = False,
) -> None:
    expected = finalization_binding(home, DatasetDataService(home, instance), dataset_id)
    if frame.attrs.get(FINALIZATION_BINDING) != expected:
        raise HumanReviewError("stale", "Collection inputs changed during finalization")
    states = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(expected)
    if complete and any(value["state"] not in {"configured", "unconfigured"} for value in states.values()):
        raise HumanReviewError("pending", "Final human review waits for every configured branch")
