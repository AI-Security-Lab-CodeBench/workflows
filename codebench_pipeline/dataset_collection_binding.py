from typing import Final, Literal

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, DagsterEventType, DagsterInstance, TextMetadataValue
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.collection_stage_settings import collection_operation
from codebench_pipeline.dataframe_io import (
    AssetReference,
    AssetSnapshot,
    DataFrameIOManager,
    materialized_reference,
)
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    Branch,
    CollectionBranch,
    asset_path,
    stage_name,
)
from codebench_pipeline.documentation_authorization import (
    frame_inventory_authorization,
    inventory_materialization,
)
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.input_provenance import current_materialization
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, collection_stage_configs
from codebench_pipeline.storage import fingerprint

FINALIZATION_BINDING: Final = "codebench_finalization"
BranchUnavailable = Literal["waiting", "failed", "stale"]


def unavailable_candidates(
    registered: DeclaredGlobalConfig, branch: Branch, state: BranchUnavailable
) -> pd.DataFrame:
    empty = validated_frame([], CommandCandidatesTable)
    empty.attrs["branch_state"] = state
    empty.attrs["branch_configuration"] = fingerprint(
        {
            "stages": {
                stage: config.model_dump_json()
                for stage, config in collection_stage_configs(registered, branch).items()
            },
        }
    )
    return empty


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


def current_candidates(
    manager: DataFrameIOManager,
    instance: DagsterInstance,
    registered: DeclaredGlobalConfig,
    descriptor: CollectionBranch,
) -> tuple[AssetReference, DataFrame[CommandCandidatesTable]]:
    """Validate one terminal branch output and its inventory authority."""
    branch = descriptor.namespace
    config = registered.source_config(branch)
    require_source_rights(config)
    dataset_id = registered.dataset_id
    key = AssetKey(["datasets", *descriptor.terminal.split("/")])
    record = current_materialization(instance, key, dataset_id)
    if record is None:
        raise HumanReviewError("pending", f"Materialize {descriptor.terminal} before merging")
    reference = materialized_reference(instance, record)
    if manager.reference_at(key.path, dataset_id) != reference:
        raise HumanReviewError("stale", "Branch candidates differ from their recorded output")
    stored = manager.read_version(reference)
    if not isinstance(stored, pd.DataFrame):
        raise HumanReviewError("conflict", "Command candidates must be a table")
    validate_source_receipt(stored, config, manager)
    inputs: dict[str, AssetSnapshot] = {}
    if descriptor.capture is not None:
        if frame_inventory_authorization(stored, branch) != inventory_materialization(
            manager, instance, registered, branch
        ):
            raise HumanReviewError("stale", "Branch candidates belong to an earlier inventory")
        capture_key = AssetKey(asset_path(descriptor.capture, branch))
        capture_record = current_materialization(instance, capture_key, dataset_id)
        if capture_record is None:
            raise HumanReviewError("pending", f"Materialize {descriptor.capture} before merging")
        capture_ref = materialized_reference(instance, capture_record)
        if manager.reference_at(capture_key.path, dataset_id) != capture_ref:
            raise HumanReviewError("stale", "Source capture differs from its recorded output")
        inputs["source_documents"] = AssetSnapshot(capture_ref, manager.read_version(capture_ref))
    expected = manager.stage_signature(
        "command_candidates",
        collection_operation(branch, "command_candidates").project(config),
        inputs,
    )
    materialization = record.asset_materialization
    assert materialization is not None
    if materialization.metadata.get("input_fingerprint") != TextMetadataValue(expected):
        raise HumanReviewError("stale", "Branch candidate inputs or implementation changed")
    return reference, CommandCandidatesTable.validate(stored)


def available_candidates(
    manager: DataFrameIOManager,
    instance: DagsterInstance,
    registered: DeclaredGlobalConfig,
) -> tuple[dict[Branch, DataFrame[CommandCandidatesTable]], dict[str, JsonValue]]:
    if manager.review_home is None:
        raise HumanReviewError("pending", "Finalization requires a registered dataset")
    binding: dict[str, JsonValue] = {}
    frames: dict[Branch, DataFrame[CommandCandidatesTable]] = {}
    for descriptor in COLLECTION_BRANCHES:
        branch = descriptor.namespace
        try:
            reference, frames[branch] = current_candidates(manager, instance, registered, descriptor)
        except (FileNotFoundError, HumanReviewError) as error:
            if isinstance(error, HumanReviewError) and error.code == "conflict":
                raise
            state = pending_branch_state(
                instance,
                registered.dataset_id,
                tuple(stage_name(stage, branch) for stage in descriptor.declared_stages),
            )
            binding[branch] = {"state": state}
            frames[branch] = CommandCandidatesTable.validate(
                unavailable_candidates(registered, branch, state)
            )
        else:
            binding[branch] = {"state": "configured", "digest": reference.digest}
    return frames, binding


def finalization_binding(
    manager: DataFrameIOManager,
    instance: DagsterInstance,
    registered: DeclaredGlobalConfig,
) -> dict[str, JsonValue]:
    return available_candidates(manager, instance, registered)[1]


def authorize_finalization_output(
    manager: DataFrameIOManager,
    instance: DagsterInstance,
    registered: DeclaredGlobalConfig,
    frame: pd.DataFrame,
    *,
    complete: bool = False,
) -> None:
    expected = finalization_binding(manager, instance, registered)
    if frame.attrs.get(FINALIZATION_BINDING) != expected:
        raise HumanReviewError("stale", "Collection inputs changed during finalization")
    states = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(expected)
    if complete and any(value["state"] != "configured" for value in states.values()):
        raise HumanReviewError("pending", "Final human review waits for every declared branch")
