"""Check the flat inventory of durable evidence required for annotation review."""

from dataclasses import dataclass

import pandas as pd
from dagster import AssetKey, DagsterInstance, EventLogRecord, JsonMetadataValue, TextMetadataValue
from dagster._core.definitions.data_version import extract_data_version_from_entry
from pydantic import JsonValue, RootModel

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import (
    REVIEW_PREPARATION,
    AssetSnapshot,
    DataFrameIOManager,
    materialized_reference,
)
from codebench_pipeline.dataset_dependencies import CACHE_INPUTS, review_evidence_stages
from codebench_pipeline.dataset_engine import implementation_signature
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.input_provenance import current_materialization
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, stage_configs
from codebench_pipeline.storage import fingerprint


@dataclass(frozen=True, slots=True)
class ReviewArtifact:
    snapshot: AssetSnapshot
    event: EventLogRecord

    def evidence(self) -> JsonValue:
        materialization = self.event.asset_materialization
        assert materialization is not None
        evidence = materialization.metadata.get("evidence")
        if not isinstance(evidence, JsonMetadataValue):
            raise DatasetViewError("not_ready", "Preparation output has no verified evidence")
        return RootModel[JsonValue].model_validate(evidence.value).root


def checked_review_artifacts(
    manager: DataFrameIOManager,
    instance: DagsterInstance,
    registered: DeclaredGlobalConfig,
    *,
    include_queue: bool,
) -> dict[str, ReviewArtifact]:
    """Validate current successful receipts against their direct inputs and the review policy."""
    artifacts: dict[str, ReviewArtifact] = {}
    partition = registered.dataset_id
    stages = review_evidence_stages()
    if include_queue:
        stages = stages | {"human_review_queue"}
    for stage in sorted(stages):
        key = AssetKey(["datasets", *stage.split("/")])
        record = current_materialization(instance, key, partition)
        if record is None:
            raise DatasetViewError("not_ready", f"Preparation output is missing: {stage}")
        reference = materialized_reference(instance, record)
        try:
            if manager.reference_at(key.path, partition) != reference:
                raise DatasetViewError("not_ready", f"Preparation reference changed: {stage}")
            value = manager.read_version(reference)
        except FileNotFoundError as error:
            raise DatasetViewError("not_ready", f"Preparation bytes are missing: {stage}") from error
        artifacts[stage] = ReviewArtifact(AssetSnapshot(reference, value), record)

    rules = artifacts["category_rules"]
    version = extract_data_version_from_entry(rules.event.event_log_entry)
    if (
        not isinstance(rules.snapshot.value, CategoryRules)
        or rules.snapshot.value != registered.rules
        or version is None
        or version.value != fingerprint(registered.rules.model_dump_json())
    ):
        raise DatasetViewError("not_ready", "Stored rules differ from registered rules")
    implementation = manager.review_store.bound_implementation()
    configurations = stage_configs(registered)
    storage_identity = manager.storage.model_dump_json()
    for stage, artifact in artifacts.items():
        if stage == "category_rules":
            continue
        config = configurations[stage]
        expected = implementation_signature(
            stage.rsplit("/", 1)[-1],
            config,
            {name: artifacts[parent].snapshot for name, parent in CACHE_INPUTS[stage].items()},
            implementation.stages[stage],
            review_policy=implementation.review_policy,
            storage_identity=storage_identity,
        )
        materialization = artifact.event.asset_materialization
        assert materialization is not None
        if materialization.metadata.get("input_fingerprint") != TextMetadataValue(expected):
            raise DatasetViewError("not_ready", f"Preparation inputs or implementation changed: {stage}")
        table = artifact.snapshot.value
        if not isinstance(table, pd.DataFrame):
            raise DatasetViewError("not_ready", f"Preparation output is not a table: {stage}")
        try:
            if isinstance(config, DeclaredSourceStageConfig):
                require_source_rights(config)
                validate_source_receipt(table, config, manager)
            if stage == "human_review_queue":
                with manager.review_store.reader() as tx:
                    batch = tx.get_current_batch(partition, "annotations", compact=True)
                if table.attrs.get(REVIEW_PREPARATION) != {
                    "batch_id": batch.batch_id,
                    "snapshot_id": batch.snapshot.snapshot_id,
                }:
                    raise DatasetViewError("not_ready", "Review queue belongs to a different batch")
        except (FileNotFoundError, HumanReviewError) as error:
            raise DatasetViewError("not_ready", f"Preparation authority changed: {stage}") from error
    return artifacts
