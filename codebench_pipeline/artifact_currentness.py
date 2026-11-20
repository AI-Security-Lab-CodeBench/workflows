from functools import cached_property
from pathlib import Path

import pandas as pd
from dagster import (
    AssetKey,
    AssetRecordsFilter,
    DagsterInstance,
    EventLogRecord,
    JsonMetadataValue,
    TextMetadataValue,
)
from dagster._core.definitions.data_version import (
    CachingStaleStatusResolver,
    DataProvenance,
    StaleCauseCategory,
    StaleStatus,
    extract_data_provenance_from_entry,
    extract_data_version_from_entry,
)
from pydantic import AliasPath, BaseModel, Field, TypeAdapter, ValidationError

from codebench_pipeline.artifact_approval import REUSABLE_PROCESSING_STAGES, effective_approval
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import GLOBAL_CONFIG, ConfigRunIOManager, mapping_key
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION, AssetReference, DataFrameIOManager
from codebench_pipeline.dataset_branches import (
    BRANCH_BY_NAMESPACE,
    DECLARED_SOURCE_STAGES,
    FINALIZATION_STAGES,
    Branch,
    asset_branch,
)
from codebench_pipeline.dataset_dependencies import DECLARED_INPUT_STAGES, RELEASE_STAGES
from codebench_pipeline.dataset_engine import scoped_code_versions
from codebench_pipeline.dataset_provenance import provenance_resolver
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded
from codebench_pipeline.finalization_publication import TABLE_REGISTRATION_METADATA
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.review_frames import REVIEW_BINDING
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.source_review_guard import frame_authorization
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, stage_configs
from codebench_pipeline.stage_implementation import ImplementationSource
from codebench_pipeline.storage import IntegrityError, fingerprint


class MaterializationStorageEvidence(BaseModel):
    revision: Digest | None = Field(
        default=None,
        validation_alias=AliasPath("resources", "dataset_io_manager", "config", "registered_revision"),
    )
    storage: StorageConfig | None = Field(
        default=None, validation_alias=AliasPath("resources", "dataset_io_manager", "config", "storage")
    )


class ArtifactCurrentness:
    """Assess existing receipts for display without authorizing an asset run."""

    def __init__(self, home: Path, instance: DagsterInstance) -> None:
        self.home = home
        self.instance = instance
        self.registry = DatasetRegistry(home)

    @cached_property
    def native(self) -> CachingStaleStatusResolver:
        return provenance_resolver(self.instance)

    def latest(self, key: AssetKey, partition: str) -> EventLogRecord | None:
        records = self.instance.fetch_materializations(
            AssetRecordsFilter(asset_key=key, asset_partitions=[partition]), limit=1
        ).records
        return records[0] if records else None

    def revision(self, record: EventLogRecord) -> Digest | None:
        materialization = record.asset_materialization
        if materialization is None or materialization.partition is None:
            return None
        partition = materialization.partition
        provenance = extract_data_provenance_from_entry(record.event_log_entry)
        config = ConfigRunIOManager(home=str(self.home))
        if provenance is not None:
            key = materialization.asset_key
            stage = key.path[-1]
            scope = (
                BRANCH_BY_NAMESPACE[asset_branch(key.path)].group
                if stage in DECLARED_SOURCE_STAGES
                else stage
            )
            mapping = mapping_key(scope)
            if GLOBAL_CONFIG not in provenance.input_storage_ids and mapping in provenance.input_storage_ids:
                mapped = self._configuration_input(record, partition, provenance, mapping)
                if mapped is None:
                    return None
                record = mapped
                provenance = extract_data_provenance_from_entry(record.event_log_entry)
                if provenance is None or GLOBAL_CONFIG not in provenance.input_storage_ids:
                    return None
            if GLOBAL_CONFIG in provenance.input_storage_ids:
                producer = self._configuration_input(record, partition, provenance, GLOBAL_CONFIG)
                if producer is None:
                    return None
                try:
                    registered = config.registered(producer.run_id, materialization=producer)
                except (IntegrityError, ValidationError):
                    return None
                return registered.registration_digest if registered.dataset_id == partition else None

        admitted = materialization.metadata.get(TABLE_REGISTRATION_METADATA)
        if isinstance(admitted, TextMetadataValue):
            try:
                return TypeAdapter(Digest).validate_python(admitted.value)
            except ValidationError:
                return None

        run = self.instance.get_run_by_id(record.run_id)
        if run is None:
            return None
        evidence = MaterializationStorageEvidence.model_validate(run.run_config)
        if evidence.revision is not None:
            return evidence.revision
        try:
            return config.registered(record.run_id).registration_digest
        except IntegrityError:
            return None

    def _configuration_input(
        self, consumer: EventLogRecord, partition: str, provenance: DataProvenance, key: AssetKey
    ) -> EventLogRecord | None:
        storage_id = provenance.input_storage_ids.get(key)
        if storage_id is None or storage_id >= consumer.storage_id:
            return None
        records = self.instance.fetch_materializations(
            AssetRecordsFilter(asset_key=key, asset_partitions=[partition], storage_ids=[storage_id]),
            limit=1,
        ).records
        if not records:
            return None
        producer = records[0]
        if not materialization_attempt_succeeded(
            self.instance,
            producer,
            output_name="result",
            retry_number="native",
            manager_keys=frozenset({"config_run_io_manager"}),
        ):
            return None
        return producer

    def stale(
        self,
        record: EventLogRecord,
        reference: AssetReference,
        value: pd.DataFrame | CategoryRules | None = None,
    ) -> bool:
        materialization = record.asset_materialization
        if materialization is None or materialization.partition is None:
            return True
        partition = materialization.partition
        try:
            if not self.registry.location(partition).has_declared_collection:
                return True
            spec = self.registry.get_snapshot(partition)
            registered = DeclaredGlobalConfig.from_spec(spec)
            return not self.record_current(record, registered, spec.storage, reference, value)
        except (FileNotFoundError, HumanReviewError, IntegrityError, ValidationError):
            return True

    def record_current(
        self,
        record: EventLogRecord,
        registered: DeclaredGlobalConfig,
        storage: StorageConfig,
        reference: AssetReference,
        value: pd.DataFrame | CategoryRules | None = None,
    ) -> bool:
        materialization = record.asset_materialization
        if materialization is None or materialization.partition != registered.dataset_id:
            return False
        key = materialization.asset_key
        stage = "/".join(key.path[1:])
        evidence = materialization.metadata
        version = extract_data_version_from_entry(record.event_log_entry)
        if (
            stage not in DECLARED_INPUT_STAGES
            or version is None
            or getattr(evidence.get("codebench/content_digest"), "value", None) != reference.digest
            or getattr(evidence.get("codebench/storage_format"), "value", None) != reference.format
        ):
            return False
        branch = asset_branch(key.path)
        source_branch = branch if key.path[-1] in DECLARED_SOURCE_STAGES else None
        revision = self.revision(record)
        if revision is None:
            return False
        producing = registered
        if revision != registered.registration_digest:
            prior = self.registry.get_snapshot(registered.dataset_id, revision=revision)
            if prior.storage != storage or not prior.has_declared_collection:
                return False
            producing = DeclaredGlobalConfig.from_spec(prior)
            if effective_approval(producing, stage, source_branch) != effective_approval(
                registered, stage, source_branch
            ):
                return False
        if not self.native_lineage_current(key, registered.dataset_id, source_branch):
            return False
        if stage == "category_rules":
            if version.value != fingerprint(registered.rules.model_dump_json()):
                return False
        elif branch == "keenable" or stage in FINALIZATION_STAGES:
            require_source_rights(registered.source_config("keenable"))
        if stage != "category_rules" and stage not in RELEASE_STAGES:
            index = evidence.get("evidence")
            if not isinstance(index, JsonMetadataValue) or not isinstance(index.value, dict):
                return False
            if index.value.get("config") != stage_configs(producing)[stage].model_dump(
                mode="json"
            ) or index.value.get("stage_implementation") != scoped_code_versions(
                key.path[-1], ImplementationSource(), source_branch
            ):
                return False
        manager = DataFrameIOManager.from_environment(storage)
        if manager.reference_at(key.path, registered.dataset_id) != reference:
            return False
        if source_branch is not None or stage in {"human_review_queue", "human_review_decisions"}:
            table = manager.read_version(reference) if value is None else value
            if not isinstance(table, pd.DataFrame):
                return False
            if source_branch is not None:
                validate_source_receipt(table, registered.source_config(branch), manager)
            elif stage == "human_review_queue":
                with HumanReviewStore(self.home).reader() as tx:
                    batch = tx.get_current_batch(registered.dataset_id, "annotations", compact=True)
                return table.attrs.get(REVIEW_PREPARATION) == {
                    "batch_id": batch.batch_id,
                    "snapshot_id": batch.snapshot.snapshot_id,
                }
            else:
                token = frame_authorization(table, REVIEW_BINDING)
                with HumanReviewStore(self.home).reader() as tx:
                    snapshot = tx.get_current_batch(registered.dataset_id, "annotations").snapshot
                    tx.authorize_completion(snapshot, token)
        return True

    def native_lineage_current(self, key: AssetKey, partition: str, source_branch: Branch | None) -> bool:
        status = self.native.get_status(key, partition)
        if status is StaleStatus.FRESH:
            return True
        stage = "/".join(key.path[1:])
        if status is not StaleStatus.STALE:
            return False
        scoped_settings = {GLOBAL_CONFIG}
        if source_branch is not None:
            scoped_settings.update(
                {
                    AssetKey(["datasets", "category_rules"]),
                    mapping_key(BRANCH_BY_NAMESPACE[source_branch].group),
                }
            )
        elif stage in REUSABLE_PROCESSING_STAGES:
            scoped_settings.update({AssetKey(["datasets", "category_rules"]), mapping_key(stage)})
        elif stage != "category_rules":
            return False
        causes = self.native.get_stale_causes(key, partition)
        return bool(causes) and all(
            cause.category is StaleCauseCategory.DATA and cause.dependency_asset_key in scoped_settings
            for cause in causes
        )
