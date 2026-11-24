from collections.abc import Sequence
from pathlib import Path
from typing import Final

from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, EventLogRecord, TextMetadataValue
from dagster._core.definitions.data_version import extract_data_version_from_entry
from pydantic import ValidationError

from codebench_pipeline.artifact_currentness import ArtifactCurrentness, MaterializationStorageEvidence
from codebench_pipeline.artifact_views import (
    ArtifactBinding,
    ArtifactKey,
    ArtifactPartitions,
    ArtifactQuery,
    ArtifactTarget,
    RunConfigBinding,
)
from codebench_pipeline.collection_stage_settings import COLLECTION_OPERATIONS
from codebench_pipeline.config_assets import (
    COLLECTION_SCOPES,
    CONFIG_DIRECTORY,
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    collection_mapping_key,
    mapping_key,
)
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import AssetReference
from codebench_pipeline.dataset_branches import (
    FINALIZATION_STAGES,
    annotation_stage_id,
    asset_branch,
    automatic_stage_id,
)
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.storage import IntegrityError

SPLITS: Final = ("train", "test")
SPLIT_CATEGORY_TABLES: Final = frozenset((split, "category_rows") for split in SPLITS)
SPLIT_TABLES: Final = SPLIT_CATEGORY_TABLES | frozenset((split, "merged_rows") for split in SPLITS)
SPLIT_CONFIGS: Final = frozenset((split, "stats") for split in SPLITS) | frozenset(
    tuple(mapping_key(f"{split}_{scope}").path) for split in SPLITS for scope in ("load", "counts", "merge")
)
CONFIG_KEYS: Final = frozenset(
    tuple(key.path)
    for key in (
        GLOBAL_CONFIG,
        CONFIG_DIRECTORY,
        mapping_key("semantic_evidence"),
        mapping_key("labeling_documentation"),
        *(collection_mapping_key(operation.branch, operation.stage) for operation in COLLECTION_OPERATIONS),
        *(mapping_key(scope) for scope in (*COLLECTION_SCOPES.values(), *FINALIZATION_STAGES)),
    )
)


def text_metadata(record: EventLogRecord, key: str) -> str | None:
    materialization = record.asset_materialization
    value = materialization.metadata.get(key) if materialization is not None else None
    return value.text if isinstance(value, TextMetadataValue) else None


class DatasetArtifactCatalog:
    """Bind dataset storage to the generic artifact viewer without resolving business inputs."""

    def __init__(self, home: Path, instance: DagsterInstance) -> None:
        self.home = home
        self.instance = instance
        self.registry = DatasetRegistry(home)

    def partitions(self, key: ArtifactKey) -> ArtifactPartitions:
        partitions: list[str] = []
        candidates = (
            sorted(self.instance.get_materialized_partitions(AssetKey(key.asset_key)))
            if key.asset_key in SPLIT_CATEGORY_TABLES or key.asset_key in SPLIT_CONFIGS
            else self.registry.keys()
        )
        for partition in candidates:
            try:
                self.resolve(ArtifactTarget(asset_key=key.asset_key, partition_key=partition))
            except DatasetViewError as error:
                if error.code not in {"not_found", "not_materialized"}:
                    raise
            else:
                partitions.append(partition)
        return ArtifactPartitions(asset_key=key.asset_key, partitions=tuple(partitions))

    def resolve(self, target: ArtifactTarget) -> ArtifactBinding | RunConfigBinding:
        split_asset = target.asset_key in SPLIT_TABLES or target.asset_key in SPLIT_CONFIGS
        if target.partition_key is None or (target.asset_key[0] != "datasets" and not split_asset):
            raise DatasetViewError("not_found", "No persisted artifact binding")
        records_filter = AssetRecordsFilter(
            asset_key=AssetKey(target.asset_key), asset_partitions=[target.partition_key]
        )
        records = self.instance.fetch_materializations(records_filter, limit=1).records
        if target.asset_key in CONFIG_KEYS or target.asset_key in SPLIT_CONFIGS:
            directory = target.asset_key == tuple(CONFIG_DIRECTORY.path)
            observations: Sequence[EventLogRecord] = (
                self.instance.fetch_observations(records_filter, limit=1).records if directory else ()
            )
            record = max((*records, *observations), key=lambda item: item.storage_id, default=None)
            if record is not None:
                path = self.home.joinpath("configuration-runs", record.run_id, *target.asset_key)
                if path.is_file():
                    retired = target.asset_key[-1] in {
                        "map_labeling_documentation_config",
                        "map_semantic_evidence_config",
                    }
                    stale = retired
                    if directory:
                        try:
                            approved = self.registry.get_snapshot(target.partition_key).config_directory
                            captured = ConfigRunIOManager(home=str(self.home)).directory(record.run_id)
                            version = extract_data_version_from_entry(record.event_log_entry)
                            stale = (
                                approved is None
                                or version is None
                                or version.value != captured.digest
                                or captured.digest != approved.digest
                            )
                        except (FileNotFoundError, IntegrityError, ValidationError):
                            stale = True
                    return RunConfigBinding(path, "stale" if stale else None, use_declared_schema=not retired)
            raise DatasetViewError("not_materialized", "This config has no persisted output")
        if not records:
            raise DatasetViewError("not_materialized", "This artifact has no persisted materialization")
        latest = records[0]
        requested = target.snapshot_id if isinstance(target, ArtifactQuery) else None
        selected: list[EventLogRecord] = []
        if requested is None:
            selected.append(latest)
        else:
            cursor = None
            while True:
                page = self.instance.fetch_materializations(records_filter, limit=100, cursor=cursor)
                selected.extend(
                    record
                    for record in page.records
                    if text_metadata(record, "codebench/content_digest") == requested
                )
                if not page.has_more:
                    break
                cursor = page.cursor
            if not selected:
                raise DatasetViewError(
                    "not_materialized", "No materialization identifies the selected snapshot"
                )

        binding = self.record_binding(target.partition_key, selected[0])
        if any(self.record_binding(target.partition_key, record) != binding for record in selected[1:]):
            raise DatasetViewError(
                "not_materialized", "The selected snapshot has conflicting storage bindings"
            )
        storage, reference = binding
        is_latest = selected[0].storage_id == latest.storage_id
        state = None if is_latest else "stale"
        if target.asset_key[-1] == "dataset_release":
            branch = asset_branch(target.asset_key)
            review_stages = {annotation_stage_id(branch), automatic_stage_id(branch)}
            with HumanReviewStore(self.home).reader() as tx:
                published = tx.last_publication(target.partition_key, branch=branch)
                if not is_latest and published is not None:
                    if not any(
                        item.dagster_run_id == selected[0].run_id
                        and item.payload_hash == reference.digest
                        and item.authorization.review_stage in review_stages
                        and not item.authorization.artifact_id
                        and not item.authorization.scope_id
                        for item in tx.list_publications(target.partition_key)
                    ):
                        raise DatasetViewError(
                            "not_materialized", "The selected release has no publication receipt"
                        )
            if published is None:
                try:
                    declared = self.registry.location(target.partition_key).has_declared_collection
                except FileNotFoundError:
                    declared = True
                if declared:
                    raise DatasetViewError("not_materialized", "This release has no publication receipt")
                return ArtifactBinding(storage, reference, "legacy_unreviewed" if is_latest else "stale")
            if is_latest:
                if published.dagster_run_id != selected[0].run_id or (
                    reference.format != "parquet" or reference.digest != published.payload_hash
                ):
                    raise DatasetViewError(
                        "not_materialized", "The release does not match its publication receipt"
                    )
                try:
                    current_storage = self.registry.location(target.partition_key).storage
                except FileNotFoundError:
                    state = "stale"
                else:
                    if current_storage != storage:
                        state = "stale"
                    else:
                        from codebench_pipeline.publication_inspection import read_inspected_publication

                        release = read_inspected_publication(
                            self.home, target.partition_key, self.instance, branch
                        )
                        if release is None or release.reference != reference:
                            raise DatasetViewError(
                                "not_materialized", "The release does not match its publication receipt"
                            )
                        state = release.publication_state
            return ArtifactBinding(storage, reference, state)

        if is_latest and target.asset_key not in SPLIT_TABLES:
            if ArtifactCurrentness(self.home, self.instance).stale(latest, reference):
                state = "stale"
        return ArtifactBinding(storage, reference, state)

    def record_binding(self, partition: str, record: EventLogRecord) -> tuple[StorageConfig, AssetReference]:
        try:
            reference = AssetReference.model_validate(
                {
                    "format": text_metadata(record, "codebench/storage_format"),
                    "digest": text_metadata(record, "codebench/content_digest"),
                }
            )
        except ValidationError as error:
            raise DatasetViewError(
                "not_materialized", "The materialization does not identify immutable bytes"
            ) from error
        run = self.instance.get_run_by_id(record.run_id)
        if run is None:
            raise DatasetViewError("not_materialized", "No persisted run identifies this artifact's storage")
        try:
            evidence = MaterializationStorageEvidence.model_validate(run.run_config)
            key = tuple(record.asset_key.path) if record.asset_key is not None else ()
            if key in SPLIT_TABLES:
                revision, inline = evidence.split_revision, None
            else:
                revision, inline = evidence.revision, evidence.storage
            dataset_partition = (
                run.tags.get("dagster/partition/dataset") if key in SPLIT_CATEGORY_TABLES else partition
            )
            if dataset_partition is None:
                raise DatasetViewError("not_materialized", "The materializing run has no dataset partition")
            if inline is None:
                revision = revision or ArtifactCurrentness(self.home, self.instance).revision(
                    record, dataset_partition=dataset_partition
                )
                if revision is None:
                    raise ValueError("Missing storage binding")
                storage = self.registry.storage(dataset_partition, revision=revision)
            elif revision is None and {"backend", "prefix"} <= inline.model_fields_set:
                storage = inline
                if storage.backend == "local" and "local_root" not in inline.model_fields_set:
                    raise ValueError("Missing local root")
            else:
                raise ValueError("Missing or conflicting storage binding")
            if storage.backend == "local" and not Path(storage.local_root).is_absolute():
                raise ValueError("Relative local root has no recorded base")
        except (FileNotFoundError, ValueError) as error:
            raise DatasetViewError(
                "not_materialized", "The materializing run has no usable storage binding"
            ) from error
        return storage, reference
