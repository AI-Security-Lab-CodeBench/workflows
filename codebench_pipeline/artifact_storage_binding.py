from pathlib import Path
from typing import Final

from dagster import DagsterInstance, EventLogRecord, TextMetadataValue
from pydantic import ValidationError

from codebench_pipeline.artifact_currentness import (
    ArtifactCurrentness,
    MaterializationStorageEvidence,
    materialization_registration,
)
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import AssetReference
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.rules_artifacts import RULES_EVAL_KEY
from codebench_pipeline.storage import IntegrityError

_SPLITS: Final = ("train", "test")
SPLIT_CATEGORY_TABLES: Final = frozenset((split, "category_rows") for split in _SPLITS)
SPLIT_MERGE_TABLES: Final = frozenset((split, "merged_rows") for split in _SPLITS)
RULES_TABLES: Final = frozenset({tuple(RULES_EVAL_KEY.path), ("codebench", "rules_eval_report")})
REGISTERED_TABLES: Final = SPLIT_CATEGORY_TABLES | SPLIT_MERGE_TABLES | RULES_TABLES


def text_metadata(record: EventLogRecord, key: str) -> str | None:
    materialization = record.asset_materialization
    value = materialization.metadata.get(key) if materialization is not None else None
    return value.text if isinstance(value, TextMetadataValue) else None


def record_binding(
    home: Path, instance: DagsterInstance, record: EventLogRecord
) -> tuple[StorageConfig, AssetReference]:
    """Resolve immutable bytes using the registration or run that produced them."""
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
    try:
        materialization = record.asset_materialization
        if materialization is None or materialization.partition is None:
            raise IntegrityError("Artifact output has no producing partition")
        key = tuple(materialization.asset_key.path)
        registry = DatasetRegistry(home)
        if key in REGISTERED_TABLES:
            revision = materialization_registration(record)
            if revision is None:
                raise IntegrityError("Split output has no producing registration")
            storage = registry.storage(CODEBENCH_DATASET_ID, revision=revision)
        else:
            run = instance.get_run_by_id(record.run_id)
            if run is None:
                raise IntegrityError("No persisted run identifies this artifact's storage")
            evidence = MaterializationStorageEvidence.model_validate(run.run_config)
            revision, inline = evidence.revision, evidence.storage
            if inline is None:
                revision = revision or ArtifactCurrentness(home, instance).revision(record)
                if revision is None:
                    raise IntegrityError("Missing storage binding")
                storage = registry.storage(materialization.partition, revision=revision)
            elif revision is None and {"backend", "prefix"} <= inline.model_fields_set:
                storage = inline
                if storage.backend == "local" and "local_root" not in inline.model_fields_set:
                    raise IntegrityError("Missing local root")
            else:
                raise IntegrityError("Missing or conflicting storage binding")
        if storage.backend == "local" and not Path(storage.local_root).is_absolute():
            raise IntegrityError("Relative local root has no recorded base")
    except (FileNotFoundError, IntegrityError, ValueError) as error:
        raise DatasetViewError(
            "not_materialized", "The materializing run has no usable storage binding"
        ) from error
    return storage, reference
