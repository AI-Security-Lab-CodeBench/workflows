from __future__ import annotations

import base64
import json
import os
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Final, Literal, assert_never

import pandas as pd
import pyarrow
import pyarrow.parquet as parquet
from dagster import (
    AssetKey,
    Config,
    ConfigurableIOManager,
    DagsterInstance,
    EventLogRecord,
    InputContext,
    OutputContext,
    TextMetadataValue,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue, PrivateAttr, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dagster_table_metadata import MetadataSink, record_preview
from codebench_pipeline.dataframe_schemas import current_labeling_attempts
from codebench_pipeline.dataset_records import LabelDecision
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.input_provenance import (
    configuration_producer,
    input_materialization,
    producer_run_id,
)
from codebench_pipeline.models import Digest, SemanticSnippet
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.repository_receipts import RepositoryRun
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.stage_implementation import ImplementationSource
from codebench_pipeline.storage import IntegrityError, open_directory

REVIEW_PREPARATION: Final = "codebench_review_preparation"
_LABELING_NESTED_TYPES: Final = {
    "label": pyarrow.struct(
        [
            (
                field.alias or name,
                pyarrow.list_(pyarrow.string()) if name == "documentation_urls" else pyarrow.string(),
            )
            for name, field in LabelDecision.model_fields.items()
        ]
    ),
    "documentation": pyarrow.list_(
        pyarrow.struct(
            [
                (name, pyarrow.list_(pyarrow.string()) if name == "subcommands" else pyarrow.string())
                for name in SemanticSnippet.model_fields
            ]
        )
    ),
}


def asset_storage_key(asset_key: Sequence[str], partition_key: str | None) -> str:
    identity = [list(asset_key)]
    if partition_key is not None:
        identity.append([partition_key])
    return f"assets/{sha256(json.dumps(identity, ensure_ascii=False).encode())}.json"


class AssetReference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    format: Literal["parquet", "rules"]
    digest: str = Field(pattern=r"^[a-f0-9]{64}$")


class PlannedOutputReferenceMismatch(IntegrityError):
    """A current table pointer disagrees with the planned materialization."""


def materialized_reference(
    instance: DagsterInstance, record: EventLogRecord, output_name: str = "result"
) -> AssetReference:
    from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded

    asset = record.asset_materialization
    if asset is None or not materialization_attempt_succeeded(
        instance, record, output_name=output_name, retry_number="native"
    ):
        raise IntegrityError("Materialization does not belong to a successful output")
    digest = asset.metadata.get("codebench/content_digest")
    format_name = asset.metadata.get("codebench/storage_format")
    if not isinstance(digest, TextMetadataValue) or not isinstance(format_name, TextMetadataValue):
        raise IntegrityError("Table materialization does not identify immutable bytes")
    return AssetReference.model_validate({"format": format_name.value, "digest": digest.value})


class UpstreamTableReference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    asset_key: tuple[str, ...] = Field(strict=False)
    partition: str
    reference: AssetReference


class CategoryDtype(BaseModel):
    categories: str
    ordered: bool


@dataclass(frozen=True, slots=True)
class AssetSnapshot:
    reference: AssetReference
    value: pd.DataFrame | CategoryRules
    publication_state: Literal["published", "legacy_unreviewed", "stale"] | None = None


def record_reference(context: MetadataSink, reference: AssetReference) -> None:
    context.add_output_metadata(
        {"codebench/content_digest": reference.digest, "codebench/storage_format": reference.format}
    )


def frame_bytes(frame: pd.DataFrame) -> bytes:
    if not frame.columns.is_unique:
        raise ValueError("DataFrames require unique column names")
    TypeAdapter(list[str]).validate_python(frame.columns.tolist(), strict=True)
    table = pd.DataFrame(frame, copy=False)
    attributes = frame.attrs
    categories: dict[str, dict[str, JsonValue]] = {}
    for name in table.select_dtypes(include="category").columns:
        values = table[name].cat
        payload = pd.DataFrame(index=values.categories).to_parquet(engine="pyarrow")
        categories[name] = {"categories": base64.b64encode(payload).decode(), "ordered": values.ordered}
    if categories:
        if "_codebench_categories" in frame.attrs:
            raise ValueError("Reserved Parquet category metadata")
        attributes = {**attributes, "_codebench_categories": categories}
    arrow = pyarrow.Table.from_pandas(table)
    if {"documentation_manifest_digest", *_LABELING_NESTED_TYPES} <= set(table.columns):
        for name, dtype in _LABELING_NESTED_TYPES.items():
            arrow = arrow.set_column(
                arrow.schema.get_field_index(name), name, pyarrow.array(table[name].tolist(), type=dtype)
            )
    if attributes:
        metadata = arrow.schema.metadata or {}
        pandas_metadata = TypeAdapter(dict[str, JsonValue]).validate_json(metadata[b"pandas"])
        arrow = arrow.replace_schema_metadata(
            {
                **metadata,
                b"pandas": json.dumps({**pandas_metadata, "attributes": attributes}).encode(),
                b"PANDAS_ATTRS": json.dumps(attributes).encode(),
            }
        )
    with BytesIO() as output:
        with parquet.ParquetWriter(output, arrow.schema, compression="snappy") as writer:
            writer.write_table(arrow)
        return output.getvalue()


def read_frame(payload: bytes) -> pd.DataFrame:
    frame = pd.read_parquet(BytesIO(payload), engine="pyarrow")
    metadata = TypeAdapter(dict[str, CategoryDtype]).validate_python(
        frame.attrs.pop("_codebench_categories", {})
    )
    for name, value in metadata.items():
        categories = pd.read_parquet(
            BytesIO(base64.b64decode(value.categories, validate=True)), engine="pyarrow"
        ).index
        frame[name] = pd.Categorical(frame[name], categories=categories, ordered=value.ordered)
    return frame


class DataFrameIOManager(ConfigurableIOManager):
    """Persist pandas tables and taxonomy; asset functions own Pandera validation."""

    storage: StorageConfig
    review_home: str | None = None
    review_token: str | None = None
    registered_revision: Digest | None = None
    _review_store: HumanReviewStore | None = PrivateAttr(default=None)
    _object_store: ObjectStore | None = PrivateAttr(default=None)
    _configuration_producers: dict[str, str] = PrivateAttr(default_factory=dict)

    @staticmethod
    def from_environment(storage: StorageConfig, *, review_home: str | None = None) -> DataFrameIOManager:
        manager = DataFrameIOManager(storage=storage, review_home=review_home)
        manager.bind_object_store(ObjectStore.from_environment(storage))
        return manager

    def bind_object_store(self, store: ObjectStore) -> None:
        if store.config != self.storage:
            raise IntegrityError("Object store does not match the manager's storage configuration")
        self._object_store = store

    def read_registered_version(
        self, storage: StorageConfig, reference: AssetReference
    ) -> pd.DataFrame | CategoryRules:
        if storage != self.storage:
            raise IntegrityError("Review manifest storage differs from the bound object store")
        return self.read_version(reference)

    def bind_review_store(self, store: HumanReviewStore) -> None:
        self._review_store = store

    @property
    def review_store(self) -> HumanReviewStore:
        if self._review_store is None:
            from codebench_pipeline.human_review import HumanReviewError

            raise HumanReviewError("conflict", "Review database is not bound to this run")
        return self._review_store

    def run_configuration(self, run_id: str, dataset_id: str) -> DeclaredGlobalConfig:
        from codebench_pipeline.config_assets import ConfigRunIOManager
        from codebench_pipeline.dataset_registry import DatasetRegistry
        from codebench_pipeline.human_review import HumanReviewError

        if self.review_home is None or self.registered_revision is None:
            raise IntegrityError("Dataset processing requires a pinned registration")
        producer = self._configuration_producers.get(run_id, run_id)
        registered = ConfigRunIOManager(home=self.review_home).registered(producer)
        if (
            registered.dataset_id != dataset_id
            or registered.registration_digest != self.registered_revision
            or DatasetRegistry(Path(self.review_home)).current_revision(dataset_id)
            != self.registered_revision
        ):
            raise HumanReviewError("stale", "Registered dataset configuration changed during the run")
        return registered

    def bind_configuration_producer(self, context: InputContext) -> None:
        """Bind indirect registration reads to Dagster's producer for this run."""
        if self.review_home is None:
            return
        from codebench_pipeline.config_assets import (
            GLOBAL_CONFIG,
            ConfigRunIOManager,
            current_configuration_record,
        )

        step = context.step_context
        if step.run_id in self._configuration_producers:
            return
        producer = configuration_producer(step, GLOBAL_CONFIG)
        if producer is None:
            record = current_configuration_record(context.instance, GLOBAL_CONFIG, context.partition_key)
            producer = record.run_id
            ConfigRunIOManager(home=self.review_home).registered(producer, materialization=record)
        self._configuration_producers[step.run_id] = producer

    def object_store(self) -> ObjectStore:
        return self._object_store or ObjectStore(self.storage)

    def repository_run(self, dataset_id: str, run_id: str) -> RepositoryRun:
        if self.review_home is None:
            raise IntegrityError("Declared source acquisition requires a registered dataset home")
        return RepositoryRun(Path(self.review_home), dataset_id, run_id)

    @property
    def implementation_source(self) -> ImplementationSource:
        return ImplementationSource()

    def stage_signature(
        self,
        stage: str,
        config: Config,
        inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
    ) -> str:
        from codebench_pipeline.dataset_engine import stage_signature

        return stage_signature(
            stage,
            config,
            inputs,
            source=self.implementation_source,
            storage_identity=self.storage.model_dump_json(),
        )

    def _asset_key(self, context: InputContext | OutputContext) -> str:
        return asset_storage_key(
            context.asset_key.path,
            context.asset_partition_key if context.has_asset_partitions else None,
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | CategoryRules) -> None:
        reference = self.persist_value(obj)
        self.publish_reference(context, reference)
        self.record_output(context, obj, reference)

    def record_output(
        self, context: OutputContext, obj: pd.DataFrame | CategoryRules, reference: AssetReference
    ) -> None:
        if isinstance(obj, pd.DataFrame):
            record_preview(context, obj, self.storage.sample_rows)
        record_reference(context, reference)

    def persist_value(self, obj: pd.DataFrame | CategoryRules) -> AssetReference:
        format_name: Literal["parquet", "rules"]
        match obj:
            case pd.DataFrame():
                payload, format_name = frame_bytes(obj), "parquet"
            case CategoryRules():
                payload, format_name = obj.model_dump_json().encode(), "rules"
            case _:
                assert_never(obj)
        reference = AssetReference(format=format_name, digest=sha256(payload))
        store = self.object_store()
        store.write(self._payload_key(reference), payload)
        return reference

    def publish_reference(self, context: InputContext | OutputContext, reference: AssetReference) -> None:
        store = self.object_store()
        key = self._asset_key(context)
        try:
            previous = store.read(key)
        except FileNotFoundError:
            previous = None
        if previous is not None:
            backup = f"reference-history/{sha256(key.encode())}/{sha256(previous)}.json"
            store.write(backup, previous)
            if store.read(backup) != previous:
                raise IntegrityError("Asset-reference backup verification failed")
        store.write(key, reference.model_dump_json().encode(), replace=True)

    def _payload_key(self, reference: AssetReference) -> str:
        suffix = "parquet" if reference.format == "parquet" else "json"
        return f"objects/{reference.digest}.{suffix}"

    def current_reference(self, context: InputContext | OutputContext) -> AssetReference:
        return self.reference_at(
            context.asset_key.path, context.asset_partition_key if context.has_asset_partitions else None
        )

    def reference_at(self, asset_key: Sequence[str], partition: str | None) -> AssetReference:
        return AssetReference.model_validate_json(
            self.object_store().read(asset_storage_key(asset_key, partition))
        )

    def read_version(self, reference: AssetReference) -> pd.DataFrame | CategoryRules:
        return self._read_version(self.object_store(), reference)

    def read_planned_output(self, key: AssetKey, partition: str, recorded_digest: str) -> AssetSnapshot:
        store = self.object_store()
        reference = AssetReference.model_validate_json(store.read(asset_storage_key(key.path, partition)))
        if reference.digest != recorded_digest:
            raise PlannedOutputReferenceMismatch(
                f"Current output differs from its recorded materialization: {key.to_user_string()}"
            )
        return AssetSnapshot(reference, self._read_version(store, reference))

    def _read_version(self, store: ObjectStore, reference: AssetReference) -> pd.DataFrame | CategoryRules:
        payload = store.read(self._payload_key(reference))
        if sha256(payload) != reference.digest:
            raise IntegrityError("Asset reference content mismatch")
        match reference.format:
            case "parquet":
                return current_labeling_attempts(read_frame(payload))
            case "rules":
                return CategoryRules.model_validate_json(payload)
            case _:
                assert_never(reference.format)

    def load_input(self, context: InputContext) -> pd.DataFrame | CategoryRules:
        return self.load_versioned_input(context).value

    def input_reference(self, context: InputContext) -> AssetReference:
        upstream = context.upstream_output
        if upstream is None:
            return self.current_reference(context)

        producer = producer_run_id(upstream)
        record = input_materialization(context)
        if record is None:
            raise IntegrityError(f"No materialization for input {context.asset_key.to_user_string()}")
        reference = materialized_reference(
            context.instance, record, upstream.name if producer is not None else "result"
        )
        if producer is None and reference != self.current_reference(context):
            raise IntegrityError("Current table reference differs from its recorded materialization")
        return reference

    def load_versioned_input(self, context: InputContext) -> AssetSnapshot:
        reference = self.input_reference(context)
        return AssetSnapshot(reference, self.read_version(reference))

    def cached_frame(self, key: str, compute: Callable[[], pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
        """The caller supplies a key containing every content and schema version."""
        frame = self.read_cached_frame(key)
        if frame is not None:
            return frame, True
        frame = compute()
        self.write_cached_frame(key, frame)
        return frame, False

    def read_cached_frame(self, key: str) -> pd.DataFrame | None:
        try:
            payload = self.object_store().read(f"cache/{sha256(key.encode())}.parquet")
        except FileNotFoundError:
            return None
        return read_frame(payload)

    def write_cached_frame(self, key: str, frame: pd.DataFrame) -> None:
        self.object_store().write(f"cache/{sha256(key.encode())}.parquet", frame_bytes(frame))

    def retain_attempt_evidence(self, directory: Path, prefix: str) -> dict[str, JsonValue]:
        """Retain the files produced in one computation's private scratch directory."""
        receipts: dict[str, JsonValue] = {}
        with open_directory(directory) as root_fd:
            for parent, children, files, parent_fd in os.fwalk(".", follow_symlinks=False, dir_fd=root_fd):
                for child in children:
                    if not stat.S_ISDIR(os.stat(child, dir_fd=parent_fd, follow_symlinks=False).st_mode):
                        raise IntegrityError(
                            "Attempt evidence must contain only directories and regular files"
                        )
                for filename in sorted(files):
                    if not stat.S_ISREG(os.stat(filename, dir_fd=parent_fd, follow_symlinks=False).st_mode):
                        raise IntegrityError("Attempt evidence must contain only regular files")
                    fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
                    with os.fdopen(fd, "rb") as stream:
                        if not stat.S_ISREG(os.fstat(fd).st_mode):
                            raise IntegrityError("Attempt evidence must contain only regular files")
                        name = Path(parent, filename).as_posix()
                        receipts[name] = self.write_evidence(f"{prefix}/{name}", stream.read())
        return receipts

    def write_evidence(self, key: str, data: bytes) -> str:
        """Evidence keys are immutable; a different receipt needs a different key."""
        return self.object_store().write(f"evidence/{sha256(key.encode())}", data)

    def read_evidence(self, key: str) -> bytes:
        return self.object_store().read(f"evidence/{sha256(key.encode())}")

    def exists(self, key: str) -> bool:
        """Check evidence existence while still rejecting corrupted content."""
        try:
            self.read_evidence(key)
        except FileNotFoundError:
            return False
        return True
