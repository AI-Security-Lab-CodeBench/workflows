from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Literal, Protocol, assert_never

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableMetadataValue,
    TableRecord,
    TableSchema,
    TableSchemaMetadataValue,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataset_branches import asset_branch
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.storage import IntegrityError


class AssetReference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    format: Literal["parquet", "rules"]
    digest: str = Field(pattern=r"^[a-f0-9]{64}$")


class CategoryDtype(BaseModel):
    categories: str
    ordered: bool


@dataclass(frozen=True, slots=True)
class AssetSnapshot:
    reference: AssetReference
    value: pd.DataFrame | CategoryRules
    publication_state: Literal["published", "legacy_unreviewed", "stale"] | None = None


class MetadataSink(Protocol):
    def add_output_metadata(
        self, metadata: Mapping[str, str | int | TableMetadataValue | TableSchemaMetadataValue]
    ) -> None: ...


def record_reference(context: MetadataSink, reference: AssetReference) -> None:
    context.add_output_metadata(
        {"codebench/content_digest": reference.digest, "codebench/storage_format": reference.format}
    )


def frame_bytes(frame: pd.DataFrame) -> bytes:
    if not frame.columns.is_unique:
        raise ValueError("DataFrames require unique column names")
    TypeAdapter(list[str]).validate_python(frame.columns.tolist(), strict=True)
    table = frame.copy(deep=False)
    categories: dict[str, dict[str, JsonValue]] = {}
    for name in frame.select_dtypes(include="category").columns:
        values = frame[name].cat
        payload = pd.DataFrame(index=values.categories).to_parquet(engine="pyarrow")
        categories[name] = {"categories": base64.b64encode(payload).decode(), "ordered": values.ordered}
    if categories:
        if "_codebench_categories" in frame.attrs:
            raise ValueError("Reserved Parquet category metadata")
        table.attrs = {**frame.attrs, "_codebench_categories": categories}
    return table.to_parquet(engine="pyarrow")


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


def preview_value(value: JsonValue) -> str | int | float | bool | None:
    match value:
        case None | bool() | int() | float():
            return value
        case str():
            return value[:500]
        case list() | dict():
            return json.dumps(value, ensure_ascii=False)[:500]
        case _:
            assert_never(value)


def record_preview(context: MetadataSink, frame: pd.DataFrame, sample_rows: int) -> None:
    schema = TableSchema(
        columns=[TableColumn(name=str(name), type=str(dtype)) for name, dtype in frame.dtypes.items()]
    )
    rows = TypeAdapter(list[dict[str, JsonValue]]).validate_json(
        frame.head(sample_rows).to_json(
            orient="records",
            date_format="iso",
            default_handler=str,
        )
    )
    sample = [TableRecord({name: preview_value(value) for name, value in row.items()}) for row in rows]
    context.add_output_metadata(
        {
            "dagster/row_count": len(frame),
            "dagster/column_schema": MetadataValue.table_schema(schema),
            "sample": MetadataValue.table(sample, schema=schema),
        }
    )


class DataFrameIOManager(ConfigurableIOManager):
    """Persist pandas tables and taxonomy; asset functions own Pandera validation."""

    storage: StorageConfig
    review_home: str | None = None
    review_token: str | None = None

    def _store(self) -> ObjectStore:
        return ObjectStore(self.storage)

    def _asset_key(self, context: InputContext | OutputContext) -> str:
        identity = [list(context.asset_key.path)]
        if context.has_asset_partitions:
            identity.append([context.asset_partition_key])
        return f"assets/{sha256(json.dumps(identity, ensure_ascii=False).encode())}.json"

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | CategoryRules) -> None:
        if context.asset_key.path[0] == "datasets" and context.asset_key.path[-1] in {
            "human_review_decisions",
            "dataset_release",
            "automatic_dataset_release",
        }:
            from codebench_pipeline.release_admission import write_review_output

            match obj:
                case pd.DataFrame():
                    reference = write_review_output(self, context, obj)
                case CategoryRules():
                    raise IntegrityError("Review outputs must be tables")
                case _:
                    assert_never(obj)
        else:
            reference = self.persist_value(obj)
            if self.review_home is not None and context.asset_key.path[0] == "datasets":
                from codebench_pipeline.source_review_guard import SOURCE_GATED_STAGES, authorize_output

                with HumanReviewStore(Path(self.review_home)).transaction(timeout=10):
                    from codebench_pipeline.dataset_branches import FINALIZATION_STAGES

                    if context.asset_key.path[-1] in FINALIZATION_STAGES:
                        from codebench_pipeline.dataset_finalization import authorize_finalization_output

                        match obj:
                            case pd.DataFrame():
                                authorize_finalization_output(
                                    Path(self.review_home),
                                    context.step_context.instance,
                                    context.asset_partition_key,
                                    obj,
                                    complete=context.asset_key.path[-1]
                                    not in {
                                        "merged_proposed_annotations",
                                        "reviewed_annotations",
                                    },
                                )
                            case CategoryRules():
                                raise IntegrityError("Finalization outputs must be tables")
                            case _:
                                assert_never(obj)
                    if context.asset_key.path[-1] in SOURCE_GATED_STAGES:
                        match obj:
                            case pd.DataFrame():
                                authorize_output(context, self, obj)
                            case CategoryRules():
                                raise IntegrityError("Source-dependent outputs must be tables")
                            case _:
                                assert_never(obj)
                    self.publish_reference(context, reference)
            else:
                self.publish_reference(context, reference)
        match obj:
            case pd.DataFrame():
                record_preview(context, obj, self.storage.sample_rows)
            case CategoryRules():
                pass
            case _:
                assert_never(obj)
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
        store = self._store()
        store.write(self._payload_key(reference), payload)
        return reference

    def publish_reference(self, context: InputContext | OutputContext, reference: AssetReference) -> None:
        store = self._store()
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
        return AssetReference.model_validate_json(self._store().read(self._asset_key(context)))

    def read_version(self, reference: AssetReference) -> pd.DataFrame | CategoryRules:
        store = self._store()
        payload = store.read(self._payload_key(reference))
        if sha256(payload) != reference.digest:
            raise IntegrityError("Asset reference content mismatch")
        match reference.format:
            case "parquet":
                return read_frame(payload)
            case "rules":
                return CategoryRules.model_validate_json(payload)
            case _:
                assert_never(reference.format)

    def load_input(self, context: InputContext) -> pd.DataFrame | CategoryRules:
        if (
            context.asset_key.path[-1] == "proposed_annotations"
            and self.review_home is not None
            and context.has_input_name
            and context.name in {"dataset_annotations", "documentation_annotations"}
        ):
            from codebench_pipeline.data_views import DatasetDataService
            from codebench_pipeline.dataset_finalization import available_proposals

            home = Path(self.review_home)
            frames, _ = available_proposals(
                home, DatasetDataService(home, context.instance), context.asset_partition_key
            )
            return frames[asset_branch(context.asset_key.path)]
        snapshot = self.read_current(context)
        if snapshot.publication_state in {"stale", "legacy_unreviewed"}:
            from codebench_pipeline.human_review import HumanReviewError

            raise HumanReviewError("stale", "This materialization is no longer authorized by current review")
        return snapshot.value

    def read_current(self, context: InputContext) -> AssetSnapshot:
        state: Literal["published", "legacy_unreviewed", "stale"] | None = None
        if context.asset_key.path[-1] == "dataset_release" and self.review_home is not None:
            from codebench_pipeline.release_admission import read_publication

            published = read_publication(
                Path(self.review_home),
                context.asset_partition_key,
                context.instance,
                asset_branch(context.asset_key.path),
            )
            if published is not None:
                return published
            state = "legacy_unreviewed"
        reference = self.current_reference(context)
        value = self.read_version(reference)
        if self.review_home is not None:
            from codebench_pipeline.review_output_state import materialization_stale

            if materialization_stale(Path(self.review_home), context, value):
                state = "stale"
        return AssetSnapshot(reference, value, state)

    def cached_frame(self, key: str, compute: Callable[[], pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
        """The caller supplies a key containing every content and schema version."""
        storage_key = f"cache/{sha256(key.encode())}.parquet"
        store = self._store()
        try:
            payload = store.read(storage_key)
        except FileNotFoundError:
            frame = compute()
            store.write(storage_key, frame_bytes(frame))
            return frame, False
        return read_frame(payload), True

    def write_evidence(self, key: str, data: bytes) -> str:
        """Evidence keys are immutable; a different receipt needs a different key."""
        return self._store().write(f"evidence/{sha256(key.encode())}", data)

    def read_evidence(self, key: str) -> bytes:
        return self._store().read(f"evidence/{sha256(key.encode())}")

    def exists(self, key: str) -> bool:
        """Check evidence existence while still rejecting corrupted content."""
        try:
            self.read_evidence(key)
        except FileNotFoundError:
            return False
        return True
