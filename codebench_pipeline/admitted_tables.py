"""Resolve current annotation tables from successful Dagster output attempts."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Final

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetRecordsFilter,
    BoolMetadataValue,
    DagsterInstance,
    EventLogRecord,
    JsonMetadataValue,
    TextMetadataValue,
)
from pydantic import ValidationError

from codebench_pipeline.dataframe_io import AssetReference
from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded
from codebench_pipeline.finalization_publication import (
    FINALIZATION_PUBLICATION_METADATA,
    FinalizationPublicationV1,
    StepAttempt,
)
from codebench_pipeline.models import FrozenModel

ADMITTED_TABLE_STAGES: Final = frozenset(
    {"proposed_annotations", "labeling_failures", "deduplicated_annotations", "category_eligible_annotations"}
)
TABLE_REUSE_PUBLICATION_METADATA: Final = "codebench/finalization_reuse_publication"


class TableReusePublication(FrozenModel):
    publisher: StepAttempt
    artifact_attempt: str


@dataclass(frozen=True, slots=True)
class AdmittedTable:
    reference: AssetReference
    record: EventLogRecord
    materialization: AssetMaterialization
    producer: FinalizationPublicationV1


def uses_admitted_table(path: Sequence[str]) -> bool:
    return len(path) == 2 and path[0] == "datasets" and path[1] in ADMITTED_TABLE_STAGES


def _records(instance: DagsterInstance, key: AssetKey, partition: str) -> Iterator[EventLogRecord]:
    scope = AssetRecordsFilter(asset_key=key, asset_partitions=[partition])
    cursor = None
    while True:
        page = instance.fetch_materializations(scope, limit=100, cursor=cursor)
        yield from page.records
        if not page.has_more:
            return
        cursor = page.cursor


def resolve_labeling_completion(
    instance: DagsterInstance, key: AssetKey, partition: str
) -> tuple[EventLogRecord, AssetReference] | None:
    """Select the latest successful native labeling output, ignoring draft events."""
    for record in _records(instance, key, partition):
        event = record.asset_materialization
        if event is None or event.metadata.get("codebench/storage_format") != TextMetadataValue("parquet"):
            continue
        digest = event.metadata.get("codebench/content_digest")
        if not isinstance(digest, TextMetadataValue):
            continue
        if materialization_attempt_succeeded(
            instance,
            record,
            output_name="result",
            retry_number="native",
            manager_keys=frozenset({"dataset_io_manager", "finalization_table_io_manager"}),
        ):
            reference = AssetReference.model_validate({"format": "parquet", "digest": digest.value})
            return record, reference
    return None


def _succeeded(instance: DagsterInstance, record: EventLogRecord, step: StepAttempt) -> bool:
    return (
        record.run_id == step.run_id
        and record.event_log_entry.step_key == step.step_key
        and materialization_attempt_succeeded(
            instance, record, output_name="result", retry_number=step.retry_number
        )
    )


def _matches_producer(event: AssetMaterialization, producer: FinalizationPublicationV1) -> bool:
    metadata = event.metadata
    return (
        metadata.get("input_fingerprint") == TextMetadataValue(producer.input_signature)
        and metadata.get("codebench/content_digest") == TextMetadataValue(producer.output.digest)
        and metadata.get("codebench/storage_format") == TextMetadataValue(producer.output.format)
    )


def _producer(instance: DagsterInstance, record: EventLogRecord) -> FinalizationPublicationV1 | None:
    event = record.asset_materialization
    if event is None or event.metadata.get("cache_hit") != BoolMetadataValue(False):
        return None
    tag = event.metadata.get(FINALIZATION_PUBLICATION_METADATA)
    if not isinstance(tag, JsonMetadataValue):
        return None
    try:
        producer = FinalizationPublicationV1.model_validate(tag.value)
    except ValidationError:
        return None
    if _matches_producer(event, producer) and _succeeded(instance, record, producer.step_attempt):
        return producer
    return None


def resolve_admitted_table(
    instance: DagsterInstance,
    key: AssetKey,
    partition: str,
    *,
    upstream_step: tuple[str, str] | None = None,
) -> AdmittedTable | None:
    for record in _records(instance, key, partition):
        if upstream_step is not None and (record.run_id, record.event_log_entry.step_key) != upstream_step:
            continue
        event = record.asset_materialization
        if event is None:
            continue
        producer = _producer(instance, record)
        if producer is not None:
            return AdmittedTable(producer.output, record, event, producer)
        tag = event.metadata.get(TABLE_REUSE_PUBLICATION_METADATA)
        if not isinstance(tag, JsonMetadataValue) or event.metadata.get("cache_hit") != BoolMetadataValue(
            True
        ):
            continue
        try:
            reuse = TableReusePublication.model_validate(tag.value)
        except ValidationError:
            continue
        if not _succeeded(instance, record, reuse.publisher):
            continue
        for origin in _records(instance, key, partition):
            if origin.storage_id >= record.storage_id or origin.asset_materialization is None:
                continue
            producer = _producer(instance, origin)
            if (
                producer is not None
                and producer.artifact_attempt == reuse.artifact_attempt
                and _matches_producer(event, producer)
                and event.metadata.get("evidence") == origin.asset_materialization.metadata.get("evidence")
            ):
                return AdmittedTable(producer.output, record, event, producer)
    return None
