"""Verify finalization reuse against an admitted materialization."""

import hashlib
import json
from typing import Final, Literal

import pandas as pd
import pandera.pandas as pa
from dagster import (
    AssetKey,
    AssetRecordsFilter,
    BoolMetadataValue,
    DagsterEventType,
    DagsterInstance,
    EventLogRecord,
    JsonMetadataValue,
    TextMetadataValue,
)
from dagster._core.events import HandledOutputData
from dagster._core.execution.stats import StepEventStatus
from pydantic import JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.dataframe_io import DataFrameIOManager, UpstreamTableReference
from codebench_pipeline.dataset_engine import ReusedTable, TableCache
from codebench_pipeline.finalization_publication import (
    FINALIZATION_PUBLICATION_METADATA,
    FinalizationPublicationV1,
)
from codebench_pipeline.storage import IntegrityError

_MATERIALIZATION: Final = DagsterEventType.ASSET_MATERIALIZATION
_HANDLED_OUTPUT: Final = DagsterEventType.HANDLED_OUTPUT


def materialization_attempt_succeeded(
    instance: DagsterInstance,
    record: EventLogRecord,
    *,
    output_name: str,
    retry_number: int | Literal["native"] = 0,
    manager_keys: frozenset[str] | None = None,
) -> bool:
    """Verify the claimed attempt, or derive it from Dagster for a native lookup."""
    entry = record.event_log_entry
    if not entry.step_key:
        return False
    stats = instance.get_run_step_stats(entry.run_id, step_keys=[entry.step_key])
    if len(stats) != 1 or stats[0].status != StepEventStatus.SUCCESS:
        return False
    attempts = stats[0].attempts_list
    if not attempts or (retry_number != "native" and retry_number != len(attempts) - 1):
        return False
    start, end = attempts[-1].start_time, attempts[-1].end_time
    if start is None or end is None or not start <= entry.timestamp <= end:
        return False
    outputs = [
        item
        for item in instance.get_records_for_run(
            entry.run_id, of_type={_MATERIALIZATION, _HANDLED_OUTPUT}
        ).records
        if item.event_log_entry.step_key == entry.step_key and start <= item.event_log_entry.timestamp <= end
    ]
    return (
        len(outputs) == 2
        and outputs[0].event_log_entry == entry
        and (event := outputs[1].event_log_entry.dagster_event) is not None
        and isinstance(output := event.event_specific_data, HandledOutputData)
        and output.output_name == output_name
        and (manager_keys is None or output.manager_key in manager_keys)
    )


def _verify_admitted_finalization[Schema: pa.DataFrameModel](
    storage: DataFrameIOManager,
    cache: TableCache,
    *,
    index: dict[str, JsonValue],
    publication: FinalizationPublicationV1,
    evidence_attribute: str | None,
    schema: type[Schema],
) -> ReusedTable[Schema] | None:
    prefix = f"{cache.key}/{publication.artifact_attempt}"
    payload = storage.read_evidence(f"{prefix}/evidence-index.json")
    if hashlib.sha256(payload).hexdigest() != publication.receipt_index_digest:
        raise IntegrityError("Finalization evidence index digest differs from admission")
    retained = TypeAdapter(dict[str, JsonValue]).validate_json(payload)
    if retained != index or index.get("attempt") != publication.artifact_attempt:
        raise IntegrityError("Finalization evidence differs from its admitted materialization")
    if (
        index.get("config") != cache.settings
        or index.get("stage_implementation") != cache.scoped_versions
        or TypeAdapter(dict[str, JsonValue]).validate_json(
            storage.read_evidence(f"{prefix}/stage-config.json")
        )
        != cache.settings
    ):
        raise IntegrityError("Admitted finalization configuration or implementation differs")
    if index.get("implementation") != cache.versions:
        return None
    for name, digest in cache.versions.items():
        payload = storage.read_evidence(f"implementation/{digest}/{name}")
        if hashlib.sha256(payload).hexdigest() != digest:
            raise IntegrityError(f"Admitted finalization implementation differs: {name}")
    frame = storage.read_version(publication.output)
    if not isinstance(frame, pd.DataFrame):
        raise IntegrityError("Admitted finalization reference is not a table")
    receipts = TypeAdapter(dict[str, str]).validate_json(storage.read_evidence(f"{prefix}/receipts.json"))
    if receipts != index.get("receipts"):
        raise IntegrityError("Finalization attempt receipts differ from admitted evidence")
    if evidence_attribute is None:
        if receipts:
            raise IntegrityError("Unexpected projection sidecar receipt")
    else:
        filename = f"{evidence_attribute.replace('_', '-')}.json"
        key = f"{prefix}/{filename}"
        uri = storage.object_store().uri(f"evidence/{hashlib.sha256(key.encode()).hexdigest()}")
        if (
            receipts != {filename: uri}
            or evidence_attribute not in frame.attrs
            or storage.read_evidence(key) != json.dumps(frame.attrs[evidence_attribute]).encode()
        ):
            raise IntegrityError("Finalization sidecar differs from the admitted table")
    return ReusedTable(schema.validate(frame), cache.signature, retained, publication.output)


def recover_admitted_finalization[Schema: pa.DataFrameModel](
    instance: DagsterInstance,
    asset_key: AssetKey,
    partition: str,
    storage: DataFrameIOManager,
    cache: TableCache,
    *,
    upstreams: tuple[UpstreamTableReference, ...],
    registration_digest: str,
    evidence_attribute: str | None,
    output_schema: type[Schema],
) -> ReusedTable[Schema] | None:
    records_filter = AssetRecordsFilter(asset_key=asset_key, asset_partitions=[partition])
    cursor = None
    while True:
        page = instance.fetch_materializations(records_filter, limit=100, cursor=cursor)
        for record in page.records:
            materialization = record.asset_materialization
            if (
                materialization is None
                or materialization.asset_key != asset_key
                or materialization.partition != partition
            ):
                continue
            metadata = materialization.metadata
            tag = metadata.get(FINALIZATION_PUBLICATION_METADATA)
            hit = metadata.get("cache_hit")
            signature = metadata.get("input_fingerprint")
            digest = metadata.get("codebench/content_digest")
            output_format = metadata.get("codebench/storage_format")
            evidence = metadata.get("evidence")
            if not (
                isinstance(tag, JsonMetadataValue)
                and isinstance(hit, BoolMetadataValue)
                and not hit.value
                and isinstance(signature, TextMetadataValue)
                and signature.value == cache.signature
                and isinstance(digest, TextMetadataValue)
                and isinstance(output_format, TextMetadataValue)
                and output_format.value == "parquet"
                and isinstance(evidence, JsonMetadataValue)
            ):
                continue
            try:
                publication = FinalizationPublicationV1.model_validate(tag.value)
            except ValidationError:
                continue
            producer = publication.step_attempt
            entry = record.event_log_entry
            if not (
                publication.input_signature == cache.signature
                and publication.output.format == output_format.value
                and publication.output.digest == digest.value
                and publication.registration_digest == registration_digest
                and set(publication.upstreams) == set(upstreams)
                and len(publication.upstreams) == len(upstreams)
                and producer.run_id == entry.run_id
                and producer.step_key == entry.step_key
                and materialization_attempt_succeeded(
                    instance, record, output_name="result", retry_number=producer.retry_number
                )
            ):
                continue
            try:
                index = TypeAdapter(dict[str, JsonValue]).validate_python(evidence.value)
                result = _verify_admitted_finalization(
                    storage,
                    cache,
                    index=index,
                    publication=publication,
                    evidence_attribute=evidence_attribute,
                    schema=output_schema,
                )
            except (FileNotFoundError, ValidationError) as error:
                raise IntegrityError("Admitted finalization artifacts are missing or malformed") from error
            if result is not None:
                return result
        if not page.has_more:
            return None
        cursor = page.cursor
