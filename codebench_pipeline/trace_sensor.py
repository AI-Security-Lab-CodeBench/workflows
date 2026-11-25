import os
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Final

import httpx2
from dagster import (
    DagsterInstance,
    DefaultSensorStatus,
    EventLogEntry,
    EventLogRecord,
    RunStatusChangeRecordsFilter,
    SensorDefinition,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
)
from pydantic import BaseModel, ConfigDict, FiniteFloat, ValidationError

from codebench_pipeline.trace_mlflow import (
    TraceDeliveryError,
    experiment_id,
    export_trace,
    mlflow_client,
)
from codebench_pipeline.trace_projection import TERMINAL_EVENTS, MissingTerminalEvent, project_run

_BATCH_SIZE: Final = 4
_LOG_PAGE_SIZE: Final = 256
_INTERVAL_SECONDS: Final = 15


class _Cursor(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    enabled_at: FiniteFloat
    storage_id: int = 0
    ready: bool = False


def _initial_cursor(context: SensorEvaluationContext) -> _Cursor:
    start_at = os.environ.get("CODEBENCH_MLFLOW_TRACE_START_AT")
    if start_at:
        try:
            start = datetime.fromisoformat(start_at)
            if start.tzinfo is None:
                raise TraceDeliveryError("CODEBENCH_MLFLOW_TRACE_START_AT requires a UTC offset")
            return _Cursor(enabled_at=start.timestamp())
        except ValueError:
            raise TraceDeliveryError("CODEBENCH_MLFLOW_TRACE_START_AT must be an ISO 8601 timestamp") from None
    return _Cursor(enabled_at=context.last_sensor_start_time or datetime.now(UTC).timestamp())


def _terminals(instance: DagsterInstance, cursor: _Cursor) -> list[EventLogRecord]:
    events = [
        record
        for event_type in TERMINAL_EVENTS
        for record in instance.fetch_run_status_changes(
            RunStatusChangeRecordsFilter(
                event_type=event_type,
                after_timestamp=cursor.enabled_at,
                after_storage_id=cursor.storage_id,
            ),
            ascending=True,
            limit=_BATCH_SIZE,
        ).records
    ]
    return sorted(events, key=lambda event: event.storage_id)[:_BATCH_SIZE]


def _run_log(instance: DagsterInstance, run_id: str) -> Iterator[EventLogEntry]:
    cursor = None
    while True:
        page = instance.get_records_for_run(run_id, cursor=cursor, limit=_LOG_PAGE_SIZE)
        yield from (record.event_log_entry for record in page.records)
        if not page.has_more:
            return
        cursor = page.cursor


def _result(cursor: _Cursor, reason: str) -> SensorResult:
    return SensorResult(cursor=cursor.model_dump_json(), skip_reason=SkipReason(reason))


def _observe(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    uri = os.environ.get("MLFLOW_TRACKING_URI", "").strip()
    if not uri:
        return SkipReason("MLFLOW_TRACKING_URI is not configured.")
    cursor = _Cursor.model_validate_json(context.cursor) if context.cursor else _initial_cursor(context)
    terminals = _terminals(context.instance, cursor)
    if not terminals and cursor.ready:
        return _result(cursor, "MLflow trace observation ready; no new terminal runs.")

    exported = 0
    try:
        with mlflow_client(uri) as client:
            experiment = experiment_id(client)
            for event in terminals:
                entry = event.event_log_entry
                projection = project_run(entry, _run_log(context.instance, entry.run_id), cursor.enabled_at)
                if projection.rejected_graphs:
                    context.log.warning(
                        "Ignored %d invalid graph records for run %s.",
                        projection.rejected_graphs, entry.run_id,
                    )
                if projection.spans:
                    export_trace(client, experiment, projection.spans)
                    exported += 1
                cursor = _Cursor(enabled_at=cursor.enabled_at, storage_id=event.storage_id, ready=True)
    except (
        httpx2.HTTPError, httpx2.InvalidURL, ValidationError, TraceDeliveryError, MissingTerminalEvent
    ) as error:
        detail = str(error) if isinstance(error, TraceDeliveryError) else type(error).__name__
        reason = f"MLflow trace observation deferred; unacknowledged runs remain pending ({detail})."
        context.log.warning(reason)
        return _result(cursor, reason)
    return _result(
        _Cursor(enabled_at=cursor.enabled_at, storage_id=cursor.storage_id, ready=True),
        f"MLflow trace observation ready; acknowledged {exported} run traces.",
    )


def mlflow_trace_sensor() -> SensorDefinition:
    return SensorDefinition(
        name="mlflow_run_trace_projection",
        description="Project completed Dagster runs to MLflow. Never launches runs.",
        evaluation_fn=_observe,
        minimum_interval_seconds=_INTERVAL_SECONDS,
        default_status=(
            DefaultSensorStatus.RUNNING if os.environ.get("MLFLOW_TRACKING_URI", "").strip()
            else DefaultSensorStatus.STOPPED
        ),
    )
