"""Resolve the materialization that identifies an asset input's bytes."""

from typing import Protocol

from dagster import (
    AssetKey,
    AssetRecordsFilter,
    DagsterEventType,
    DagsterInstance,
    EventLogRecord,
    InputContext,
    OutputContext,
)
from dagster._core.execution.context.output import RUN_ID_PLACEHOLDER
from dagster._core.execution.plan.handle import StepHandle
from dagster._core.execution.plan.outputs import StepOutputHandle
from dagster._core.execution.plan.plan import ExecutionPlan


class _OutputContextLookup(Protocol):
    @property
    def execution_plan(self) -> ExecutionPlan: ...

    def get_output_context(self, step_output_handle: StepOutputHandle) -> OutputContext: ...


def configuration_producer(step: _OutputContextLookup, key: AssetKey) -> str | None:
    step_key = key.to_python_identifier()
    if not step.execution_plan.has_step(StepHandle.parse_from_key(step_key)):
        return None
    return producer_run_id(step.get_output_context(StepOutputHandle(step_key, "result")))


def producer_run_id(upstream: OutputContext | None) -> str | None:
    if upstream is None:
        return None
    run_id = upstream.run_id
    return None if run_id == RUN_ID_PLACEHOLDER else run_id


def input_materialization(context: InputContext) -> EventLogRecord | None:
    upstream = context.upstream_output
    partition = context.asset_partition_key if context.has_asset_partitions else None
    if upstream is not None and (run_id := producer_run_id(upstream)) is not None:
        records = context.instance.get_records_for_run(
            run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION, ascending=False
        ).records
        return next(
            (
                record
                for record in records
                if record.event_log_entry.step_key == upstream.step_key
                and (asset := record.asset_materialization) is not None
                and asset.asset_key == context.asset_key
                and asset.partition == partition
            ),
            None,
        )
    return current_materialization(context.instance, context.asset_key, partition)


def current_materialization(
    instance: DagsterInstance, key: AssetKey, partition: str | None
) -> EventLogRecord | None:
    records = instance.fetch_materializations(
        AssetRecordsFilter(asset_key=key, asset_partitions=[partition] if partition is not None else None),
        limit=1,
    ).records
    return records[0] if records else None
