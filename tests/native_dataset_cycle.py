import logging
from pathlib import Path

from dagster import AssetKey, AssetSelection, AutomationConditionSensorDefinition, DagsterInstance, RunRequest
from dagster._core.definitions.asset_daemon_cursor import AssetDaemonCursor
from dagster._core.definitions.automation_tick_evaluation_context import AutomationTickEvaluationContext

from codebench_pipeline.dataset_dependencies import DECLARED_INPUT_STAGES
from codebench_pipeline.definitions import build_definitions


class NativeDatasetCycle:
    """Execute the installed engine's requests locally, without a daemon in the test process."""

    def __init__(self, home: Path) -> None:
        self.definitions = build_definitions(home)
        sensor = self.definitions.get_sensor_def("dataset_build_requests")
        assert isinstance(sensor, AutomationConditionSensorDefinition)
        self.sensor = sensor
        self.cursor = AssetDaemonCursor.empty()

    def tick(self, instance: DagsterInstance, *, prepare_only: bool = False) -> tuple[RunRequest, ...]:
        selection = self.sensor.asset_selection
        if prepare_only:
            selection &= AssetSelection.assets(["datasets", "filtered_commands"]).upstream()
        requests, self.cursor, _ = AutomationTickEvaluationContext(
            evaluation_id=self.cursor.evaluation_id + 1,
            instance=instance,
            asset_graph=self.definitions.resolve_asset_graph(),
            cursor=self.cursor,
            materialize_run_tags={},
            observe_run_tags={},
            auto_observe_asset_keys=set(),
            asset_selection=selection,
            logger=logging.getLogger(__name__),
            emit_backfills=False,
        ).evaluate()
        return tuple(requests)

    def drain(
        self,
        instance: DagsterInstance,
        *,
        prepare_only: bool = False,
        partition: str | None = None,
        requested_assets: set[AssetKey] | None = None,
    ) -> dict[str, bool]:
        job = self.definitions.get_implicit_global_asset_job_def()
        materialized: dict[str, bool] = {}
        for _ in range(20):
            requests = self.tick(instance, prepare_only=prepare_only)
            if not requests:
                return materialized
            for request in requests:
                if partition is not None and request.partition_key != partition:
                    raise AssertionError(f"Unexpected native request for partition {request.partition_key}")
                if request.run_config:
                    raise AssertionError(
                        "Native processing should consume persisted inputs without launch config"
                    )
                if requested_assets is not None:
                    assert request.asset_selection is not None
                    requested_assets.update(request.asset_selection)
                result = job.execute_in_process(
                    instance=instance,
                    partition_key=request.partition_key,
                    asset_selection=request.asset_selection,
                    run_config=request.run_config,
                    tags=request.tags,
                )
                for event in result.get_asset_materialization_events():
                    asset = event.step_materialization_data.materialization
                    stage = "/".join(asset.asset_key.path[1:])
                    if stage in DECLARED_INPUT_STAGES:
                        materialized[stage] = asset.metadata["cache_hit"].value is True
        raise AssertionError("Native automation did not settle in twenty evaluation ticks")
