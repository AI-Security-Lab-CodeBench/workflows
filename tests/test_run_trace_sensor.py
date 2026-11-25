import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time
from unittest.mock import patch

from dagster import DagsterInstance, DefaultSensorStatus, build_sensor_context
from test_run_trace_projection import MlflowWire, cli_job, read_spans

from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.trace_sensor import mlflow_trace_sensor


class TraceSensorTests(unittest.TestCase):
    def test_sensor_defaults_leave_materialization_sensors_unchanged(self) -> None:
        with TemporaryDirectory() as directory:
            for uri, status in (("", DefaultSensorStatus.STOPPED), ("http://unused", DefaultSensorStatus.RUNNING)):
                with self.subTest(uri=uri), patch.dict(os.environ, {"MLFLOW_TRACKING_URI": uri}):
                    definitions = build_definitions(Path(directory))
                    self.assertEqual(definitions.get_sensor_def("mlflow_run_trace_projection").default_status, status)
                    self.assertEqual(
                        definitions.get_sensor_def("dataset_build_requests").default_status, DefaultSensorStatus.STOPPED,
                    )
                    self.assertEqual(
                        definitions.get_sensor_def("dataset_release_requests").default_status, DefaultSensorStatus.RUNNING,
                    )
                    if not uri:
                        tick = mlflow_trace_sensor().evaluate_tick(build_sensor_context())
                        self.assertFalse(tick.run_requests)
                        self.assertIsNone(tick.cursor)

    def test_terminal_index_pages_across_cancellation_and_success(self) -> None:
        private = "private cancellation report"
        wire = MlflowWire()
        with (
            TemporaryDirectory() as directory, DagsterInstance.local_temp(directory) as instance,
            wire.serve() as uri, patch.dict(os.environ, {
                "MLFLOW_TRACKING_URI": uri, "CODEBENCH_MLFLOW_TRACE_START_AT": "",
            }),
        ):
            enable = time()
            sensor = mlflow_trace_sensor()
            with build_sensor_context(instance=instance, last_sensor_start_time=enable) as ctx:
                ready = sensor.evaluate_tick(ctx)
            self.assertIn("ready; acknowledged 0", ready.skip_message or "")
            run_ids: list[str] = []
            for index in range(6):
                if index == 4:
                    run_id = cli_job.execute_in_process(instance=instance).run_id
                else:
                    canceled = instance.create_run_for_job(cli_job)
                    instance.report_run_canceled(canceled, message=private)
                    run_id = canceled.run_id
                run_ids.append(run_id)
            cursor = ready.cursor
            for expected in (4, 2):
                with build_sensor_context(instance=instance, cursor=cursor) as ctx:
                    tick = sensor.evaluate_tick(ctx)
                self.assertIn(f"acknowledged {expected}", tick.skip_message or "")
                cursor = tick.cursor
            self.assertEqual(
                [read_spans(post)[0]["name"] for post in wire.posts],
                [f"capture_rules [{run_id}]" for run_id in run_ids],
            )
            self.assertEqual([len(read_spans(post)) for post in wire.posts], [1, 1, 1, 1, 2, 1])
            self.assertNotIn(private, json.dumps(wire.posts))
