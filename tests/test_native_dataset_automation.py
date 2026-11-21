import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from dagster import AssetKey, AssetRecordsFilter
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle

from codebench_pipeline.dataset_cli import execute_registered_build
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.runtime_instance import dataset_instance


class NativeDatasetAutomationTests(unittest.TestCase):
    def test_refresh_drives_native_partition_requests_and_effective_changes_relabel(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, native=True, category_confidence="medium")
        spec = fixture.spec
        registry = DatasetRegistry(home)
        partition = registry.register(spec)
        other = "unconfigured-peer"
        cycle = NativeDatasetCycle(home)
        labeling = AssetKey(["datasets", "labeling_attempts"])

        def refresh() -> str:
            with redirect_stdout(output := StringIO()):
                execute_registered_build(partition, home)
            receipt = json.loads(output.getvalue())
            self.assertEqual(receipt["status"], "configuration_refreshed")
            return str(receipt["configuration_run_id"])

        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [other])
            refresh_id = refresh()
            refresh_run = instance.get_run_by_id(refresh_id)
            assert refresh_run is not None
            self.assertEqual(refresh_run.job_name, "refresh_dataset_configuration")
            self.assertEqual(refresh_run.run_config, {})
            self.assertEqual(fixture.remote_rows, [])
            self.assertIn("labeling_attempts", cycle.drain(instance, partition=partition))
            inference_count = len(fixture.remote_rows)
            self.assertGreater(inference_count, 0)
            print(f"NATIVE before: partition={partition} inference={inference_count}; peer={other} absent")

            record_filter = AssetRecordsFilter(asset_key=labeling, asset_partitions=[partition])
            (first_labeling,) = instance.fetch_materializations(record_filter, limit=1).records
            unchanged_requests: set[AssetKey] = set()
            refresh()
            cycle.drain(instance, partition=partition, requested_assets=unchanged_requests)
            self.assertNotIn(labeling, unchanged_requests)
            (latest_labeling,) = instance.fetch_materializations(record_filter, limit=1).records
            self.assertEqual(latest_labeling.storage_id, first_labeling.storage_id)
            print("NATIVE unchanged: no labeling request or materialization")

            model = spec.annotation.model.model_copy(
                update={"timeout_seconds": spec.annotation.model.timeout_seconds + 1}
            )
            registry.register(
                spec.model_copy(update={"annotation": spec.annotation.model_copy(update={"model": model})})
            )
            refresh()
            changed_requests: set[AssetKey] = set()
            cycle.drain(instance, partition=partition, requested_assets=changed_requests)
            self.assertIn(labeling, changed_requests)
            (changed_labeling,) = instance.fetch_materializations(record_filter, limit=1).records
            self.assertGreater(changed_labeling.storage_id, first_labeling.storage_id)
            added = len(fixture.remote_rows) - inference_count
            self.assertEqual(added, inference_count)
            print(f"NATIVE changed labeler: relabeled partition={partition}; new inference={added}")
