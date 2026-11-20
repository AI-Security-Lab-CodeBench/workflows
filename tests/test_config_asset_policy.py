import tempfile
import unittest
from pathlib import Path

from dagster import AssetKey
from guarded_labeling_fixture import LabelingFixture

from codebench_pipeline.config_assets import configured_selection, current_configuration_record, mapping_key
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import declared_source_config
from codebench_pipeline.storage import IntegrityError


class ConfigAssetPolicyTests(unittest.TestCase):
    def test_selective_documentation_capture_uses_verified_pinned_mapping_across_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            original = LabelingFixture(self, home).spec
            pinned = original.model_copy(
                update={"rules": original.rules.model_copy(update={"category_name": "Pinned fixture policy"})}
            )
            registry = DatasetRegistry(home)
            dataset_id = registry.register(original)
            stale_launch = dataset_run_config(original)
            registry.register(pinned)
            capture = AssetKey(["datasets", "documentation", "capture_documentation"])
            rules = AssetKey(["datasets", "category_rules"])
            definitions = build_definitions(home)
            job = definitions.resolve_job_def("build_documentation")
            selected_job = definitions.get_implicit_global_asset_job_def()
            selection = configured_selection([rules, capture])
            current_launch = dataset_run_config(pinned)
            config = declared_source_config(pinned, "documentation")

            with dataset_instance(home) as instance:
                instance.add_dynamic_partitions("dataset_builds", [dataset_id])
                with self.assertRaisesRegex(IntegrityError, "no longer matches the registered specification"):
                    job.execute_in_process(
                        instance=instance,
                        partition_key=dataset_id,
                        asset_selection=selection,
                        run_config=stale_launch,
                    )

                result = job.execute_in_process(
                    instance=instance,
                    partition_key=dataset_id,
                    asset_selection=selection,
                    run_config=current_launch,
                )
                self.assertEqual(
                    {event.asset_key for event in result.get_asset_materialization_events()}, set(selection)
                )
                (materialization,) = result.asset_materializations_for_node(capture.to_python_identifier())
                actual = materialization.metadata["input_fingerprint"].value
                self.assertEqual(
                    actual, stage_signature("capture_documentation", config, {"category_rules": pinned.rules})
                )
                self.assertNotEqual(
                    actual,
                    stage_signature("capture_documentation", config, {"category_rules": original.rules}),
                )

                selected = selected_job.execute_in_process(
                    instance=instance,
                    partition_key=dataset_id,
                    asset_selection=[capture],
                )
                self.assertTrue(selected.success)
                (event,) = selected.get_asset_materialization_events()
                self.assertEqual(event.asset_key, capture)
                (captured,) = selected.asset_materializations_for_node(capture.to_python_identifier())
                self.assertEqual(captured.metadata["input_fingerprint"].value, actual)

                mapping = mapping_key("documentation_crawl")
                producer = current_configuration_record(instance, mapping, dataset_id)
                self.assertEqual(producer.run_id, result.run_id)
                payload = home.joinpath("configuration-runs", producer.run_id, *mapping.path)
                payload.write_bytes(payload.read_bytes() + b"\n")
                altered = selected_job.execute_in_process(
                    instance=instance,
                    partition_key=dataset_id,
                    asset_selection=[capture],
                    raise_on_error=False,
                )
                self.assertFalse(altered.success)
                self.assertEqual(altered.get_asset_materialization_events(), [])
                (failure,) = altered.get_step_failure_events()
                self.assertEqual(failure.step_key, capture.to_python_identifier())
                error = failure.step_failure_data.error
                assert error is not None
                self.assertIn('loading input "mapped_config"', error.to_string())
                self.assertIn("Persisted configuration differs from its producer evidence", error.to_string())
