from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from dagster import AssetKey, DagsterInvalidConfigError, InputContext, OutputContext
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import BaseModel, JsonValue, RootModel

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    RegisteredConfig,
    configured_selection,
    current_configuration_record,
    mapping_key,
)
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.runtime_instance import dataset_instance


class NativeConfigConsumptionTests(TestCase):
    def test_filter_requires_valid_mapping_from_its_dagster_producer(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home).spec
        dataset_id = DatasetRegistry(home).register(spec)
        cycle = NativeDatasetCycle(home)
        job = cycle.definitions.get_implicit_global_asset_job_def()
        write_config = ConfigRunIOManager.handle_output
        key = AssetKey(["datasets", "filtered_commands"])
        mapping = mapping_key("filtered_commands")
        selection = configured_selection([key])
        payloads: tuple[tuple[dict[str, JsonValue], str], ...] = (
            ({"another_scope": {}}, "another_scope"),
            (
                {
                    "category_rules": spec.rules.model_dump(mode="json"),
                    "filtered_commands": {"unregistered_setting": True},
                },
                "unregistered_setting",
            ),
        )

        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            refreshed = cycle.definitions.resolve_job_def("refresh_dataset_configuration").execute_in_process(
                instance=instance, partition_key=dataset_id
            )
            self.assertTrue(refreshed.success)
            prepared = cycle.drain(instance, prepare_only=True, partition=dataset_id)
            self.assertIn("filtered_commands", prepared)

            current = job.execute_in_process(
                instance=instance,
                partition_key=dataset_id,
                asset_selection=[key],
            )
            self.assertTrue(current.success)
            self.assertEqual({event.asset_key for event in current.get_asset_materialization_events()}, {key})
            (materialization,) = current.asset_materializations_for_node(key.to_python_identifier())
            producer = current_configuration_record(instance, mapping, dataset_id)
            path = Path(home, "configuration-runs", producer.run_id, *mapping.path)
            original = path.read_bytes()
            path.unlink()
            try:
                missing = job.execute_in_process(
                    instance=instance, partition_key=dataset_id, asset_selection=[key], raise_on_error=False
                )
            finally:
                path.write_bytes(original)
            self.assertFalse(missing.success)
            (failure,) = missing.get_step_failure_events()
            error = failure.step_failure_data.error
            assert error is not None
            self.assertEqual(failure.step_key, key.to_python_identifier())
            self.assertIn('loading input "mapped_config"', error.to_string())
            self.assertIn("Missing configuration", error.to_string())

            for payload, diagnostic in payloads:

                def write_wrong_config(
                    manager: ConfigRunIOManager, context: OutputContext, obj: BaseModel
                ) -> None:
                    write_config(
                        manager,
                        context,
                        RootModel[dict[str, JsonValue]](payload) if context.asset_key == mapping else obj,
                    )

                with (
                    self.subTest(mapping=payload),
                    patch.object(ConfigRunIOManager, "handle_output", write_wrong_config),
                ):
                    wrong = job.execute_in_process(
                        instance=instance,
                        partition_key=dataset_id,
                        asset_selection=selection,
                        raise_on_error=False,
                    )
                    self.assertFalse(wrong.success)
                    (failure,) = wrong.get_step_failure_events()
                    self.assertEqual(failure.step_key, key.to_python_identifier())
                    error = failure.step_failure_data.error
                    assert error is not None
                    self.assertIn(diagnostic, error.to_string())

            override = {"ops": {key.to_python_identifier(): {"config": {"maximum_command_characters": 1}}}}
            with self.assertRaisesRegex(DagsterInvalidConfigError, "maximum_command_characters"):
                job.execute_in_process(
                    instance=instance,
                    partition_key=dataset_id,
                    run_config=override,
                    asset_selection=[key],
                )
            repeated_run = job.execute_in_process(
                instance=instance,
                partition_key=dataset_id,
                asset_selection=selection,
            )
            self.assertTrue(repeated_run.success)
            self.assertEqual(
                {event.asset_key for event in repeated_run.get_asset_materialization_events()}, set(selection)
            )
            (repeated,) = repeated_run.asset_materializations_for_node(key.to_python_identifier())
            self.assertTrue(repeated.metadata["cache_hit"].value)
            self.assertEqual(
                repeated.metadata["input_fingerprint"], materialization.metadata["input_fingerprint"]
            )

    def test_direct_policy_consumer_rejects_rules_from_another_registration(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home).spec
        dataset_id = DatasetRegistry(home).register(spec)
        other_spec = spec.model_copy(
            update={"rules": spec.rules.model_copy(update={"policy": "New policy."})}
        )
        other_registration = RegisteredConfig.from_spec(other_spec)
        cycle = NativeDatasetCycle(home)
        job = cycle.definitions.get_implicit_global_asset_job_def()
        proposed = AssetKey(["datasets", "proposed_annotations"])
        read_config = ConfigRunIOManager.load_input

        def read_other_registration(manager: ConfigRunIOManager, context: InputContext) -> BaseModel | None:
            if context.asset_key == GLOBAL_CONFIG:
                return other_registration
            return read_config(manager, context)

        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            refreshed = cycle.definitions.resolve_job_def("refresh_dataset_configuration").execute_in_process(
                instance=instance, partition_key=dataset_id
            )
            self.assertTrue(refreshed.success)
            self.assertIn("labeling_attempts", cycle.drain(instance, partition=dataset_id))
            with patch.object(ConfigRunIOManager, "load_input", read_other_registration):
                result = job.execute_in_process(
                    instance=instance,
                    partition_key=dataset_id,
                    asset_selection=[proposed],
                    raise_on_error=False,
                )
        self.assertFalse(result.success)
        (failure,) = result.get_step_failure_events()
        self.assertEqual(failure.step_key, proposed.to_python_identifier())
        error = failure.step_failure_data.error
        assert error is not None
        self.assertIn("Category rules differ from the run configuration", error.to_string())
