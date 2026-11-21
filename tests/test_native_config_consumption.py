from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from dagster import AssetKey, DagsterInvalidConfigError, InputContext, OutputContext
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import BaseModel, JsonValue, RootModel, ValidationError

from codebench_pipeline.config_assets import (
    ConfigRunIOManager,
    RegisteredConfig,
    configured_selection,
    current_configuration_record,
    mapping_key,
)
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.labeling_stage_settings import ProposalRules
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import FilterConfig
from codebench_pipeline.typed_config import ConfigArtifact


class NativeConfigConsumptionTests(TestCase):
    def test_filter_requires_valid_mapping_from_its_dagster_producer(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home, native=True).spec
        dataset_id = DatasetRegistry(home).register(spec)
        cycle = NativeDatasetCycle(home)
        job = cycle.definitions.get_implicit_global_asset_job_def()
        write_config = ConfigRunIOManager.handle_output
        key = AssetKey(["datasets", "filtered_commands"])
        mapping = mapping_key("filtered_commands")
        selection = configured_selection([key])
        expected = FilterConfig.project(RegisteredConfig.from_spec(spec), spec.rules).model_dump(mode="json")
        for field, value in (("another_scope", {}), ("category_rules", spec.rules.model_dump(mode="json"))):
            with self.subTest(field=field), self.assertRaisesRegex(ValidationError, field):
                FilterConfig.model_validate({**expected, field: value})

        def write_wrong_config(manager: ConfigRunIOManager, context: OutputContext, obj: BaseModel) -> None:
            if context.asset_key == mapping:
                obj = RootModel[dict[str, JsonValue]](
                    {
                        **expected,
                        "filtered_commands": {
                            **spec.filtering.model_dump(mode="json"),
                            "unregistered_setting": True,
                        },
                    }
                )
            write_config(manager, context, obj)

        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            refreshed = cycle.definitions.resolve_job_def("refresh_dataset_configuration").execute_in_process(
                instance=instance, partition_key=dataset_id
            )
            self.assertTrue(refreshed.success)
            prepared = cycle.drain(instance, prepare_only=True, partition=dataset_id)
            self.assertIn("filtered_commands", prepared)

            event = instance.get_latest_materialization_event(key)
            assert event is not None and event.asset_materialization is not None
            materialization = event.asset_materialization
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

            with patch.object(ConfigRunIOManager, "handle_output", write_wrong_config):
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
            self.assertIn("unregistered_setting", error.to_string())

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
            (repeated,) = repeated_run.asset_materializations_for_node(key.to_python_identifier())
            self.assertTrue(repeated.metadata["cache_hit"].value)
            self.assertEqual(
                repeated.metadata["input_fingerprint"], materialization.metadata["input_fingerprint"]
            )

    def test_scoped_policy_consumer_rejects_routes_from_another_registration(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home, native=True).spec
        dataset_id = DatasetRegistry(home).register(spec)
        risk, *rest = spec.rules.risks
        other_risk = risk.model_copy(update={"command_patterns": ["^fixture-different-route$"]})
        other_policy = ProposalRules.from_rules(spec.rules.model_copy(update={"risks": [other_risk, *rest]}))
        cycle = NativeDatasetCycle(home)
        job = cycle.definitions.get_implicit_global_asset_job_def()
        proposed = AssetKey(["datasets", "proposed_annotations"])
        mapping = mapping_key("proposed_annotations")
        read_scope = ConfigRunIOManager.load_scope

        def read_other_policy(manager: ConfigRunIOManager, context: InputContext) -> ConfigArtifact:
            if context.asset_key == mapping:
                return other_policy
            return read_scope(manager, context)

        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            refreshed = cycle.definitions.resolve_job_def("refresh_dataset_configuration").execute_in_process(
                instance=instance, partition_key=dataset_id
            )
            self.assertTrue(refreshed.success)
            self.assertIn("labeling_attempts", cycle.drain(instance, partition=dataset_id))
            with patch.object(ConfigRunIOManager, "load_scope", read_other_policy):
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
        self.assertIn("Mapped proposal rules differ from the run configuration", error.to_string())
