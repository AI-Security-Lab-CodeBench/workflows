import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from dagster import AssetKey, OutputContext
from guarded_labeling_fixture import LabelingFixture
from pydantic import BaseModel, JsonValue

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    configured_selection,
    mapping_key,
)
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig


class NativeConfigConsumptionTests(TestCase):
    def test_filter_requires_valid_mapping_from_the_same_run(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home).spec
        dataset_id = DatasetRegistry(home).register(spec)
        self.assertEqual(run_registered(dataset_id, home).status, "prepared")
        job = build_definitions(home).resolve_job_def("build_dataset")
        config = dataset_run_config(spec, home)
        write_config = ConfigRunIOManager.handle_output
        key = AssetKey(["datasets", "filtered_commands"])
        mapping = mapping_key("filtered_commands")
        selection = configured_selection([key], declared=spec.has_declared_collection)
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
            missing = job.execute_in_process(
                instance=instance,
                partition_key=dataset_id,
                run_config=config,
                asset_selection=[key],
                raise_on_error=False,
            )
            self.assertFalse(missing.success)
            (failure,) = missing.get_step_failure_events()
            error = failure.step_failure_data.error
            assert error is not None
            self.assertIn(f"Missing configuration {mapping.to_user_string()} in this run", error.to_string())

            for payload, diagnostic in payloads:

                def write_wrong_config(
                    manager: ConfigRunIOManager, context: OutputContext, obj: BaseModel
                ) -> None:
                    write_config(manager, context, obj)
                    if context.asset_key == mapping:
                        path = Path(manager.home, "configuration-runs", context.run_id, *mapping.path)
                        path.write_text(json.dumps(payload), encoding="utf-8")

                with (
                    self.subTest(mapping=payload),
                    patch.object(ConfigRunIOManager, "handle_output", write_wrong_config),
                ):
                    wrong = job.execute_in_process(
                        instance=instance,
                        partition_key=dataset_id,
                        run_config=config,
                        asset_selection=selection,
                        raise_on_error=False,
                    )
                    self.assertFalse(wrong.success)
                    (failure,) = wrong.get_step_failure_events()
                    self.assertEqual(failure.step_key, key.to_python_identifier())
                    error = failure.step_failure_data.error
                    assert error is not None
                    self.assertIn(diagnostic, error.to_string())

            current = job.execute_in_process(
                instance=instance, partition_key=dataset_id, run_config=config, asset_selection=selection
            )
            self.assertTrue(current.success)
            self.assertEqual(
                {event.asset_key for event in current.get_asset_materialization_events()}, set(selection)
            )

    def test_direct_policy_consumer_rejects_rules_from_another_registration(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home).spec
        dataset_id = DatasetRegistry(home).register(spec)
        self.assertEqual(run_registered(dataset_id, home).status, "prepared")
        other_spec = spec.model_copy(
            update={"rules": spec.rules.model_copy(update={"policy": "New policy."})}
        )
        other_registration = DeclaredGlobalConfig.from_spec(other_spec)
        job = build_definitions(home, specification=spec).resolve_job_def("build_dataset")
        proposed = AssetKey(["datasets", "proposed_annotations"])
        write_config = ConfigRunIOManager.handle_output

        def write_other_registration(
            manager: ConfigRunIOManager, context: OutputContext, obj: BaseModel
        ) -> None:
            write_config(manager, context, other_registration if context.asset_key == GLOBAL_CONFIG else obj)

        with (
            dataset_instance(home) as instance,
            patch.object(ConfigRunIOManager, "handle_output", write_other_registration),
        ):
            result = job.execute_in_process(
                instance=instance,
                partition_key=dataset_id,
                asset_selection=configured_selection([proposed], declared=True),
                run_config=dataset_run_config(spec, home),
                raise_on_error=False,
            )
        self.assertFalse(result.success)
        (failure,) = result.get_step_failure_events()
        self.assertEqual(failure.step_key, proposed.to_python_identifier())
        error = failure.step_failure_data.error
        assert error is not None
        self.assertIn("Category rules differ from the run configuration", error.to_string())
