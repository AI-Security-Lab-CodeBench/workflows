import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from dagster import AssetKey, OutputContext
from guarded_labeling_fixture import LabelingFixture
from pydantic import JsonValue

from codebench_pipeline.config_assets import ConfigRunIOManager, configured_selection, mapping_key
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.definitions import build_definitions, dataset_run_config, release_run_config
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.runtime_instance import dataset_instance


class NativeConfigConsumptionTests(TestCase):
    def test_settings_only_assets_require_valid_mapping_from_the_same_run(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home).spec
        dataset_id = DatasetRegistry(home).register(spec)
        self.assertEqual(run_registered(dataset_id, home).status, "prepared")
        with HumanReviewStore(home).reader() as tx:
            token = tx.get_current_batch(dataset_id).completion
        assert token is not None

        definitions = build_definitions(home)
        prepare_job = definitions.resolve_job_def("build_dataset")
        release_job = definitions.resolve_job_def("release_dataset")
        prepare_config = dataset_run_config(spec, home)
        release_config = release_run_config(spec, home, token)
        write_config = ConfigRunIOManager.handle_output
        bad_settings: tuple[tuple[str, dict[str, JsonValue]], ...] = (
            ("merged_command_candidates", {"unregistered_setting": True}),
            ("labeling_failures", {"unregistered_setting": True}),
            ("deduplicated_annotations", {"unregistered_setting": True}),
            ("human_review_decisions", {"authorization": token.model_dump_json()}),
        )

        with dataset_instance(home) as instance:
            for stage, invalid in bad_settings:
                job, config = (
                    (release_job, release_config)
                    if stage == "human_review_decisions"
                    else (prepare_job, prepare_config)
                )
                key = AssetKey(["datasets", stage])
                mapping = mapping_key(stage)
                selection = configured_selection([key])
                with self.subTest(stage=stage, mapping="missing"):
                    missing = job.execute_in_process(
                        instance=instance,
                        partition_key=dataset_id,
                        run_config=config,
                        asset_selection=[key],
                        raise_on_error=False,
                    )
                    failures = missing.get_step_failure_events()
                    self.assertFalse(missing.success)
                    self.assertEqual(len(failures), 1)
                    error = failures[0].step_failure_data.error
                    assert error is not None
                    self.assertIn(
                        f"Missing configuration {mapping.to_user_string()} in this run", error.to_string()
                    )

                payloads: tuple[tuple[dict[str, JsonValue], str], ...] = (
                    ({"another_scope": {}}, f"Expected same-run settings scoped to {stage}"),
                    (
                        {stage: invalid},
                        "Scoped review settings cannot supply run authorization"
                        if stage == "human_review_decisions"
                        else "unregistered_setting",
                    ),
                )
                for payload, diagnostic in payloads:

                    def write_wrong_config(
                        manager: ConfigRunIOManager, context: OutputContext, obj: str
                    ) -> None:
                        write_config(
                            manager, context, json.dumps(payload) if context.asset_key == mapping else obj
                        )

                    with (
                        self.subTest(stage=stage, mapping=payload),
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
                        failures = wrong.get_step_failure_events()
                        self.assertEqual(len(failures), 1)
                        self.assertEqual(failures[0].step_key, key.to_python_identifier())
                        error = failures[0].step_failure_data.error
                        assert error is not None
                        self.assertIn(diagnostic, error.to_string())

                with self.subTest(stage=stage, mapping="current"):
                    current = job.execute_in_process(
                        instance=instance,
                        partition_key=dataset_id,
                        run_config=config,
                        asset_selection=selection,
                    )
                    self.assertTrue(current.success)
                    self.assertEqual(
                        {event.asset_key for event in current.get_asset_materialization_events()},
                        set(selection),
                    )
