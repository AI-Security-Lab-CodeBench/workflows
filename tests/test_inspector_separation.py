import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from dagster import AssetKey, DataVersion, IOManagerDefinition, Output, asset, materialize
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import JsonValue, TypeAdapter
from test_dataframe_io import s3_fixture
from test_declared_dataset_graph import BUNDLE, fixture_spec
from test_review_release import attest_fixture_release

from codebench_pipeline.artifact_views import ArtifactJSON, ArtifactQuery, ArtifactTable
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import CONFIG_DIRECTORY, GLOBAL_CONFIG, mapping_key
from codebench_pipeline.data_view_models import DatasetQueryRequest, DatasetViewError, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, asset_storage_key
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, RULES_TYPE
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration, run_registered
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.keenable_policy import KeenableError
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.runtime_instance import dataset_instance


class InspectorSeparationTests(unittest.TestCase):
    def test_s3_data_readers_check_current_pointer_only_for_evidenced_outputs(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        storage, s3 = self.enterContext(s3_fixture())
        spec = LabelingFixture(self, home).spec.model_copy(update={"storage": storage})
        registry = DatasetRegistry(home)
        dataset = registry.register(spec)
        stage = "filtered_commands"
        key = ("datasets", stage)
        pointer = f"/{storage.bucket}/{storage.prefix}/{asset_storage_key(key, dataset)}"
        query = ArtifactQuery(asset_key=key, partition_key=dataset)
        request = DatasetQueryRequest(dataset_id=dataset)
        self.assertIn(stage, run_registered(dataset, home, prepare_only=True).materialized_stages)
        original_pointer = s3.objects[pointer]
        cycle = NativeDatasetCycle(home)

        with dataset_instance(home) as instance:
            self.assertEqual(cycle.tick(instance, prepare_only=True), ())
            data = DatasetDataService(home, instance)
            original = data.artifacts.query(query)
            assert isinstance(original, ArtifactTable)
            self.assertGreater(original.page.total_rows, 0)
            self.assertIsNone(original.publication_state)
            current = data.query(stage, request)
            assert isinstance(current, TableResponse)
            self.assertIsNone(current.publication_state)

            changed = spec.model_copy(
                update={"filtering": spec.filtering.model_copy(update={"maximum_command_characters": 1})}
            )
            registry.register(changed)
            s3.reads.clear()
            unproven = data.artifacts.query(query)
            self.assertEqual(unproven.publication_state, "stale")
            self.assertNotIn(pointer, s3.reads)

            refresh_registered_configuration(dataset, home)
            self.assertIn(stage, cycle.drain(instance, prepare_only=True, partition=dataset))
            latest = data.artifacts.query(query)
            assert isinstance(latest, ArtifactTable)
            self.assertNotEqual(latest.snapshot_id, original.snapshot_id)
            self.assertIsNone(latest.publication_state)
            current = data.query(stage, request)
            assert isinstance(current, TableResponse)
            self.assertEqual(current.snapshot_id, latest.snapshot_id)
            self.assertIsNone(current.publication_state)

            s3.objects[pointer] = original_pointer
            moved = data.query(stage, request)
            assert isinstance(moved, TableResponse)
            self.assertEqual((moved.snapshot_id, moved.publication_state), (original.snapshot_id, "stale"))
            mismatch = data.artifacts.query(query)
            assert isinstance(mismatch, ArtifactTable)
            self.assertEqual(
                (mismatch.snapshot_id, mismatch.publication_state), (latest.snapshot_id, "stale")
            )

            del s3.objects[pointer]
            self.assertEqual(data.artifacts.query(query).publication_state, "stale")
            with self.assertRaises(DatasetViewError) as missing:
                data.query(stage, request)
            self.assertEqual(missing.exception.code, "not_materialized")

            s3.reads.clear()
            archived = data.artifacts.query(query.model_copy(update={"snapshot_id": original.snapshot_id}))
            assert isinstance(archived, ArtifactTable)
            self.assertEqual(
                (archived.snapshot_id, archived.publication_state), (original.snapshot_id, "stale")
            )
            self.assertNotIn(pointer, s3.reads)

    def test_processing_data_currentness_uses_effective_approval(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home, category_confidence="medium").spec
        registry = DatasetRegistry(home)
        dataset = registry.register(spec)
        run_registered(dataset, home)
        cycle = NativeDatasetCycle(home)
        stages = (
            "merged_command_candidates",
            "filtered_commands",
            "labeling_attempts",
            "proposed_annotations",
            "labeling_failures",
            "deduplicated_annotations",
            "category_eligible_annotations",
        )
        with dataset_instance(home) as instance:
            self.assertEqual(cycle.tick(instance), ())
            data = DatasetDataService(home, instance)

            def inspect() -> dict[str, tuple[str, str | None]]:
                before = len(instance.get_runs())
                status: dict[str, tuple[str, str | None]] = {}
                with patch(
                    "dagster.JobDefinition.execute_in_process",
                    side_effect=AssertionError("Data browsing launched a run"),
                ):
                    for stage in stages:
                        artifact = data.artifacts.query(
                            ArtifactQuery(asset_key=("datasets", stage), partition_key=dataset)
                        )
                        assert isinstance(artifact, ArtifactTable)
                        status[stage] = (artifact.snapshot_id, artifact.publication_state)
                    filtered = data.query("filtered_commands", DatasetQueryRequest(dataset_id=dataset))
                    assert isinstance(filtered, TableResponse)
                    self.assertEqual(
                        (filtered.snapshot_id, filtered.publication_state), status["filtered_commands"]
                    )
                self.assertEqual(len(instance.get_runs()), before)
                return status

            initial = inspect()
            self.assertEqual({state for _, state in initial.values()}, {None})
            registry.register(
                spec.model_copy(
                    update={
                        "deduplication": spec.deduplication.model_copy(
                            update={"normalize_safe_whitespace": False}
                        )
                    }
                )
            )
            self.assertEqual(
                inspect(),
                {
                    stage: (
                        initial[stage][0],
                        "stale"
                        if stage in {"deduplicated_annotations", "category_eligible_annotations"}
                        else None,
                    )
                    for stage in stages
                },
            )
            metadata = spec.model_copy(update={"revision": "inspector metadata only"})
            registry.register(metadata)
            self.assertEqual(inspect(), initial)
            with patch(
                "codebench_pipeline.batch_labeling.guarded_complete",
                side_effect=AssertionError("Metadata-only approval triggered inference"),
            ):
                refresh_registered_configuration(dataset, home)
                self.assertEqual(cycle.drain(instance, partition=dataset), {})
            self.assertEqual(inspect(), initial)

            labeler = metadata.annotation.model
            annotation = metadata.annotation.model_copy(
                update={"model": labeler.model_copy(update={"timeout_seconds": labeler.timeout_seconds + 1})}
            )
            registry.register(metadata.model_copy(update={"annotation": annotation}))
            self.assertEqual(
                inspect(),
                {stage: (initial[stage][0], "stale" if stage in stages[2:] else None) for stage in stages},
            )

            filtering = metadata.filtering.model_copy(
                update={"maximum_command_characters": metadata.filtering.maximum_command_characters - 1}
            )
            registry.register(metadata.model_copy(update={"filtering": filtering}))
            self.assertEqual(
                inspect(),
                {stage: (initial[stage][0], "stale" if stage in stages[1:] else None) for stage in stages},
            )

    def test_observed_directory_is_browsable_with_its_declared_schema(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        dataset = DatasetRegistry(home).register(spec)
        key = tuple(CONFIG_DIRECTORY.path)
        assert spec.config_directory is not None
        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset])
            artifacts = DatasetDataService(home, instance).artifacts
            query = ArtifactQuery(asset_key=key, partition_key=dataset)
            with self.assertRaises(DatasetViewError):
                artifacts.query(query)
            with patch.dict("os.environ", {"CODEBENCH_CONFIG_ROOT": str(BUNDLE.parent.parent)}):
                observation = build_definitions(home).resolve_job_def("observe_dataset_configuration")
                observation.execute_in_process(instance=instance, partition_key=dataset)
            response = artifacts.query(query)
            self.assertIsInstance(response, ArtifactJSON)
            assert isinstance(response, ArtifactJSON)
            self.assertEqual(response.value, spec.config_directory.model_dump(mode="json"))
            self.assertEqual(response.data_schema.source, "declared")
            self.assertIsNone(response.publication_state)

    def test_release_browsing_does_not_plan_and_displays_expired_and_stale_evidence(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, limit_per_risk=4)
        spec = fixture.spec
        registry = DatasetRegistry(home)
        dataset = registry.register(spec)
        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset])
            job = build_definitions(home).resolve_job_def("build_dataset")
            job.execute_in_process(
                instance=instance, partition_key=dataset, run_config=dataset_run_config(spec)
            )
            review = HumanReviewStore(home)
            with review.reader() as tx:
                token = tx.get_current_batch(dataset, "annotations").completion
            assert token is not None
            attest_fixture_release(home, instance, dataset)
            runner = ReleaseRunner(home, instance)
            runner(token)
            data = DatasetDataService(home, instance)
            table = ArtifactQuery(asset_key=("datasets", "labeling_attempts"), partition_key=dataset)
            release = DatasetQueryRequest(dataset_id=dataset)
            run_count = len(instance.get_runs())

            def inspect(expected: str) -> None:
                with patch(
                    "dagster.JobDefinition.execute_in_process",
                    side_effect=AssertionError("Browsing must not run the build planner"),
                ):
                    attempts = data.artifacts.query(table)
                    assert isinstance(attempts, ArtifactTable)
                    self.assertGreater(attempts.page.total_rows, 0)
                    result = data.query("dataset_release", release)
                    assert isinstance(result, TableResponse)
                    self.assertEqual(result.publication_state, expected)
                    self.assertEqual(result.page.total_rows, 4)

            inspect("published")
            policy = data.artifacts.query(
                ArtifactQuery(asset_key=tuple(mapping_key("dataset_release").path), partition_key=dataset)
            )
            self.assertIsInstance(policy, ArtifactJSON)
            assert isinstance(policy, ArtifactJSON)
            object_adapter = TypeAdapter(dict[str, JsonValue])
            effective = object_adapter.validate_python(policy.value)
            grounding = object_adapter.validate_python(effective["grounding"])
            self.assertEqual(effective["limit_per_risk"], 4)
            self.assertNotIn("dataset_id", effective)
            self.assertNotIn("category_rules", effective)
            self.assertEqual(grounding["allowed_domains"], fixture.grounding.allowed_domains)
            self.assertNotIn("keenable_api_key_env", grounding)
            with patch(
                "codebench_pipeline.keenable_policy.KeenableDeclaration.check_current",
                side_effect=KeenableError("authorization_expired", 200),
            ):
                inspect("stale")
                with self.assertRaises((HumanReviewError, DatasetViewError)):
                    runner(token)
            self.assertEqual(len(instance.get_runs()), run_count)

            @asset(
                key=AssetKey(["datasets", "category_rules"]),
                partitions_def=DATASET_PARTITIONS,
                dagster_type=RULES_TYPE,
            )
            def changed_rules() -> Output[CategoryRules]:
                return Output(spec.rules, data_version=DataVersion("different-upstream-version"))

            storage = DataFrameIOManager.from_environment(spec.storage)
            materialize(
                [changed_rules],
                instance=instance,
                partition_key=dataset,
                resources={"io_manager": IOManagerDefinition.hardcoded_io_manager(storage)},
            )
            inspect("stale")
            with self.assertRaises((HumanReviewError, DatasetViewError)):
                runner(token)
            job.execute_in_process(
                instance=instance,
                partition_key=dataset,
                run_config=dataset_run_config(spec),
                asset_selection=[GLOBAL_CONFIG, changed_rules.key],
            )
            inspect("published")
            registry.register(
                spec.model_copy(
                    update={"publication": spec.publication.model_copy(update={"limit_per_risk": 1})}
                )
            )
            inspect("stale")
            with self.assertRaises((HumanReviewError, DatasetViewError)):
                runner(token)
