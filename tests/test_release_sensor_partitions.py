import unittest
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory
from unittest.mock import patch

from dagster import (
    AssetKey,
    DataVersion,
    IOManagerDefinition,
    Output,
    asset,
    build_sensor_context,
    materialize,
)
from guarded_labeling_fixture import LabelingFixture
from test_declared_dataset_graph import with_keenable_declaration
from test_review_release import attest_fixture_release

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, RULES_TYPE
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.keenable_policy import KeenableDeclaration
from codebench_pipeline.runtime_instance import dataset_instance


class ReleaseSensorPartitionTests(unittest.TestCase):
    def test_stale_preparation_does_not_discard_another_partitions_release_request(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, limit_per_risk=4)
        healthy = fixture.spec
        stale_id = "zz_stale_preparation"
        declaration = fixture.declaration
        assert isinstance(declaration, KeenableDeclaration)
        stale = with_keenable_declaration(
            healthy.model_copy(
                update={
                    "dataset_id": stale_id,
                    "storage": healthy.storage.model_copy(update={"local_root": str(home / "stale-tables")}),
                }
            ),
            declaration.model_copy(update={"dataset": stale_id}),
        )
        grounding = home / "label-grounding-v2"
        copytree(grounding / healthy.dataset_id / "rights", grounding / stale_id / "rights")
        registry = DatasetRegistry(home)
        for spec in (healthy, stale):
            registry.register(spec)
        self.assertEqual(registry.keys(), [healthy.dataset_id, stale_id])

        definitions = build_definitions(home)
        instance = self.enterContext(dataset_instance(home))
        instance.add_dynamic_partitions("dataset_builds", registry.keys())
        job = definitions.resolve_job_def("build_dataset")
        for spec in (healthy, stale):
            self.assertTrue(job.execute_in_process(instance=instance, partition_key=spec.dataset_id).success)

        sensor = definitions.resolve_sensor_def("dataset_release_requests")
        context = self.enterContext(
            build_sensor_context(instance=instance, repository_def=definitions.get_repository_def())
        )
        self.assertFalse(sensor.evaluate_tick(context).run_requests)
        for spec in (healthy, stale):
            attest_fixture_release(home, instance, spec.dataset_id)
        before = sensor.evaluate_tick(context).run_requests or []
        self.assertEqual([request.partition_key for request in before], [healthy.dataset_id, stale_id])

        @asset(
            key=AssetKey(["datasets", "category_rules"]),
            partitions_def=DATASET_PARTITIONS,
            dagster_type=RULES_TYPE,
        )
        def changed_rules() -> Output[CategoryRules]:
            return Output(stale.rules, data_version=DataVersion("stale-preparation-fixture"))

        materialize(
            [changed_rules],
            instance=instance,
            partition_key=stale_id,
            resources={
                "io_manager": IOManagerDefinition.hardcoded_io_manager(
                    DataFrameIOManager.from_environment(stale.storage)
                )
            },
        )
        self.assertEqual(sensor.evaluate_tick(context).run_requests, before[:1])

        unexpected = DatasetViewError("not_found", "unexpected view failure")
        with patch("codebench_pipeline.release_runner.current_proposal", side_effect=unexpected):
            with self.assertRaises(DatasetViewError) as failure:
                sensor.evaluate_tick(context)
        self.assertIs(failure.exception, unexpected)
