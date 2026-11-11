import tempfile
import unittest
from pathlib import Path

import pandas as pd
from configuration_fixture import write_fixture
from dagster import AssetKey, build_input_context, build_output_context

from codebench_pipeline.data_view_models import (
    DatasetQueryRequest,
    DatasetViewError,
    RulesResponse,
    TableResponse,
)
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.table_query import TableQuery, TableQueryError, TextFilter


class DatasetViewTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.spec = load_build_config(write_fixture(self.home), self.home)
        DatasetRegistry(self.home).register(self.spec)
        self.instance = self.enterContext(dataset_instance(self.home))
        self.instance.add_dynamic_partitions("dataset_builds", [self.spec.dataset_id])
        self.manager = DataFrameIOManager(storage=self.spec.storage)
        self.service = DatasetDataService(self.home, self.instance)
        self.context = "<script>inert evidence</script>" + "complete context " * 100
        self.frame = validated_frame(
            [
                {
                    "candidate_id": str(index),
                    "command": command,
                    "util": "probe",
                    "context": self.context,
                    "source_id": "source",
                    "source_url": "https://example.org/source",
                    "source_position": str(index),
                    "transformations": [],
                    "risk_candidates": ["CAT-99.1"],
                }
                for index, command in enumerate(("probe inspect", "probe inspect --all"))
            ],
            CommandCandidatesTable,
        )
        for stage, value in (("command_candidates", self.frame), ("category_rules", self.spec.rules)):
            with build_output_context(
                asset_key=AssetKey(["datasets", stage]),
                partition_key=self.spec.dataset_id,
                asset_partitions_def=DATASET_PARTITIONS,
                instance=self.instance,
            ) as output:
                self.manager.handle_output(output, value)

    def test_query_reads_full_persisted_values_without_materializing(self) -> None:
        response = self.service.query(
            "command_candidates",
            DatasetQueryRequest(
                dataset_id=self.spec.dataset_id,
                query=TableQuery(
                    limit=1,
                    filters=(TextFilter(field="command", operator="contains", value="--all"),),
                ),
            ),
        )
        assert isinstance(response, TableResponse)
        self.assertEqual(response.page.matched_rows, 1)
        self.assertEqual(response.page.rows[0].row_index, 1)
        self.assertEqual(response.page.rows[0].values["context"], self.context)
        self.assertEqual(self.instance.get_runs(), [])

    def test_legacy_release_is_inspectable_but_not_authorized_as_pipeline_input(self) -> None:
        legacy = pd.DataFrame(
            [
                {
                    "command": "git status",
                    "description": "Inspects status.",
                    "util": "git",
                    "severity": "low",
                    "category": self.spec.rules.canonical,
                    "risk": self.spec.rules.risks[0].canonical,
                    "source": "fixture",
                    "seed": "-",
                    "class": "allow",
                }
            ],
        )
        with build_output_context(
            asset_key=AssetKey(["datasets", "dataset_release"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as output:
            self.manager.publish_reference(output, self.manager.persist_value(legacy))
        with HumanReviewStore(self.home).transaction():
            response = self.service.query(
                "dataset_release", DatasetQueryRequest(dataset_id=self.spec.dataset_id)
            )
        assert isinstance(response, TableResponse)
        self.assertEqual(response.publication_state, "legacy_unreviewed")
        self.assertEqual(response.page.total_rows, 1)
        self.assertNotIn("human_reviewed", response.page.columns)
        guarded = DataFrameIOManager(storage=self.spec.storage, review_home=str(self.home))
        with build_input_context(
            asset_key=AssetKey(["datasets", "dataset_release"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as incoming:
            with self.assertRaises(HumanReviewError) as unauthorized:
                guarded.load_input(incoming)
        self.assertEqual(unauthorized.exception.code, "stale")

    def test_changed_snapshot_rejects_old_page_binding_and_preserves_old_bytes(self) -> None:
        original = self.service.read_current(self.spec.dataset_id, "command_candidates")
        with build_output_context(
            asset_key=AssetKey(["datasets", "command_candidates"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as output:
            self.manager.handle_output(output, self.frame.iloc[:1])
        with self.assertRaises(HumanReviewError) as stale:
            self.service.query(
                "command_candidates",
                DatasetQueryRequest(dataset_id=self.spec.dataset_id, snapshot_id=original.reference.digest),
            )
        self.assertEqual(stale.exception.code, "stale")
        old = self.manager.read_version(original.reference)
        assert isinstance(old, pd.DataFrame)
        self.assertEqual(len(old), 2)
        fresh = self.service.query("command_candidates", DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        assert isinstance(fresh, TableResponse)
        self.assertEqual(fresh.page.total_rows, 1)
        self.assertNotEqual(fresh.snapshot_id, original.reference.digest)

    def test_rules_and_missing_states_are_distinct_from_empty_query_results(self) -> None:
        request = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        rules = self.service.query("category_rules", request)
        assert isinstance(rules, RulesResponse)
        self.assertEqual(rules.rules, self.spec.rules)
        for stage, code in (("unknown", "not_found"), ("reviewed_annotations", "not_materialized")):
            with self.subTest(stage=stage), self.assertRaises(DatasetViewError) as error:
                self.service.query(stage, request)
            self.assertEqual(error.exception.code, code)
        with self.assertRaises(TableQueryError):
            self.service.query(
                "category_rules",
                DatasetQueryRequest(dataset_id=self.spec.dataset_id, query=TableQuery(search="policy")),
            )
        self.assertEqual(self.service.datasets().datasets[0].dataset_id, self.spec.dataset_id)


if __name__ == "__main__":
    unittest.main()
