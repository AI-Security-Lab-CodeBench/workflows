import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from dagster import AssetKey, DagsterInstance, build_output_context

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.viewer import launch_ui, preview_asset, viewer_environment


class DatasetViewerTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.spec = load_build_config(write_fixture(self.home), self.home)
        DatasetRegistry(self.home).register(self.spec)
        self.long_context = "Full context " * 100
        self.frame = validated_frame(
            [
                {
                    "candidate_id": str(number),
                    "command": command,
                    "util": "git",
                    "context": self.long_context,
                    "source_id": "inspected-source",
                    "source_url": "https://example.org/git",
                    "source_position": f"line {number}",
                    "transformations": [],
                    "risk_candidates": ["CAT-99.1"],
                }
                for number, command in enumerate(("git status", "git status --show-stash"), 1)
            ],
            CommandCandidatesTable,
        )
        with dataset_instance(self.home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [self.spec.dataset_id])
            manager = DataFrameIOManager(storage=self.spec.storage)
            for stage, value in (("command_candidates", self.frame), ("category_rules", self.spec.rules)):
                with build_output_context(
                    asset_key=AssetKey(["datasets", stage]),
                    partition_key=self.spec.dataset_id,
                    asset_partitions_def=DATASET_PARTITIONS,
                    instance=instance,
                ) as context:
                    manager.handle_output(context, value)

    def test_viewer_overrides_stale_homes_and_reads_the_built_instance(self) -> None:
        with patch.dict(os.environ, {"CODEBENCH_HOME": "/wrong/registry", "DAGSTER_HOME": "/wrong/history"}):
            environment = viewer_environment(self.home)
            with patch.dict(os.environ, environment):
                with DagsterInstance.get() as instance:
                    self.assertEqual(
                        instance.get_dynamic_partitions("dataset_builds"), [self.spec.dataset_id]
                    )
                registry = DatasetRegistry(Path(os.environ["CODEBENCH_HOME"]))
                self.assertEqual(registry.get(self.spec.dataset_id), self.spec)
            self.assertEqual(os.environ["DAGSTER_HOME"], "/wrong/history")

    def test_show_preserves_full_cells_and_limits_rows_without_creating_runs(self) -> None:
        result = preview_asset(self.home, self.spec.dataset_id, "command_candidates", 1)
        self.assertEqual((result["row_count"], result["shown_rows"]), (2, 1))
        self.assertEqual(
            result["data"],
            [
                {
                    "candidate_id": "1",
                    "command": "git status",
                    "util": "git",
                    "context": self.long_context,
                    "source_id": "inspected-source",
                    "source_url": "https://example.org/git",
                    "source_position": "line 1",
                    "transformations": [],
                    "risk_candidates": ["CAT-99.1"],
                }
            ],
        )
        with dataset_instance(self.home) as instance:
            self.assertEqual(instance.get_runs(), [])

    def test_show_reads_rules_as_typed_json_not_a_table(self) -> None:
        result = preview_asset(self.home, self.spec.dataset_id, "category_rules", 1)
        self.assertEqual(result["kind"], "rules")
        self.assertEqual(result["data"], self.spec.rules.model_dump(mode="json"))

    def test_missing_home_and_invalid_preview_parameters_fail_without_starting_work(self) -> None:
        missing = self.home / "not-built"
        with self.assertRaises(FileNotFoundError):
            viewer_environment(missing)
        self.assertFalse(missing.exists())
        with self.assertRaises(ValueError):
            preview_asset(self.home, self.spec.dataset_id, "unknown", 1)
        for limit in (0, 1001):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                preview_asset(self.home, self.spec.dataset_id, "command_candidates", limit)
        with self.assertRaises(ValueError):
            launch_ui(self.home, 0)

    def test_each_dagster_asset_exposes_description_metadata(self) -> None:
        graph = build_definitions(self.home).resolve_asset_graph()
        for key in graph.get_all_asset_keys():
            with self.subTest(asset=key.to_user_string()):
                self.assertTrue(graph.get(key).description)
