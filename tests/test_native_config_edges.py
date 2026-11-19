from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from dagster import AssetKey
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.definitions import build_definitions


class NativeConfigEdgesTests(TestCase):
    def test_every_active_stage_depends_on_its_config_and_not_directly_on_rules(self) -> None:
        with TemporaryDirectory() as home:
            graph = build_definitions(
                Path(home), specification=fixture_spec(Path(home))
            ).resolve_asset_graph()

        branch_stages = ("source_catalog", "source_documents", "document_adapters", "command_candidates")
        branch_mappings = (
            ((), "existing_datasets"),
            (("documentation",), "documentation_crawl"),
            (("keenable",), "keenable_use_cases"),
        )
        shared = (
            "merged_command_candidates",
            "filtered_commands",
            "labeling_attempts",
            "proposed_annotations",
            "labeling_failures",
            "deduplicated_annotations",
            "category_eligible_annotations",
            "human_review_queue",
            "human_review_decisions",
            "dataset_release",
        )
        expected = {
            AssetKey(["datasets", *prefix, stage]): AssetKey(["datasets", f"map_{scope}_config"])
            for prefix, scope in branch_mappings
            for stage in branch_stages
        } | {AssetKey(["datasets", stage]): AssetKey(["datasets", f"map_{stage}_config"]) for stage in shared}
        rules = AssetKey(["datasets", "category_rules"])
        global_config = AssetKey(["datasets", "global_config"])
        all_keys = graph.get_all_asset_keys()
        audit = AssetKey(["datasets", "bashguard_syntax_audit"])
        self.assertEqual(
            set(all_keys), set(expected) | set(expected.values()) | {rules, global_config, audit}
        )
        self.assertEqual(graph.get(rules).parent_keys, {global_config})

        for stage, mapping in expected.items():
            with self.subTest(stage=stage.to_user_string()):
                parents = graph.get(stage).parent_keys
                self.assertEqual(parents & set(expected.values()), {mapping})
                self.assertNotIn(rules, parents)
