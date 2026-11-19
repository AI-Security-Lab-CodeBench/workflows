from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from dagster import asset, materialize
from test_declared_dataset_graph import fixture_spec, with_keenable_declaration

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import ConfigRunIOManager, configuration_assets, mapping_key
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, RULES_TYPE
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.keenable_policy import CurrentKeenableDeclaration
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import DeclaredBranchConfig, declared_source_config


class KeenablePreviewTests(TestCase):
    def test_mapped_preview_persists_effective_cases_and_bytes_without_a_search_plan(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        shipped = fixture_spec(home)
        collection = shipped.declared_collection
        assert collection is not None
        declaration = collection.keenable_declaration
        assert isinstance(declaration, CurrentKeenableDeclaration)
        planless = declaration.model_copy(update={"search_plan": None, "sources": declaration.sources[::-1]})
        spec = with_keenable_declaration(shipped, planless)
        expected = declared_source_config(spec, "keenable")

        @asset(key=["datasets", "category_rules"], partitions_def=DATASET_PARTITIONS, dagster_type=RULES_TYPE)
        def rules_fixture() -> CategoryRules:
            return spec.rules

        with patch.object(DatasetRegistry, "get", return_value=spec), dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            result = materialize(
                [rules_fixture, *configuration_assets(home, declared=True)],
                instance=instance,
                partition_key=spec.dataset_id,
                resources={"config_run_io_manager": ConfigRunIOManager(home=str(home))},
            )
        self.assertTrue(result.success)

        root = home.joinpath("configuration-runs", result.run_id)
        path = root.joinpath(*mapping_key("keenable_use_cases").path)
        artifact = DeclaredBranchConfig.model_validate_json(path.read_bytes())
        assert artifact.keenable_preview is not None
        self.assertEqual(artifact.keenable_preview.effective_cases, ("workloads", "containers"))
        self.assertEqual(artifact.keenable_preview.effective_byte_capacity, 13_107_200)
        self.assertEqual(artifact.collection, expected)
        frozen = artifact.collection.source.keenable_config()
        self.assertEqual(frozen.model_dump_json(), planless.model_dump_json())
        self.assertEqual(frozen.declaration_sha256, planless.declaration_sha256)
        for scope in ("existing_datasets", "documentation_crawl"):
            repository = root.joinpath(*mapping_key(scope).path).read_text(encoding="utf-8")
            self.assertNotIn('"keenable_preview"', repository)
