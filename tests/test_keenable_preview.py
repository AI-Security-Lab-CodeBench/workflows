from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from test_declared_dataset_graph import fixture_spec, with_keenable_declaration

from codebench_pipeline.config_assets import CONFIG_DIRECTORY, mapping_key
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
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

        dataset_id = DatasetRegistry(home).register(spec)
        definitions = build_definitions(home)
        scopes = ("existing_datasets", "documentation_crawl", "keenable_use_cases")
        projections = [mapping_key(scope) for scope in scopes]
        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            refreshed = definitions.resolve_job_def("refresh_dataset_configuration").execute_in_process(
                instance=instance, partition_key=dataset_id
            )
            self.assertTrue(refreshed.success)
            self.assertNotIn(
                CONFIG_DIRECTORY, {event.asset_key for event in refreshed.get_asset_materialization_events()}
            )
            result = definitions.get_implicit_global_asset_job_def().execute_in_process(
                instance=instance, partition_key=dataset_id, asset_selection=projections
            )
        self.assertTrue(result.success)
        self.assertCountEqual(
            [event.asset_key for event in result.get_asset_materialization_events()], projections
        )

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
