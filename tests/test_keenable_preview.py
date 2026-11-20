from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from test_declared_dataset_graph import fixture_spec, with_keenable_declaration

from codebench_pipeline.collection_stage_settings import KeenableDiscoverySettings
from codebench_pipeline.config_assets import CONFIG_DIRECTORY, collection_mapping_key
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.keenable_policy import CurrentKeenableDeclaration, KeenableSourceAccess
from codebench_pipeline.runtime_instance import dataset_instance


class KeenableMappingTests(TestCase):
    def test_planless_discovery_and_access_keep_effective_scope_and_receipt_identity(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        shipped = fixture_spec(home)
        collection = shipped.declared_collection
        assert collection is not None
        declaration = collection.keenable_declaration
        assert isinstance(declaration, CurrentKeenableDeclaration)
        planless = declaration.model_copy(update={"search_plan": None, "sources": declaration.sources[::-1]})
        spec = with_keenable_declaration(shipped, planless)
        dataset_id = DatasetRegistry(home).register(spec)
        definitions = build_definitions(home)
        discovery = collection_mapping_key("keenable", "discover_use_cases")
        access_keys = [
            collection_mapping_key("keenable", stage)
            for stage in ("capture_origin_pages", "command_candidates")
        ]
        projections = [discovery, *access_keys]
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
        raw_discovery = root.joinpath(*discovery.path).read_bytes()
        artifact = KeenableDiscoverySettings.model_validate_json(raw_discovery, strict=True)
        frozen = artifact.declaration
        self.assertEqual(frozen.search_cases, ("workloads", "containers"))
        self.assertEqual(frozen.model_dump_json(), planless.model_dump_json())
        self.assertEqual(frozen.declaration_sha256, planless.declaration_sha256)
        self.assertNotIn(b'"keenable_preview":', raw_discovery)
        self.assertNotIn(b'"search_plan":', raw_discovery)
        for key in access_keys:
            with self.subTest(key=key.to_user_string()):
                raw_access = root.joinpath(*key.path).read_bytes()
                access = KeenableSourceAccess.model_validate_json(raw_access, strict=True)
                self.assertEqual(access.declaration_sha256, planless.declaration_sha256)
                self.assertEqual(access.sources, planless.sources)
                self.assertEqual(access.raw_licenses, planless.raw_licenses)
                self.assertEqual(access.budget.origin_calls, planless.budget.origin_calls)
                self.assertEqual(access.budget.total_bytes, planless.budget.total_bytes)
                for field in (b'"search_plan":', b'"search_calls":', b'"planner":'):
                    self.assertNotIn(field, raw_access)
