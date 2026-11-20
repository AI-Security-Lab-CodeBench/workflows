from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from dagster import AssetKey
from test_declared_dataset_graph import fixture_spec, with_keenable_declaration

from codebench_pipeline.collection_stage_settings import KeenableDiscoverySettings
from codebench_pipeline.config_assets import collection_mapping_key
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.runtime_instance import dataset_instance


class KeenableConfigurationTests(TestCase):
    def test_adaptive_planner_revision_changes_discovery_without_changing_access_mappings(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = fixture_spec(home)
        collection = fixture.declared_collection
        assert collection is not None
        declaration = collection.keenable_declaration
        assert declaration.search_plan is not None
        adaptive_declaration = declaration.model_copy(
            update={"search_plan": declaration.search_plan.model_copy(update={"mode": "adaptive"})}
        )
        adaptive = with_keenable_declaration(fixture, adaptive_declaration)
        batch = adaptive.annotation.batch
        assert batch is not None
        new_revision = "fixture-backend-2"
        revised = adaptive.model_copy(
            update={
                "annotation": adaptive.annotation.model_copy(
                    update={"batch": batch.model_copy(update={"backend_revision": new_revision})}
                ),
            }
        )
        discovery = collection_mapping_key("keenable", "discover_use_cases")
        access = [
            collection_mapping_key("keenable", stage)
            for stage in ("capture_origin_pages", "command_candidates")
        ]
        keys = [discovery, *access]
        registry = DatasetRegistry(home)
        definitions = build_definitions(home)
        refresh = definitions.resolve_job_def("refresh_dataset_configuration")
        job = definitions.get_implicit_global_asset_job_def()
        snapshots: list[dict[AssetKey, tuple[str, bytes]]] = []
        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [adaptive.dataset_id])
            for spec in (adaptive, revised):
                dataset_id = registry.register(spec)
                self.assertTrue(
                    refresh.execute_in_process(instance=instance, partition_key=dataset_id).success
                )
                result = job.execute_in_process(
                    instance=instance, partition_key=dataset_id, asset_selection=keys
                )
                self.assertTrue(result.success)
                self.assertCountEqual(
                    [event.asset_key for event in result.get_asset_materialization_events()], keys
                )
                snapshot: dict[AssetKey, tuple[str, bytes]] = {}
                for key in keys:
                    (materialization,) = result.asset_materializations_for_node(key.to_python_identifier())
                    assert materialization.tags is not None
                    wire = home.joinpath("configuration-runs", result.run_id, *key.path).read_bytes()
                    snapshot[key] = (materialization.tags["dagster/data_version"], wire)
                snapshots.append(snapshot)
        before, after = snapshots
        self.assertNotEqual(before[discovery][0], after[discovery][0])
        for snapshot, expected_revision in ((before, batch.backend_revision), (after, new_revision)):
            settings = KeenableDiscoverySettings.model_validate_json(snapshot[discovery][1], strict=True)
            assert settings.planner is not None
            self.assertEqual(settings.planner.backend_revision, expected_revision)
            self.assertEqual(
                settings.declaration.declaration_sha256,
                adaptive_declaration.declaration_sha256,
            )
        for key in access:
            self.assertEqual(before[key], after[key])
