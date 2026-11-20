from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from dagster import AssetKey
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.collection_stage_settings import COLLECTION_OPERATIONS
from codebench_pipeline.config_assets import (
    CONFIG_DIRECTORY,
    GLOBAL_CONFIG,
    collection_mapping_key,
    mapping_key,
)
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, FilterConfig


class ConfigurationRefreshTests(TestCase):
    def test_filter_only_registration_changes_no_collection_mapping_bytes_or_versions(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        original = fixture_spec(home)
        updated = original.model_copy(update={"filtering": FilteringConfig(maximum_command_characters=1)})
        registry = DatasetRegistry(home)
        definitions = build_definitions(home)
        refresh = definitions.resolve_job_def("refresh_dataset_configuration")
        job = definitions.get_implicit_global_asset_job_def()
        collection = [
            collection_mapping_key(operation.branch, operation.stage) for operation in COLLECTION_OPERATIONS
        ]
        filter_key = mapping_key("filtered_commands")
        keys = [*collection, filter_key]
        snapshots: list[dict[AssetKey, tuple[str, bytes]]] = []
        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [original.dataset_id])
            for spec in (original, updated):
                dataset_id = registry.register(spec)
                refreshed = refresh.execute_in_process(instance=instance, partition_key=dataset_id)
                self.assertTrue(refreshed.success)
                for event in refreshed.get_asset_materialization_events():
                    assert event.asset_key is not None
                    self.assertNotEqual(event.asset_key, CONFIG_DIRECTORY)
                    self.assertEqual(
                        definitions.resolve_asset_graph().get(event.asset_key).group_name, "configuration"
                    )
                payload = home.joinpath(
                    "configuration-runs", refreshed.run_id, *GLOBAL_CONFIG.path
                ).read_bytes()
                registered = DeclaredGlobalConfig.model_validate_json(payload, strict=True)
                self.assertEqual(registered.scopes.filtered_commands.filtered_commands, spec.filtering)
                result = job.execute_in_process(
                    instance=instance, partition_key=dataset_id, asset_selection=keys
                )
                self.assertTrue(result.success)
                snapshot: dict[AssetKey, tuple[str, bytes]] = {}
                for key in keys:
                    (materialization,) = result.asset_materializations_for_node(key.to_python_identifier())
                    assert materialization.tags is not None
                    wire = home.joinpath("configuration-runs", result.run_id, *key.path).read_bytes()
                    snapshot[key] = (materialization.tags["dagster/data_version"], wire)
                snapshots.append(snapshot)
        before, after = snapshots
        self.assertNotEqual(before[filter_key][0], after[filter_key][0])
        for spec, snapshot in ((original, before), (updated, after)):
            settings = FilterConfig.model_validate_json(snapshot[filter_key][1], strict=True)
            self.assertEqual(settings.filtered_commands, spec.filtering)
        for key in collection:
            with self.subTest(mapping=key.to_user_string()):
                self.assertEqual(before[key], after[key])
