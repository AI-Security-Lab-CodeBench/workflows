from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from test_declared_dataset_graph import fixture_spec, with_keenable_declaration

from codebench_pipeline.collection_config import KeenableSnippetDeclaration
from codebench_pipeline.collection_stage_settings import KeenableCandidateSettings
from codebench_pipeline.config_assets import CONFIG_DIRECTORY, collection_mapping_key
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.runtime_instance import dataset_instance


class KeenableMappingTests(TestCase):
    def test_candidate_mapping_keeps_declared_seeds_and_receipt_identity(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        shipped = fixture_spec(home)
        assert shipped.declared_collection is not None
        declaration = shipped.declared_collection.keenable_declaration
        assert isinstance(declaration, KeenableSnippetDeclaration)
        seeds = ("kubernetes_inventory", "docker_inventory")
        search = declaration.search.model_copy(update={"seeds": seeds})
        spec = with_keenable_declaration(shipped, declaration.model_copy(update={"search": search}))
        dataset_id = DatasetRegistry(home).register(spec)
        definitions = build_definitions(home)
        candidates = collection_mapping_key("keenable", "command_candidates")
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
                instance=instance, partition_key=dataset_id, asset_selection=[candidates]
            )
        self.assertTrue(result.success)
        self.assertCountEqual(
            [event.asset_key for event in result.get_asset_materialization_events()], [candidates]
        )

        root = home.joinpath("configuration-runs", result.run_id)
        raw = root.joinpath(*candidates.path).read_bytes()
        artifact = KeenableCandidateSettings.model_validate_json(raw, strict=True)
        self.assertEqual(artifact.search.seeds, seeds)
        assert spec.declared_collection is not None
        self.assertEqual(artifact.declaration_digest, spec.declared_collection.keenable_use_cases.digest)
