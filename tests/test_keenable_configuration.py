import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pydantic import JsonValue, ValidationError
from test_declared_dataset_graph import fixture_spec, with_keenable_declaration
from test_keenable import source_declaration

from codebench_pipeline.collection_config import (
    KeenableSnippetDeclaration,
    SourceDeclaration,
    parse_collection_keenable,
)
from codebench_pipeline.collection_stage_settings import KeenableCandidateSettings
from codebench_pipeline.config_assets import collection_mapping_key
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.storage import IntegrityError


class KeenableConfigurationTests(TestCase):
    def test_authored_seed_requires_a_reviewed_profile_and_a_unique_id(self) -> None:
        seed = {
            "id": "cat3-container-usage",
            "instruction": "Find inventory.",
            "risk_id": "CAT-3.3",
            "severity": "low",
        }
        for seeds, error in (
            ([seed], "requires a reviewed profile"),
            ([{**seed, "id": "docker_inventory", "profile": "unsupported_profile"}], "profile"),
            (
                [{**seed, "profile": "docker_inventory"}, {**seed, "profile": "kubernetes_inventory"}],
                "Inspection seeds must be unique",
            ),
        ):
            raw = json.dumps({"schema_version": "4", "dataset_id": "fixture", "search": {"seeds": seeds}})
            with self.subTest(seeds=seeds), self.assertRaisesRegex(ValidationError, error):
                parse_collection_keenable(raw)

    def test_archived_authored_seed_round_trips_without_changing_the_declaration_digest(self) -> None:
        payload: dict[str, JsonValue] = {
            "schema_version": "4",
            "dataset_id": "fixture",
            "search": {
                "seeds": [
                    {
                        "id": "docker_inventory",
                        "instruction": "Find inventory.",
                        "risk_id": "CAT-3.3",
                        "severity": "low",
                    }
                ],
                "max_results": 5,
                "snippet_max_length": 4000,
            },
        }
        raw = json.dumps(payload, separators=(",", ":"))
        declaration = parse_collection_keenable(raw)
        assert isinstance(declaration, KeenableSnippetDeclaration)
        self.assertEqual(declaration.model_dump_json(), raw)
        explicit = KeenableSnippetDeclaration.model_validate_json(
            raw.replace('"id":"docker_inventory"', '"id":"docker_inventory","profile":"docker_inventory"')
        )
        self.assertEqual(
            declaration.search.authored_seeds[0].inspection, explicit.search.authored_seeds[0].inspection
        )

    def test_origin_permission_does_not_authorize_new_snippet_searches(self) -> None:
        scoped = SourceDeclaration.from_declaration(source_declaration())
        with self.assertRaisesRegex(IntegrityError, "explicit snippet search"):
            scoped.keenable_search()

    def test_changed_seeds_advance_the_candidate_mapping_version(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = fixture_spec(home)
        collection = fixture.declared_collection
        assert collection is not None
        declaration = collection.keenable_declaration
        assert isinstance(declaration, KeenableSnippetDeclaration)
        replacement = declaration.search.model_copy(update={"seeds": ("docker_inventory",)})
        revised = with_keenable_declaration(fixture, declaration.model_copy(update={"search": replacement}))
        key = collection_mapping_key("keenable", "command_candidates")
        registry = DatasetRegistry(home)
        definitions = build_definitions(home)
        refresh = definitions.resolve_job_def("refresh_dataset_configuration")
        job = definitions.get_implicit_global_asset_job_def()
        snapshots: list[tuple[str, bytes]] = []
        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [fixture.dataset_id])
            for spec in (fixture, revised):
                dataset_id = registry.register(spec)
                self.assertTrue(
                    refresh.execute_in_process(instance=instance, partition_key=dataset_id).success
                )
                result = job.execute_in_process(
                    instance=instance, partition_key=dataset_id, asset_selection=[key]
                )
                self.assertTrue(result.success)
                self.assertCountEqual(
                    [event.asset_key for event in result.get_asset_materialization_events()], [key]
                )
                (materialization,) = result.asset_materializations_for_node(key.to_python_identifier())
                assert materialization.tags is not None
                wire = home.joinpath("configuration-runs", result.run_id, *key.path).read_bytes()
                snapshots.append((materialization.tags["dagster/data_version"], wire))
        before, after = snapshots
        self.assertNotEqual(before[0], after[0])
        for snapshot, expected in ((before, declaration.search), (after, replacement)):
            settings = KeenableCandidateSettings.model_validate_json(snapshot[1], strict=True)
            self.assertEqual(settings.search, expected)
