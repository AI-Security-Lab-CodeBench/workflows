import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from configuration_fixture import write_fixture
from dagster import AssetKey
from pydantic import ValidationError
from test_keenable import PEER, PLAN, SOURCE_BODY, permission, result
from test_keenable import source_declaration as source_scope_declaration
from test_repository import (
    DOCKER,
    TLDR,
    RepositoryFixture,
    repository_branch_fixture,
    repository_config_fixture,
)

from codebench_pipeline.artifact_views import ArtifactJSON, ArtifactQuery, ArtifactTable
from codebench_pipeline.collection_config import CAT3_DATASET_ID, SourceDeclaration
from codebench_pipeline.config_assets import configured_selection
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_cli import main
from codebench_pipeline.dataset_config import AnnotationConfig, DatasetBuildSpec, load_build_config
from codebench_pipeline.dataset_finalization import available_candidates
from codebench_pipeline.dataset_plan import plan_dataset
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.declared_dataset_bundle import DeclaredDatasetBundle, DeclaredLabeling
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.keenable_policy import CASES, Declaration, KeenableDeclaration
from codebench_pipeline.keenable_transport import DirectResponse
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.repository_receipts import RepositoryReceipt
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import IntegrityError, fingerprint

BUNDLE = Path("business-configs/datasets/cat3.json")


def declared_labeling_fixture(home: Path) -> DeclaredLabeling:
    return DeclaredLabeling.model_validate(
        {
            "max_request_bytes": 65536,
            "model": {
                "provider": "openai_compatible",
                "model": "fixture/guarded",
                "base_url": "http://fixture.invalid/v1",
            },
            "batch": {
                "backend_revision": "fixture-backend-1",
                "grounding": {"allowed_domains": ["example.org"]},
                "capacity": {
                    "state_directory": str(home / "capacity"),
                    "expected_model_path": "/fixture/model",
                },
            },
        }
    )


def guarded_fixture(home: Path) -> AnnotationConfig:
    return declared_labeling_fixture(home).registered()


def fixture_spec(home: Path) -> DatasetBuildSpec:
    bundle = DeclaredDatasetBundle.model_validate_json(BUNDLE.read_bytes())
    configured = bundle.model_copy(update={"labeling": declared_labeling_fixture(home), "storage": None})
    return configured.resolve(BUNDLE.resolve().parent, home=home)


def with_keenable_declaration(spec: DatasetBuildSpec, declaration: Declaration) -> DatasetBuildSpec:
    collection = spec.declared_collection
    assert collection is not None
    source = SourceDeclaration.from_declaration(declaration)
    return spec.model_copy(
        update={"declared_collection": collection.model_copy(update={"keenable_use_cases": source})}
    )


class DeclaredDatasetGraphTests(unittest.TestCase):
    def test_adaptive_catalog_rejects_changed_backend_before_search(self) -> None:
        from codebench_pipeline.declared_source_assets import provider_collection
        from codebench_pipeline.repository import RepositoryRun
        from codebench_pipeline.stage_configuration import declared_source_config

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            spec = fixture_spec(home)
            fixed = declared_source_config(spec, "keenable")
            declaration = fixed.source.keenable_config()
            assert declaration.search_plan is not None
            adaptive = with_keenable_declaration(
                spec,
                declaration.model_copy(
                    update={"search_plan": declaration.search_plan.model_copy(update={"mode": "adaptive"})}
                ),
            )
            config = declared_source_config(adaptive, "keenable")
            run = RepositoryRun(home, spec.dataset_id, "offline")
            with patch.object(DatasetRegistry, "get_snapshot", return_value=adaptive):
                provider = provider_collection(config, run, plan_receipts=home)
                self.assertIsNotNone(provider.planner)
                self.assertIsNone(provider_collection(config, run).planner)
            batch = adaptive.annotation.batch
            assert batch is not None
            new_annotation = adaptive.annotation.model_copy(
                update={"batch": batch.model_copy(update={"backend_revision": "different-backend"})}
            )
            updated = adaptive.model_copy(update={"annotation": new_annotation})
            with patch.object(DatasetRegistry, "get_snapshot", return_value=updated):
                with self.assertRaisesRegex(IntegrityError, "settings changed"):
                    provider_collection(config, run, plan_receipts=home)

    def test_declared_collection_requires_guarded_batch_and_rejects_local_documentation(self) -> None:
        payload = json.loads(BUNDLE.read_bytes())
        forbidden = json.loads(BUNDLE.read_bytes())
        forbidden["documentation"] = {"snippets": []}
        with self.assertRaises(ValidationError):
            DeclaredDatasetBundle.model_validate(forbidden)

        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = fixture_spec(home)
            self.assertNotIn("documentation", spec.model_dump(mode="json"))
            with self.assertRaisesRegex(IntegrityError, "requires existing registered annotation"):
                load_build_config(BUNDLE, home)
            payload["labeling"] = declared_labeling_fixture(home).model_dump(mode="json")
            payload["labeling"]["batch"] = None
            with self.assertRaises(ValidationError) as invalid:
                DeclaredDatasetBundle.model_validate(payload)
            self.assertIn(
                ("labeling", "batch", "model_type"),
                {(error["loc"][0], error["loc"][-1], error["type"]) for error in invalid.exception.errors()},
            )

    def test_historical_declared_snapshot_stays_readable_and_digest_checked(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = fixture_spec(home)
            snapshot = spec.model_dump(mode="json")
            snapshot.pop("refresh_external_inputs")
            snapshot["bundle_version"] = 2
            snapshot["collection_v2"] = snapshot.pop("declared_collection")
            checksum = fingerprint(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")))
            registry = DatasetRegistry(home)
            directory = registry.root / spec.dataset_id
            directory.mkdir(parents=True)
            path = directory / f"{checksum}.json"
            path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            (directory / "current").write_text(checksum, encoding="ascii")

            self.assertEqual(registry.get_snapshot(spec.dataset_id), spec)
            self.assertTrue(registry.location(spec.dataset_id).has_declared_collection)

            snapshot["revision"] = "modified"
            path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(IntegrityError, "registry revision differs"):
                registry.get_snapshot(spec.dataset_id)

    def test_graph_preparation_merges_three_real_branch_outputs_and_replans_only_changed_branch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, dataset_instance(Path(temporary)) as instance:
            home = Path(temporary)
            spec = self.collection_fixture(home)
            registry = DatasetRegistry(home)
            registry.register(spec)
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            definitions = build_definitions(home)
            job = definitions.resolve_job_def("prepare_dataset")
            with (
                patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
                patch(
                    "codebench_pipeline.keenable.request_direct",
                    side_effect=(result(PLAN), DirectResponse(SOURCE_BODY, PEER)),
                ),
            ):
                run = job.execute_in_process(instance=instance, partition_key=spec.dataset_id)
            self.assertTrue(run.success)
            self.assertEqual(
                definitions.resolve_asset_graph()
                .get(AssetKey(["datasets", "labeling_attempts"]))
                .parent_keys,
                {
                    AssetKey(["datasets", stage])
                    for stage in ("filtered_commands", "map_labeling_attempts_config")
                },
            )
            data = DatasetDataService(home, instance)
            documentation = data.read_current(spec.dataset_id, "documentation/capture_documentation")
            assert isinstance(documentation.value, pd.DataFrame)
            scope = RepositoryReceipt.model_validate_json(
                documentation.value.attrs["documentation_scope_receipt_json"]
            )
            self.assertEqual((scope.stage, scope.branch), ("source_catalog", "documentation"))
            self.assertEqual(scope.observations["complete"], True)
            filtered = data.read_current(spec.dataset_id, "filtered_commands")
            self.assertIsNone(filtered.publication_state)
            assert isinstance(filtered.value, pd.DataFrame)
            self.assertEqual(
                {
                    (str(row["command"]), str(row["candidate_id"]).partition(":")[0], row["source_weight"])
                    for row in frame_records(filtered.value)
                },
                {
                    ("docker compose ps --all", "dataset", 0.62),
                    ("docker ps --all", "documentation", 0.9),
                    ("terraform plan", "keenable", 0.24),
                },
            )
            self.assertEqual(
                plan_dataset(
                    spec, stage_configs(spec), instance, home=home, through="filtered_commands"
                ).selected,
                (),
            )

            collection = spec.declared_collection
            assert collection is not None
            declaration = collection.existing_datasets.declaration
            assert isinstance(declaration, RepositoryBranchDeclaration)
            origin, *remaining = declaration.origins
            rights = origin.rights.model_copy(update={"attribution": "Updated fixture attribution"})
            revised = declaration.model_copy(
                update={"origins": (origin.model_copy(update={"rights": rights}), *remaining)}
            )
            revised_source = SourceDeclaration.from_declaration(revised)
            changed = spec.model_copy(
                update={
                    "declared_collection": collection.model_copy(update={"existing_datasets": revised_source})
                }
            )
            registry.register(changed)
            plan = plan_dataset(
                changed, stage_configs(changed), instance, home=home, through="filtered_commands"
            )
            self.assertEqual(
                plan.selected,
                (
                    "enumerate_corpus_files",
                    "capture_corpus_files",
                    "command_candidates",
                    "merged_command_candidates",
                    "filtered_commands",
                ),
            )
            self.assertEqual(
                data.read_current(spec.dataset_id, "filtered_commands").publication_state, "stale"
            )
            self.assertIsNone(
                data.read_current(spec.dataset_id, "documentation/command_candidates").publication_state
            )
            reloaded = run_registered(spec.dataset_id, home, prepare_only=True)
            self.assertEqual(reloaded.materialized_stages, plan.selected)
            self.assertIsNone(data.read_current(spec.dataset_id, "filtered_commands").publication_state)

    def test_healthy_empty_provider_is_current_and_failed_generation_cannot_merge(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, dataset_instance(Path(temporary)) as instance:
            home = Path(temporary)
            empty = KeenableDeclaration(
                dataset=CAT3_DATASET_ID,
                generation="fixture-empty",
                generation_review="fixture-review",
                cases=CASES,
                revision="2",
            )
            spec = with_keenable_declaration(fixture_spec(home), empty)
            registry = DatasetRegistry(home)
            registry.register(spec)
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            definitions = build_definitions(home)
            provider = definitions.resolve_job_def("build_keenable")
            with patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=AssertionError("No sources configured for acquisition"),
            ):
                healthy = provider.execute_in_process(instance=instance, partition_key=spec.dataset_id)
            self.assertTrue(healthy.success)
            data = DatasetDataService(home, instance)
            frames, bindings = available_candidates(home, data, spec.dataset_id)
            self.assertEqual(len(frames["keenable"]), 0)
            binding = bindings["keenable"]
            assert isinstance(binding, dict)
            self.assertEqual(binding["state"], "configured")

            broken = KeenableDeclaration(
                dataset=spec.dataset_id,
                generation="fixture-failed",
                generation_review="fixture-review",
                cases=CASES,
                permissions=(permission(),),
            )
            revised = with_keenable_declaration(spec, broken)
            registry.register(revised)
            with (
                patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
                patch(
                    "codebench_pipeline.keenable.request_direct",
                    side_effect=(
                        result(PLAN),
                        *(result() for _ in CASES[1:]),
                        DirectResponse(SOURCE_BODY, PEER),
                    ),
                ),
            ):
                failed = provider.execute_in_process(
                    instance=instance, partition_key=spec.dataset_id, raise_on_error=False
                )
            self.assertFalse(failed.success)
            (failure,) = failed.get_step_failure_events()
            error = failure.step_failure_data.error
            assert error is not None
            self.assertIn("origin_pin_mismatch (200)", error.to_string())
            _, bindings = available_candidates(home, data, spec.dataset_id)
            self.assertEqual(bindings["keenable"], {"state": "failed"})
            with self.assertRaisesRegex(
                HumanReviewError, "Declared collection merge requires current outputs"
            ):
                definitions.resolve_job_def("prepare_dataset").execute_in_process(
                    instance=instance,
                    partition_key=spec.dataset_id,
                    asset_selection=configured_selection(
                        [AssetKey(["datasets", "merged_command_candidates"])], declared=True
                    ),
                )

    def test_prepared_documentation_and_mapping_stay_readable_after_declared_registration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, dataset_instance(Path(temporary)) as instance:
            home = Path(temporary)
            legacy = load_build_config(write_fixture(home), home).model_copy(
                update={"dataset_id": CAT3_DATASET_ID, "annotation": guarded_fixture(home)}
            )
            registry = DatasetRegistry(home)
            registry.register(legacy)
            instance.add_dynamic_partitions("dataset_builds", [legacy.dataset_id])
            plan = plan_dataset(
                legacy, stage_configs(legacy), instance, home=home, through="labeling_documentation"
            )
            build_definitions(home).resolve_job_def("build_dataset").execute_in_process(
                instance=instance,
                partition_key=legacy.dataset_id,
                run_config=dataset_run_config(legacy, home),
                asset_selection=plan.asset_selection,
            )
            declared = load_build_config(BUNDLE, home)
            self.assertEqual(declared.annotation, legacy.annotation)
            self.assertEqual(declared.storage, legacy.storage)
            with (
                patch(
                    "sys.argv",
                    [
                        "dataset_cli",
                        "inspect-config",
                        "--config",
                        str(BUNDLE),
                        "--home",
                        str(home),
                        "--stage",
                        "labeling_attempts",
                    ],
                ),
                redirect_stdout(StringIO()) as stdout,
            ):
                main()
            self.assertEqual(
                json.loads(stdout.getvalue())["settings"], legacy.annotation.model_dump(mode="json")
            )
            registry.register(declared)

            data = DatasetDataService(home, instance)
            history = data.artifacts.query(
                ArtifactQuery(
                    asset_key=("datasets", "labeling_documentation"), partition_key=legacy.dataset_id
                )
            )
            config = data.artifacts.query(
                ArtifactQuery(
                    asset_key=("datasets", "map_labeling_documentation_config"),
                    partition_key=legacy.dataset_id,
                )
            )
            assert isinstance(history, ArtifactTable) and isinstance(config, ArtifactJSON)
            self.assertEqual(history.publication_state, "stale")
            self.assertEqual(config.publication_state, "stale")
            self.assertEqual(history.page.total_rows, 1)
            graph = build_definitions(home).resolve_asset_graph()
            self.assertNotIn(AssetKey(["datasets", "labeling_documentation"]), graph.get_all_asset_keys())
            self.assertEqual(
                registry.get_snapshot(legacy.dataset_id, revision=fingerprint(legacy.model_dump_json())),
                legacy,
            )

    def collection_fixture(self, home: Path) -> DatasetBuildSpec:
        spec = fixture_spec(home)
        collection = spec.declared_collection
        assert collection is not None
        repositories = repository_config_fixture()
        docker = RepositoryFixture(repositories.dataset[2])
        docker.file("sample/README.md", DOCKER)
        docker.file("sample/compose.yaml", b"services: {}")
        tldr = RepositoryFixture(repositories.documentation[0])
        tldr.file("pages/common/docker.md", TLDR)
        wire: dict[str, bytes] = {}
        for fixture in (docker, tldr):
            origin = fixture.origin
            wire[
                f"https://api.github.com/repos/{origin.repository}/git/trees/{origin.commit}?recursive=1"
            ] = fixture.response()
            for path, payload in fixture.bodies.items():
                wire[f"https://raw.githubusercontent.com/{origin.repository}/{origin.commit}/{path}"] = (
                    payload
                )

        def fetch(url: str, limit: int) -> bytes:
            payload = wire[url]
            self.assertLessEqual(len(payload), limit)
            return payload

        self.enterContext(
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                side_effect=fetch,
            )
        )
        spec = spec.model_copy(
            update={
                "declared_collection": collection.model_copy(
                    update={
                        "existing_datasets": SourceDeclaration.from_declaration(
                            repository_branch_fixture("dataset", (docker,))
                        ),
                        "documentation_crawl": SourceDeclaration.from_declaration(
                            repository_branch_fixture("documentation", (tldr,))
                        ),
                        "merge_keenable_weight": 0.24,
                    }
                )
            }
        )
        return with_keenable_declaration(spec, source_scope_declaration())
