import json
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from hashlib import sha256
from io import StringIO
from pathlib import Path
from shutil import copytree, rmtree
from time import time
from unittest.mock import patch

import pandas as pd
from configuration_fixture import record_artifact, seed_prepared_snapshot
from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetRecordsFilter,
    DagsterEvent,
    DagsterEventType,
    EventLogRecord,
)
from dagster._core.definitions.data_version import get_input_data_version_tag, get_input_event_pointer_tag
from dagster._core.events import StepMaterializationData
from dagster._core.events.log import EventLogEntry
from native_dataset_cycle import NativeDatasetCycle
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
from codebench_pipeline.collection_stage_settings import KeenableDiscoverySettings, RepositoryCatalogSettings
from codebench_pipeline.config_assets import collection_mapping_key
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, asset_storage_key
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_cli import main
from codebench_pipeline.dataset_config import DatasetBuildSpec, load_build_config
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_finalization import available_candidates
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration
from codebench_pipeline.dataset_tables import documentation_table
from codebench_pipeline.declared_dataset_bundle import (
    DeclaredDatasetBundle,
    DeclaredLabeling,
    DeclaredS3Storage,
)
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.keenable_policy import CASES, Declaration, KeenableDeclaration, KeenableSourceAccess
from codebench_pipeline.keenable_transport import DirectResponse
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.repository_receipts import RepositoryReceipt
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, stage_configs
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


def fixture_spec(home: Path, bundle_path: Path = BUNDLE) -> DatasetBuildSpec:
    bundle = DeclaredDatasetBundle.model_validate_json(bundle_path.read_bytes())
    configured = bundle.model_copy(
        update={
            "labeling": declared_labeling_fixture(home),
            "storage": DeclaredS3Storage(backend="s3", bucket="fixture-artifacts"),
        }
    )
    resolved = configured.resolve(bundle_path.resolve().parent, home=home)
    return resolved.model_copy(update={"storage": StorageConfig(local_root=str(home / "dataframes"))})


def with_keenable_declaration(spec: DatasetBuildSpec, declaration: Declaration) -> DatasetBuildSpec:
    collection = spec.declared_collection
    assert collection is not None
    source = SourceDeclaration.from_declaration(declaration)
    return spec.model_copy(
        update={
            "declared_collection": collection.model_copy(update={"keenable_use_cases": source}),
            "config_directory": None,
        }
    )


class DeclaredDatasetGraphTests(unittest.TestCase):
    def test_adaptive_catalog_constructs_planner_from_mapped_config_without_registry(self) -> None:
        from codebench_pipeline.declared_source_assets import provider_collection
        from codebench_pipeline.repository import RepositoryRun

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            spec = fixture_spec(home)
            collection = spec.declared_collection
            assert collection is not None
            declaration = collection.keenable_declaration
            assert declaration.search_plan is not None
            adaptive = with_keenable_declaration(
                spec,
                declaration.model_copy(
                    update={"search_plan": declaration.search_plan.model_copy(update={"mode": "adaptive"})}
                ),
            )
            configs = stage_configs(adaptive)
            config = KeenableDiscoverySettings.model_validate_json(
                configs["keenable/discover_use_cases"].model_dump_json(), strict=True
            )
            access = KeenableSourceAccess.model_validate_json(
                configs["keenable/capture_origin_pages"].model_dump_json(), strict=True
            )
            batch = adaptive.annotation.batch
            assert batch is not None
            run = RepositoryRun(home, spec.dataset_id, "offline")
            with (
                patch.object(DatasetRegistry, "get", side_effect=AssertionError("Registry read")),
                patch.object(DatasetRegistry, "get_snapshot", side_effect=AssertionError("Snapshot read")),
            ):
                provider = provider_collection(config, run, plan_receipts=home, capacity=batch.capacity)
                planner = provider.planner
                assert planner is not None
                self.assertEqual(planner.endpoint, adaptive.annotation.model.base_url)
                self.assertEqual(planner.model, adaptive.annotation.model.model)
                self.assertEqual(planner.capacity, batch.capacity)
                self.assertEqual(planner.backend_revision, batch.backend_revision)
                self.assertEqual(planner.receipt_directory, home)
                self.assertIsNone(provider_collection(access, run).planner)
                self.assertEqual(access.declaration_sha256, config.declaration.declaration_sha256)

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
            inputs = copytree(BUNDLE.parent.parent, home / "business-configs")
            spec = self.enterContext(
                collection_fixture(fixture_spec(home, inputs / "datasets" / BUNDLE.name))
            )
            registry = DatasetRegistry(home)
            registry.register(spec)
            rmtree(inputs)
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
                    AssetKey(["datasets", "filtered_commands"]),
                    AssetKey(["datasets", "map_labeling_attempts_config"]),
                    AssetKey(["datasets", "documentation", "capture_documentation"]),
                },
            )
            data = DatasetDataService(home, instance)
            for stage in (
                "command_candidates",
                "documentation/command_candidates",
                "keenable/command_candidates",
            ):
                candidates = data.read_current(spec.dataset_id, stage).value
                assert isinstance(candidates, pd.DataFrame)
                self.assertNotIn("source_weight", candidates)
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
            cycle = NativeDatasetCycle(home)
            cycle.drain(instance, prepare_only=True, partition=spec.dataset_id)
            self.assertFalse(cycle.tick(instance, prepare_only=True))

            collection_nodes = (
                "datasets__enumerate_corpus_files",
                "datasets__capture_corpus_files",
                "datasets__command_candidates",
                "datasets__documentation__capture_documentation",
                "datasets__documentation__command_candidates",
                "datasets__keenable__discover_use_cases",
                "datasets__keenable__capture_origin_pages",
                "datasets__keenable__command_candidates",
            )
            with (
                patch(
                    "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                    side_effect=AssertionError("Cache hit reacquired repository bytes"),
                ),
                patch(
                    "codebench_pipeline.keenable.request_direct",
                    side_effect=AssertionError("Cache hit called the provider"),
                ),
            ):
                reused = job.execute_in_process(instance=instance, partition_key=spec.dataset_id)
            self.assertTrue(reused.success)
            for node in collection_nodes:
                with self.subTest(node=node):
                    (first,) = run.asset_materializations_for_node(node)
                    (cached,) = reused.asset_materializations_for_node(node)
                    self.assertIs(first.metadata["cache_hit"].value, False)
                    self.assertIs(cached.metadata["cache_hit"].value, True)
                    for name in ("input_fingerprint", "evidence", "codebench/content_digest"):
                        self.assertEqual(first.metadata[name], cached.metadata[name])

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
            self.assertEqual(
                data.read_current(spec.dataset_id, "filtered_commands").publication_state, "stale"
            )
            self.assertIsNone(
                data.read_current(spec.dataset_id, "documentation/command_candidates").publication_state
            )
            mapping_key = collection_mapping_key("dataset", "enumerate_corpus_files")
            mapping_filter = AssetRecordsFilter(
                asset_key=mapping_key,
                asset_partitions=[spec.dataset_id],
            )
            (prior,) = instance.fetch_materializations(mapping_filter, limit=1).records
            siblings = (
                AssetKey(["datasets", "documentation", "capture_documentation"]),
                AssetKey(["datasets", "documentation", "command_candidates"]),
                AssetKey(["datasets", "keenable", "discover_use_cases"]),
                AssetKey(["datasets", "keenable", "capture_origin_pages"]),
                AssetKey(["datasets", "keenable", "command_candidates"]),
            )

            def record_for(key: AssetKey) -> EventLogRecord:
                (record,) = instance.fetch_materializations(
                    AssetRecordsFilter(asset_key=key, asset_partitions=[spec.dataset_id]), limit=1
                ).records
                return record

            untouched = {key: record_for(key).storage_id for key in siblings}
            refresh_registered_configuration(spec.dataset_id, home)
            reloaded = cycle.drain(instance, prepare_only=True, partition=spec.dataset_id)
            (updated,) = instance.fetch_materializations(mapping_filter, limit=1).records
            self.assertGreater(updated.storage_id, prior.storage_id)
            assert prior.asset_materialization is not None and updated.asset_materialization is not None
            prior_tags = prior.asset_materialization.tags
            updated_tags = updated.asset_materialization.tags
            assert prior_tags is not None and updated_tags is not None
            self.assertNotEqual(
                prior_tags["dagster/data_version"],
                updated_tags["dagster/data_version"],
            )
            payload = home.joinpath("configuration-runs", updated.run_id, *mapping_key.path).read_bytes()
            mapped = RepositoryCatalogSettings.model_validate_json(payload, strict=True)
            self.assertEqual(mapped.origins[0].attribution, rights.attribution)
            self.assertEqual(mapped.origins[0].license.sha256, origin.rights.license_file.sha256)
            self.assertEqual(
                set(reloaded),
                {
                    "enumerate_corpus_files",
                    "capture_corpus_files",
                    "command_candidates",
                    "merged_command_candidates",
                    "filtered_commands",
                },
            )
            capture_key = AssetKey(["datasets", "capture_corpus_files"])
            capture = record_for(capture_key)
            candidates = record_for(AssetKey(["datasets", "command_candidates"]))
            filtered_record = record_for(AssetKey(["datasets", "filtered_commands"]))
            self.assertLess(updated.storage_id, capture.storage_id)
            self.assertLess(capture.storage_id, candidates.storage_id)
            self.assertLess(candidates.storage_id, filtered_record.storage_id)
            self.assertEqual(capture.run_id, candidates.run_id)
            assert capture.asset_materialization is not None and candidates.asset_materialization is not None
            capture_tags = capture.asset_materialization.tags
            candidate_tags = candidates.asset_materialization.tags
            assert capture_tags is not None and candidate_tags is not None
            self.assertEqual(
                candidate_tags[get_input_event_pointer_tag(capture_key)],
                str(capture.storage_id),
            )
            capture_version = capture_tags["dagster/data_version"]
            self.assertEqual(
                candidate_tags[get_input_data_version_tag(capture_key)],
                sha256(capture_version.encode()).hexdigest(),
            )
            self.assertEqual({key: record_for(key).storage_id for key in siblings}, untouched)
            print(
                f"NATIVE rights: mapping {prior.storage_id}->{updated.storage_id}; "
                f"capture={capture.storage_id} version={capture_version}; candidate={candidates.storage_id}; "
                f"filtered={filtered_record.storage_id}; sibling IDs unchanged"
            )
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
            from guarded_labeling_fixture import LabelingFixture

            spec = with_keenable_declaration(LabelingFixture(self, home).spec, empty)
            registry = DatasetRegistry(home)
            registry.register(spec)
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            definitions = build_definitions(home)
            provider = definitions.resolve_job_def("build_keenable")
            with patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=AssertionError("No sources configured for acquisition"),
            ):
                healthy = definitions.resolve_job_def("prepare_dataset").execute_in_process(
                    instance=instance, partition_key=spec.dataset_id
                )
            self.assertTrue(healthy.success)
            merge_key = AssetKey(["datasets", "merged_command_candidates"])
            merge_job = definitions.get_implicit_global_asset_job_def()
            listing = AssetRecordsFilter(asset_key=merge_key, asset_partitions=[spec.dataset_id])
            (previous_merge,) = instance.fetch_materializations(listing, limit=1).records
            manager = DataFrameIOManager(storage=spec.storage, review_home=str(home))
            frames, bindings = available_candidates(
                manager=manager, instance=instance, registered=DeclaredGlobalConfig.from_spec(spec)
            )
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
                    instance=instance,
                    partition_key=spec.dataset_id,
                    raise_on_error=False,
                )
            self.assertFalse(failed.success)
            (failure,) = failed.get_step_failure_events()
            error = failure.step_failure_data.error
            assert error is not None
            self.assertIn("origin_pin_mismatch (200)", error.to_string())
            _, bindings = available_candidates(
                manager=manager, instance=instance, registered=DeclaredGlobalConfig.from_spec(revised)
            )
            self.assertEqual(bindings["keenable"], {"state": "failed"})
            with self.assertRaisesRegex(HumanReviewError, "keenable acquisition receipt binding changed"):
                merge_job.execute_in_process(
                    instance=instance,
                    partition_key=spec.dataset_id,
                    asset_selection=[merge_key],
                )
            (latest_merge,) = instance.fetch_materializations(listing, limit=1).records
            self.assertEqual(latest_merge.storage_id, previous_merge.storage_id)

    def test_prepared_documentation_and_mapping_stay_readable_after_declared_registration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, dataset_instance(Path(temporary)) as instance:
            home = Path(temporary)
            documentation = DocumentationConfig.model_validate(
                {
                    "snippets": [
                        {
                            "utility": "git",
                            "subcommands": ["status"],
                            "url": "https://example.org/git/status",
                            "retrieved_at": "2026-11-01",
                            "snippet": "Status inspects repository state.",
                            "version_limits": "Synthetic archived evidence.",
                        }
                    ]
                }
            )
            legacy, revision = seed_prepared_snapshot(home, fixture_spec(home), documentation)
            registry = DatasetRegistry(home)
            with self.assertRaisesRegex(IntegrityError, "read-only"):
                registry.get(legacy.dataset_id)
            with self.assertRaisesRegex(IntegrityError, "Only declared"):
                registry.register(legacy)
            reference = DataFrameIOManager(storage=legacy.storage).persist_value(
                documentation_table(documentation)
            )
            ObjectStore(legacy.storage).write(
                asset_storage_key(("datasets", "labeling_documentation"), legacy.dataset_id),
                reference.model_dump_json().encode(),
            )
            record_artifact(
                instance,
                legacy.dataset_id,
                AssetKey(["datasets", "labeling_documentation"]),
                reference,
                {"registered_revision": revision},
            )
            run_id = "archived-prepared-build"
            key = AssetKey(["datasets", "map_labeling_documentation_config"])
            path = home.joinpath("configuration-runs", run_id, *key.path)
            path.parent.mkdir(parents=True)
            mapping = {"labeling_documentation": documentation.model_dump(mode="json")}
            path.write_text(json.dumps(mapping), encoding="utf-8")
            instance.store_event(
                EventLogEntry(
                    error_info=None,
                    level="INFO",
                    user_message="",
                    run_id=run_id,
                    timestamp=time(),
                    dagster_event=DagsterEvent(
                        event_type_value=DagsterEventType.ASSET_MATERIALIZATION.value,
                        job_name="build_dataset",
                        event_specific_data=StepMaterializationData(
                            AssetMaterialization(asset_key=key, partition=legacy.dataset_id)
                        ),
                    ),
                )
            )
            business = home / "business"
            copytree(BUNDLE.parent.parent, business)
            declaration = business / "datasets" / BUNDLE.name
            payload = json.loads(declaration.read_bytes())
            payload["storage"] = {"backend": "s3", "bucket": "fixture-artifacts"}
            declaration.write_text(json.dumps(payload), encoding="utf-8")
            declared = load_build_config(declaration, home)
            self.assertEqual(declared.annotation, legacy.annotation)
            self.assertEqual(declared.storage.backend, "s3")
            with (
                patch(
                    "sys.argv",
                    [
                        "dataset_cli",
                        "inspect-config",
                        "--config",
                        str(declaration),
                        "--home",
                        str(home),
                        "--stage",
                        "labeling_attempts",
                    ],
                ),
                redirect_stdout(StringIO()) as stdout,
            ):
                main()
            inspected = json.loads(stdout.getvalue())
            self.assertEqual(
                inspected["settings"], stage_configs(declared)["labeling_attempts"].model_dump(mode="json")
            )
            self.assertNotIn("policy", inspected)
            self.assertEqual(inspected["settings"]["rules"]["policy"], declared.rules.policy)
            self.assertEqual(
                [risk["id"] for risk in inspected["settings"]["rules"]["risks"]],
                [risk.id for risk in declared.rules.risks],
            )
            self.assertNotIn(str(home), stdout.getvalue())
            registry.register(declared)

            runs = instance.get_runs()
            data = DatasetDataService(home, instance)
            history = data.artifacts.query(
                ArtifactQuery(
                    asset_key=("datasets", "labeling_documentation"),
                    partition_key=legacy.dataset_id,
                    snapshot_id=reference.digest,
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
            self.assertEqual(config.value, mapping)
            self.assertEqual(history.page.total_rows, 1)
            self.assertEqual(history.page.rows[0].values["snippet"], documentation.snippets[0].snippet)
            graph = build_definitions(home).resolve_asset_graph()
            self.assertNotIn(AssetKey(["datasets", "labeling_documentation"]), graph.get_all_asset_keys())
            self.assertEqual(
                registry.get_snapshot(legacy.dataset_id, revision=revision),
                legacy,
            )
            self.assertEqual(instance.get_runs(), runs)


@contextmanager
def collection_fixture(spec: DatasetBuildSpec) -> Iterator[DatasetBuildSpec]:
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
        wire[f"https://api.github.com/repos/{origin.repository}/git/trees/{origin.commit}?recursive=1"] = (
            fixture.response()
        )
        for path, payload in fixture.bodies.items():
            wire[f"https://raw.githubusercontent.com/{origin.repository}/{origin.commit}/{path}"] = payload

    def fetch(url: str, limit: int) -> bytes:
        payload = wire[url]
        assert len(payload) <= limit
        return payload

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
                    "merge": collection.merge_config.model_copy(
                        update={
                            "keenable_source_weight": 0.24,
                            "repository_origin_weights": {"docker-awesome-compose": 0.62},
                        }
                    ),
                }
            )
        }
    )
    with patch("codebench_pipeline.repository_acquisition.fetch_repository_bytes", side_effect=fetch):
        yield with_keenable_declaration(spec, source_scope_declaration())
