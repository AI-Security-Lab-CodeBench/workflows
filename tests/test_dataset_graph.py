import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from unittest.mock import Mock, patch

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, AssetSelection
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pandera.errors import SchemaError
from pydantic import ValidationError
from test_declared_dataset_graph import fixture_spec, with_keenable_declaration
from test_keenable import source_declaration

from codebench_pipeline.config_assets import mapping_key
from codebench_pipeline.dataframe_io import AssetSnapshot, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandsTable
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_evidence import require_source_rights
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, stage_configs
from codebench_pipeline.stage_implementation import ImplementationSource
from codebench_pipeline.storage import IntegrityError


class DatasetGraphTests(unittest.TestCase):
    def test_selective_collection_and_policy_scopes_have_distinct_taxonomy_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            graph = build_definitions(Path(temporary)).resolve_asset_graph()
        taxonomy = AssetKey(["datasets", "category_rules"])
        for candidate in (
            AssetKey(["datasets", "command_candidates"]),
            AssetKey(["datasets", "documentation", "command_candidates"]),
        ):
            with self.subTest(candidate=candidate):
                self.assertNotIn(taxonomy, AssetSelection.assets(candidate).upstream().resolve(graph))
        for scope in ("filtered_commands", "labeling_attempts"):
            with self.subTest(scope=scope):
                self.assertIn(taxonomy, AssetSelection.assets(mapping_key(scope)).upstream().resolve(graph))

    def test_configuration_assets_have_consumers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            graph = build_definitions(Path(temporary)).resolve_asset_graph()
        configuration = AssetSelection.groups("configuration").resolve(graph) | {
            key
            for key in graph.materializable_asset_keys
            if graph.get(key).io_manager_key == "config_run_io_manager"
        }
        orphans = {key.to_user_string() for key in configuration if not graph.get(key).child_keys}
        self.assertEqual(orphans, set())

    def test_verified_storage_inputs_preserve_stage_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = fixture_spec(home)
            configs = stage_configs(spec)
            manager = DataFrameIOManager(storage=spec.storage)
            frame = pd.DataFrame({"command": ["git status", "git diff"], "context": [None, "inspection"]})
            frame.attrs["provenance"] = {"sources": [{"id": "fixture", "positions": [1, 2]}]}
            for value in (frame, spec.rules):
                reference = manager.persist_value(value)
                stored = AssetSnapshot(reference, manager.read_version(reference))
                for stage, config in (
                    ("enumerate_corpus_files", configs["enumerate_corpus_files"]),
                    ("filtered_commands", FilteringConfig()),
                    ("labeling_attempts", configs["labeling_attempts"]),
                ):
                    with self.subTest(format=reference.format, stage=stage):
                        self.assertEqual(
                            stage_signature(stage, config, {"input": value}),
                            stage_signature(stage, config, {"input": stored}),
                        )

    def test_publication_policy_change_invalidates_only_the_publication_signature(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            spec = fixture_spec(Path(temporary))
            before = stage_signature("human_review_queue", spec.publication, {})
            config = stage_configs(spec)["command_candidates"]
            extraction = stage_signature("command_candidates", config, {})

            read_source = ImplementationSource.read

            def changed_policy(source: ImplementationSource, name: str) -> bytes:
                payload = read_source(source, name)
                return payload + b"\n" if name == "codebench_pipeline/review_stages.py" else payload

            with patch.object(ImplementationSource, "read", changed_policy):
                self.assertNotEqual(stage_signature("human_review_queue", spec.publication, {}), before)
                self.assertEqual(stage_signature("command_candidates", config, {}), extraction)

    def test_local_submission_reaches_launcher_without_waiting_for_a_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            with dataset_instance(home) as instance:
                spec = fixture_spec(home)
                run_config = dataset_run_config(spec).to_config_dict()
                self.assertEqual(set(run_config["ops"]), {"datasets__global_config"})
                run = instance.create_run_for_job(
                    build_definitions(home).resolve_job_def("build_dataset"),
                    run_config=run_config,
                )
                context = SubmitRunContext(run, Mock(spec=BaseWorkspaceRequestContext))
                with patch.object(instance, "launch_run", side_effect=RuntimeError("launcher reached")):
                    with self.assertRaisesRegex(RuntimeError, "launcher reached"):
                        instance.run_coordinator.submit_run(context)

    def test_registered_partition_requests_a_native_build(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = fixture_spec(home)
            DatasetRegistry(home).register(spec)
            refresh_registered_configuration(spec.dataset_id, home)
            with dataset_instance(home) as instance:
                requests = NativeDatasetCycle(home).tick(instance)
            self.assertEqual(len(requests), 1)
            self.assertEqual(requests[0].partition_key, spec.dataset_id)

    def test_historical_expired_source_rights_do_not_enable_native_snippet_search(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            declaration = source_declaration()
            expired = declaration.model_copy(
                update={
                    "sources": tuple(
                        source.model_copy(update={"valid_until": datetime(2000, 1, 1, tzinfo=UTC)})
                        for source in declaration.sources
                    )
                }
            )
            spec = with_keenable_declaration(LabelingFixture(self, home).spec, expired)
            with self.assertRaisesRegex(HumanReviewError, "Keenable source authorization has expired"):
                require_source_rights(DeclaredGlobalConfig.from_spec(spec).source_config("keenable"))
            DatasetRegistry(home).register(spec)
            refresh_registered_configuration(spec.dataset_id, home)
            with dataset_instance(home) as instance:
                with self.assertRaisesRegex(
                    IntegrityError, "requires an explicit snippet search declaration"
                ):
                    NativeDatasetCycle(home).drain(instance, partition=spec.dataset_id)
                for stage in (
                    "keenable/command_candidates",
                    "filtered_commands",
                    "labeling_attempts",
                    "human_review_queue",
                ):
                    records = instance.fetch_materializations(
                        AssetRecordsFilter(
                            asset_key=AssetKey(["datasets", *stage.split("/")]),
                            asset_partitions=[spec.dataset_id],
                        ),
                        limit=1,
                    )
                    self.assertFalse(records.records, stage)

    def test_malformed_release_and_unknown_config_are_rejected(self) -> None:
        with self.assertRaises(SchemaError):
            CommandsTable.validate(pd.DataFrame({"command": ["git status"]}))
        with self.assertRaises(ValidationError):
            FilteringConfig.model_validate({"unknown_option": True})

    def test_concurrent_first_initialization_preserves_both_partition_updates(self) -> None:
        barrier = Barrier(2)
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)

            def initialize(key: str) -> None:
                barrier.wait(timeout=30)
                with dataset_instance(home) as instance:
                    instance.add_dynamic_partitions("initialization_test", [key])

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(initialize, key) for key in ("first", "second")]
                for future in futures:
                    future.result(timeout=60)
            with dataset_instance(home) as instance:
                self.assertEqual(
                    set(instance.get_dynamic_partitions("initialization_test")),
                    {"first", "second"},
                )
