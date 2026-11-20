import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from unittest.mock import Mock, patch

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pandera.errors import SchemaError
from pydantic import ValidationError
from test_declared_dataset_graph import fixture_spec, with_keenable_declaration
from test_keenable import source_declaration

from codebench_pipeline.collection_config import SourceDeclaration
from codebench_pipeline.collection_stage_settings import COLLECTION_OPERATIONS
from codebench_pipeline.config_assets import (
    CONFIG_DIRECTORY,
    collection_mapping_key,
    configured_selection,
    mapping_key,
)
from codebench_pipeline.dataframe_io import AssetSnapshot, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandsTable
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_dependencies import DECLARED_INPUT_STAGES
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.stage_implementation import ImplementationSource


class DatasetGraphTests(unittest.TestCase):
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

    def test_named_graph_has_pandera_table_edges(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            definitions = build_definitions(home)
            job = definitions.resolve_job_def("build_dataset")
            self.assertEqual(job.name, "build_dataset")
            self.assertNotIn("datasets__dataset_release", {node.name for node in job.nodes})
            self.assertNotIn("datasets__human_review_decisions", {node.name for node in job.nodes})
            assets = definitions.resolve_asset_graph()
            self.assertIn(
                AssetKey(["datasets", "human_review_decisions"]),
                assets.get(AssetKey(["datasets", "dataset_release"])).parent_keys,
            )
            self.assertEqual(
                assets.get_all_asset_keys(),
                set(
                    configured_selection(
                        AssetKey(["datasets", *name.split("/")]) for name in DECLARED_INPUT_STAGES
                    )
                )
                | {CONFIG_DIRECTORY, AssetKey(["datasets", "bashguard_syntax_audit"])},
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "deduplicated_annotations"])).parent_keys,
                {AssetKey(["datasets", "proposed_annotations"]), mapping_key("deduplicated_annotations")},
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "category_eligible_annotations"])).parent_keys,
                {
                    AssetKey(["datasets", "deduplicated_annotations"]),
                    mapping_key("category_eligible_annotations"),
                },
            )
            queue = AssetKey(["datasets", "human_review_queue"])
            decisions = AssetKey(["datasets", "human_review_decisions"])
            self.assertEqual(
                assets.get(queue).parent_keys,
                {AssetKey(["datasets", "category_eligible_annotations"]), mapping_key("human_review_queue")},
            )
            self.assertEqual(
                assets.get(decisions).parent_keys,
                {queue},
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "dataset_release"])).parent_keys,
                {
                    AssetKey(["datasets", "category_eligible_annotations"]),
                    decisions,
                    mapping_key("dataset_release"),
                },
            )
            self.assertNotIn(AssetKey(["datasets", "human_review_snapshot"]), assets.get_all_asset_keys())
            self.assertEqual(
                assets.get(AssetKey(["datasets", "merged_command_candidates"])).parent_keys,
                {
                    AssetKey(["datasets", "command_candidates"]),
                    AssetKey(["datasets", "documentation", "command_candidates"]),
                    AssetKey(["datasets", "keenable", "command_candidates"]),
                    AssetKey(["datasets", "map_merged_command_candidates_config"]),
                },
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "filtered_commands"])).parent_keys,
                {
                    AssetKey(["datasets", "merged_command_candidates"]),
                    AssetKey(["datasets", "map_filtered_commands_config"]),
                },
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "capture_corpus_files"])).parent_keys,
                {
                    AssetKey(["datasets", "enumerate_corpus_files"]),
                    collection_mapping_key("dataset", "capture_corpus_files"),
                },
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "labeling_attempts"])).parent_keys,
                {
                    AssetKey(["datasets", "filtered_commands"]),
                    AssetKey(["datasets", "map_labeling_attempts_config"]),
                    AssetKey(["datasets", "documentation", "capture_documentation"]),
                },
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "documentation", "capture_documentation"])).parent_keys,
                {collection_mapping_key("documentation", "capture_documentation")},
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "proposed_annotations"])).parent_keys,
                {AssetKey(["datasets", "labeling_attempts"]), mapping_key("proposed_annotations")},
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "labeling_failures"])).parent_keys,
                {AssetKey(["datasets", "labeling_attempts"])},
            )
            self.assertIn("datasets__documentation__capture_documentation", {node.name for node in job.nodes})
            self.assertNotIn("datasets__documentation__dataset_release", {node.name for node in job.nodes})
            release = definitions.resolve_job_def("release_dataset")
            self.assertEqual(
                {node.name for node in release.nodes},
                {
                    key.to_python_identifier()
                    for key in configured_selection(
                        AssetKey(["datasets", name]) for name in ("human_review_decisions", "dataset_release")
                    )
                },
            )
            for node in job.nodes:
                with self.subTest(node=node.name):
                    if node.name.startswith(("datasets__map_", "datasets__global_config")):
                        continue
                    for output in node.definition.output_defs:
                        self.assertNotEqual(output.dagster_type.key, "String")
                    for incoming in node.definition.input_defs:
                        if incoming.name not in {"mapped_config", "registered"}:
                            self.assertNotEqual(incoming.dagster_type.key, "String")

    def test_registered_partition_enables_native_mapping_for_all_declared_branches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = fixture_spec(home)
            DatasetRegistry(home).register(spec)
            refresh_registered_configuration(spec.dataset_id, home)
            with dataset_instance(home) as instance:
                requests = NativeDatasetCycle(home).tick(instance)
            self.assertEqual(len(requests), 1)
            self.assertEqual(requests[0].partition_key, spec.dataset_id)
            self.assertTrue(
                {
                    collection_mapping_key(operation.branch, operation.stage)
                    for operation in COLLECTION_OPERATIONS
                }.issubset(requests[0].asset_selection or [])
            )

    def test_expired_source_rights_prevent_branch_and_finalization_materializations(self) -> None:
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
            DatasetRegistry(home).register(spec)
            refresh_registered_configuration(spec.dataset_id, home)
            with dataset_instance(home) as instance:
                with self.assertRaisesRegex(HumanReviewError, "Keenable source authorization has expired"):
                    NativeDatasetCycle(home).drain(instance, partition=spec.dataset_id)
                for stage in (
                    "keenable/discover_use_cases",
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

    def test_branch_configuration_keeps_source_inventories_separate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = fixture_spec(home)
            collection = spec.declared_collection
            assert collection is not None
            declaration = collection.documentation_crawl.declaration
            assert isinstance(declaration, RepositoryBranchDeclaration)
            updated = SourceDeclaration.from_declaration(
                declaration.model_copy(update={"refresh_revision": "docs-only"})
            )
            configurations = stage_configs(spec)
            revised = stage_configs(
                spec.model_copy(
                    update={
                        "declared_collection": collection.model_copy(update={"documentation_crawl": updated})
                    }
                )
            )
            self.assertEqual(configurations["enumerate_corpus_files"], revised["enumerate_corpus_files"])
            self.assertEqual(
                configurations["keenable/discover_use_cases"], revised["keenable/discover_use_cases"]
            )
            self.assertNotEqual(
                configurations["documentation/capture_documentation"],
                revised["documentation/capture_documentation"],
            )
            self.assertNotEqual(configurations["labeling_attempts"], revised["labeling_attempts"])
            self.assertNotIn("documentation/proposed_annotations", configurations)

    def test_malformed_release_and_unknown_config_are_rejected(self) -> None:
        with self.assertRaises(SchemaError):
            CommandsTable.validate(pd.DataFrame({"command": ["git status"]}))
        with self.assertRaises(ValidationError):
            FilteringConfig.model_validate({"unknown_option": True})

    def test_policy_and_labeler_changes_preserve_unaffected_stage_signatures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            spec = fixture_spec(Path(temporary))
            changed = spec.rules.model_copy(update={"policy": spec.rules.policy + " Additional review rule."})
            original_configs = stage_configs(spec)
            changed_configs = stage_configs(spec.model_copy(update={"rules": changed}))
            for stage in ("enumerate_corpus_files", "filtered_commands"):
                self.assertEqual(
                    stage_signature(stage, original_configs[stage], {}),
                    stage_signature(stage, changed_configs[stage], {}),
                )
            original = original_configs["labeling_attempts"]
            self.assertNotEqual(
                stage_signature("labeling_attempts", original, {}),
                stage_signature("labeling_attempts", changed_configs["labeling_attempts"], {}),
            )
            annotation = spec.annotation.model_copy(
                update={"model": spec.annotation.model.model_copy(update={"model": "another/labeler"})}
            )
            labeler = stage_configs(spec.model_copy(update={"annotation": annotation}))["labeling_attempts"]
            self.assertNotEqual(
                stage_signature("labeling_attempts", original, {}),
                stage_signature("labeling_attempts", labeler, {}),
            )

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
