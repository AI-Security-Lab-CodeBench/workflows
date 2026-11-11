import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import Mock, patch

import pandas as pd
from configuration_fixture import write_fixture
from dagster import AssetKey
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from pandera.errors import SchemaError
from pydantic import ValidationError

from codebench_pipeline.dataframe_schemas import CommandsTable
from codebench_pipeline.dataset_config import AnnotationConfig, FilteringConfig, load_build_config
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_plan import INPUT_STAGES, PREPARATION_STAGES, plan_dataset
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import digest


class DatasetGraphTests(unittest.TestCase):
    def test_publication_policy_change_invalidates_only_the_publication_signature(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            spec = load_build_config(write_fixture(Path(temporary)), Path(temporary))
            before = stage_signature("human_review_queue", spec.publication, {})
            extraction = stage_signature("command_candidates", spec.extraction, {})

            def changed_policy(path: Path) -> str:
                return "f" * 64 if path.name == "review_stages.py" else digest(path)

            with patch("codebench_pipeline.dataset_engine.digest", side_effect=changed_policy):
                self.assertNotEqual(stage_signature("human_review_queue", spec.publication, {}), before)
                self.assertEqual(stage_signature("command_candidates", spec.extraction, {}), extraction)

    def test_local_submission_reaches_launcher_without_waiting_for_a_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            with dataset_instance(home) as instance:
                spec = load_build_config(write_fixture(home), home)
                run = instance.create_run_for_job(
                    build_definitions(home).resolve_job_def("build_dataset"),
                    run_config=dataset_run_config(spec, home).to_config_dict(),
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
                {AssetKey(["datasets", *name.split("/")]) for name in INPUT_STAGES},
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "source_documents"])).parent_keys,
                {AssetKey(["datasets", "source_catalog"])},
            )
            self.assertIn(
                AssetKey(["datasets", "filtered_commands"]),
                assets.get(AssetKey(["datasets", "labeling_documentation"])).parent_keys,
            )
            self.assertEqual(
                assets.get(AssetKey(["datasets", "documentation", "source_documents"])).parent_keys,
                {AssetKey(["datasets", "documentation", "source_catalog"])},
            )
            self.assertIn(
                AssetKey(["datasets", "category_rules"]),
                assets.get(AssetKey(["datasets", "documentation", "proposed_annotations"])).parent_keys,
            )
            self.assertIn("datasets__documentation__source_catalog", {node.name for node in job.nodes})
            self.assertNotIn("datasets__documentation__dataset_release", {node.name for node in job.nodes})
            for branch, job_name in (("dataset", "release_dataset"),):
                release = definitions.resolve_job_def(job_name)
                prefix = "datasets__documentation__" if branch == "documentation" else "datasets__"
                self.assertEqual(
                    {node.name for node in release.nodes},
                    {f"{prefix}human_review_decisions", f"{prefix}dataset_release"},
                )
            for node in job.nodes:
                with self.subTest(node=node.name):
                    for output in node.definition.output_defs:
                        self.assertNotEqual(output.dagster_type.key, "String")
                    for incoming in node.definition.input_defs:
                        self.assertNotEqual(incoming.dagster_type.key, "String")

    def test_both_configured_branches_plan_without_source_approval(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = load_build_config(write_fixture(home), home)
            catalog = spec.documentation_catalog or spec.catalog
            spec = spec.model_copy(update={"catalog": catalog, "documentation_catalog": catalog})
            with dataset_instance(home) as instance:
                plan = plan_dataset(spec, stage_configs(spec), instance, home=home)
            self.assertIsNone(plan.waiting_for)
            self.assertEqual(set(plan.selected), set(PREPARATION_STAGES))
            self.assertEqual(plan.blocked, ())
            self.assertIn(AssetKey(["datasets", "documentation", "source_documents"]), plan.asset_selection)

    def test_unconfigured_branches_do_not_schedule_empty_inventories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = load_build_config(write_fixture(home), home)
            empty = spec.catalog.model_copy(update={"documents": [], "search_request": None})
            spec = spec.model_copy(update={"catalog": empty, "documentation_catalog": None})
            with dataset_instance(home) as instance:
                plan = plan_dataset(spec, stage_configs(spec), instance, home=home)
            self.assertEqual(
                plan.selected,
                (
                    "category_rules",
                    "merged_proposed_annotations",
                    "reviewed_annotations",
                    "deduplicated_annotations",
                    "human_review_queue",
                ),
            )
            self.assertEqual(plan.unconfigured, ("dataset", "documentation"))
            self.assertIsNone(plan.waiting_for)
            self.assertEqual(plan.blocked, ())

    def test_branch_configuration_keeps_source_inventories_separate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            spec = load_build_config(write_fixture(home), home)
            catalog = spec.documentation_catalog or spec.catalog
            spec = spec.model_copy(
                update={
                    "catalog": catalog.model_copy(update={"collection_revision": "dataset-only"}),
                    "documentation_catalog": catalog.model_copy(update={"collection_revision": "docs-only"}),
                }
            )
            configurations = stage_configs(spec)
            self.assertEqual(
                configurations["source_catalog"].model_dump()["collection_revision"], "dataset-only"
            )
            self.assertEqual(
                configurations["documentation/source_catalog"].model_dump()["collection_revision"],
                "docs-only",
            )
            self.assertIs(
                configurations["proposed_annotations"], configurations["documentation/proposed_annotations"]
            )
            run_config = dataset_run_config(spec, home).to_config_dict()
            self.assertIn("datasets__documentation__source_catalog", run_config["ops"])
            self.assertNotIn("datasets__documentation/source_catalog", run_config["ops"])

    def test_malformed_release_and_unknown_config_are_rejected(self) -> None:
        with self.assertRaises(SchemaError):
            CommandsTable.validate(pd.DataFrame({"command": ["git status"]}))
        with self.assertRaises(ValidationError):
            FilteringConfig.model_validate({"unknown_option": True})

    def test_policy_and_reviewer_changes_preserve_unaffected_stage_signatures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            spec = load_build_config(write_fixture(Path(temporary)), Path(temporary))
            changed = spec.rules.model_copy(update={"policy": spec.rules.policy + " Additional review rule."})
            original_inputs = {"category_rules": spec.rules}
            changed_inputs = {"category_rules": changed}
            for stage, config in (("source_catalog", spec.catalog), ("filtered_commands", spec.filtering)):
                self.assertEqual(
                    stage_signature(stage, config, original_inputs),
                    stage_signature(stage, config, changed_inputs),
                )
            self.assertNotEqual(
                stage_signature("proposed_annotations", spec.annotation, original_inputs),
                stage_signature("proposed_annotations", spec.annotation, changed_inputs),
            )
            reviewer = AnnotationConfig(
                model=spec.review.model.model_copy(update={"model": "another/reviewer"})
            )
            self.assertNotEqual(
                stage_signature("reviewed_annotations", spec.review, original_inputs),
                stage_signature("reviewed_annotations", reviewer, original_inputs),
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
