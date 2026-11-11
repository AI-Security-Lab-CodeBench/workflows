import io
import json
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from dagster import AssetKey, AssetRecordsFilter
from test_dataset_incremental import provider_response

from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_cli import execute_build, execute_registered_build
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.human_review import DecisionChange
from codebench_pipeline.review_api_models import CompletionRequest, DecisionRequest, RetractionRequest
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_stage


class SourceReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.home = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        original = write_fixture(self.home)
        config = json.loads(original.read_text())
        config["seeds"] = config.pop("documentation_sources")
        for seed in config["seeds"]["documents"]:
            seed["local_path"] = str((original.parent / seed["local_path"]).resolve())
        config["seeds"]["documents"].append(
            {
                **config["seeds"]["documents"][0],
                "source_id": "second-source-fixture",
            }
        )
        config["storage"] = {"local_root": str(self.home / "tables")}
        self.config = self.home / "config.json"
        self.config.write_text(json.dumps(config))
        for module in ("dataset_catalog", "dataset_documents", "dataset_annotation"):
            self.stack.enter_context(
                patch(f"codebench_pipeline.{module}.infer_typed", side_effect=provider_response)
            )
        with redirect_stdout(io.StringIO()):
            self.discovered = execute_build(self.config, self.home)
        self.spec = load_build_config(self.config, self.home)
        self.instance = self.stack.enter_context(dataset_instance(self.home))
        self.data = DatasetDataService(self.home, self.instance)
        self.query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        self.workflow = ReviewWorkflow(
            self.home, source_stage(), SourceReviewSource(self.home, self.instance)
        )

    def decide(self, identity: str, choice: str, revision: int = 0) -> None:
        view = self.workflow.view(self.query)
        self.workflow.decide(
            DecisionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=view.batch.snapshot.snapshot_id,
                change=DecisionChange.model_validate(
                    {
                        "batch_id": view.batch.batch_id,
                        "generation": view.batch.generation,
                        "candidate_id": identity,
                        "expected_row_revision": revision,
                        "decision": choice,
                        "reviewer": "source reviewer",
                    }
                ),
            )
        )

    def complete(self) -> None:
        view = self.workflow.view(self.query)
        self.workflow.complete(
            CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=view.batch.snapshot.snapshot_id,
                expected=view.batch,
                reviewer="source reviewer",
            )
        )

    def test_discovery_waits_and_manual_gate_materialization_cannot_bypass_selection(self) -> None:
        self.assertEqual(
            set(self.discovered),
            {"category_rules", "source_catalog", "merged_proposed_annotations", "reviewed_annotations"},
        )
        outcome = run_registered(self.spec.dataset_id, self.home)
        self.assertEqual(outcome.status, "awaiting_source_review")
        self.assertIsNone(outcome.run_id)
        premature = (
            build_definitions(self.home)
            .resolve_job_def("build_dataset")
            .execute_in_process(
                instance=self.instance,
                partition_key=self.spec.dataset_id,
                asset_selection=[AssetKey(["datasets", "human_source_decisions"])],
                raise_on_error=False,
            )
        )
        self.assertFalse(premature.success)
        self.assertEqual(
            self.instance.fetch_materializations(
                AssetRecordsFilter(asset_key=AssetKey(["datasets", "source_documents"])),
                limit=1,
            ).records,
            [],
        )

    def test_retraction_stales_descendants_and_reselection_changes_the_prepared_records(self) -> None:
        identities = self.workflow.view(self.query).batch.snapshot.candidate_ids
        self.decide(identities[0], "approve")
        self.decide(identities[1], "reject")
        self.complete()
        with redirect_stdout(io.StringIO()):
            prepared = execute_registered_build(self.spec.dataset_id, self.home)
        self.assertIn("labeling_documentation", prepared)
        documents = self.data.query("source_documents", self.query)
        assert isinstance(documents, TableResponse)
        self.assertEqual([row.values["source_id"] for row in documents.page.rows], [identities[0]])
        before = (
            self.instance.fetch_materializations(
                AssetRecordsFilter(asset_key=AssetKey(["datasets", "source_catalog"])),
                limit=1,
            )
            .records[0]
            .storage_id
        )
        completed = self.workflow.view(self.query)
        changed = self.workflow.retract(
            RetractionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=completed.batch.snapshot.snapshot_id,
                expected=completed.batch,
                candidate_id=identities[0],
                expected_row_revision=1,
                reviewer="source reviewer",
            )
        )
        stale = self.data.query("reviewed_annotations", self.query)
        assert isinstance(stale, TableResponse)
        self.assertEqual(stale.publication_state, "stale")
        paused = run_registered(self.spec.dataset_id, self.home)
        self.assertEqual(paused.status, "awaiting_source_review")
        self.assertIsNotNone(paused.run_id)
        self.assertEqual(
            set(paused.materialized_stages), {"merged_proposed_annotations", "reviewed_annotations"}
        )
        self.assertIsNone(run_registered(self.spec.dataset_id, self.home).run_id)
        self.decide(identities[0], "reject", changed.decision.row_revision)
        self.complete()
        with redirect_stdout(io.StringIO()):
            rebuilt = execute_registered_build(self.spec.dataset_id, self.home)
        self.assertNotIn("source_catalog", rebuilt)
        after = (
            self.instance.fetch_materializations(
                AssetRecordsFilter(asset_key=AssetKey(["datasets", "source_catalog"])),
                limit=1,
            )
            .records[0]
            .storage_id
        )
        self.assertEqual(after, before)
        empty = self.data.query("reviewed_annotations", self.query)
        assert isinstance(empty, TableResponse)
        self.assertEqual(empty.page.total_rows, 0)
        self.assertIsNone(empty.publication_state)
