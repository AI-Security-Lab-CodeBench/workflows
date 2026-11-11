import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from configuration_fixture import write_fixture
from dagster import AssetKey
from review_test_support import approve_fixture_sources
from test_dataset_incremental import provider_response

from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_catalog import build_catalog
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.review_api_models import DecisionRequest, EvidenceTarget, RetractionRequest
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_stage
from codebench_pipeline.storage import IntegrityError


class IndependentBranchDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.home = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.spec = load_build_config(write_fixture(self.home), self.home)
        for module in ("dataset_catalog", "dataset_documents", "dataset_annotation"):
            self.stack.enter_context(
                patch(f"codebench_pipeline.{module}.infer_typed", side_effect=provider_response)
            )

    def test_runtime_corpus_metadata_cannot_bypass_admission_as_documentation(self) -> None:
        catalog = self.spec.documentation_catalog
        assert catalog is not None
        corpus = catalog.documents[0].model_copy(
            update={"source_type": "human_command_corpus", "provenance_type": "human_collected"}
        )
        configured = catalog.model_copy(update={"documents": [corpus]})
        with self.assertRaises(IntegrityError):
            build_catalog(configured, self.spec.rules, self.home / "attempt", branch="documentation")

    def test_documentation_job_needs_no_dataset_discovery_or_source_approval(self) -> None:
        DatasetRegistry(self.home).register(self.spec)
        with dataset_instance(self.home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [self.spec.dataset_id])
            result = (
                build_definitions(self.home)
                .resolve_job_def("build_documentation")
                .execute_in_process(instance=instance, partition_key=self.spec.dataset_id)
            )
            self.assertTrue(result.success)
            data = DatasetDataService(self.home, instance)
            reviewed = data.query(
                "reviewed_annotations", DatasetQueryRequest(dataset_id=self.spec.dataset_id)
            )
            assert isinstance(reviewed, TableResponse)
            self.assertGreater(reviewed.page.total_rows, 0)
        with HumanReviewStore(self.home).reader() as transaction:
            with self.assertRaises(HumanReviewError) as missing:
                transaction.get_current_batch(self.spec.dataset_id, "sources")
            self.assertEqual(missing.exception.code, "not_found")

    def test_documentation_prepares_while_dataset_sources_await_user_completion(self) -> None:
        spec = self.spec.model_copy(
            update={
                "catalog": self.spec.documentation_catalog,
                "snapshots": self.spec.documentation_snapshots,
            }
        )
        DatasetRegistry(self.home).register(spec)
        outcome = run_registered(spec.dataset_id, self.home)
        self.assertEqual(outcome.branches["dataset"].status, "awaiting_source_review")
        self.assertEqual(outcome.branches["documentation"].status, "reviewed")
        self.assertIsNone(outcome.branches["documentation"].review_stage)
        self.assertIn("documentation/proposed_annotations", outcome.materialized_stages)
        self.assertIn("reviewed_annotations", outcome.materialized_stages)
        self.assertNotIn("human_review_queue", outcome.materialized_stages)
        self.assertNotIn("source_documents", outcome.materialized_stages)
        self.assertEqual(outcome.branches["documentation"].blocked_stages, ())
        with HumanReviewStore(self.home).reader() as transaction:
            self.assertEqual(transaction.get_current_batch(spec.dataset_id, "sources").state, "open")
            with self.assertRaises(HumanReviewError) as missing:
                transaction.get_current_batch(spec.dataset_id, "annotations")
            self.assertEqual(missing.exception.code, "not_found")
        with dataset_instance(self.home) as instance:
            result = (
                build_definitions(self.home)
                .resolve_job_def("build_dataset")
                .execute_in_process(
                    instance=instance,
                    partition_key=spec.dataset_id,
                    asset_selection=[AssetKey(["datasets", "human_source_decisions"])],
                    raise_on_error=False,
                )
            )
        self.assertFalse(result.success)

    def test_documentation_only_build_reports_unconfigured_dataset_and_reuses_outputs(self) -> None:
        DatasetRegistry(self.home).register(self.spec)
        initial = run_registered(self.spec.dataset_id, self.home)
        self.assertEqual(initial.branches["dataset"].status, "unconfigured")
        self.assertIsNone(initial.branches["dataset"].review_batch_id)
        self.assertEqual(initial.branches["documentation"].status, "awaiting_human_review")
        self.assertNotIn("source_catalog", initial.materialized_stages)
        repeated = run_registered(self.spec.dataset_id, self.home)
        self.assertIsNone(repeated.run_id)
        self.assertEqual(repeated.materialized_stages, ())
        self.assertEqual(repeated.branches["documentation"].status, "unchanged")
        self.assertEqual(
            repeated.branches["documentation"].review_snapshot_id,
            initial.branches["documentation"].review_snapshot_id,
        )

    def test_both_unconfigured_branches_finalize_explicit_empty_inputs_without_inference(self) -> None:
        empty = self.spec.catalog.model_copy(update={"documents": [], "search_request": None})
        spec = self.spec.model_copy(update={"catalog": empty, "documentation_catalog": None})
        DatasetRegistry(self.home).register(spec)
        outcome = run_registered(spec.dataset_id, self.home)
        self.assertEqual(
            set(outcome.materialized_stages),
            {
                "category_rules",
                "merged_proposed_annotations",
                "reviewed_annotations",
                "deduplicated_annotations",
                "human_review_queue",
            },
        )
        with dataset_instance(self.home) as instance:
            data = DatasetDataService(self.home, instance)
            merged = data.read_current(spec.dataset_id, "merged_proposed_annotations")
            assert isinstance(merged.value, pd.DataFrame)
            self.assertEqual(len(merged.value), 0)
            self.assertEqual(
                merged.value.attrs["codebench_finalization"],
                {"dataset": {"state": "unconfigured"}, "documentation": {"state": "unconfigured"}},
            )

    def test_added_documentation_source_preserves_unchanged_row_fingerprint_and_human_decision(self) -> None:
        DatasetRegistry(self.home).register(self.spec)
        run_registered(self.spec.dataset_id, self.home)
        with dataset_instance(self.home) as instance:
            service = ReviewService(
                self.home,
                DatasetDataService(self.home, instance),
                ReleaseRunner(self.home, instance),
            )
            query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
            initial = service.view(query)
            identity = initial.batch.snapshot.candidate_ids[0]
            service.decide(
                DecisionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=initial.batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=initial.batch.batch_id,
                        generation=initial.batch.generation,
                        candidate_id=identity,
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="fixture human",
                        notes="Keep unchanged evidence.",
                    ),
                )
            )
        catalog = self.spec.documentation_catalog
        assert catalog is not None
        added = catalog.documents[0].model_copy(update={"source_id": "additional-fixture-source"})
        expanded = [*catalog.documents, added]
        changed = self.spec.model_copy(
            update={
                "documentation_catalog": catalog.model_copy(update={"documents": expanded}),
                "documentation_snapshots": self.spec.documentation_snapshots.model_copy(
                    update={"documents": expanded}
                ),
            }
        )
        DatasetRegistry(self.home).register(changed)
        run_registered(changed.dataset_id, self.home)
        with dataset_instance(self.home) as instance:
            service = ReviewService(
                self.home,
                DatasetDataService(self.home, instance),
                ReleaseRunner(self.home, instance),
            )
            current = service.view(query)
            self.assertNotEqual(current.batch.snapshot.snapshot_id, initial.batch.snapshot.snapshot_id)
            self.assertEqual(
                current.batch.snapshot.record_fingerprints[identity],
                initial.batch.snapshot.record_fingerprints[identity],
            )
            carried = next(row for row in current.decisions if row.candidate_id == identity)
            self.assertEqual(
                (carried.decision, carried.notes, carried.row_revision, carried.carried_from_batch_id),
                ("approve", "Keep unchanged evidence.", 1, initial.batch.batch_id),
            )
            self.assertEqual(current.batch.state, "open")
            self.assertIsNone(current.batch.completion)

    def test_unconfigured_branch_never_substitutes_its_historical_reviewed_output(self) -> None:
        spec = self.spec.model_copy(
            update={
                "catalog": self.spec.documentation_catalog,
                "snapshots": self.spec.documentation_snapshots,
            }
        )
        DatasetRegistry(self.home).register(spec)
        run_registered(spec.dataset_id, self.home)
        approve_fixture_sources(self.home, spec.dataset_id)
        run_registered(spec.dataset_id, self.home)
        empty = spec.catalog.model_copy(update={"documents": [], "search_request": None})
        DatasetRegistry(self.home).register(spec.model_copy(update={"catalog": empty}))
        with patch(
            "codebench_pipeline.dataset_annotation.infer_typed",
            side_effect=AssertionError("Unchanged documentation must not repeat inference"),
        ):
            outcome = run_registered(spec.dataset_id, self.home)
        self.assertEqual(
            set(outcome.materialized_stages),
            {
                "merged_proposed_annotations",
                "reviewed_annotations",
                "deduplicated_annotations",
                "human_review_queue",
            },
        )
        with dataset_instance(self.home) as instance:
            merged = DatasetDataService(self.home, instance).query(
                "merged_proposed_annotations",
                DatasetQueryRequest(dataset_id=spec.dataset_id),
            )
            assert isinstance(merged, TableResponse)
            self.assertEqual(
                {str(row.values["candidate_id"]).split(":")[0] for row in merged.page.rows},
                {"documentation"},
            )

    def test_dataset_retraction_preserves_documentation_but_documentation_changes_stale_it(self) -> None:
        spec = self.spec.model_copy(
            update={
                "catalog": self.spec.documentation_catalog,
                "snapshots": self.spec.documentation_snapshots,
            }
        )
        DatasetRegistry(self.home).register(spec)
        run_registered(spec.dataset_id, self.home)
        approve_fixture_sources(self.home, spec.dataset_id)
        run_registered(spec.dataset_id, self.home)
        with dataset_instance(self.home) as instance:
            data = DatasetDataService(self.home, instance)
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            service = ReviewService(self.home, data, ReleaseRunner(self.home, instance))
            view = service.view(query)
            evidence = service.evidence(
                EvidenceTarget(
                    dataset_id=spec.dataset_id,
                    batch_id=view.batch.batch_id,
                    candidate_id=view.batch.snapshot.candidate_ids[0],
                )
            )
            self.assertTrue(evidence.fields["duplicate_evidence"])
            before = data.query("documentation/proposed_annotations", query)
            assert isinstance(before, TableResponse)
            self.assertIsNone(before.publication_state)
            workflow = ReviewWorkflow(self.home, source_stage(), SourceReviewSource(self.home, instance))
            view = workflow.view(query)
            decision = view.decisions[0]
            workflow.retract(
                RetractionRequest(
                    dataset_id=spec.dataset_id,
                    snapshot_id=view.batch.snapshot.snapshot_id,
                    expected=view.batch,
                    candidate_id=decision.candidate_id,
                    expected_row_revision=decision.row_revision,
                    reviewer="fixture human",
                )
            )
            after = data.query("documentation/proposed_annotations", query)
            assert isinstance(after, TableResponse)
            self.assertIsNone(after.publication_state)
            self.assertEqual(after.snapshot_id, before.snapshot_id)
            corpus = data.query("deduplicated_annotations", query)
            assert isinstance(corpus, TableResponse)
            self.assertEqual(corpus.publication_state, "stale")
            assert spec.documentation_catalog is not None
            changed = spec.model_copy(
                update={
                    "documentation_catalog": spec.documentation_catalog.model_copy(
                        update={"collection_revision": "changed-documentation"}
                    )
                }
            )
            DatasetRegistry(self.home).register(changed)
            stale = data.query("documentation/proposed_annotations", query)
            assert isinstance(stale, TableResponse)
            self.assertEqual(stale.publication_state, "stale")
