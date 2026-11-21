import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg
from guarded_labeling_fixture import LabelingFixture
from psycopg import sql
from test_dataset_rules import make_label

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_config import require_declared_annotation
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.guarded_publication_controls import record_control
from codebench_pipeline.guarded_publication_runtime import preview_proposal
from codebench_pipeline.human_review import DecisionChange, HumanReviewError, ReviewDecision
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.release_admission import read_publication
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.review_api_models import CompletionRequest, DecisionRequest, PublicationTarget
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.runtime_instance import dataset_instance


class ReviewRegistrationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        fixture = LabelingFixture(
            self, self.home, native=True, limit_per_risk=1, category_confidence="medium"
        )
        self.spec = fixture.spec
        self.registry = DatasetRegistry(self.home)
        self.registry.register(self.spec)
        self.store = HumanReviewStore(self.home)
        self.addCleanup(self.drop_fixture_schema)
        self.instance = self.enterContext(dataset_instance(self.home))
        self.instance.add_dynamic_partitions("dataset_builds", [self.spec.dataset_id])
        self.build()
        self.data = DatasetDataService(self.home, self.instance)
        self.runner = ReleaseRunner(self.home, self.instance)
        self.service = ReviewService(self.home, self.data, self.runner)
        self.query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        view = self.service.view(self.query)
        self.assertGreaterEqual(len(view.decisions), 2)
        self.changed_row = view.page.rows[0].review_record_id
        self.changed_command = view.page.rows[0].values["command"]
        for row in view.decisions:
            self.decide(row)
        with self.store.transaction(dataset_id=self.spec.dataset_id) as tx:
            batch = tx.get_current_batch(self.spec.dataset_id)
            self.original_token = tx.complete(batch.snapshot, batch, "synthetic QA")
        self.original_proposal, _ = preview_proposal(self.home, self.instance, self.spec.dataset_id)
        with self.store.transaction(dataset_id=self.spec.dataset_id) as tx:
            record_control(tx, self.spec.dataset_id, "attest", actor=200, proposal=self.original_proposal)
        self.original_publication = self.runner(self.original_token)

    def drop_fixture_schema(self) -> None:
        with psycopg.connect(self.store.dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self.store.schema))
            )

    def build(self) -> None:
        spec = self.registry.get(self.spec.dataset_id)
        result = (
            build_definitions(self.home)
            .resolve_job_def("build_dataset")
            .execute_in_process(
                instance=self.instance,
                partition_key=spec.dataset_id,
                run_config=dataset_run_config(spec),
            )
        )
        self.assertTrue(result.success)

    def decide(self, row: ReviewDecision) -> None:
        batch = self.service.view(self.query).batch
        self.service.decide(
            DecisionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=batch.snapshot.snapshot_id,
                change=DecisionChange(
                    batch_id=batch.batch_id,
                    generation=batch.generation,
                    candidate_id=row.candidate_id,
                    expected_row_revision=row.row_revision,
                    decision="approve",
                    reviewer="synthetic QA",
                ),
            )
        )

    def test_revision_only_registration_reuses_exact_publication_and_still_honors_hold(self) -> None:
        prior_revision = self.registry.current_revision(self.spec.dataset_id)
        self.registry.register(self.spec.model_copy(update={"revision": "synthetic-metadata-only"}))
        self.assertNotEqual(self.registry.current_revision(self.spec.dataset_id), prior_revision)
        self.assertEqual(self.registry.get_snapshot(self.spec.dataset_id, revision=prior_revision), self.spec)
        with self.store.reader() as tx:
            batch = tx.get_current_batch(self.spec.dataset_id)
            self.assertEqual(
                tx.authorize_completion(batch.snapshot, self.original_token), self.original_token
            )
        proposal, _ = preview_proposal(self.home, self.instance, self.spec.dataset_id)
        self.assertEqual(proposal, self.original_proposal)
        self.assertEqual(self.runner(self.original_token), self.original_publication)
        inspected = self.data.query("dataset_release", self.query)
        assert isinstance(inspected, TableResponse)
        self.assertEqual(inspected.publication_state, "published")
        with self.store.transaction(dataset_id=self.spec.dataset_id) as tx:
            record_control(tx, self.spec.dataset_id, "hold", actor=300)
        held = self.data.query("dataset_release", self.query)
        assert isinstance(held, TableResponse)
        self.assertEqual(held.publication_state, "stale")
        with self.assertRaises(HumanReviewError) as blocked:
            self.runner(self.original_token)
        self.assertEqual(blocked.exception.code, "pending")

    def test_changed_original_label_carries_only_other_decisions_and_requires_completion(self) -> None:
        old = self.service.download(
            PublicationTarget(
                dataset_id=self.spec.dataset_id, publication_id=self.original_publication.publication_id
            )
        )
        with self.store.reader() as tx:
            historical = {row.candidate_id: row for row in tx.list_decisions(self.original_token.batch_id)}

        def changed_label(candidate: CandidateRecord, rules: CategoryRules) -> LabelDecision:
            label = make_label(candidate, rules)
            if candidate.command == self.changed_command:
                return label.model_copy(update={"severity": "high"})
            return label

        batch = require_declared_annotation(self.spec.annotation).model_copy(
            update={"backend_revision": "synthetic-new-label"}
        )
        updated = self.spec.model_copy(
            update={"annotation": self.spec.annotation.model_copy(update={"batch": batch})}
        )
        self.registry.register(updated)
        with patch("test_batch_labeling.make_label", side_effect=changed_label):
            self.build()
        view = self.service.view(self.query)
        self.assertEqual({row.candidate_id for row in view.decisions}, set(historical))
        self.assertEqual(view.counts.pending, 1)
        self.assertIsNone(view.batch.completion)
        (pending,) = (row for row in view.decisions if row.decision == "pending")
        self.assertEqual(pending.candidate_id, self.changed_row)
        self.assertEqual(pending.row_revision, 0)
        for carried in (row for row in view.decisions if row.candidate_id != self.changed_row):
            self.assertEqual(
                carried,
                historical[carried.candidate_id].model_copy(
                    update={
                        "batch_id": view.batch.batch_id,
                        "carried_from_batch_id": self.original_token.batch_id,
                    }
                ),
            )
        request = CompletionRequest(
            dataset_id=self.spec.dataset_id,
            snapshot_id=view.batch.snapshot.snapshot_id,
            expected=view.batch,
            reviewer="synthetic QA",
        )
        with self.assertRaises(HumanReviewError) as incomplete:
            self.service.complete(request)
        self.assertEqual(incomplete.exception.code, "pending")
        self.decide(pending)
        self.assertEqual(self.service.view(self.query).counts.pending, 0)
        with self.assertRaises(HumanReviewError) as uncompleted:
            preview_proposal(self.home, self.instance, self.spec.dataset_id)
        self.assertEqual(uncompleted.exception.code, "pending")
        stale = read_publication(self.home, self.spec.dataset_id, self.instance)
        assert stale is not None
        self.assertEqual(stale.publication_state, "stale")
        completed_view = self.service.view(self.query)
        with self.store.transaction(dataset_id=self.spec.dataset_id) as tx:
            batch = tx.get_current_batch(self.spec.dataset_id)
            completed = tx.complete(batch.snapshot, completed_view.batch, "synthetic QA")
            self.assertEqual(tx.list_publications(self.spec.dataset_id), (self.original_publication,))
        self.assertNotEqual(completed, self.original_token)
        proposal, _ = preview_proposal(self.home, self.instance, self.spec.dataset_id)
        self.assertNotEqual(proposal, self.original_proposal)
        with self.assertRaises(HumanReviewError) as unattested:
            self.runner(completed)
        self.assertEqual(unattested.exception.code, "pending")
        with self.store.transaction(dataset_id=self.spec.dataset_id) as tx:
            record_control(tx, self.spec.dataset_id, "attest", actor=200, proposal=proposal)
        replacement = self.runner(completed)
        self.assertNotEqual(replacement.publication_id, self.original_publication.publication_id)
        self.assertEqual(replacement.proposal_digest, proposal)
        self.assertEqual(
            self.service.download(
                PublicationTarget(
                    dataset_id=self.spec.dataset_id, publication_id=self.original_publication.publication_id
                )
            ),
            old,
        )
