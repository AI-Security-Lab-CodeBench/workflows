import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from configuration_fixture import write_fixture
from starlette.testclient import TestClient

from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    EvidenceTarget,
    PublicationResponse,
    ReviewViewResponse,
)
from codebench_pipeline.review_stages import ReviewRegistration, ReviewStage, ReviewStageList
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance

ORIGIN = "http://127.0.0.1:3000"


class ReviewWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.spec = load_build_config(write_fixture(self.home), self.home)
        DatasetRegistry(self.home).register(self.spec)
        self.table = ReviewTable(
            pd.DataFrame([{"model_id": "small", "score": 0.8}, {"model_id": "large", "score": 0.9}]),
            "model_id",
            "evaluation-1",
            {"metric": "held-out accuracy"},
            ("small", "large"),
        )
        self.stage = ReviewStage(
            stage_id="model_selection",
            title="Choose a model",
            asset_keys=(("models", "evaluations"),),
            min_approvals=1,
            max_approvals=1,
        )
        self.workflow = self.make_workflow(self.stage)
        self.query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)

    def resolve(self, dataset_id: str) -> ReviewTable:
        self.assertEqual(dataset_id, self.spec.dataset_id)
        return self.table

    def make_workflow(self, stage: ReviewStage) -> ReviewWorkflow:
        return ReviewWorkflow(self.home, stage, TableReviewSource(self.home, stage, self.resolve))

    def decide(self, candidate_id: str, decision: str, revision: int = 0) -> None:
        batch = self.workflow.view(self.query).batch
        self.workflow.decide(
            DecisionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=batch.snapshot.snapshot_id,
                change=DecisionChange.model_validate(
                    {
                        "batch_id": batch.batch_id,
                        "generation": batch.generation,
                        "candidate_id": candidate_id,
                        "expected_row_revision": revision,
                        "decision": decision,
                        "reviewer": "model reviewer",
                    }
                ),
            )
        )

    def completion(self) -> CompletionRequest:
        batch = self.workflow.view(self.query).batch
        return CompletionRequest(
            dataset_id=self.spec.dataset_id,
            snapshot_id=batch.snapshot.snapshot_id,
            expected=batch,
            reviewer="model reviewer",
        )

    def test_model_selection_is_independent_and_consumable_without_publication(self) -> None:
        other = self.make_workflow(self.stage.model_copy(update={"stage_id": "quality_check"}))
        original_other = other.view(self.query)
        self.decide("small", "reject")
        self.decide("large", "approve")
        result = self.workflow.complete(self.completion())
        self.assertIsNone(result.publication)
        self.assertIsNone(result.run_id)
        self.assertEqual(result.batch.state, "completed")
        assert result.completion is not None
        self.assertEqual(
            self.workflow.approved_records(self.spec.dataset_id, result.completion).to_dict(orient="records"),
            [{"model_id": "large", "score": 0.9}],
        )
        self.assertEqual(other.view(self.query).batch, original_other.batch)
        self.assertEqual(other.view(self.query).counts.pending, 2)
        with self.assertRaises(HumanReviewError):
            other.approved_records(self.spec.dataset_id, result.completion)

    def test_selection_bounds_are_enforced_at_completion(self) -> None:
        for choice in ("reject", "approve"):
            with self.subTest(choice=choice):
                for record in ("small", "large"):
                    revision = 0 if choice == "reject" else 1
                    self.decide(record, choice, revision)
                with self.assertRaises(HumanReviewError) as error:
                    self.workflow.complete(self.completion())
                self.assertEqual(error.exception.code, "selection")
                self.assertEqual(self.workflow.view(self.query).batch.state, "open")

    def test_changed_model_evidence_invalidates_completion_but_preserves_old_evidence(self) -> None:
        self.decide("small", "reject")
        self.decide("large", "approve")
        completed = self.workflow.complete(self.completion())
        assert completed.completion is not None
        self.table = ReviewTable(
            pd.DataFrame([{"model_id": "small", "score": 0.95}, {"model_id": "large", "score": 0.7}]),
            "model_id",
            "evaluation-2",
            {"metric": "new evaluation"},
            ("small", "large"),
        )
        with self.assertRaises(HumanReviewError) as stale:
            self.workflow.approved_records(self.spec.dataset_id, completed.completion)
        self.assertEqual(stale.exception.code, "stale")
        self.assertEqual(self.workflow.view(self.query).counts.pending, 2)
        evidence = self.workflow.evidence(
            EvidenceTarget(
                dataset_id=self.spec.dataset_id,
                batch_id=completed.batch.batch_id,
                candidate_id="large",
            )
        )
        self.assertEqual(evidence.fields["record"], {"model_id": "large", "score": 0.9})

    def test_generic_stage_http_routes_discover_review_and_complete_without_release(self) -> None:
        with dataset_instance(self.home) as instance:
            data = DatasetDataService(self.home, instance)
            app = make_dataset_api(
                data, self.workflow, 3000, stages=(ReviewRegistration(self.stage, self.workflow),)
            )
            with TestClient(app, base_url=ORIGIN, headers={"Origin": ORIGIN}) as client:
                catalog = ReviewStageList.model_validate_json(client.get("/review-stages").content)
                self.assertIn(self.stage, catalog.stages)
                path = "/reviews/model_selection"
                response = client.post(path, json={"dataset_id": self.spec.dataset_id})
                self.assertEqual(response.status_code, 200)
                view = ReviewViewResponse.model_validate_json(response.content)
                self.assertEqual(view.page.rows[1].review_record_id, "large")
                self.assertNotIn("candidate_id", view.page.columns)
                for record, decision in (("small", "reject"), ("large", "approve")):
                    response = client.put(
                        path + "/decision",
                        json={
                            "dataset_id": self.spec.dataset_id,
                            "snapshot_id": view.batch.snapshot.snapshot_id,
                            "change": {
                                "batch_id": view.batch.batch_id,
                                "generation": view.batch.generation,
                                "candidate_id": record,
                                "expected_row_revision": 0,
                                "decision": decision,
                                "reviewer": "human",
                            },
                        },
                    )
                    self.assertEqual(response.status_code, 200)
                response = client.get(
                    f"{path}/{view.batch.batch_id}/evidence/large",
                    params={"dataset_id": self.spec.dataset_id},
                )
                self.assertEqual(response.status_code, 200)
                response = client.post(path + "/complete", json=self.completion().model_dump(mode="json"))
                self.assertEqual(response.status_code, 200)
                result = PublicationResponse.model_validate_json(response.content)
                self.assertEqual(result.batch.snapshot.review_stage, "model_selection")
                self.assertIsNone(result.publication)
                self.assertIsNotNone(result.completion)

    def test_existing_single_stage_database_migrates_without_losing_decisions(self) -> None:
        self.decide("large", "approve")
        with self.workflow.store.transaction() as tx:
            saved = tx.get_current_batch(self.spec.dataset_id, self.stage.stage_id)
            rows = tx.list_decisions(saved.batch_id)
        old = saved.model_dump(mode="json")
        old["snapshot"].pop("review_stage")
        old["snapshot"].pop("min_approvals")
        old["snapshot"].pop("max_approvals")
        with sqlite3.connect(self.workflow.store.path) as connection:
            connection.execute("DROP INDEX human_review_current_stage")
            connection.execute("ALTER TABLE human_review_batches DROP COLUMN review_stage")
            connection.execute(
                "CREATE UNIQUE INDEX human_review_current ON human_review_batches(dataset_id) WHERE current=1"
            )
            connection.execute(
                "UPDATE human_review_batches SET data=? WHERE batch_id=?", (json.dumps(old), saved.batch_id)
            )
        with self.workflow.store.transaction() as tx:
            migrated = tx.get_current_batch(self.spec.dataset_id)
            self.assertEqual(migrated.snapshot.review_stage, "annotations")
            self.assertEqual(tx.list_decisions(migrated.batch_id), rows)
        fresh = self.workflow.view(self.query)
        self.assertEqual(fresh.counts.pending, 2)
        self.assertNotEqual(fresh.batch.batch_id, migrated.batch_id)
