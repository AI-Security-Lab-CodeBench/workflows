import csv
import io
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from dagster import DagsterInstance
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration
from codebench_pipeline.dataset_table_contract import FIELDS
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.guarded_publication_controls import record_control
from codebench_pipeline.guarded_publication_runtime import preview_proposal
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.release_admission import read_publication
from codebench_pipeline.release_runner import ReleaseContinuation, ReleaseRunner
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    PublicationResponse,
    PublicationTarget,
    RetractionRequest,
)
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.runtime_instance import dataset_instance


def attest_fixture_release(home: Path, instance: DagsterInstance, dataset_id: str) -> None:
    proposal, _ = preview_proposal(home, instance, dataset_id)
    with HumanReviewStore(home).transaction(dataset_id=dataset_id) as tx:
        record_control(tx, dataset_id, "attest", actor=200, proposal=proposal)


class ReviewReleaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.home = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        fixture = LabelingFixture(self, self.home, limit_per_risk=1, category_confidence="medium")
        self.spec = fixture.spec
        DatasetRegistry(self.home).register(self.spec)
        refresh_registered_configuration(self.spec.dataset_id, self.home)
        self.instance = self.stack.enter_context(dataset_instance(self.home))
        self.cycle = NativeDatasetCycle(self.home)
        self.prepared = self.cycle.drain(self.instance, partition=self.spec.dataset_id)
        self.data = DatasetDataService(self.home, self.instance)
        self.service = ReviewService(self.home, self.data, ReleaseRunner(self.home, self.instance))

    def complete_release(self, request: CompletionRequest) -> PublicationResponse:
        with self.assertRaises(HumanReviewError) as pending:
            self.service.complete(request)
        self.assertEqual(pending.exception.code, "pending")
        attest_fixture_release(self.home, self.instance, self.spec.dataset_id)
        return self.service.continue_review(
            request.model_copy(
                update={
                    "expected": self.service.view(DatasetQueryRequest(dataset_id=self.spec.dataset_id)).batch
                }
            )
        )

    def test_prepare_waits_and_real_release_applies_human_exclusions_before_cap(self) -> None:
        self.assertNotIn("dataset_release", self.prepared)
        self.assertNotIn("human_review_decisions", self.prepared)
        premature = (
            build_definitions(self.home)
            .resolve_job_def("release_dataset")
            .execute_in_process(
                instance=self.instance,
                partition_key=self.spec.dataset_id,
                raise_on_error=False,
            )
        )
        self.assertFalse(premature.success)
        view = self.service.view(DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        identities = tuple(row.review_record_id for row in view.page.rows)
        self.assertGreaterEqual(len(identities), 2)
        expected_command = next(
            row.values["command"] for row in view.page.rows if row.values["candidate_id"] == identities[1]
        )
        for candidate_id in identities:
            self.service.decide(
                DecisionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=view.batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=view.batch.batch_id,
                        generation=view.batch.generation,
                        candidate_id=candidate_id,
                        expected_row_revision=0,
                        decision="approve" if candidate_id == identities[1] else "reject",
                        reviewer="QA reviewer",
                        notes="QA exclusion" if candidate_id != identities[1] else "",
                        edits={"severity": "critical", "class": "deny"}
                        if candidate_id == identities[1]
                        else {},
                    ),
                )
            )
        saved = self.service.view(DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        result = self.complete_release(
            CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=saved.batch.snapshot.snapshot_id,
                expected=saved.batch,
                reviewer="QA reviewer",
            )
        )
        assert result.publication is not None
        payload = self.service.download(
            PublicationTarget(
                dataset_id=self.spec.dataset_id, publication_id=result.publication.publication_id
            )
        )
        reader = csv.DictReader(io.StringIO(payload.decode()))
        self.assertEqual(tuple(reader.fieldnames or ()), FIELDS)
        rows = list(reader)
        self.assertEqual([row["command"] for row in rows], [expected_command])
        self.assertEqual([row["human_reviewed"] for row in rows], ["true"])
        self.assertEqual([(row["severity"], row["class"]) for row in rows], [("critical", "deny")])
        original = self.service.snapshots.current(self.spec.dataset_id).reviewed
        self.assertEqual(
            original.loc[original["candidate_id"] == identities[1], ["severity", "class"]].values.tolist(),
            [["low", "allow"]],
        )
        released = self.data.query("dataset_release", DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        assert isinstance(released, TableResponse)
        self.assertEqual(released.publication_state, "published")
        self.assertEqual(released.page.total_rows, 1)
        self.assertEqual(tuple(released.page.columns), FIELDS)
        snapshot = read_publication(self.home, self.spec.dataset_id, self.instance)
        assert snapshot is not None and isinstance(snapshot.value, pd.DataFrame)
        self.assertEqual(snapshot.publication_state, "published")
        self.assertEqual(snapshot.value["command"].tolist(), [rows[0]["command"]])
        before = len(self.instance.get_runs())
        self.assertEqual(self.cycle.drain(self.instance, partition=self.spec.dataset_id), {})
        self.assertEqual(len(self.instance.get_runs()), before)
        completed = self.service.view(DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        retried = self.service.complete(
            CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=completed.batch.snapshot.snapshot_id,
                expected=completed.batch,
                reviewer="QA reviewer",
            )
        )
        self.assertEqual(retried.publication, result.publication)
        self.assertEqual(len(self.instance.get_runs()), before)
        retracted = self.service.retract(
            RetractionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=completed.batch.snapshot.snapshot_id,
                expected=completed.batch,
                candidate_id=identities[1],
                expected_row_revision=1,
                reviewer="QA reviewer",
            )
        )
        stale = self.data.query("dataset_release", DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        assert isinstance(stale, TableResponse)
        self.assertEqual(stale.publication_state, "stale")
        stale_decisions = self.data.query(
            "human_review_decisions", DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        )
        assert isinstance(stale_decisions, TableResponse)
        self.assertEqual(stale_decisions.publication_state, "stale")
        changed = self.service.decide(
            DecisionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=retracted.batch.snapshot.snapshot_id,
                change=DecisionChange(
                    batch_id=retracted.batch.batch_id,
                    generation=retracted.batch.generation,
                    candidate_id=identities[1],
                    expected_row_revision=retracted.decision.row_revision,
                    decision="reject",
                    reviewer="QA reviewer",
                    notes="Retracted inclusion",
                ),
            )
        )
        replacement = self.complete_release(
            CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=changed.batch.snapshot.snapshot_id,
                expected=changed.batch,
                reviewer="QA reviewer",
            )
        )
        assert replacement.publication is not None
        self.assertNotEqual(replacement.publication.publication_id, result.publication.publication_id)
        updated = self.data.query("dataset_release", DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        assert isinstance(updated, TableResponse)
        self.assertEqual(updated.page.total_rows, 0)
        self.assertEqual(updated.publication_state, "published")
        self.assertEqual(
            self.service.download(
                PublicationTarget(
                    dataset_id=self.spec.dataset_id, publication_id=result.publication.publication_id
                )
            ),
            payload,
        )

    def test_native_completion_returns_pending_run_and_reuses_its_signal(self) -> None:
        service = ReviewService(self.home, self.data, ReleaseContinuation(self.home, self.instance, 3040))
        query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        view = service.view(query)
        for row in view.decisions:
            service.decide(
                DecisionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=view.batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=view.batch.batch_id,
                        generation=view.batch.generation,
                        candidate_id=row.candidate_id,
                        expected_row_revision=row.row_revision,
                        decision="approve"
                        if next(
                            item for item in view.page.rows if item.review_record_id == row.candidate_id
                        ).blocked_reason
                        is None
                        else "reject",
                        reviewer="QA reviewer",
                    ),
                )
            )

        def launch(_port: int, params: dict[str, JsonValue]) -> str:
            metadata = TypeAdapter(dict[str, list[dict[str, str]]]).validate_python(
                params["executionMetadata"]
            )
            run = self.instance.create_run_for_job(
                build_definitions(self.home).resolve_job_def("release_dataset"),
                run_config=TypeAdapter(dict[str, JsonValue]).validate_python(params["runConfigData"]),
                tags={tag["key"]: tag["value"] for tag in metadata["tags"]},
            )
            return run.run_id

        batch = service.view(query).batch
        with patch("codebench_pipeline.release_runner.launch_reviewed_run", side_effect=launch):
            pending = service.complete(
                CompletionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=batch.snapshot.snapshot_id,
                    expected=batch,
                    reviewer="QA reviewer",
                )
            )
            self.assertIsNone(pending.run_id)
            attest_fixture_release(self.home, self.instance, self.spec.dataset_id)
            completed = service.continue_review(
                CompletionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=pending.batch.snapshot.snapshot_id,
                    expected=pending.batch,
                    reviewer="QA reviewer",
                )
            )
            self.assertIsNone(completed.publication)
            assert completed.run_id is not None
            native_run = self.instance.get_run_by_id(completed.run_id)
            assert native_run is not None
            self.assertFalse(native_run.is_finished)
            before = len(self.instance.get_runs())
            repeated = service.continue_review(
                CompletionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=completed.batch.snapshot.snapshot_id,
                    expected=completed.batch,
                    reviewer="QA reviewer",
                )
            )
            self.assertEqual(repeated.run_id, completed.run_id)
            self.assertEqual(len(self.instance.get_runs()), before)


if __name__ == "__main__":
    unittest.main()
