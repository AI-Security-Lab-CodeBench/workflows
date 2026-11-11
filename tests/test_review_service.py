import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from configuration_fixture import write_fixture
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import ReviewedAnnotationsTable, validated_frame
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.human_review import (
    CompletionToken,
    DecisionChange,
    HumanReviewError,
    PublicationPayload,
    PublicationReference,
    ReviewBatch,
    ReviewSnapshot,
)
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    EvidenceTarget,
    PublicationTarget,
    RetractionRequest,
)
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.review_snapshots import ResolvedReview, ReviewManifest, ReviewSnapshots
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.table_query import TableQuery, TextFilter


class ReviewServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.spec = load_build_config(write_fixture(self.home), self.home)
        DatasetRegistry(self.home).register(self.spec)
        self.data = DatasetDataService(self.home, self.enterContext(dataset_instance(self.home)))
        self.service = ReviewService(self.home, self.data, self.fail_publication)
        self.manager = DataFrameIOManager(storage=self.spec.storage)
        self.resolved = self.resolve_evidence("  old\n<script>inert</script>  ")
        self.enterContext(patch.object(ReviewSnapshots, "current", self.current))
        self.query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)

    def current(self, dataset_id: str) -> ResolvedReview:
        self.data.specification(dataset_id)
        return self.resolved

    def locked_current(self, dataset_id: str) -> ResolvedReview:
        with self.assertRaises(HumanReviewError) as locked, self.service.store.transaction():
            self.fail("Mutating snapshot resolution must hold the review lock")
        self.assertEqual(locked.exception.code, "conflict")
        return self.current(dataset_id)

    def fail_publication(self, token: CompletionToken) -> PublicationReference:
        with self.service.store.transaction() as tx:
            self.assertEqual(tx.get_batch(token.batch_id).completion, token)
        raise HumanReviewError("publication_failed", "Fixture runner failed")

    def resolve_evidence(self, context: str) -> ResolvedReview:
        row: dict[str, JsonValue] = {
            "candidate_id": "first",
            "command": "git status",
            "util": "git",
            "context": context,
            "source_id": "source",
            "source_url": "https://example.org/source",
            "source_position": "line 1",
            "transformations": [],
            "risk_candidates": ["CAT-99.1"],
            "decision": "accept",
            "category_id": "CAT-99",
            "risk_id": "CAT-99.1",
            "severity": "low",
            "class": "allow",
            "description": "Inspects repository state.",
            "category_confidence": "medium",
            "risk_confidence": "high",
            "severity_confidence": "high",
            "class_confidence": "high",
            "rationale": "Inspection fixture.",
            "documentation_urls": ["https://example.org/docs"],
            "review_verdict": "approve",
            "review_rationale": "Fixture independently checked.",
            "review_confidence": "high",
        }
        reviewed = validated_frame([row], ReviewedAnnotationsTable)
        sources = pd.DataFrame([{"source_id": "source", "content": context}])
        docs = pd.DataFrame(
            [{"utility": "git", "url": "https://example.org/docs", "snippet": "  full docs\n"}]
        )
        references = {
            "reviewed_annotations": self.manager.persist_value(reviewed),
            "source_documents": self.manager.persist_value(sources),
            "labeling_documentation": self.manager.persist_value(docs),
            "category_rules": self.manager.persist_value(self.spec.rules),
        }
        candidate_hash = sha256(b"first").hexdigest()
        receipt = json.dumps({"provider_config": {"api_key_env": "PRIVATE_ENV"}, "status": "validated"})
        receipt_uri = self.manager.write_evidence(
            f"{sha256(context.encode()).hexdigest()}/receipt", receipt.encode()
        )
        evidence: dict[str, JsonValue] = {
            stage: {
                "receipts": {f"{name}/{candidate_hash}/receipt.json": receipt_uri},
                "config": {"model": {"api_key_env": "PRIVATE_ENV"}},
            }
            for stage, name in (("proposed_annotations", "label"), ("reviewed_annotations", "review"))
        }
        manifest = ReviewManifest(
            dataset_id=self.spec.dataset_id,
            storage=self.spec.storage,
            publication=self.spec.publication,
            references=references,
            evidence=evidence,
            implementation={},
        )
        snapshot_id = sha256(manifest.payload()).hexdigest()
        self.service.snapshots.store.write(f"{snapshot_id}.json", manifest.payload())
        snapshot = ReviewSnapshot(
            dataset_id=self.spec.dataset_id,
            snapshot_id=snapshot_id,
            candidate_ids=("first",),
            eligible_candidate_ids=("first",),
        )
        prepared = publication(reviewed, self.spec.rules, self.spec.publication)
        return ResolvedReview(manifest, snapshot, reviewed, self.spec.rules, prepared)

    def change(self, batch: ReviewBatch, revision: int = 0) -> DecisionRequest:
        return DecisionRequest(
            dataset_id=self.spec.dataset_id,
            snapshot_id=batch.snapshot.snapshot_id,
            change=DecisionChange(
                batch_id=batch.batch_id,
                generation=batch.generation,
                candidate_id="first",
                expected_row_revision=revision,
                decision="approve",
                reviewer="human",
                notes="  retain\nnotes  ",
            ),
        )

    def completion(self, batch: ReviewBatch) -> CompletionRequest:
        return CompletionRequest(
            dataset_id=self.spec.dataset_id,
            snapshot_id=batch.snapshot.snapshot_id,
            expected=batch,
            reviewer="human",
        )

    def test_mutations_resolve_evidence_under_writer_lock_and_release_it_for_continuation(self) -> None:
        batch = self.service.view(self.query).batch
        with patch.object(ReviewSnapshots, "current", self.locked_current):
            saved = self.service.decide(self.change(batch))
            with self.assertRaises(HumanReviewError) as failed:
                self.service.complete(self.completion(saved.batch))
            self.assertEqual(failed.exception.code, "publication_failed")
            with self.service.store.reader() as reader:
                completed = reader.get_current_batch(self.spec.dataset_id)
            with self.assertRaises(HumanReviewError) as continued:
                self.service.continue_review(self.completion(completed))
            self.assertEqual(continued.exception.code, "publication_failed")
            reopened = self.service.reopen(self.completion(completed))
            retracted = self.service.retract(
                RetractionRequest(
                    **self.completion(reopened.batch).model_dump(),
                    candidate_id="first",
                    expected_row_revision=saved.decision.row_revision,
                )
            )
            self.assertEqual(retracted.decision.decision, "pending")

    def test_refresh_queries_persisted_notes_without_mutating_annotations(self) -> None:
        batch = self.service.view(self.query).batch
        request = self.change(batch)
        saved = self.service.decide(request)
        restarted = ReviewService(self.home, self.data, self.fail_publication)
        query = TableQuery(filters=(TextFilter(field="human_notes", operator="contains", value="retain"),))
        view = restarted.view(self.query.model_copy(update={"query": query}))
        self.assertEqual(view.page.rows[0].values["human_notes"], "  retain\nnotes  ")
        self.assertEqual(view.decisions, (saved.decision,))
        self.assertEqual((view.counts.approve, view.batch.batch_revision), (1, 1))
        self.assertNotIn("human_notes", self.resolved.reviewed.columns)
        with self.assertRaises(HumanReviewError) as stale:
            self.service.decide(request)
        self.assertEqual(stale.exception.code, "stale")

    def test_changed_snapshot_rejects_old_requests_and_refresh_starts_pending(self) -> None:
        saved = self.service.decide(self.change(self.service.view(self.query).batch))
        self.resolved = self.resolve_evidence("new context")
        actions = (
            lambda: self.service.view(
                self.query.model_copy(update={"snapshot_id": saved.batch.snapshot.snapshot_id})
            ),
            lambda: self.service.decide(self.change(saved.batch, 1)),
            lambda: self.service.complete(self.completion(saved.batch)),
            lambda: self.service.reopen(self.completion(saved.batch)),
        )
        for action in actions:
            with self.subTest(action=action), self.assertRaises(HumanReviewError) as stale:
                action()
            self.assertEqual(stale.exception.code, "stale")
        fresh = self.service.view(self.query)
        self.assertNotEqual(fresh.batch.batch_id, saved.batch.batch_id)
        self.assertEqual(fresh.counts.pending, 1)
        with self.service.store.transaction() as tx:
            self.assertEqual(tx.list_decisions(saved.batch.batch_id), (saved.decision,))

    def test_failed_publication_releases_lock_and_requires_explicit_reopen(self) -> None:
        saved = self.service.decide(self.change(self.service.view(self.query).batch))
        with self.assertRaises(HumanReviewError) as failed:
            self.service.complete(self.completion(saved.batch))
        self.assertEqual(failed.exception.code, "publication_failed")
        view = self.service.view(self.query)
        self.assertEqual(view.batch.state, "completed")
        self.assertIsNone(view.publication)
        with self.assertRaises(HumanReviewError) as completed:
            self.service.decide(self.change(view.batch, 1))
        self.assertEqual(completed.exception.code, "completed")
        reopened = self.service.reopen(self.completion(view.batch))
        self.assertEqual(reopened.batch.generation, 2)
        self.assertEqual(self.service.view(self.query).decisions[0].notes, saved.decision.notes)

    def test_old_batch_reads_immutable_evidence_and_hides_provider_configuration(self) -> None:
        batch = self.service.view(self.query).batch
        original = self.resolved.reviewed.iloc[0]["context"]
        self.resolved = self.resolve_evidence("new context")
        self.service.view(self.query)
        response = self.service.evidence(
            EvidenceTarget(
                dataset_id=self.spec.dataset_id,
                batch_id=batch.batch_id,
                candidate_id="first",
            )
        )
        fields = response.fields
        self.assertEqual(
            TypeAdapter(dict[str, JsonValue]).validate_python(fields["source_document"])["content"], original
        )
        self.assertEqual(
            TypeAdapter(dict[str, JsonValue]).validate_python(fields["reviewed_annotation"])["context"],
            original,
        )
        self.assertEqual(
            TypeAdapter(list[dict[str, JsonValue]]).validate_python(fields["documentation"])[0]["snippet"],
            "  full docs\n",
        )
        self.assertNotIn("PRIVATE_ENV", response.model_dump_json())
        self.assertNotIn("local_root", response.model_dump_json())
        inference = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(fields["inference"])
        self.assertEqual(inference["proposed_annotations"]["receipt.json"], {"status": "validated"})
        self.assertEqual(inference["reviewed_annotations"]["receipt.json"], {"status": "validated"})

    def test_publication_download_is_historical_and_dataset_bound_after_reopen(self) -> None:
        saved = self.service.decide(self.change(self.service.view(self.query).batch))
        payload = b"command,description,util,severity,category,risk,source,seed,class,human_reviewed\r\n"
        ObjectStore(self.spec.storage).write("release/fixture/commands.csv", payload)
        with self.service.store.transaction() as tx:
            token = tx.complete(self.resolved.snapshot, saved.batch, "human")
            reference = tx.record_publication(
                self.resolved.snapshot,
                token,
                PublicationPayload(
                    payload_key="fixture",
                    payload_hash=sha256(payload).hexdigest(),
                    csv_reference="release/fixture/commands.csv",
                    bundle_reference="release/fixture/bundle",
                    dagster_run_id="fixture-not-a-Dagster-run",
                ),
            )
        view = self.service.view(self.query)
        self.assertEqual(view.publication, reference)
        self.service.reopen(self.completion(view.batch))
        self.assertIsNone(self.service.view(self.query).publication)
        target = PublicationTarget(dataset_id=self.spec.dataset_id, publication_id=reference.publication_id)
        self.assertEqual(self.service.download(target), payload)
        with self.assertRaises(HumanReviewError) as missing:
            self.service.download(target.model_copy(update={"dataset_id": "other"}))
        self.assertEqual(missing.exception.code, "not_found")
