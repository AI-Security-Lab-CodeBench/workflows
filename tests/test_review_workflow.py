import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from configuration_fixture import example_config
from jsonschema.exceptions import ValidationError
from pydantic import ValidationError as ConfigValidationError
from starlette.testclient import TestClient
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import (
    DecisionChange,
    HumanReviewError,
    PreparedReview,
    ReviewSnapshot,
    review_artifact_id,
    review_partition_id,
)
from codebench_pipeline.human_review_transactions import schema_validator
from codebench_pipeline.label_corrections import annotation_review_validation
from codebench_pipeline.labeling_stage_settings import ProposalRules
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.release_stage_settings import ReviewQueueConfig
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    EvidenceTarget,
    PublicationResponse,
    ReviewQueryRequest,
    ReviewViewResponse,
)
from codebench_pipeline.review_legacy_import import import_legacy
from codebench_pipeline.review_stages import ReviewRegistration, ReviewStage, ReviewStageList
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.table_query import TableQuery, TextFilter

ORIGIN = "http://127.0.0.1:3000"


class TableReviewSourceTests(unittest.TestCase):
    def test_annotation_adapter_schema_preserves_routing_taxonomy_and_documentation_constraints(self) -> None:
        rules = example_config().rules
        assert rules is not None
        candidate = CandidateRecord(
            candidate_id="one",
            command="git status",
            util="git",
            context="Inert fixture",
            source_id="qa",
            source_url="https://example.org/qa",
            source_position="line 1",
            transformations=[],
            risk_candidates=["CAT-99.1"],
        )
        label = LabelDecision(
            candidate_id="one",
            decision="needs_review",
            category_id="CAT-99",
            risk_id="CAT-99.1",
            severity="low",
            class_="allow",
            description="Inert fixture",
            util="git",
            category_confidence="high",
            risk_confidence="high",
            severity_confidence="medium",
            class_confidence="high",
            rationale="Synthetic",
            documentation_urls=["https://example.org/docs"],
        )
        row = {**candidate.model_dump(mode="json"), **label.model_dump(mode="json", by_alias=True)}
        schema = annotation_review_validation(ProposalRules.from_rules(rules))["row_schema"]
        assert isinstance(schema, dict)
        validator = schema_validator(schema)
        validator.validate({**row, "command": "git status --short", "severity": "high", "class": "ask"})
        for edits in (
            {"command": "git push"},
            {"risk_id": "CAT-99.2"},
            {"category_id": "CAT-98"},
            {"class": "modify"},
            {"severity": "unknown"},
            {"documentation_urls": []},
            {"decision": "reject"},
        ):
            with self.subTest(edits=edits), self.assertRaises(ValidationError):
                validator.validate({**row, **edits})

    def test_review_queue_projection_ignores_prompt_but_versions_routing_and_review_columns(self) -> None:
        rules = example_config().rules
        baseline = ReviewQueueConfig.from_rules(rules, ("command", "severity"), 4)
        prompt = rules.model_copy(update={"policy": "Modified model instructions."})
        risk = rules.risks[0].model_copy(update={"command_patterns": ["^git status --short$"]})
        routing = rules.model_copy(update={"risks": [risk]})

        self.assertEqual(ReviewQueueConfig.from_rules(prompt, baseline.review_columns, 4), baseline)
        self.assertNotEqual(ReviewQueueConfig.from_rules(routing, baseline.review_columns, 4), baseline)
        self.assertNotEqual(ReviewQueueConfig.from_rules(rules, ("command", "class"), 4), baseline)
        self.assertNotEqual(ReviewQueueConfig.from_rules(rules, baseline.review_columns, 1), baseline)
        with self.assertRaises(ConfigValidationError) as rejected:
            ReviewQueueConfig.model_validate_json(
                json.dumps({**baseline.model_dump(mode="json"), "dataset_id": "unrelated"})
            )
        self.assertEqual(rejected.exception.errors()[0]["loc"], ("dataset_id",))

    def test_native_partition_mapping_distinguishes_null_and_literal_strings(self) -> None:
        self.assertEqual(review_partition_id(None), "partition-" + hashlib.sha256(b"null").hexdigest())
        self.assertEqual(
            len({review_partition_id(value) for value in (None, "", "null", "unpartitioned")}), 4
        )
        self.assertEqual(review_artifact_id(("models", "a/b")), '["models","a/b"]')
        self.assertNotEqual(review_artifact_id(("models", "a/b")), review_artifact_id(("models", "a", "b")))

    def test_captured_schema_marks_only_review_columns_editable_and_keeps_declared_constraints(self) -> None:
        snapshot = ReviewSnapshot(
            dataset_id="opaque",
            snapshot_id="a" * 64,
            candidate_ids=("one",),
            eligible_candidate_ids=("one",),
            review_columns=("score",),
        )
        prepared = PreparedReview(
            snapshot=snapshot,
            id_column="id",
            columns=("id", "score"),
            rows=({"id": "one", "score": 0.5},),
            column_schema={"score": {"type": "number", "minimum": 0, "maximum": 1}},
        )
        self.assertEqual(
            prepared.column_schema["score"],
            {
                "type": "number",
                "minimum": 0,
                "maximum": 1,
                "editable": True,
                "readOnly": False,
            },
        )
        self.assertEqual(
            prepared.column_schema["id"],
            {
                "type": ["string"],
                "editable": False,
                "readOnly": True,
            },
        )
        with self.assertRaises(ValueError):
            PreparedReview(
                snapshot=snapshot,
                id_column="id",
                columns=("id", "score"),
                rows=prepared.rows,
                column_schema={"id": {"editable": True}},
            )

    def test_preparation_filter_selects_occurrences_before_dedup_without_paging(self) -> None:
        with tempfile.TemporaryDirectory(prefix="review-source-") as directory:
            stage = ReviewStage(
                stage_id="selection",
                title="Selection",
                asset_keys=(("models",),),
                review_columns=("score",),
            )
            frame = pd.DataFrame(
                [
                    {"id": str(index), "score": 0.5, "group": "keep" if index % 2 else "exclude"}
                    for index in range(405)
                ]
            )
            table = ReviewTable(
                frame,
                "id",
                "revision",
                {},
                tuple(str(index) for index in range(405)),
                artifact_id="models",
                scope_id="kept",
                membership=TableQuery(
                    filters=(TextFilter(field="group", operator="equals", value="keep"),),
                    limit=1,
                ),
            )
            home = Path(directory)
            source = TableReviewSource(
                home, stage, lambda _: table, ObjectStore(StorageConfig(local_root=str(home / "objects")))
            )
            captured = source.current("opaque-partition")
            self.assertEqual(captured.snapshot.candidate_ids, tuple(str(index) for index in range(1, 405, 2)))
            self.assertEqual(captured.snapshot.eligible_candidate_ids, captured.snapshot.candidate_ids)
            self.assertEqual(captured.snapshot.artifact_id, "models")
            self.assertEqual(captured.snapshot.scope_id, "kept")
            self.assertEqual(len(set(captured.snapshot.record_fingerprints.values())), 1)
            self.assertEqual(captured.frame["group"].unique().tolist(), ["keep"])


class ReviewWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.spec = fixture_spec(self.home)
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
            review_columns=("model_id", "score"),
        )
        self.workflow = self.make_workflow(self.stage)
        self.query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        self.workflow.prepare(self.spec.dataset_id)

    def resolve(self, dataset_id: str) -> ReviewTable:
        self.assertEqual(dataset_id, self.spec.dataset_id)
        return self.table

    def make_workflow(self, stage: ReviewStage) -> ReviewWorkflow:
        return ReviewWorkflow.for_home(
            self.home,
            stage,
            TableReviewSource(
                self.home,
                stage,
                self.resolve,
                ObjectStore(StorageConfig(local_root=str(self.home / "objects"))),
            ),
        )

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
        other.prepare(self.spec.dataset_id)
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

    def test_policy_completes_only_empty_queue_and_reuses_its_completion(self) -> None:
        workflow = self.make_workflow(self.stage.model_copy(update={"min_approvals": 0}))
        actor = "automatic low-uncertainty policy"
        key = self.spec.dataset_id
        self.assertIsNone(workflow.prepare(key, empty_queue_actor=actor).completion)
        self.table = ReviewTable(self.table.frame.iloc[:0], "model_id", "empty", {}, ())
        self.assertIsNone(workflow.prepare(key).completion)
        completed = workflow.prepare(key, empty_queue_actor=actor)
        self.assertIsNotNone(completed.completion)
        self.assertEqual(completed.reviewer, actor)
        self.assertEqual(workflow.prepare(key, empty_queue_actor=actor), completed)
        self.assertEqual(workflow.view(self.query).counts.pending, 0)

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
        self.workflow.prepare(self.spec.dataset_id)
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
        self.assertEqual(evidence.fields["occurrences"], [{"model_id": "large", "score": 0.9}])

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
        old["snapshot"]["candidate_ids"] = ["small", "large"]
        old["snapshot"]["eligible_candidate_ids"] = ["small", "large"]
        old["snapshot"].pop("review_stage")
        old["snapshot"].pop("min_approvals")
        old["snapshot"].pop("max_approvals")
        with self.workflow.store.reader() as reader:
            history = reader.history(self.spec.dataset_id)
        legacy = self.home / "legacy.sqlite3"
        with sqlite3.connect(legacy) as connection:
            connection.execute("CREATE TABLE human_review_batches (data TEXT)")
            connection.execute(
                "CREATE TABLE human_review_decisions (batch_id TEXT, candidate_id TEXT, data TEXT)"
            )
            connection.execute("CREATE TABLE human_review_history (sequence INTEGER PRIMARY KEY, data TEXT)")
            connection.execute("CREATE TABLE human_review_publications (data TEXT)")
            connection.execute("INSERT INTO human_review_batches VALUES (?)", (json.dumps(old),))
            connection.executemany(
                "INSERT INTO human_review_decisions VALUES (?, ?, ?)",
                [(saved.batch_id, row.candidate_id, row.model_dump_json()) for row in rows],
            )
            for entry in history:
                historical = entry.model_dump(mode="json")
                historical["batch"]["snapshot"]["review_stage"] = "annotations"
                connection.execute(
                    "INSERT INTO human_review_history(data) VALUES (?)", (json.dumps(historical),)
                )
        imported_home = self.home / "imported"
        report = import_legacy(legacy, imported_home, self.home / "backup.sqlite3")
        imported = ReviewWorkflow.for_home(imported_home, self.stage, self.workflow.source)
        self.assertEqual((report.batches, report.decisions), (1, 2))
        self.assertTrue(Path(report.backup_path).is_file())
        with imported.store.reader() as tx:
            migrated = tx.get_batch(saved.batch_id)
            self.assertEqual(migrated.snapshot.review_stage, "annotations")
            self.assertEqual(tx.list_decisions(migrated.batch_id), rows)
            self.assertEqual(len(tx.history(self.spec.dataset_id)), len(history))
        imported.prepare(self.spec.dataset_id)
        fresh = imported.view(self.query)
        self.assertEqual(fresh.counts.pending, 2)
        self.assertNotEqual(fresh.batch.batch_id, migrated.batch_id)

    def test_prepared_reads_and_decisions_survive_unavailable_upstream(self) -> None:
        with patch.object(self.workflow.source, "current", side_effect=AssertionError("Resolved upstream")):
            original = self.workflow.view(self.query)
            self.decide("small", "reject")
            self.decide("large", "approve")
            completed = self.workflow.complete(self.completion())
            assert completed.completion is not None
            self.assertEqual(
                self.workflow.approved_records(self.spec.dataset_id, completed.completion).to_dict("records"),
                [{"model_id": "large", "score": 0.9}],
            )
            evidence = self.workflow.evidence(
                EvidenceTarget(
                    dataset_id=self.spec.dataset_id, batch_id=original.batch.batch_id, candidate_id="small"
                )
            )
            self.assertEqual(evidence.fields["record"]["score"], 0.8)

    def test_selected_values_reuse_ignores_ids_evidence_neighbors_and_bounds(self) -> None:
        stage = self.stage.model_copy(
            update={
                "stage_id": "scores",
                "review_columns": ("score",),
                "min_approvals": 0,
                "max_approvals": None,
            }
        )
        workflow = self.make_workflow(stage)
        batch = workflow.prepare(self.spec.dataset_id)
        saved = workflow.decide(
            DecisionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=batch.snapshot.snapshot_id,
                change=DecisionChange(
                    batch_id=batch.batch_id,
                    generation=batch.generation,
                    candidate_id="small",
                    expected_row_revision=0,
                    decision="approve",
                    reviewer="human",
                    edits={"score": 0.85},
                ),
            )
        )
        with self.assertRaises(HumanReviewError) as nonselected:
            workflow.decide(
                DecisionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=batch.batch_id,
                        generation=batch.generation,
                        candidate_id="large",
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="human",
                        edits={"model_id": "changed"},
                    ),
                )
            )
        self.assertEqual(nonselected.exception.code, "ineligible")
        self.table = ReviewTable(
            pd.DataFrame(
                [
                    {"model_id": "renamed", "score": 0.8},
                    {"model_id": "new-original", "score": 0.85},
                    {"model_id": "neighbor", "score": 0.7},
                ]
            ),
            "model_id",
            "different-implementation-and-evidence",
            {"metric": "changed"},
            ("renamed", "new-original", "neighbor"),
        )
        changed_stage = stage.model_copy(update={"max_approvals": 1})
        restarted = self.make_workflow(changed_stage)
        current = restarted.prepare(self.spec.dataset_id)
        view = restarted.view(self.query)
        self.assertEqual([row.decision for row in view.decisions], ["approve", "pending", "pending"])
        self.assertEqual(view.decisions[0].edits, {"score": 0.85})
        self.assertEqual(view.decisions[0].carried_from_batch_id, saved.batch.batch_id)
        self.assertNotEqual(current.batch_id, batch.batch_id)
        self.assertIsNone(current.completion)
        with restarted.store.reader() as reader:
            self.assertEqual(reader.prepared(batch.batch_id).rows[0], {"model_id": "small", "score": 0.8})
            self.assertEqual(reader.prepared(batch.batch_id).evidence, {"metric": "held-out accuracy"})
            self.assertEqual(reader.list_decisions(batch.batch_id)[1], saved.decision)

    def test_duplicate_occurrences_share_decision_but_selection_counts_occurrences(self) -> None:
        self.table = ReviewTable(
            pd.DataFrame([{"model_id": "first", "score": 0.8}, {"model_id": "duplicate", "score": 0.8}]),
            "model_id",
            "duplicates",
            {},
            ("first", "duplicate"),
            artifact_id="models",
            scope_id="duplicate-selection",
        )
        stage = self.stage.model_copy(
            update={
                "stage_id": "duplicates",
                "review_columns": ("score",),
                "min_approvals": 2,
                "max_approvals": 2,
            }
        )
        workflow = self.make_workflow(stage)
        batch = workflow.prepare(self.spec.dataset_id)
        query = ReviewQueryRequest(
            dataset_id=self.spec.dataset_id, artifact_id="models", scope_id="duplicate-selection"
        )
        view = workflow.view(query)
        self.assertEqual((view.page.total_rows, view.page.matched_rows, view.counts.pending), (1, 1, 1))
        self.assertEqual(view.page.rows[0].occurrence_count, 2)
        request = DecisionRequest(
            dataset_id=self.spec.dataset_id,
            snapshot_id=batch.snapshot.snapshot_id,
            artifact_id="models",
            scope_id="duplicate-selection",
            change=DecisionChange(
                batch_id=batch.batch_id,
                generation=batch.generation,
                candidate_id="first",
                expected_row_revision=0,
                decision="approve",
                reviewer="human",
                edits={"score": 0.9},
            ),
        )
        saved = workflow.decide(request)
        self.assertEqual((saved.counts.approve, saved.counts.pending), (1, 0))
        duplicate = workflow.item(
            EvidenceTarget(
                dataset_id=self.spec.dataset_id,
                batch_id=batch.batch_id,
                candidate_id="duplicate",
                artifact_id="models",
                scope_id="duplicate-selection",
            )
        )
        self.assertEqual(duplicate.decision.edits, {"score": 0.9})
        self.assertEqual(duplicate.decision.row_revision, 1)
        with self.assertRaises(HumanReviewError) as stale:
            workflow.decide(
                request.model_copy(
                    update={"change": request.change.model_copy(update={"candidate_id": "duplicate"})}
                )
            )
        self.assertEqual(stale.exception.code, "stale")
        completed = workflow.complete(
            CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=batch.snapshot.snapshot_id,
                artifact_id="models",
                scope_id="duplicate-selection",
                expected=saved.batch,
                reviewer="human",
            )
        )
        assert completed.completion is not None
        self.assertEqual(
            workflow.approved_records(self.spec.dataset_id, completed.completion).to_dict("records"),
            [{"model_id": "first", "score": 0.9}, {"model_id": "duplicate", "score": 0.9}],
        )
