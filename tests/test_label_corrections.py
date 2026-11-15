import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from configuration_fixture import write_fixture
from psycopg.types.json import Jsonb
from pydantic import JsonValue, ValidationError

from codebench_pipeline.category_rules import (
    CategoryRules,
    ClassRule,
    RiskRule,
    SeverityRule,
)
from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_schemas import ProposedAnnotationsTable, validated_frame
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.human_review import (
    CompletionEffect,
    DecisionChange,
    HumanReviewError,
    LabelCorrections,
    PreparedReview,
    ReviewDecision,
    ReviewSnapshot,
    decisions_digest,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.label_corrections import annotation_review_validation, apply_label_corrections
from codebench_pipeline.review_api_models import CompletionRequest, DecisionRequest
from codebench_pipeline.review_frames import (
    CORRECTIONS_BINDING,
    decision_frame,
    require_decision_frame,
)
from codebench_pipeline.review_identity import review_identity
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.review_snapshots import ResolvedReview, ReviewManifest, ReviewSnapshots
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.storage import fingerprint


class LabelCorrectionTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="codebench-label-corrections-")
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.store = HumanReviewStore(self.home)
        self.rules = CategoryRules(
            category_id="CAT-99",
            category_name="Synthetic correction fixture",
            version="1",
            description_language="English",
            policy="Inert test fixture only.",
            risks=[
                RiskRule(
                    id=identifier,
                    name=identifier,
                    definition="Synthetic risk.",
                    command_patterns=[r"^printf"],
                    guidance="Static test data.",
                )
                for identifier in ("CAT-99.1", "CAT-99.2")
            ],
            classes=[
                ClassRule(name=name, definition="Synthetic class.") for name in ("allow", "deny", "ask")
            ],
            severities=[
                SeverityRule(name=name, definition="Synthetic severity.")
                for name in ("low", "medium", "high", "critical")
            ],
        )
        self.candidate = CandidateRecord(
            candidate_id="first",
            command="printf inert",
            util="printf",
            context="Never executed.",
            source_id="fixture",
            source_url="https://example.org/fixture",
            source_position="line 1",
            transformations=[],
            risk_candidates=["CAT-99.1", "CAT-99.2"],
        )
        self.label = LabelDecision(
            candidate_id="first",
            decision="accept",
            category_id="CAT-99",
            risk_id="CAT-99.1",
            severity="low",
            class_="allow",
            description="Inert fixture.",
            util="printf",
            category_confidence="medium",
            risk_confidence="medium",
            severity_confidence="low",
            class_confidence="medium",
            rationale="Synthetic uncertainty.",
            documentation_urls=["https://example.org/docs"],
        )
        self.snapshot = ReviewSnapshot(
            dataset_id="correction-fixture",
            snapshot_id="a" * 64,
            candidate_ids=("first",),
            eligible_candidate_ids=("first",),
            record_fingerprints={"first": "b" * 64},
            review_columns=("command", "category_id", "risk_id", "severity", "class"),
        )
        self.corrections = LabelCorrections(
            category_id="CAT-99", risk_id="CAT-99.2", severity="critical", class_="deny"
        )
        self.annotations = validated_frame(
            [{**self.candidate.model_dump(), **self.label.model_dump(by_alias=True)}],
            ProposedAnnotationsTable,
        )
        original = {**self.candidate.model_dump(mode="json"), **self.label.model_dump(mode="json", by_alias=True)}
        self.snapshot = self.snapshot.model_copy(update={
            "record_fingerprints": {"first": review_identity(original, self.snapshot.review_columns)}
        })
        self.prepared = PreparedReview(
            snapshot=self.snapshot,
            id_column="candidate_id",
            columns=tuple(original),
            rows=(original,),
            validation=annotation_review_validation(self.rules),
        )

    def change(self, batch_id: str, **updates: JsonValue) -> DecisionChange:
        return DecisionChange.model_validate(
            {
                "batch_id": batch_id,
                "generation": 1,
                "candidate_id": "first",
                "expected_row_revision": 0,
                "decision": "approve",
                "reviewer": "synthetic QA reviewer",
                "edits": self.corrections.model_dump(by_alias=True, exclude_none=True),
                **updates,
            }
        )

    def test_generic_edits_require_approval_and_legacy_corrections_are_read_only(self) -> None:
        for decision in ("pending", "reject"):
            with self.subTest(decision=decision), self.assertRaises(ValidationError):
                self.change("fixture", decision=decision)
        with self.assertRaises(ValidationError):
            self.change("fixture", label_corrections={"class": "deny"})
        change = self.change("fixture")
        self.assertEqual(change.model_dump(mode="json")["edits"]["class"], "deny")
        self.assertEqual(self.change("fixture", edits={}).edits, {})
        legacy = ReviewDecision(
            batch_id="legacy", candidate_id="first", decision="approve",
            reviewer="original human", row_revision=1,
            created_at="2026-11-14", updated_at="2026-11-14", label_corrections=self.corrections,
        )
        self.assertEqual(ReviewDecision.model_validate_json(legacy.model_dump_json()), legacy)

    def test_selected_high_confidence_fields_and_command_remain_editable(self) -> None:
        row = {**self.prepared.rows[0], **{
            field: "high" for field in (
                "category_confidence", "risk_confidence", "severity_confidence", "class_confidence"
            )
        }}
        prepared = self.prepared.model_copy(update={"rows": (row,)})
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
            edits = {**self.corrections.model_dump(by_alias=True), "command": "printf reviewed"}
            saved = tx.save_decision(self.snapshot, self.change(batch.batch_id, edits=edits))
            self.assertEqual(saved.edits, edits)
            self.assertEqual(tx.prepared(batch.batch_id).rows, (row,))

    def test_save_rejects_nonselected_writes_invalid_types_taxonomy_and_routing(self) -> None:
        invalid: tuple[dict[str, JsonValue], ...] = (
            {"description": "invented"}, {"decision": "accept"},
            {"risk_id": None}, {"risk_id": 3}, {"severity": "urgent"}, {"class": "modify"},
            {"category_id": "CAT-98"}, {"risk_id": "CAT-98.1"}, {"command": ""},
        )
        with self.store.transaction() as tx:
            batch = tx.prepare(self.prepared)
            for edits in invalid:
                with self.subTest(edits=edits), self.assertRaises(HumanReviewError):
                    tx.save_decision(
                        self.snapshot, self.change(batch.batch_id, edits=edits),
                    )
            self.assertEqual(tx.list_decisions(batch.batch_id)[0].row_revision, 0)
            self.assertEqual([entry.event for entry in tx.history(self.snapshot.dataset_id)], ["created"])
        cases = (
            ({"risk_candidates": ["CAT-99.1"]}, self.rules, {"risk_id": "CAT-99.2"}),
            ({}, self.rules.model_copy(update={"risks": [self.rules.risks[0]]}), {"risk_id": "CAT-99.2"}),
            ({"category_id": "CAT-98"}, self.rules, {"category_id": "CAT-99"}),
            ({"documentation_urls": []}, self.rules, {"severity": "high"}),
            ({"decision": "reject"}, self.rules, {"severity": "high"}),
            ({}, self.rules.model_copy(update={"severities": [self.rules.severities[0]]}), {"severity": "critical"}),
        )
        for index, (updates, rules, edits) in enumerate(cases):
            row = {**self.prepared.rows[0], **updates}
            snapshot = self.snapshot.model_copy(update={
                "dataset_id": f"invalid-{index}",
                "record_fingerprints": {"first": review_identity(row, self.snapshot.review_columns)},
            })
            prepared = self.prepared.model_copy(update={
                "snapshot": snapshot, "rows": (row,),
                "validation": annotation_review_validation(rules),
            })
            with self.subTest(index=index), self.store.transaction() as tx:
                batch = tx.prepare(prepared)
                with self.assertRaises(HumanReviewError):
                    tx.save_decision(snapshot, self.change(batch.batch_id, edits=edits))
                self.assertEqual(tx.list_decisions(batch.batch_id)[0].row_revision, 0)

    def test_corrections_cannot_bypass_machine_eligibility_or_authoritative_evidence(self) -> None:
        ineligible = self.snapshot.model_copy(update={"eligible_candidate_ids": ()})
        with self.store.transaction() as tx:
            batch = tx.prepare(self.prepared.model_copy(update={"snapshot": ineligible}))
            with self.assertRaises(HumanReviewError) as rejected:
                tx.save_decision(ineligible, self.change(batch.batch_id))
            self.assertEqual(rejected.exception.code, "ineligible")
            self.assertEqual(tx.list_decisions(batch.batch_id)[0].decision, "pending")
        with self.store.transaction() as tx:
            eligible = self.snapshot.model_copy(update={"snapshot_id": "c" * 64, "review_columns": ()})
            batch = tx.ensure_current_batch(eligible)
            with self.assertRaises(HumanReviewError):
                tx.save_decision(eligible, self.change(batch.batch_id))

    def test_history_restart_reuse_and_digest_retain_corrections(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.prepare(self.prepared)
            saved = tx.save_decision(self.snapshot, self.change(batch.batch_id))
            token = tx.complete(
                self.snapshot, tx.get_current_batch(self.snapshot.dataset_id), "synthetic QA completer"
            )
        with HumanReviewStore(self.home).transaction() as tx:
            self.assertEqual(tx.list_decisions(batch.batch_id), (saved,))
            self.assertEqual(tx.history(self.snapshot.dataset_id)[1].decision, saved)
            self.assertEqual(tx.authorize_completion(self.snapshot, token), token)
            self.assertNotEqual(
                decisions_digest((saved,)),
                decisions_digest((saved.model_copy(update={"edits": {}}),)),
            )
            neighbor_row = {**self.prepared.rows[0], "candidate_id": "neighbor", "command": "printf neighbor"}
            neighbor_fingerprint = review_identity(neighbor_row, self.snapshot.review_columns)
            neighbor = self.snapshot.model_copy(
                update={
                    "snapshot_id": "d" * 64,
                    "candidate_ids": ("first", "neighbor"),
                    "eligible_candidate_ids": ("first", "neighbor"),
                    "record_fingerprints": {
                        **self.snapshot.record_fingerprints, "neighbor": neighbor_fingerprint,
                    },
                }
            )
            neighbors = self.prepared.model_copy(update={
                "snapshot": neighbor, "rows": (*self.prepared.rows, neighbor_row),
            })
            carried_batch = tx.prepare(neighbors)
            carried = tx.list_decisions(carried_batch.batch_id)[0]
            self.assertEqual(carried.edits, saved.edits)
            self.assertEqual(carried.carried_from_batch_id, batch.batch_id)
            changed_row = {**self.prepared.rows[0], **saved.edits}
            changed = neighbor.model_copy(
                update={
                    "snapshot_id": "f" * 64,
                    "record_fingerprints": {
                        "first": review_identity(changed_row, self.snapshot.review_columns),
                        "neighbor": neighbor_fingerprint,
                    },
                }
            )
            current = tx.prepare(neighbors.model_copy(update={"snapshot": changed, "rows": (changed_row, neighbor_row)}))
            invalidated = tx.list_decisions(current.batch_id)[0]
            self.assertEqual(invalidated.decision, "pending")
            self.assertEqual(invalidated.edits, {})
            self.assertEqual(tx.prepared(batch.batch_id).rows, self.prepared.rows)

    def test_completion_rejects_correction_tampering_and_frame_omission(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.prepare(self.prepared)
            saved = tx.save_decision(self.snapshot, self.change(batch.batch_id))
            token = tx.complete(
                self.snapshot, tx.get_current_batch(self.snapshot.dataset_id), "synthetic QA completer"
            )
        frame = decision_frame(token, (saved,))
        stripped = frame.copy(deep=True)
        stripped.attrs.pop(CORRECTIONS_BINDING)
        with self.assertRaises(HumanReviewError):
            require_decision_frame(stripped, frame)
        tampered = saved.model_copy(update={"edits": {"severity": "high"}})
        with self.store.transaction(dataset_id=self.snapshot.dataset_id) as tx:
            tx.connection.execute(
                "UPDATE review_items SET data = %s WHERE batch_id = %s",
                (Jsonb(tampered.model_dump(mode="json")), batch.batch_id),
            )
        with self.store.transaction() as tx:
            with self.assertRaises(HumanReviewError) as stale:
                tx.authorize_completion(self.snapshot, token)
            self.assertEqual(stale.exception.code, "stale")

    def test_approved_overlay_changes_only_label_dimensions_without_mutating_annotations(self) -> None:
        with self.store.transaction() as tx:
            batch = tx.prepare(self.prepared)
            saved = tx.save_decision(self.snapshot, self.change(batch.batch_id))
        before = self.annotations.copy(deep=True)
        overlaid = apply_label_corrections(self.annotations, (saved,), self.rules)
        self.assertEqual(
            overlaid.loc[0, ["category_id", "risk_id", "severity", "class"]].tolist(),
            ["CAT-99", "CAT-99.2", "critical", "deny"],
        )
        pd.testing.assert_frame_equal(self.annotations, before)
        retained = [
            field
            for field in before.columns
            if field not in self.corrections.model_dump(by_alias=True, exclude_none=True)
        ]
        pd.testing.assert_frame_equal(overlaid[retained], before[retained])

    def test_legacy_decisions_keep_original_digest_and_clear_on_retraction(self) -> None:
        legacy = ReviewDecision(
            batch_id="legacy",
            candidate_id="first",
            decision="approve",
            reviewer="synthetic QA reviewer",
            row_revision=1,
            created_at="2026-11-14",
            updated_at="2026-11-14",
        ).model_dump(exclude={"label_corrections"})
        parsed = ReviewDecision.model_validate(legacy)
        self.assertIsNone(parsed.label_corrections)
        self.assertEqual(
            decisions_digest((parsed,)),
            fingerprint([{key: value for key, value in legacy.items() if value is not None}]),
        )
        with self.store.transaction() as tx:
            batch = tx.prepare(self.prepared)
            tx.save_decision(self.snapshot, self.change(batch.batch_id))
            retracted = tx.save_decision(
                self.snapshot,
                self.change(
                    batch.batch_id,
                    expected_row_revision=1,
                    decision="pending",
                    edits={},
                ),
            )
            self.assertIsNone(retracted.label_corrections)
            self.assertEqual(retracted.edits, {})
            self.assertEqual(
                tx.history(self.snapshot.dataset_id)[1].decision.edits, self.corrections.model_dump(by_alias=True)
            )

    def test_service_saves_and_returns_corrected_approved_records_with_original_evidence(self) -> None:
        home = self.home
        spec = load_build_config(write_fixture(home), home)
        DatasetRegistry(home).register(spec)
        data = DatasetDataService(home, self.enterContext(dataset_instance(home)))
        candidate = self.candidate.model_copy(
            update={"command": "git status", "util": "git", "risk_candidates": ["CAT-99.1"]}
        )
        label = self.label.model_copy(update={"util": "git", "decision": "needs_review"})
        annotations = validated_frame(
            [{**candidate.model_dump(), **label.model_dump(by_alias=True)}], ProposedAnnotationsTable
        )
        original = {**candidate.model_dump(mode="json"), **label.model_dump(mode="json", by_alias=True)}
        snapshot = self.snapshot.model_copy(update={
            "dataset_id": spec.dataset_id,
            "record_fingerprints": {"first": review_identity(original, self.snapshot.review_columns)},
        })
        manifest = ReviewManifest(
            dataset_id=spec.dataset_id,
            storage=spec.storage,
            publication=spec.publication,
            references={},
            evidence={},
            implementation={},
        )
        resolved = ResolvedReview(
            manifest,
            snapshot,
            annotations,
            spec.rules,
            publication(annotations, spec.rules, spec.publication),
        )
        self.enterContext(patch.object(ReviewSnapshots, "capture", return_value=resolved))
        service = ReviewService(home, data, lambda token: CompletionEffect())
        service.prepare(spec.dataset_id)
        batch = service.view(DatasetQueryRequest(dataset_id=spec.dataset_id)).batch
        with self.assertRaises(HumanReviewError) as unrouted:
            service.decide(DecisionRequest(
                dataset_id=spec.dataset_id,
                snapshot_id=snapshot.snapshot_id,
                change=self.change(batch.batch_id, edits={"command": "git diff"}),
            ))
        self.assertEqual(unrouted.exception.code, "ineligible")
        saved = service.decide(
            DecisionRequest(
                dataset_id=spec.dataset_id,
                snapshot_id=snapshot.snapshot_id,
                change=self.change(
                    batch.batch_id, edits={"command": "git status --short", "severity": "high", "class": "deny"},
                ),
            )
        )
        completed = service.complete(
            CompletionRequest(
                dataset_id=spec.dataset_id,
                snapshot_id=snapshot.snapshot_id,
                expected=saved.batch,
                reviewer="synthetic QA completer",
            )
        )
        assert completed.completion is not None
        approved = service.approved_records(spec.dataset_id, completed.completion)
        restarted = ReviewService(home, data, lambda token: CompletionEffect())
        view = restarted.view(DatasetQueryRequest(dataset_id=spec.dataset_id))
        self.assertEqual(approved.loc[0, ["severity", "class"]].tolist(), ["high", "deny"])
        self.assertEqual(approved.loc[0, "command"], "git status --short")
        self.assertEqual(view.decisions[0].edits, saved.decision.edits)
        self.assertEqual(
            view.page.rows[0].values["human_edits"],
            {"command": "git status --short", "severity": "high", "class": "deny"},
        )
        self.assertEqual(annotations.loc[0, ["severity", "class"]].tolist(), ["low", "allow"])
        self.assertEqual(annotations.loc[0, "command"], "git status")


if __name__ == "__main__":
    unittest.main()
