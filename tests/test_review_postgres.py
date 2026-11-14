from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.errors import ReadOnlySqlTransaction
from pydantic import JsonValue

from codebench_pipeline.category_rules import CategoryRules, ClassRule, RiskRule, SeverityRule
from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision
from codebench_pipeline.human_review import (
    DecisionChange,
    DecisionValue,
    HumanReviewError,
    PreparedReview,
    PublicationPayload,
    ReviewSnapshot,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_api_models import EvidenceTarget, NextReviewRequest
from codebench_pipeline.storage import fingerprint
from codebench_pipeline.table_query import ColumnSort, NullFilter, TableQuery, TextFilter


class ReviewPostgresTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="review-pg-test-")
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.store = HumanReviewStore(self.home)
        self.addCleanup(self.drop_schema)

    def drop_schema(self) -> None:
        with psycopg.connect(self.store.dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self.store.schema)))

    def prepared(
        self, rows: tuple[dict[str, JsonValue], ...], *, eligible: tuple[str, ...] | None = None,
        minimum: int = 0, maximum: int | None = None, dataset: str = "qa",
        stage: str = "selection", columns: tuple[str, ...] = ("text",),
        evidence: dict[str, JsonValue] | None = None,
    ) -> PreparedReview:
        identifiers = tuple(str(row["id"]) for row in rows)
        eligible_ids = identifiers if eligible is None else eligible
        snapshot = ReviewSnapshot(
            dataset_id=dataset, review_stage=stage,
            snapshot_id=fingerprint({"rows": list(rows), "eligible": list(eligible_ids),
                                     "minimum": minimum, "maximum": maximum,
                                     "columns": list(columns), "evidence": evidence or {}}),
            candidate_ids=identifiers, eligible_candidate_ids=eligible_ids, review_columns=columns,
            min_approvals=minimum, max_approvals=maximum,
        )
        return PreparedReview(snapshot=snapshot, id_column="id", columns=tuple(rows[0]) if rows else ("id", "text"),
                              rows=rows, evidence=evidence or {})

    def decide(
        self, prepared: PreparedReview, candidate_id: str, choice: DecisionValue, *,
        revision: int = 0, edits: dict[str, JsonValue] | None = None,
    ) -> None:
        with self.store.transaction(dataset_id=prepared.snapshot.dataset_id) as tx:
            batch = tx.get_current_batch(prepared.snapshot.dataset_id, prepared.snapshot.review_stage)
            tx.save_decision(prepared.snapshot, DecisionChange(
                batch_id=batch.batch_id, generation=batch.generation, candidate_id=candidate_id,
                expected_row_revision=revision, decision=choice, reviewer="synthetic QA reviewer",
                edits=edits or {},
            ))

    def test_selected_values_share_one_item_and_expand_downstream_occurrences(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "same", "source": "a"},
                                  {"id": "two", "text": "same", "source": "b"}))
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
            self.assertEqual(tx.prepare(prepared), batch)
            self.assertEqual(tx.ensure_current_batch(prepared.snapshot), batch)
        self.decide(prepared, "two", "approve", edits={"text": "corrected"})
        with self.store.reader() as tx:
            page = tx.review_page(DatasetQueryRequest(dataset_id="qa"), "selection")
            self.assertEqual((page.page.total_rows, page.counts.approve), (1, 1))
            self.assertEqual(page.counts.approved_occurrences, 2)
            self.assertEqual(page.page.rows[0].occurrence_count, 2)
            self.assertEqual(page.page.rows[0].values["text"], "same")
            self.assertEqual(page.page.rows[0].values["human_edits"], {"text": "corrected"})
            self.assertEqual(len(page.decisions), 1)
            self.assertEqual(page.batch.snapshot.candidate_ids, ())
            self.assertEqual(page.batch.snapshot.eligible_candidate_ids, ())
            self.assertEqual(page.batch.snapshot.record_fingerprints, {})
            self.assertEqual([decision.candidate_id for decision in tx.list_decisions(batch.batch_id)], ["one", "two"])
            self.assertEqual(tx.prepared(batch.batch_id), prepared)
            self.assertEqual(tx.rows_for_hash(batch.batch_id, "two"), prepared.rows)

    def test_heads_ignore_ids_evidence_and_bounds_but_never_corrected_hashes(self) -> None:
        old = self.prepared(({"id": "one", "text": "original"},))
        with self.store.transaction() as tx:
            original_batch = tx.prepare(old)
        self.decide(old, "one", "approve", edits={"text": "corrected"})
        changed = self.prepared(({"id": "renamed", "text": "original"}, {"id": "alias", "text": "corrected"}),
                                minimum=1, maximum=2, evidence={"implementation": "changed"})
        with self.store.transaction() as tx:
            batch = tx.prepare(changed)
            decisions = {row.candidate_id: row for row in tx.list_decisions(batch.batch_id)}
            self.assertEqual(decisions["renamed"].decision, "approve")
            self.assertEqual(decisions["renamed"].edits, {"text": "corrected"})
            self.assertEqual(decisions["alias"].decision, "pending")
            self.assertEqual(tx.list_decisions(original_batch.batch_id)[0].candidate_id, "one")
            self.assertEqual(tx.list_decisions(original_batch.batch_id)[0].decision, "approve")

    def test_completion_bounds_count_occurrences_instead_of_shared_items(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "same"}, {"id": "two", "text": "same"}), minimum=2)
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
        self.decide(prepared, "one", "approve")
        with self.store.transaction() as tx:
            tx.complete(prepared.snapshot, tx.get_batch(batch.batch_id), "synthetic QA")
            bounded = self.prepared(prepared.rows, maximum=1)
            fresh = tx.prepare(bounded)
            self.assertEqual(tx.counts(fresh.batch_id).approved_occurrences, 2)
            with self.assertRaises(HumanReviewError) as invalid:
                tx.complete(bounded.snapshot, fresh, "synthetic QA")
            self.assertEqual(invalid.exception.code, "selection")

    def test_original_numeric_representation_survives_postgres_without_collapsing_hashes(self) -> None:
        prepared = self.prepared(({"id": "one", "text": 1}, {"id": "two", "text": 1.0},
                                  {"id": "three", "text": 1e20}))
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
        with self.store.reader() as tx:
            page = tx.review_page(DatasetQueryRequest(dataset_id="qa"), "selection")
            self.assertEqual(page.page.total_rows, 3)
            captured = tx.rows_for_hash(batch.batch_id, "three")[0]["text"]
            self.assertIs(type(captured), float)
            self.assertEqual(captured, 1e20)

    def test_pending_tombstone_survives_disappearance_and_historical_decision_is_pinned(self) -> None:
        old = self.prepared(({"id": "one", "text": "value"},))
        with self.store.transaction() as tx:
            old_batch = tx.prepare(old)
        self.decide(old, "one", "approve")
        with self.store.transaction() as tx:
            token = tx.complete(old.snapshot, tx.get_batch(old_batch.batch_id), "synthetic QA")
        newer = self.prepared(({"id": "new", "text": "value"},))
        with self.store.transaction() as tx:
            tx.prepare(newer)
        self.decide(newer, "new", "pending", revision=1)
        with self.store.transaction() as tx:
            tx.prepare(self.prepared(()))
            restored = tx.prepare(old)
            self.assertEqual(tx.list_decisions(restored.batch_id)[0].decision, "pending")
            self.assertEqual(tx.list_decisions(restored.batch_id)[0].row_revision, 2)
            self.assertEqual(tx.list_decisions(old_batch.batch_id)[0].decision, "approve")
            with self.assertRaises(HumanReviewError) as stale:
                tx.authorize_completion(old.snapshot, token)
            self.assertEqual(stale.exception.code, "stale")

    def test_cached_ineligible_approval_remains_blocked_until_explicit_rejection(self) -> None:
        old = self.prepared(({"id": "one", "text": "value"},))
        with self.store.transaction() as tx:
            tx.prepare(old)
        self.decide(old, "one", "approve")
        changed = self.prepared(({"id": "one", "text": "value"},), eligible=())
        with self.store.transaction() as tx:
            batch = tx.prepare(changed)
            self.assertEqual((tx.counts(batch.batch_id).approve, tx.counts(batch.batch_id).blocked), (1, 1))
            with self.assertRaises(HumanReviewError) as blocked:
                tx.complete(changed.snapshot, batch, "synthetic QA")
            self.assertEqual(blocked.exception.code, "ineligible")
        self.decide(changed, "one", "reject", revision=1)
        with self.store.transaction() as tx:
            self.assertEqual(tx.counts(batch.batch_id).blocked, 0)
            token = tx.complete(changed.snapshot, tx.get_batch(batch.batch_id), "synthetic QA")
            self.assertEqual(tx.authorize_completion(changed.snapshot, token), token)

    def test_changed_rules_block_only_invalid_occurrence_without_discarding_shared_approval(self) -> None:
        rules = CategoryRules(
            category_id="CAT-99", category_name="Synthetic QA", version="1",
            description_language="English", policy="Inert fixtures",
            risks=[RiskRule(id="CAT-99.1", name="Synthetic", definition="Synthetic",
                            command_patterns=["^printf", "^echo"], guidance="Inert fixtures")],
            classes=[ClassRule(name=name, definition="Synthetic") for name in ("allow", "deny", "ask")],
            severities=[SeverityRule(name=name, definition="Synthetic") for name in ("low", "medium", "high", "critical")],
        )
        rows: list[dict[str, JsonValue]] = []
        for identifier, utility in (("one", "printf"), ("two", "echo")):
            candidate = CandidateRecord(
                candidate_id=identifier, command=f"{utility} inert", util=utility, context="Never executed",
                source_id="qa", source_url="https://example.org/qa", source_position="line 1",
                transformations=[], risk_candidates=["CAT-99.1"],
            )
            label = LabelDecision(
                candidate_id=identifier, decision="accept", category_id="CAT-99", risk_id="CAT-99.1",
                severity="low", class_="allow", description="Inert fixture", util=utility,
                category_confidence="high", risk_confidence="high", severity_confidence="high",
                class_confidence="high", rationale="Synthetic", documentation_urls=["https://example.org/docs"],
            )
            rows.append({"id": identifier, **candidate.model_dump(mode="json"), **label.model_dump(mode="json", by_alias=True)})
        old = self.prepared(tuple(rows), columns=("severity",)).model_copy(
            update={"validation": {"rules": rules.model_dump(mode="json")}}
        )
        with self.store.transaction() as tx:
            tx.prepare(old)
        self.decide(old, "one", "approve", edits={"severity": "high"})
        changed_rules = rules.model_copy(update={"risks": [
            rules.risks[0].model_copy(update={"command_patterns": ["^printf"]})
        ]})
        changed = old.model_copy(update={
            "snapshot": old.snapshot.model_copy(update={"snapshot_id": "f" * 64}),
            "validation": {"rules": changed_rules.model_dump(mode="json")},
        })
        with self.store.transaction() as tx:
            batch = tx.prepare(changed)
            self.assertEqual(tx.counts(batch.batch_id).approve, 1)
            self.assertEqual(tx.counts(batch.batch_id).approved_occurrences, 2)
            self.assertEqual(tx.counts(batch.batch_id).blocked, 1)
            self.assertEqual(tx.list_decisions(batch.batch_id)[1].edits, {"severity": "high"})
            page = tx.review_page(DatasetQueryRequest(dataset_id="qa"), "selection")
            self.assertEqual(page.page.rows[0].occurrence_count, 2)
            self.assertIsNotNone(page.page.rows[0].blocked_reason)
            with self.assertRaises(HumanReviewError) as blocked:
                tx.complete(changed.snapshot, batch, "synthetic QA")
            self.assertEqual(blocked.exception.code, "ineligible")

    def test_edit_boundary_rejects_unselected_columns_and_changed_json_types(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "value", "count": 1},), columns=("text", "count"))
        with self.store.transaction() as tx:
            tx.prepare(prepared)
        for edits in ({"id": "renamed"}, {"count": True}, {"text": None}, {"count": "1"}):
            with self.subTest(edits=edits), self.assertRaises(HumanReviewError):
                self.decide(prepared, "one", "approve", edits=edits)
        self.decide(prepared, "one", "approve", edits={"count": 1.5})
        with self.store.reader() as tx:
            self.assertEqual(tx.get_current_batch("qa", "selection").batch_revision, 1)

    def test_sql_pages_literal_search_numeric_filters_null_last_and_casefold(self) -> None:
        prepared = self.prepared((
            {"id": "one", "text": "Stra\u00dfe %_", "score": 2},
            {"id": "two", "text": "strasse", "score": None},
            {"id": "three", "text": "other", "score": 10},
            {"id": "four", "text": "literal", "score": 1.0},
        ))
        with self.store.transaction() as tx:
            tx.prepare(prepared)
        cases = (
            (TableQuery(filters=(TextFilter(field="text", operator="contains", value="%_"),)), ("one",)),
            (TableQuery(search="STRASSE"), ("one", "two")),
            (TableQuery(filters=(TextFilter(field="score", operator="equals", value="1e0"),)), ("four",)),
            (TableQuery(filters=(TextFilter(field="score", operator="equals", value="1e100000000"),)), ()),
            (TableQuery(filters=(NullFilter(field="score", operator="is_null"),)), ("two",)),
            (TableQuery(sort=(ColumnSort(field="score", direction="desc"),), offset=1, limit=2), ("one", "four")),
        )
        with self.store.reader() as tx:
            for query, identifiers in cases:
                with self.subTest(query=query):
                    page = tx.review_page(DatasetQueryRequest(dataset_id="qa", query=query), "selection")
                    self.assertEqual(tuple(row.review_record_id for row in page.page.rows), identifiers)
                    self.assertEqual(len(page.decisions), len(identifiers))
                    self.assertEqual(page.page.total_rows, 4)

    def test_item_and_next_pending_are_independent_of_offset_and_represent_duplicates(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "a"}, {"id": "duplicate", "text": "a"},
                                  {"id": "two", "text": "b"}, {"id": "three", "text": "c"}))
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
        self.decide(prepared, "two", "reject")
        with self.store.reader() as tx:
            item = tx.item(EvidenceTarget(dataset_id="qa", batch_id=batch.batch_id, candidate_id="duplicate"))
            self.assertEqual((item.row.review_record_id, item.row.occurrence_count), ("one", 2))
            following = tx.next_pending(NextReviewRequest(dataset_id="qa", after_id="duplicate",
                                                         query=TableQuery(offset=999)), "selection")
            self.assertIsNotNone(following)
            assert following is not None
            self.assertEqual(following.row.review_record_id, "three")
            self.assertIsNone(tx.next_pending(NextReviewRequest(dataset_id="qa", after_id="three"), "selection"))

    def test_dataset_locks_are_not_global_and_stale_revisions_cannot_write(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "value"},))
        with self.store.transaction(dataset_id="qa") as tx:
            tx.prepare(prepared)
            with self.store.transaction(dataset_id="independent") as other:
                other.ensure_current_batch(ReviewSnapshot(dataset_id="independent", snapshot_id="f" * 64,
                                                         candidate_ids=(), eligible_candidate_ids=()))
            with self.assertRaises(HumanReviewError) as conflict:
                with self.store.transaction(dataset_id="qa"):
                    self.fail("Competing dataset lock succeeded")
            self.assertEqual(conflict.exception.code, "conflict")
        self.decide(prepared, "one", "reject")
        with self.assertRaises(HumanReviewError) as stale:
            self.decide(prepared, "one", "approve")
        self.assertEqual(stale.exception.code, "stale")

    def test_readers_are_repeatable_read_and_reject_writes(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "value"},))
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
        with self.store.reader() as reader:
            self.assertEqual(reader.counts(batch.batch_id).pending, 1)
            self.decide(prepared, "one", "approve")
            self.assertEqual(reader.counts(batch.batch_id).pending, 1)
        with self.store.reader() as reader:
            self.assertEqual(reader.counts(batch.batch_id).approve, 1)
        with self.assertRaises(ReadOnlySqlTransaction):
            with self.store.reader() as reader:
                reader.connection.execute("DELETE FROM review_heads")

    def test_invalidation_rejects_authority_and_prepare_reactivates_without_reusing_token(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "value"},))
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
        self.decide(prepared, "one", "approve")
        with self.store.transaction() as tx:
            token = tx.complete(prepared.snapshot, tx.get_batch(batch.batch_id), "synthetic QA")
            tx.invalidate("qa", "Upstream content must be revalidated")
        with self.store.reader() as tx:
            with self.assertRaises(HumanReviewError) as stale:
                tx.get_current_batch("qa", "selection")
            self.assertEqual(stale.exception.code, "stale")
            self.assertEqual(tx.get_batch(batch.batch_id).completion, token)
            self.assertEqual(tx.history("qa")[-1].event, "completed")
        with self.store.transaction() as tx:
            with self.assertRaises(HumanReviewError) as unprepared:
                tx.ensure_current_batch(prepared.snapshot)
            self.assertEqual(unprepared.exception.code, "stale")
            fresh = tx.prepare(prepared)
            self.assertNotEqual(fresh.batch_id, batch.batch_id)
            self.assertEqual(tx.counts(fresh.batch_id).approve, 1)
            self.assertIsNone(fresh.completion)

    def test_publication_and_continuation_survive_restart_but_reopen_revokes_token(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "value"},))
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
        self.decide(prepared, "one", "approve")
        with self.store.transaction() as tx:
            token = tx.complete(prepared.snapshot, tx.get_batch(batch.batch_id), "synthetic QA")
            publication = tx.record_publication(prepared.snapshot, token, PublicationPayload(
                payload_key="qa", payload_hash="a" * 64, csv_reference="qa.csv",
                bundle_reference="qa-bundle", dagster_run_id="qa-run",
            ))
            tx.record_continuation(prepared.snapshot, token, "continuation-run")
        restarted = HumanReviewStore(self.home)
        with restarted.transaction() as tx:
            self.assertEqual(tx.get_publication(publication.publication_id), publication)
            self.assertEqual(tx.last_publication("qa", "selection"), publication)
            self.assertEqual(tx.get_batch(batch.batch_id).continuation_run_id, "continuation-run")
            self.assertEqual(tx.authorize_completion(prepared.snapshot, token), token)
            reopened = tx.reopen(prepared.snapshot, tx.get_batch(batch.batch_id), "synthetic QA")
            self.assertEqual(reopened.generation, 2)
            self.assertIsNone(reopened.completion)
            with self.assertRaises(HumanReviewError) as stale:
                tx.authorize_completion(prepared.snapshot, token)
            self.assertEqual(stale.exception.code, "stale")

    def test_rollback_is_atomic_across_decisions_heads_history_and_publications(self) -> None:
        prepared = self.prepared(({"id": "one", "text": "value"},))
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
        with self.assertRaisesRegex(RuntimeError, "abort synthetic transaction"):
            with self.store.transaction() as tx:
                tx.save_decision(prepared.snapshot, DecisionChange(
                    batch_id=batch.batch_id, generation=1, candidate_id="one", expected_row_revision=0,
                    decision="approve", reviewer="synthetic QA",
                ))
                token = tx.complete(prepared.snapshot, tx.get_batch(batch.batch_id), "synthetic QA")
                tx.record_publication(prepared.snapshot, token, PublicationPayload(
                    payload_key="qa", payload_hash="a" * 64, csv_reference="qa.csv",
                    bundle_reference="qa-bundle", dagster_run_id="qa-run",
                ))
                raise RuntimeError("abort synthetic transaction")
        with self.store.reader() as tx:
            self.assertEqual(tx.counts(batch.batch_id).pending, 1)
            self.assertEqual(tuple(entry.event for entry in tx.history("qa")), ("created",))
            self.assertEqual(tx.list_publications("qa"), ())
            self.assertEqual(tx.connection.execute("SELECT count(*) AS count FROM review_heads").fetchone()["count"], 0)

    def test_direct_snapshots_reuse_fingerprints_with_dataset_stage_and_home_isolation(self) -> None:
        original = ReviewSnapshot(dataset_id="qa", snapshot_id="a" * 64, candidate_ids=("one",),
                                  eligible_candidate_ids=("one",), record_fingerprints={"one": "b" * 64},
                                  review_columns=("text",))
        with self.store.transaction() as tx:
            batch = tx.ensure_current_batch(original)
            tx.save_decision(original, DecisionChange(
                batch_id=batch.batch_id, generation=1, candidate_id="one", expected_row_revision=0,
                decision="reject", reviewer="synthetic QA",
            ))
            changed = original.model_copy(update={"snapshot_id": "c" * 64, "min_approvals": 1})
            fresh = tx.ensure_current_batch(changed)
            self.assertEqual(tx.list_decisions(fresh.batch_id)[0].decision, "reject")
            other = tx.ensure_current_batch(changed.model_copy(update={"review_stage": "other"}))
            self.assertEqual(tx.counts(other.batch_id).pending, 1)
        separate = HumanReviewStore(self.home / "separate")
        try:
            with separate.transaction() as tx:
                foreign = tx.ensure_current_batch(original)
                self.assertEqual(tx.counts(foreign.batch_id).pending, 1)
        finally:
            with psycopg.connect(separate.dsn, autocommit=True) as connection:
                connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(separate.schema)))
