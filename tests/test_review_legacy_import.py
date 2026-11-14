from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql
from pydantic import JsonValue

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.human_review import (
    CompletionToken,
    HumanReviewError,
    PreparedReview,
    PublicationReference,
    ReviewBatch,
    ReviewDecision,
    ReviewHistoryEntry,
    ReviewSnapshot,
    decisions_digest,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.review_legacy_import import import_legacy
from codebench_pipeline.review_snapshots import ReviewManifest


class ReviewLegacyImportTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="review-import-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.home = self.root / "home"
        self.source = self.root / "legacy.sqlite3"
        self.backup = self.root / "backup.sqlite3"
        self.manifests = self.root / "manifests"
        self.manifests.mkdir()
        self.store = HumanReviewStore(self.home)
        self.addCleanup(self.drop_schema)
        with closing(sqlite3.connect(self.source)) as connection:
            connection.executescript("""
                CREATE TABLE human_review_batches(batch_id TEXT PRIMARY KEY,dataset_id TEXT,
                    review_stage TEXT,current INTEGER,data TEXT);
                CREATE TABLE human_review_decisions(batch_id TEXT,candidate_id TEXT,data TEXT,
                    PRIMARY KEY(batch_id,candidate_id));
                CREATE TABLE human_review_history(sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT,batch_id TEXT,data TEXT);
                CREATE TABLE human_review_publications(publication_id TEXT PRIMARY KEY,
                    dataset_id TEXT,batch_id TEXT,data TEXT);
            """)
        self.now = "2026-01-01T00:00:00+00:00"

    def drop_schema(self) -> None:
        with psycopg.connect(self.store.dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self.store.schema)))

    def fixture(
        self, *, selected: bool = True, conflicting: bool = False,
    ) -> tuple[ReviewBatch, tuple[ReviewDecision, ...], PublicationReference]:
        rows: list[dict[str, JsonValue]] = [{"id": "one", "text": "same"}]
        if conflicting:
            rows.append({"id": "two", "text": "same"})
        manifest: dict[str, JsonValue] = {
            "dataset_id": "qa", "review_stage": "selection", "id_column": "id", "rows": rows,
            "evidence": {},
        }
        if selected:
            manifest["review_columns"] = ["text"]
        payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        snapshot_hash = hashlib.sha256(payload).hexdigest()
        (self.manifests / f"{snapshot_hash}.json").write_bytes(payload)
        identifiers = tuple(str(row["id"]) for row in rows)
        snapshot = ReviewSnapshot(
            dataset_id="qa", review_stage="selection", snapshot_id=snapshot_hash,
            candidate_ids=identifiers, eligible_candidate_ids=identifiers,
        )
        decisions = tuple(
            ReviewDecision(
                batch_id="legacy-batch", candidate_id=identifier,
                decision="approve" if identifier == "one" else "reject",
                reviewer="synthetic legacy reviewer", row_revision=1,
                created_at=self.now, updated_at=self.now,
            ) for identifier in identifiers
        )
        token = CompletionToken(
            dataset_id="qa", review_stage="selection", snapshot_id=snapshot_hash,
            decision_digest=decisions_digest(decisions), batch_id="legacy-batch",
            generation=1, batch_revision=3, token="legacy-token",
        )
        batch = ReviewBatch(
            batch_id="legacy-batch", snapshot=snapshot, generation=1, batch_revision=3,
            state="completed", reviewer="synthetic legacy reviewer", completion=token,
            created_at=self.now, updated_at=self.now,
        )
        publication = PublicationReference(
            publication_id="legacy-publication", payload_key="legacy-key", payload_hash="a" * 64,
            csv_reference="legacy.csv", bundle_reference="legacy-bundle", dagster_run_id="legacy-run",
            authorization=token, created_at=self.now,
        )
        entry = ReviewHistoryEntry(event="completed", batch=batch, recorded_at=self.now)
        with closing(sqlite3.connect(self.source)) as connection, connection:
            connection.execute("INSERT INTO human_review_batches VALUES(?,?,?,?,?)",
                               (batch.batch_id, "qa", "selection", 1, batch.model_dump_json()))
            for decision in decisions:
                connection.execute("INSERT INTO human_review_decisions VALUES(?,?,?)",
                                   (batch.batch_id, decision.candidate_id, decision.model_dump_json()))
            connection.execute(
                "INSERT INTO human_review_history(dataset_id,batch_id,data) VALUES(?,?,?)",
                ("qa", batch.batch_id, entry.model_dump_json()),
            )
            connection.execute("INSERT INTO human_review_publications VALUES(?,?,?,?)",
                               (publication.publication_id, "qa", batch.batch_id, publication.model_dump_json()))
        return batch, decisions, publication

    def test_import_preserves_ids_history_publications_and_never_authorizes_legacy_completion(self) -> None:
        batch, decisions, publication = self.fixture()
        original_bytes = self.source.read_bytes()
        report = import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        self.assertEqual((report.batches, report.decisions, report.history_entries, report.publications), (1, 1, 1, 1))
        self.assertEqual(report.reusable_heads, 1)
        self.assertEqual(self.source.read_bytes(), original_bytes)
        self.assertEqual(report.backup_sha256, hashlib.sha256(self.backup.read_bytes()).hexdigest())
        with self.store.reader() as tx:
            self.assertEqual(tx.list_decisions(batch.batch_id), decisions)
            self.assertEqual(tx.get_batch(batch.batch_id).completion, batch.completion)
            self.assertEqual(tx.history("qa")[0].batch, batch)
            self.assertEqual(tx.get_publication(publication.publication_id), publication)
        assert batch.completion is not None
        with self.store.transaction() as tx:
            with self.assertRaises(HumanReviewError) as stale:
                tx.authorize_completion(batch.snapshot, batch.completion)
            self.assertEqual(stale.exception.code, "stale")

    def test_reimport_is_idempotent_and_does_not_duplicate_history(self) -> None:
        self.fixture()
        first = import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        repeated = import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        self.assertTrue(repeated.already_imported)
        self.assertEqual(repeated.source_hash, first.source_hash)
        with self.store.reader() as tx:
            self.assertEqual(len(tx.history("qa")), 1)
            self.assertEqual(len(tx.list_publications("qa")), 1)

    def test_annotation_manifest_recovers_selected_original_values_from_pinned_parquet(self) -> None:
        batch, _, publication = self.fixture()
        storage = StorageConfig(local_root=str(self.root / "objects"))
        original = {
            "candidate_id": "one", "command": "printf original", "category_id": "CAT-99",
            "risk_id": "CAT-99.1", "severity": "low", "class": "allow",
        }
        reference = DataFrameIOManager(storage=storage).persist_value(pd.DataFrame([original]))
        manifest = ReviewManifest(
            dataset_id="qa", storage=storage, publication=PublicationConfig(),
            references={"category_eligible_annotations": reference}, evidence={}, implementation={},
        )
        legacy = manifest.model_dump(mode="json")
        legacy.pop("review_columns")
        legacy["publication"].pop("review_columns")
        payload = json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode()
        snapshot_id = hashlib.sha256(payload).hexdigest()
        (self.manifests / f"{snapshot_id}.json").write_bytes(payload)
        snapshot = batch.snapshot.model_copy(update={"review_stage": "annotations", "snapshot_id": snapshot_id})
        assert batch.completion is not None
        token = batch.completion.model_copy(update={"review_stage": "annotations", "snapshot_id": snapshot_id})
        batch = batch.model_copy(update={"snapshot": snapshot, "completion": token})
        publication = publication.model_copy(update={"authorization": token})
        with closing(sqlite3.connect(self.source)) as connection, connection:
            connection.execute("UPDATE human_review_batches SET review_stage=?, data=?",
                               ("annotations", batch.model_dump_json()))
            connection.execute("UPDATE human_review_history SET data=?", (
                ReviewHistoryEntry(event="completed", batch=batch, recorded_at=self.now).model_dump_json(),
            ))
            connection.execute("UPDATE human_review_publications SET data=?", (publication.model_dump_json(),))
        report = import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        self.assertEqual(report.reusable_heads, 1)
        renamed = {**original, "candidate_id": "returned"}
        prepared = PreparedReview(
            snapshot=ReviewSnapshot(
                dataset_id="qa", snapshot_id="f" * 64, candidate_ids=("returned",),
                eligible_candidate_ids=("returned",),
                review_columns=tuple(PublicationConfig().review_columns),
            ),
            id_column="candidate_id", columns=tuple(renamed), rows=(renamed,),
        )
        with self.store.transaction() as tx:
            current = tx.prepare(prepared)
            self.assertEqual(tx.list_decisions(current.batch_id)[0].decision, "approve")
            self.assertIsNone(current.completion)

    def test_verified_original_values_reuse_under_new_ids_without_legacy_completion(self) -> None:
        self.fixture()
        import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        prepared = PreparedReview(
            snapshot=ReviewSnapshot(dataset_id="qa", review_stage="selection", snapshot_id="b" * 64,
                                    candidate_ids=("new-id",), eligible_candidate_ids=("new-id",), review_columns=("text",)),
            id_column="id", columns=("id", "text"), rows=({"id": "new-id", "text": "same"},),
        )
        with self.store.transaction() as tx:
            batch = tx.prepare(prepared)
            self.assertEqual(tx.list_decisions(batch.batch_id)[0].decision, "approve")
            self.assertIsNone(batch.completion)
            self.assertEqual(batch.state, "open")
            self.assertEqual(tx.list_decisions(batch.batch_id)[0].carried_from_batch_id, "legacy-batch")

    def test_unverifiable_or_conflicting_values_remain_historical_only(self) -> None:
        for selected, conflicting in ((False, False), (True, True)):
            with self.subTest(selected=selected, conflicting=conflicting):
                if self.source.exists():
                    with closing(sqlite3.connect(self.source)) as connection, connection:
                        for table in ("human_review_publications", "human_review_history",
                                      "human_review_decisions", "human_review_batches"):
                            connection.execute(f"DELETE FROM {table}")
                self.fixture(selected=selected, conflicting=conflicting)
                home = self.home / f"{selected}-{conflicting}"
                backup = self.root / f"backup-{selected}-{conflicting}.sqlite3"
                isolated = HumanReviewStore(home)
                try:
                    report = import_legacy(self.source, home, backup, manifests=self.manifests)
                    self.assertEqual(report.reusable_heads, 0)
                    self.assertEqual(len(report.historical_only), 2 if conflicting else 1)
                    with isolated.reader() as tx:
                        self.assertEqual(len(tx.list_decisions("legacy-batch")), 2 if conflicting else 1)
                finally:
                    with psycopg.connect(isolated.dsn, autocommit=True) as connection:
                        connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(isolated.schema)))

    def test_manifest_tampering_preserves_history_without_authorizing_reuse(self) -> None:
        batch, decisions, _ = self.fixture()
        path = self.manifests / f"{batch.snapshot.snapshot_id}.json"
        path.write_text('{"dataset_id":"tampered"}')
        report = import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        self.assertEqual(report.reusable_heads, 0)
        self.assertEqual(len(report.historical_only), 1)
        with self.store.reader() as tx:
            self.assertEqual(len(tx.history("qa")), 1)
            self.assertEqual(tx.list_decisions(batch.batch_id), decisions)

    def test_later_unreviewed_batch_does_not_erase_a_verified_approval(self) -> None:
        self.check_later_pending(0, "approve")

    def test_later_explicit_retraction_prevents_old_approval_reuse(self) -> None:
        self.check_later_pending(2, "pending")

    def check_later_pending(self, revision: int, expected: str) -> None:
        previous, _, _ = self.fixture()
        raw = json.loads((self.manifests / f"{previous.snapshot.snapshot_id}.json").read_bytes())
        raw["revision"] = "later"
        payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
        snapshot_id = hashlib.sha256(payload).hexdigest()
        (self.manifests / f"{snapshot_id}.json").write_bytes(payload)
        later = previous.model_copy(update={
            "batch_id": "later-batch", "state": "open", "completion": None,
            "snapshot": previous.snapshot.model_copy(update={"snapshot_id": snapshot_id}),
        })
        pending = ReviewDecision(
            batch_id=later.batch_id, candidate_id="one", row_revision=revision,
            reviewer="synthetic retracting reviewer" if revision else "",
            created_at=self.now, updated_at="2026-01-02T00:00:00+00:00",
        )
        with closing(sqlite3.connect(self.source)) as connection, connection:
            connection.execute("UPDATE human_review_batches SET current=0")
            connection.execute("INSERT INTO human_review_batches VALUES(?,?,?,?,?)",
                               (later.batch_id, "qa", "selection", 1, later.model_dump_json()))
            connection.execute("INSERT INTO human_review_decisions VALUES(?,?,?)",
                               (later.batch_id, "one", pending.model_dump_json()))
        import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        prepared = PreparedReview(
            snapshot=ReviewSnapshot(
                dataset_id="qa", review_stage="selection", snapshot_id="e" * 64,
                candidate_ids=("returned",), eligible_candidate_ids=("returned",), review_columns=("text",),
            ),
            id_column="id", columns=("id", "text"), rows=({"id": "returned", "text": "same"},),
        )
        with self.store.transaction() as tx:
            current = tx.prepare(prepared)
            self.assertEqual(tx.list_decisions(current.batch_id)[0].decision, expected)

    def test_mismatching_existing_backup_is_never_overwritten(self) -> None:
        self.fixture()
        with closing(sqlite3.connect(self.backup)) as connection:
            connection.execute("CREATE TABLE unrelated(value TEXT)")
        before = self.backup.read_bytes()
        with self.assertRaises(HumanReviewError):
            import_legacy(self.source, self.home, self.backup, manifests=self.manifests)
        self.assertEqual(self.backup.read_bytes(), before)
