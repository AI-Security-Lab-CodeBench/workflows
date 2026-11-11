from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Final

from pydantic import TypeAdapter

from codebench_pipeline.dataset_branches import Branch, annotation_stage_id, automatic_stage_id
from codebench_pipeline.human_review import (
    HumanReviewError,
    PublicationReference,
    ReviewBatch,
    ReviewCounts,
    ReviewDecision,
    ReviewHistoryEntry,
)

SCHEMA: Final = (
    """CREATE TABLE IF NOT EXISTS human_review_batches (
        batch_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL,
        review_stage TEXT NOT NULL DEFAULT 'annotations',
        current INTEGER NOT NULL CHECK(current IN (0, 1)), data TEXT NOT NULL)""",
    """CREATE UNIQUE INDEX IF NOT EXISTS human_review_current_stage
        ON human_review_batches(dataset_id, review_stage) WHERE current = 1""",
    """CREATE TABLE IF NOT EXISTS human_review_decisions (
        batch_id TEXT NOT NULL REFERENCES human_review_batches(batch_id),
        candidate_id TEXT NOT NULL, data TEXT NOT NULL,
        PRIMARY KEY(batch_id, candidate_id))""",
    """CREATE TABLE IF NOT EXISTS human_review_history (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id TEXT NOT NULL, batch_id TEXT NOT NULL, data TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS human_review_publications (
        publication_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL,
        batch_id TEXT NOT NULL REFERENCES human_review_batches(batch_id),
        data TEXT NOT NULL)""",
    """CREATE INDEX IF NOT EXISTS human_review_publications_dataset
        ON human_review_publications(dataset_id)""",
)


def initialize(connection: sqlite3.Connection) -> None:
    connection.execute(SCHEMA[0])
    columns = {row[1] for row in connection.execute("PRAGMA table_info(human_review_batches)")}
    if "review_stage" not in columns:
        connection.execute(
            "ALTER TABLE human_review_batches ADD COLUMN review_stage TEXT NOT NULL DEFAULT 'annotations'"
        )
        connection.execute("DROP INDEX IF EXISTS human_review_current")
    for statement in SCHEMA[1:]:
        connection.execute(statement)


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


class ReviewRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get_current_batch(self, dataset_id: str, review_stage: str = "annotations") -> ReviewBatch:
        row = self._connection.execute(
            "SELECT data FROM human_review_batches WHERE dataset_id = ? AND review_stage = ? AND current = 1",
            (dataset_id, review_stage),
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "No current review batch")
        return ReviewBatch.model_validate_json(TypeAdapter(str).validate_python(row[0]))

    def get_batch(self, batch_id: str) -> ReviewBatch:
        row = self._connection.execute(
            "SELECT data FROM human_review_batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Review batch not found")
        return ReviewBatch.model_validate_json(TypeAdapter(str).validate_python(row[0]))

    def list_decisions(self, batch_id: str) -> tuple[ReviewDecision, ...]:
        self.get_batch(batch_id)
        rows = self._connection.execute(
            "SELECT data FROM human_review_decisions WHERE batch_id = ? ORDER BY candidate_id",
            (batch_id,),
        )
        return tuple(
            ReviewDecision.model_validate_json(TypeAdapter(str).validate_python(row[0])) for row in rows
        )

    def counts(self, batch_id: str) -> ReviewCounts:
        decisions = self.list_decisions(batch_id)
        return ReviewCounts(
            pending=sum(row.decision == "pending" for row in decisions),
            approve=sum(row.decision == "approve" for row in decisions),
            reject=sum(row.decision == "reject" for row in decisions),
        )

    def history(self, dataset_id: str) -> tuple[ReviewHistoryEntry, ...]:
        rows = self._connection.execute(
            "SELECT data FROM human_review_history WHERE dataset_id = ? ORDER BY sequence",
            (dataset_id,),
        )
        return tuple(
            ReviewHistoryEntry.model_validate_json(TypeAdapter(str).validate_python(row[0])) for row in rows
        )

    def get_publication(self, publication_id: str) -> PublicationReference:
        row = self._connection.execute(
            "SELECT data FROM human_review_publications WHERE publication_id = ?",
            (publication_id,),
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Publication reference not found")
        return PublicationReference.model_validate_json(TypeAdapter(str).validate_python(row[0]))

    def list_publications(self, dataset_id: str) -> tuple[PublicationReference, ...]:
        rows = self._connection.execute(
            "SELECT data FROM human_review_publications WHERE dataset_id = ? ORDER BY rowid",
            (dataset_id,),
        )
        return tuple(
            PublicationReference.model_validate_json(TypeAdapter(str).validate_python(row[0])) for row in rows
        )

    def last_publication(
        self, dataset_id: str, review_stage: str | None = None, *, branch: Branch = "dataset"
    ) -> PublicationReference | None:
        row = self._connection.execute(
            """SELECT p.data FROM human_review_publications p
               JOIN human_review_batches b ON b.batch_id = p.batch_id
               WHERE p.dataset_id = ?
               AND ((? IS NULL AND b.review_stage IN (?, ?)) OR b.review_stage = ?)
               ORDER BY p.rowid DESC LIMIT 1""",
            (dataset_id, review_stage, annotation_stage_id(branch), automatic_stage_id(branch), review_stage),
        ).fetchone()
        if row is None:
            return None
        return PublicationReference.model_validate_json(TypeAdapter(str).validate_python(row[0]))

    def _write_batch(self, batch: ReviewBatch, *, current: bool = True) -> None:
        self._connection.execute(
            """UPDATE human_review_batches SET current = ?, data = ? WHERE batch_id = ?""",
            (int(current), batch.model_dump_json(), batch.batch_id),
        )

    def _audit(self, entry: ReviewHistoryEntry) -> None:
        self._connection.execute(
            """INSERT INTO human_review_history(dataset_id, batch_id, data) VALUES (?, ?, ?)""",
            (entry.batch.snapshot.dataset_id, entry.batch.batch_id, entry.model_dump_json()),
        )
