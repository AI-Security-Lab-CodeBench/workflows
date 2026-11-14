from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Final, LiteralString

from psycopg import Connection, sql
from psycopg.types.json import Jsonb
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.dataset_branches import Branch, annotation_stage_id, automatic_stage_id
from codebench_pipeline.human_review import (
    HumanReviewError,
    PublicationReference,
    ReviewBatch,
    ReviewCounts,
    ReviewDecision,
    ReviewHistoryEntry,
)

MIGRATIONS: Final[tuple[tuple[LiteralString, ...], ...]] = (
    (
        """CREATE TABLE review_batches (
            batch_id text PRIMARY KEY, dataset_id text NOT NULL, review_stage text NOT NULL,
            current boolean NOT NULL, invalidated text, binding text NOT NULL,
            data jsonb NOT NULL, prepared jsonb)""",
        """CREATE UNIQUE INDEX review_current ON review_batches(dataset_id, review_stage)
            WHERE current""",
        """CREATE TABLE review_items (
            batch_id text NOT NULL REFERENCES review_batches(batch_id),
            record_hash text NOT NULL, data jsonb NOT NULL,
            PRIMARY KEY(batch_id, record_hash))""",
        """CREATE TABLE review_rows (
            batch_id text NOT NULL REFERENCES review_batches(batch_id),
            candidate_id text NOT NULL, row_index bigint NOT NULL, record_hash text NOT NULL,
            eligible boolean NOT NULL, row_data text NOT NULL, cells jsonb NOT NULL,
            PRIMARY KEY(batch_id, candidate_id))""",
        "CREATE INDEX review_rows_hash ON review_rows(batch_id, record_hash, row_index)",
        """CREATE TABLE review_heads (
            dataset_id text NOT NULL, review_stage text NOT NULL, record_hash text NOT NULL,
            data jsonb NOT NULL, PRIMARY KEY(dataset_id, review_stage, record_hash))""",
        """CREATE TABLE review_history (
            sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            dataset_id text NOT NULL, batch_id text NOT NULL, data jsonb NOT NULL)""",
        "CREATE INDEX review_history_dataset ON review_history(dataset_id, sequence)",
        """CREATE TABLE review_publications (
            sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            publication_id text NOT NULL UNIQUE, dataset_id text NOT NULL,
            batch_id text NOT NULL REFERENCES review_batches(batch_id), data jsonb NOT NULL)""",
        "CREATE INDEX review_publications_dataset ON review_publications(dataset_id, sequence)",
        "CREATE TABLE review_imports(source_hash text PRIMARY KEY, data jsonb NOT NULL)",
    ),
    (
        "ALTER TABLE review_rows ADD COLUMN blocked_reason text",
        "ALTER TABLE review_items ADD COLUMN cells jsonb NOT NULL DEFAULT '{}'::jsonb",
    ),
)

COMPACT_DATA: Final = """data || jsonb_build_object('snapshot',
    (data->'snapshot') || '{"candidate_ids":[],"eligible_candidate_ids":[],"record_fingerprints":{}}'::jsonb)"""


def initialize(connection: Connection[dict[str, JsonValue]], schema: str) -> None:
    exists = connection.execute(
        "SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,)
    ).fetchone()
    if exists:
        connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        row = connection.execute("SELECT max(version) AS version FROM review_schema_versions").fetchone()
        if row and row["version"] == len(MIGRATIONS):
            return
    connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (schema + ":migration",))
    connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
    connection.execute(
        "CREATE TABLE IF NOT EXISTS review_schema_versions(version integer PRIMARY KEY, applied_at text NOT NULL)"
    )
    row = connection.execute("SELECT coalesce(max(version), 0) AS version FROM review_schema_versions").fetchone()
    version = TypeAdapter(int).validate_python(row["version"]) if row else 0
    if version > len(MIGRATIONS):
        raise HumanReviewError("conflict", "Review schema is newer than this implementation")
    for index, migration in enumerate(MIGRATIONS, start=1):
        if index > version:
            for statement in migration:
                connection.execute(statement)
            connection.execute(
                "INSERT INTO review_schema_versions VALUES (%s, %s)", (index, timestamp())
            )


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


class ReviewRepository:
    def __init__(
        self, connection: Connection[dict[str, JsonValue]], *, namespace: str = "", timeout: float = 0,
    ) -> None:
        self.connection = connection
        self._namespace = namespace
        self._timeout = timeout
        self._locked: set[str] = set()

    def get_current_batch(
        self, dataset_id: str, review_stage: str = "annotations", *, compact: bool = False
    ) -> ReviewBatch:
        projection = COMPACT_DATA if compact else "data"
        row = self.connection.execute(
            f"SELECT {projection} AS data, invalidated FROM review_batches WHERE dataset_id = %s AND review_stage = %s AND current",
            (dataset_id, review_stage),
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "No current review batch")
        if row["invalidated"] is not None:
            raise HumanReviewError("stale", TypeAdapter(str).validate_python(row["invalidated"]))
        return ReviewBatch.model_validate_json(json.dumps(row["data"]))

    def _full_batch(self, batch_id: str) -> ReviewBatch:
        row = self.connection.execute(
            "SELECT data FROM review_batches WHERE batch_id = %s", (batch_id,)
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Review batch not found")
        return ReviewBatch.model_validate_json(json.dumps(row["data"]))

    def get_batch(self, batch_id: str, *, compact: bool = False) -> ReviewBatch:
        projection = COMPACT_DATA if compact else "data"
        row = self.connection.execute(
            f"SELECT {projection} AS data FROM review_batches WHERE batch_id = %s", (batch_id,)
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Review batch not found")
        return ReviewBatch.model_validate_json(json.dumps(row["data"]))

    def list_decisions(self, batch_id: str) -> tuple[ReviewDecision, ...]:
        self.get_batch(batch_id)
        rows = self.connection.execute(
            """SELECT i.data, r.candidate_id FROM review_rows r JOIN review_items i
                USING(batch_id, record_hash) WHERE r.batch_id = %s ORDER BY r.candidate_id""",
            (batch_id,),
        )
        return tuple(
            ReviewDecision.model_validate(row["data"]).model_copy(
                update={"batch_id": batch_id, "candidate_id": row["candidate_id"]}
            ) for row in rows
        )

    def counts(self, batch_id: str) -> ReviewCounts:
        self.get_batch(batch_id, compact=True)
        row = self.connection.execute(
            """SELECT count(*) FILTER(WHERE i.data->>'decision' = 'pending') AS pending,
                count(*) FILTER(WHERE i.data->>'decision' = 'approve') AS approve,
                count(*) FILTER(WHERE i.data->>'decision' = 'reject') AS reject,
                count(*) FILTER(WHERE i.data->>'decision' = 'approve' AND EXISTS (
                    SELECT 1 FROM review_rows r WHERE r.batch_id=i.batch_id
                    AND r.record_hash=i.record_hash AND r.blocked_reason IS NOT NULL)) AS blocked,
                coalesce(sum((SELECT count(*) FROM review_rows r WHERE r.batch_id=i.batch_id
                    AND r.record_hash=i.record_hash)) FILTER(WHERE i.data->>'decision'='approve'),0)::bigint
                    AS approved_occurrences
                FROM review_items i WHERE i.batch_id = %s""", (batch_id,),
        ).fetchone()
        return ReviewCounts.model_validate(row)

    def history(self, dataset_id: str) -> tuple[ReviewHistoryEntry, ...]:
        return tuple(
            ReviewHistoryEntry.model_validate_json(json.dumps(row["data"])) for row in self.connection.execute(
                "SELECT data FROM review_history WHERE dataset_id = %s ORDER BY sequence", (dataset_id,)
            )
        )

    def get_publication(self, publication_id: str) -> PublicationReference:
        row = self.connection.execute(
            "SELECT data FROM review_publications WHERE publication_id = %s", (publication_id,)
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Publication reference not found")
        return PublicationReference.model_validate(row["data"])

    def list_publications(self, dataset_id: str) -> tuple[PublicationReference, ...]:
        return tuple(
            PublicationReference.model_validate(row["data"]) for row in self.connection.execute(
                "SELECT data FROM review_publications WHERE dataset_id = %s ORDER BY sequence", (dataset_id,)
            )
        )

    def last_publication(
        self, dataset_id: str, review_stage: str | None = None, *, branch: Branch = "dataset"
    ) -> PublicationReference | None:
        row = self.connection.execute(
            """SELECT p.data FROM review_publications p JOIN review_batches b USING(batch_id)
                WHERE p.dataset_id = %s AND
                ((%s::text IS NULL AND b.review_stage IN (%s, %s)) OR b.review_stage = %s)
                ORDER BY p.sequence DESC LIMIT 1""",
            (dataset_id, review_stage, annotation_stage_id(branch), automatic_stage_id(branch), review_stage),
        ).fetchone()
        return PublicationReference.model_validate(row["data"]) if row else None

    def insert_batch(
        self, batch: ReviewBatch, binding: str, prepared_metadata: dict[str, JsonValue] | None = None,
        *, current: bool = True, invalidated: str | None = None,
    ) -> None:
        self.connection.execute(
            """INSERT INTO review_batches
                (batch_id,dataset_id,review_stage,current,invalidated,binding,data,prepared)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
            (batch.batch_id, batch.snapshot.dataset_id, batch.snapshot.review_stage, current,
             invalidated, binding, Jsonb(batch.model_dump(mode="json")),
             Jsonb(prepared_metadata) if prepared_metadata is not None else None),
        )

    def _write_batch(self, batch: ReviewBatch, *, current: bool = True) -> None:
        full = self._full_batch(batch.batch_id)
        updated = batch.model_copy(update={"snapshot": full.snapshot})
        self.connection.execute(
            "UPDATE review_batches SET current = %s, data = %s WHERE batch_id = %s",
            (current, Jsonb(updated.model_dump(mode="json")), batch.batch_id),
        )

    def audit(self, entry: ReviewHistoryEntry) -> None:
        self.connection.execute(
            "INSERT INTO review_history(dataset_id,batch_id,data) VALUES(%s,%s,%s)",
            (entry.batch.snapshot.dataset_id, entry.batch.batch_id, Jsonb(entry.model_dump(mode="json"))),
        )
