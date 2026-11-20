from __future__ import annotations

import json
from uuid import uuid4

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from jsonschema.protocols import Validator
from psycopg.types.json import Jsonb
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.human_review import (
    DecisionChange,
    HumanReviewError,
    PreparedReview,
    ReviewBatch,
    ReviewDecision,
    ReviewHistoryEntry,
    ReviewSnapshot,
    Text,
)
from codebench_pipeline.human_review_schema import timestamp
from codebench_pipeline.review_identity import review_identity
from codebench_pipeline.review_postgres_lifecycle import ReviewLifecycle
from codebench_pipeline.review_postgres_values import decision_values, row_cells
from codebench_pipeline.storage import fingerprint


def schema_validator(schema: dict[str, JsonValue] | bool) -> Validator:
    return Draft202012Validator(schema)


class HumanReviewTransaction(ReviewLifecycle):
    def prepare(self, prepared: PreparedReview) -> ReviewBatch:
        prepared = PreparedReview.model_validate(prepared.model_dump())
        snapshot = prepared.snapshot
        if len(set(prepared.columns)) != len(prepared.columns):
            raise HumanReviewError("conflict", "Prepared columns must be distinct")
        if not snapshot.review_columns or not set(snapshot.review_columns).issubset(prepared.columns):
            raise HumanReviewError("conflict", "Selected review columns must belong to the prepared table")
        if len(set(snapshot.review_columns)) != len(snapshot.review_columns):
            raise HumanReviewError("conflict", "Selected review columns must be distinct")
        if prepared.id_column not in prepared.columns or set(prepared.columns).intersection(
            {"human_decision", "human_reviewer", "human_notes", "human_edits"}
        ):
            raise HumanReviewError(
                "conflict", "Prepared columns conflict with review metadata or lack the identity"
            )
        try:
            for schema in prepared.column_schema.values():
                Draft202012Validator.check_schema(schema)
            for name in ("original_row_schema", "row_schema"):
                if name in prepared.validation:
                    Draft202012Validator.check_schema(
                        TypeAdapter(dict[str, JsonValue] | bool).validate_python(prepared.validation[name])
                    )
        except SchemaError as error:
            raise HumanReviewError("conflict", f"Invalid declared review schema: {error.message}") from error
        ids: list[str] = []
        for row in prepared.rows:
            if set(row) != set(prepared.columns):
                raise HumanReviewError("conflict", "Prepared row columns differ from the captured schema")
            candidate_id = TypeAdapter(Text).validate_python(row.get(prepared.id_column), strict=True)
            try:
                json.dumps(row, allow_nan=False)
            except ValueError as error:
                raise HumanReviewError(
                    "ineligible", "Prepared rows must contain finite JSON values"
                ) from error
            record_hash = review_identity(row, snapshot.review_columns)
            if snapshot.record_fingerprints and snapshot.record_fingerprints.get(candidate_id) != record_hash:
                raise HumanReviewError(
                    "conflict", "Prepared review values differ from the snapshot fingerprint"
                )
            ids.append(candidate_id)
        if tuple(ids) != snapshot.candidate_ids:
            raise HumanReviewError("conflict", "Prepared identities differ from the snapshot")
        metadata = prepared.model_dump(mode="json", exclude={"snapshot", "rows"})
        return self._activate(snapshot, prepared.rows, prepared.id_column, metadata)

    def ensure_current_batch(self, snapshot: ReviewSnapshot) -> ReviewBatch:
        self.lock_dataset(snapshot.dataset_id)
        existing = self.connection.execute(
            "SELECT batch_id,invalidated,prepared IS NOT NULL AS prepared FROM review_batches "
            "WHERE dataset_id=%s AND review_stage=%s AND artifact_id=%s AND scope_id=%s AND current",
            (snapshot.dataset_id, snapshot.review_stage, snapshot.artifact_id, snapshot.scope_id),
        ).fetchone()
        if existing is not None:
            batch_id = TypeAdapter(str).validate_python(existing["batch_id"])
            if existing["prepared"] and existing["invalidated"] is not None:
                raise HumanReviewError("stale", "Prepared review authority must be revalidated by prepare")
            full = self._full_batch(batch_id)
            if existing["invalidated"] is None and snapshot == full.snapshot:
                return full
        rows: tuple[dict[str, JsonValue], ...] = tuple(
            {"candidate_id": identifier} for identifier in snapshot.candidate_ids
        )
        return self._activate(snapshot, rows, "candidate_id", None)

    def _activate(
        self,
        snapshot: ReviewSnapshot,
        rows: tuple[dict[str, JsonValue], ...],
        id_column: str,
        metadata: dict[str, JsonValue] | None,
    ) -> ReviewBatch:
        self.lock_dataset(snapshot.dataset_id)
        binding = fingerprint(
            {"snapshot": snapshot.model_dump(mode="json"), "rows": list(rows), "metadata": metadata}
        )
        current_row = self.connection.execute(
            "SELECT batch_id,binding,invalidated,prepared FROM review_batches "
            "WHERE dataset_id=%s AND review_stage=%s AND artifact_id=%s AND scope_id=%s AND current",
            (snapshot.dataset_id, snapshot.review_stage, snapshot.artifact_id, snapshot.scope_id),
        ).fetchone()
        if current_row:
            current = self._full_batch(TypeAdapter(str).validate_python(current_row["batch_id"]))
            if current_row["binding"] == binding and current_row["invalidated"] is None:
                return current
            previous_metadata = TypeAdapter(dict[str, JsonValue] | None).validate_python(
                current_row["prepared"]
            )
            schema_upgrade = (
                current_row["invalidated"] is not None
                and previous_metadata is not None
                and "column_schema" not in previous_metadata
            )
            if (
                current.snapshot.snapshot_id == snapshot.snapshot_id
                and current_row["binding"] != binding
                and not schema_upgrade
            ):
                raise HumanReviewError("conflict", "Snapshot digest has a different identity binding")
            superseded = current.model_copy(update={"state": "superseded", "updated_at": timestamp()})
            self._write_batch(superseded, current=False)
            self.audit(
                ReviewHistoryEntry(event="superseded", batch=superseded, recorded_at=superseded.updated_at)
            )
        now = timestamp()
        batch = ReviewBatch(
            batch_id=uuid4().hex,
            snapshot=snapshot,
            generation=1,
            batch_revision=0,
            state="open",
            created_at=now,
            updated_at=now,
        )
        self.insert_batch(batch, binding, metadata)
        eligible = set(snapshot.eligible_candidate_ids)
        grouped: dict[str, list[tuple[int, str, dict[str, JsonValue]]]] = {}
        for index, row in enumerate(rows):
            identifier = TypeAdapter(str).validate_python(row[id_column], strict=True)
            record_hash = (
                review_identity(row, snapshot.review_columns)
                if metadata is not None
                else snapshot.record_fingerprints.get(identifier)
                or fingerprint({"snapshot": snapshot.snapshot_id, "candidate": identifier})
            )
            grouped.setdefault(record_hash, []).append((index, identifier, row))
        carried: list[ReviewDecision] = []
        heads = {
            TypeAdapter(str).validate_python(row["record_hash"]): ReviewDecision.model_validate(row["data"])
            for row in self.connection.execute(
                "SELECT record_hash,data FROM review_heads WHERE dataset_id=%s AND review_stage=%s "
                "AND artifact_id=%s AND scope_id=%s AND record_hash=ANY(%s)",
                (
                    snapshot.dataset_id,
                    snapshot.review_stage,
                    snapshot.artifact_id,
                    snapshot.scope_id,
                    list(grouped),
                ),
            )
        }
        items: list[tuple[str, str, Jsonb, Jsonb]] = []
        occurrences_to_insert: list[tuple[str, str, int, str, bool, str, Jsonb, str | None]] = []
        for record_hash, occurrences in grouped.items():
            previous = heads.get(record_hash)
            identifier = occurrences[0][1]
            if previous is not None:
                decision = previous.model_copy(
                    update={
                        "batch_id": batch.batch_id,
                        "candidate_id": identifier,
                        "carried_from_batch_id": previous.batch_id,
                    }
                )
                carried.append(decision)
            else:
                decision = ReviewDecision(
                    batch_id=batch.batch_id, candidate_id=identifier, created_at=now, updated_at=now
                )
            items.append(
                (
                    batch.batch_id,
                    record_hash,
                    Jsonb(decision.model_dump(mode="json")),
                    Jsonb(row_cells(decision_values(decision))),
                )
            )
            for index, candidate_id, row in occurrences:
                blocked_reason = None if candidate_id in eligible else "Machine-ineligible occurrence"
                if decision.decision == "approve":
                    try:
                        self._validate_edits(row, decision.edits, snapshot.review_columns, metadata)
                    except HumanReviewError as error:
                        blocked_reason = error.detail
                occurrences_to_insert.append(
                    (
                        batch.batch_id,
                        candidate_id,
                        index,
                        record_hash,
                        candidate_id in eligible,
                        json.dumps(row, ensure_ascii=False, allow_nan=False),
                        Jsonb(row_cells(row)),
                        blocked_reason,
                    )
                )
        with self.connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO review_items(batch_id,record_hash,data,cells) VALUES(%s,%s,%s,%s)", items
            )
            cursor.executemany(
                """INSERT INTO review_rows(batch_id,candidate_id,row_index,record_hash,eligible,row_data,cells,blocked_reason)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                occurrences_to_insert,
            )
        batch = batch.model_copy(update={"batch_revision": len(carried)})
        self._write_batch(batch)
        self.audit(ReviewHistoryEntry(event="created", batch=batch, recorded_at=now))
        for decision in carried:
            self.audit(ReviewHistoryEntry(event="carried", batch=batch, decision=decision, recorded_at=now))
        return batch

    def _validate_edits(
        self,
        row: dict[str, JsonValue],
        edits: dict[str, JsonValue],
        columns: tuple[str, ...],
        metadata: dict[str, JsonValue] | None,
    ) -> None:
        if metadata is None:
            if edits:
                raise HumanReviewError("ineligible", "Edits require captured prepared column values")
            return
        if not set(edits).issubset(columns):
            raise HumanReviewError("ineligible", "Edits must be limited to selected review columns")
        schemas = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(metadata["column_schema"])
        for column in columns:
            if schemas[column].get("editable") is False and column in edits and edits[column] != row[column]:
                raise HumanReviewError(
                    "ineligible", f"Reviewed value for {column} must retain its original value"
                )
            try:
                schema_validator(schemas[column]).validate(edits.get(column, row[column]))
            except ValidationError as error:
                raise HumanReviewError(
                    "ineligible", f"Invalid reviewed value for {column}: {error.message}"
                ) from error
        if columns:
            review_identity({**row, **edits}, columns)
        validation = TypeAdapter(dict[str, JsonValue]).validate_python(metadata["validation"])
        for name, value in (("original_row_schema", row), ("row_schema", {**row, **edits})):
            if name in validation:
                schema = TypeAdapter(dict[str, JsonValue] | bool).validate_python(validation[name])
                try:
                    schema_validator(schema).validate(value)
                except ValidationError as error:
                    raise HumanReviewError("ineligible", f"Invalid reviewed row: {error.message}") from error

    def invalidate(
        self,
        dataset_id: str,
        reason: str,
        *,
        review_stage: str | None = None,
        artifact_id: str = "",
        scope_id: str = "",
    ) -> None:
        self.lock_dataset(dataset_id)
        reason = TypeAdapter(Text).validate_python(reason)
        self.connection.execute(
            "UPDATE review_batches SET invalidated=%s WHERE dataset_id=%s AND artifact_id=%s "
            "AND scope_id=%s AND (%s::text IS NULL OR review_stage=%s) AND current",
            (reason, dataset_id, artifact_id, scope_id, review_stage, review_stage),
        )

    def save_decision(
        self,
        snapshot: ReviewSnapshot,
        change: DecisionChange,
    ) -> ReviewDecision:
        batch = self._bound(snapshot, change.batch_id, change.generation)
        batch.require_open()
        row = self.connection.execute(
            """SELECT i.data,r.record_hash FROM review_rows r JOIN review_items i USING(batch_id,record_hash)
                WHERE r.batch_id=%s AND r.candidate_id=%s""",
            (batch.batch_id, change.candidate_id),
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Candidate is not in this batch")
        previous = ReviewDecision.model_validate(row["data"])
        if previous.row_revision != change.expected_row_revision:
            raise HumanReviewError("stale", "Decision row revision changed")
        metadata = self._metadata(batch.batch_id)
        for original in self.rows_for_hash(batch.batch_id, change.candidate_id):
            if change.decision == "approve":
                self._validate_edits(original, change.edits, batch.snapshot.review_columns, metadata)
        if change.decision == "approve":
            blocked = self.connection.execute(
                "SELECT 1 FROM review_rows WHERE batch_id=%s AND record_hash=%s AND NOT eligible LIMIT 1",
                (batch.batch_id, row["record_hash"]),
            ).fetchone()
            if blocked:
                raise HumanReviewError("ineligible", "Human approval cannot override eligibility")
        now = timestamp()
        decision = ReviewDecision(
            batch_id=batch.batch_id,
            candidate_id=previous.candidate_id,
            decision=change.decision,
            edits=change.edits,
            notes=change.notes,
            reviewer=change.reviewer,
            row_revision=previous.row_revision + 1,
            created_at=previous.created_at,
            updated_at=now,
        )
        result = self.connection.execute(
            """UPDATE review_items SET data=%s,cells=%s WHERE batch_id=%s AND record_hash=%s
                AND (data->>'row_revision')::bigint=%s""",
            (
                Jsonb(decision.model_dump(mode="json")),
                Jsonb(row_cells(decision_values(decision))),
                batch.batch_id,
                row["record_hash"],
                change.expected_row_revision,
            ),
        )
        if result.rowcount != 1:
            raise HumanReviewError("stale", "Decision row revision changed")
        self.connection.execute(
            """INSERT INTO review_heads(dataset_id,review_stage,record_hash,data,artifact_id,scope_id)
                VALUES(%s,%s,%s,%s,%s,%s)
                ON CONFLICT(dataset_id,review_stage,artifact_id,scope_id,record_hash)
                DO UPDATE SET data=excluded.data""",
            (
                snapshot.dataset_id,
                snapshot.review_stage,
                row["record_hash"],
                Jsonb(decision.model_dump(mode="json")),
                snapshot.artifact_id,
                snapshot.scope_id,
            ),
        )
        if change.decision == "approve":
            self.connection.execute(
                "UPDATE review_rows SET blocked_reason=NULL WHERE batch_id=%s AND record_hash=%s",
                (batch.batch_id, row["record_hash"]),
            )
        updated = batch.model_copy(update={"batch_revision": batch.batch_revision + 1, "updated_at": now})
        self._write_batch(updated)
        self.audit(ReviewHistoryEntry(event="decision", batch=updated, decision=decision, recorded_at=now))
        return decision.model_copy(update={"candidate_id": change.candidate_id})
