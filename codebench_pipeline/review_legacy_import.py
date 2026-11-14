from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path

from psycopg.types.json import Jsonb
from pydantic import JsonValue

from codebench_pipeline.human_review import (
    HumanReviewError,
    PublicationReference,
    ReviewBatch,
    ReviewDecision,
    ReviewHistoryEntry,
    ReviewModel,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_identity import review_identity
from codebench_pipeline.review_legacy_manifest import load_legacy_manifest
from codebench_pipeline.review_postgres_values import decision_values, row_cells, same_json_type
from codebench_pipeline.storage import fingerprint


class ImportReport(ReviewModel):
    source_hash: str
    backup_path: str
    backup_sha256: str
    batches: int = 0
    decisions: int = 0
    history_entries: int = 0
    publications: int = 0
    reusable_heads: int = 0
    historical_only: tuple[str, ...] = ()
    already_imported: bool = False


def _logical_hash(connection: sqlite3.Connection) -> str:
    digest = hashlib.sha256()
    for statement in connection.iterdump():
        digest.update(statement.encode())
        digest.update(b"\n")
    return digest.hexdigest()


def _verify(connection: sqlite3.Connection) -> None:
    if tuple(connection.execute("PRAGMA integrity_check")) != (("ok",),):
        raise HumanReviewError("conflict", "Legacy SQLite integrity verification failed")


def import_legacy(
    source: Path, home: Path, backup: Path, *, manifests: Path | None = None,
) -> ImportReport:
    source, home, backup = source.resolve(), home.resolve(), backup.resolve()
    if source == backup or not source.is_file() or (backup.exists() and source.samefile(backup)):
        raise HumanReviewError("conflict", "Import requires an existing source and a distinct backup")
    backup.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as original:
        original.execute("BEGIN")
        _verify(original)
        source_hash = _logical_hash(original)
        if not backup.exists():
            with backup.open("xb"):
                pass
            with closing(sqlite3.connect(backup)) as destination:
                original.backup(destination)
        with closing(sqlite3.connect(backup.as_uri() + "?mode=ro", uri=True)) as saved:
            saved.execute("BEGIN")
            _verify(saved)
            if _logical_hash(saved) != source_hash:
                raise HumanReviewError("conflict", "Backup does not match the captured SQLite snapshot")
    backup_hash = hashlib.sha256(backup.read_bytes()).hexdigest()
    store = HumanReviewStore(home)
    with closing(sqlite3.connect(backup.as_uri() + "?mode=ro", uri=True)) as legacy:
        legacy.execute("BEGIN")
        batches = tuple(
            ReviewBatch.model_validate_json(row[0]) for row in legacy.execute(
                "SELECT data FROM human_review_batches ORDER BY rowid"
            )
        )
        with store.transaction(timeout=10) as tx:
            for dataset in sorted({batch.snapshot.dataset_id for batch in batches}):
                tx.lock_dataset(dataset)
            previous = tx.connection.execute(
                "SELECT data FROM review_imports WHERE source_hash=%s", (source_hash,)
            ).fetchone()
            if previous:
                return ImportReport.model_validate_json(json.dumps(previous["data"])).model_copy(
                    update={"already_imported": True}
                )
            historical: list[str] = []
            heads: dict[tuple[str, str, str], list[ReviewDecision]] = {}
            decision_count = history_count = publication_count = 0
            for batch in batches:
                collision = tx.connection.execute(
                    "SELECT 1 FROM review_batches WHERE batch_id=%s", (batch.batch_id,)
                ).fetchone()
                if collision:
                    raise HumanReviewError("conflict", f"Existing batch ID would be overwritten: {batch.batch_id}")
                tx.insert_batch(batch, fingerprint(batch.model_dump(mode="json")), current=False,
                                 invalidated="Legacy completion is historical; fresh preparation is required")
                manifest = load_legacy_manifest(home, manifests, batch)
                originals = {
                    str(row[manifest.id_column]): row for row in manifest.rows
                } if manifest is not None else {}
                eligible = set(batch.snapshot.eligible_candidate_ids)
                decisions = tuple(
                    ReviewDecision.model_validate_json(row[0]) for row in legacy.execute(
                        "SELECT data FROM human_review_decisions WHERE batch_id=? ORDER BY candidate_id",
                        (batch.batch_id,),
                    )
                )
                for index, decision in enumerate(decisions):
                    decision_count += 1
                    candidate_id = decision.candidate_id
                    original = originals.get(candidate_id)
                    historical_hash = fingerprint({"legacy_batch": batch.batch_id, "candidate_id": candidate_id})
                    values: dict[str, JsonValue] = original if original is not None else {"candidate_id": candidate_id}
                    tx.connection.execute(
                        "INSERT INTO review_items(batch_id,record_hash,data,cells) VALUES(%s,%s,%s,%s)",
                        (batch.batch_id, historical_hash, Jsonb(decision.model_dump(mode="json")),
                         Jsonb(row_cells(decision_values(decision)))),
                    )
                    tx.connection.execute(
                        """INSERT INTO review_rows(batch_id,candidate_id,row_index,record_hash,eligible,row_data,cells,blocked_reason)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (batch.batch_id, candidate_id, index, historical_hash, candidate_id in eligible,
                         json.dumps(values, ensure_ascii=False, allow_nan=False), Jsonb(row_cells(values)),
                         None if candidate_id in eligible else "Legacy machine-ineligible occurrence"),
                    )
                    reason = "selected original values could not be verified"
                    if original is not None and manifest is not None:
                        edits = decision.edits or (
                            decision.label_corrections.model_dump(mode="json", by_alias=True)
                            if decision.label_corrections is not None else {}
                        )
                        if set(edits).issubset(manifest.review_columns) and all(
                            column in original and same_json_type(original[column], value) for column, value in edits.items()
                        ):
                            try:
                                record_hash = review_identity(original, manifest.review_columns)
                            except HumanReviewError:
                                historical.append(f"{batch.batch_id}/{candidate_id}: selected values are not verifiable finite JSON")
                                continue
                            head = decision.model_copy(update={"edits": edits, "label_corrections": None})
                            if head.decision == "pending" and head.row_revision == 0:
                                continue
                            heads.setdefault((batch.snapshot.dataset_id, batch.snapshot.review_stage, record_hash), []).append(head)
                            continue
                        reason = "legacy edits do not match selected captured column types"
                    historical.append(f"{batch.batch_id}/{candidate_id}: {reason}")
            for row in legacy.execute("SELECT data FROM human_review_history ORDER BY sequence"):
                entry = ReviewHistoryEntry.model_validate_json(row[0])
                tx.audit(entry)
                history_count += 1
            for row in legacy.execute("SELECT data FROM human_review_publications ORDER BY rowid"):
                reference = PublicationReference.model_validate_json(row[0])
                tx.connection.execute(
                    "INSERT INTO review_publications(publication_id,dataset_id,batch_id,data) VALUES(%s,%s,%s,%s)",
                    (reference.publication_id, reference.authorization.dataset_id,
                     reference.authorization.batch_id, Jsonb(reference.model_dump(mode="json"))),
                )
                publication_count += 1
            reusable = 0
            for (dataset, stage, record_hash), versions in heads.items():
                ordered = sorted(versions, key=lambda decision: (decision.updated_at, decision.row_revision))
                latest = ordered[-1]
                latest_key = latest.updated_at, latest.row_revision
                peers = [decision for decision in ordered if (decision.updated_at, decision.row_revision) == latest_key]
                signatures = {
                    fingerprint({"decision": decision.decision, "edits": decision.edits,
                                 "reviewer": decision.reviewer, "notes": decision.notes}) for decision in peers
                }
                by_batch: dict[str, set[str]] = {}
                for decision in versions:
                    by_batch.setdefault(decision.batch_id, set()).add(fingerprint({
                        "decision": decision.decision, "edits": decision.edits,
                    }))
                if any(len(states) != 1 for states in by_batch.values()) or len(signatures) != 1 or (
                    latest.decision != "pending" and (not latest.reviewer.strip() or latest.row_revision < 1)
                ):
                    historical.extend(f"{decision.batch_id}/{decision.candidate_id}: ambiguous or unproven decision head" for decision in versions)
                    continue
                inserted = tx.connection.execute(
                    """INSERT INTO review_heads VALUES(%s,%s,%s,%s)
                        ON CONFLICT(dataset_id,review_stage,record_hash) DO NOTHING""",
                    (dataset, stage, record_hash, Jsonb(latest.model_dump(mode="json"))),
                )
                if inserted.rowcount != 1:
                    historical.append(f"{dataset}/{stage}/{record_hash}: existing PostgreSQL head retained")
                else:
                    reusable += 1
            report = ImportReport(
                source_hash=source_hash, backup_path=str(backup), backup_sha256=backup_hash,
                batches=len(batches), decisions=decision_count, history_entries=history_count,
                publications=publication_count, reusable_heads=reusable, historical_only=tuple(historical),
            )
            tx.connection.execute(
                "INSERT INTO review_imports VALUES(%s,%s)", (source_hash, Jsonb(report.model_dump(mode="json")))
            )
            return report


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Explicitly import a verified read-only SQLite review ledger")
    parser.add_argument("source", type=Path)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--manifests", type=Path)
    args = parser.parse_args(argv)
    print(import_legacy(args.source, args.home, args.backup, manifests=args.manifests).model_dump_json(indent=2))


if __name__ == "__main__":
    main()
