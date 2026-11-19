import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from fcntl import LOCK_EX, flock
from pathlib import Path
from typing import Literal

from .keenable_policy import (
    MAX_BYTES,
    MAX_ORIGINS,
    MAX_SEARCH,
    PAGE_CASES,
    RESPONSE_BYTES,
    AcquisitionBudget,
    CallBudget,
    Declaration,
    Digest,
    KeenableError,
    PagePermission,
    Record,
    digest,
)
from .keenable_receipts import CallReceipt, CaptureLock, CatalogReceipt, VerifiedPage

PLAN_RESPONSE_BYTES = 16 * 1024


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


@contextmanager
def open_ledger(root: Path, declaration: Declaration) -> Generator["Ledger"]:
    namespace = root / declaration.dataset / "keenable" / declaration.generation
    namespace.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (namespace / "writer.lock").open("a+b") as guard:
        flock(guard, LOCK_EX)
        with closing(sqlite3.connect(namespace / "generation.sqlite3")) as db:
            db.executescript(
                """
                PRAGMA synchronous = FULL;
                CREATE TABLE IF NOT EXISTS artifact (
                    name TEXT PRIMARY KEY, payload BLOB NOT NULL, sha256 TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS call (
                    slot TEXT PRIMARY KEY, kind TEXT NOT NULL, reserved_bytes INTEGER NOT NULL,
                    state TEXT NOT NULL, receipt BLOB NOT NULL, payload BLOB
                );
                """
            )
            ledger = Ledger(db, declaration.budget)
            binding = ledger.artifact("declaration")
            current = declaration.declaration_sha256.encode()
            if binding is None:
                ledger.publish("declaration", current)
            elif binding != current:
                raise KeenableError("generation_declaration_changed")
            declaration.check_current(datetime.now(UTC))
            yield ledger


class Ledger:
    """One generation's serialized receipts and immutable objects."""

    def __init__(self, db: sqlite3.Connection, budget: AcquisitionBudget | CallBudget | None) -> None:
        self.db = db
        self.budget = budget

    def artifact(self, name: str) -> bytes | None:
        row = self.db.execute("SELECT payload, sha256 FROM artifact WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        payload, expected = row
        if not isinstance(payload, bytes) or digest(payload) != expected:
            raise KeenableError("artifact_integrity")
        return payload

    def publish(self, name: str, payload: bytes) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO artifact VALUES (?, ?, ?) ON CONFLICT(name) DO NOTHING",
                (name, payload, digest(payload)),
            )
            if self.artifact(name) != payload:
                raise KeenableError("immutable_artifact_conflict")

    def require_healthy(self) -> None:
        rows = self.db.execute("SELECT receipt FROM call WHERE state = 'dispatched'").fetchall()
        for (raw,) in rows:
            receipt = CallReceipt.model_validate_json(raw)
            self.finish(receipt, state="failed", reason="ambiguous_dispatch")
        row = self.db.execute(
            "SELECT receipt FROM call WHERE state = 'failed' ORDER BY slot LIMIT 1"
        ).fetchone()
        if row is not None:
            receipt = CallReceipt.model_validate_json(row[0])
            raise KeenableError(receipt.reason or "generation_failed", receipt.status)

    def reserve(self, kind: Literal["search", "origin", "plan"], identity: "SlotIdentity") -> CallReceipt:
        count, reserved = self.db.execute(
            """
            SELECT COUNT(CASE WHEN kind = ? THEN 1 END),
                   COALESCE(SUM(CASE WHEN (kind = 'plan') = ? THEN reserved_bytes END), 0)
            FROM call
            """,
            (kind, kind == "plan"),
        ).fetchone()
        size = RESPONSE_BYTES
        if kind == "plan":
            size = PLAN_RESPONSE_BYTES
            limit = self.budget.search_calls if self.budget is not None else MAX_SEARCH
            maximum = limit * size
        elif self.budget is not None:
            limit = self.budget.search_calls if kind == "search" else self.budget.origin_calls
            maximum = self.budget.total_bytes
        else:
            limit = MAX_SEARCH if kind == "search" else MAX_ORIGINS
            maximum = MAX_BYTES
        if count >= limit or reserved + size > maximum:
            raise KeenableError("generation_budget_exhausted")
        receipt = CallReceipt(
            slot=identity.slot,
            kind=kind,
            target=identity.target,
            request_sha256=identity.request_sha256,
            reserved_at=timestamp(),
            reserved_bytes=size,
            state="dispatched",
        )
        with self.db:
            self.db.execute(
                "INSERT INTO call VALUES (?, ?, ?, ?, ?, NULL)",
                (receipt.slot, kind, size, receipt.state, receipt.model_dump_json()),
            )
        return receipt

    def finish(
        self,
        receipt: CallReceipt,
        *,
        state: Literal["succeeded", "failed", "withheld"],
        reason: str | None = None,
        status: int | None = None,
        peer: str | None = None,
        response: bytes | None = None,
        payload: bytes | None = None,
    ) -> CallReceipt:
        outcome = receipt.model_copy(
            update={
                "state": state,
                "reason": reason,
                "observed_at": timestamp(),
                "status": status,
                "peer": peer,
                "response_sha256": digest(response) if response is not None else None,
                "response_bytes": len(response) if response is not None else None,
                "retained_sha256": digest(payload) if payload is not None else None,
            }
        )
        with self.db:
            update = self.db.execute(
                "UPDATE call SET state = ?, receipt = ?, payload = ? WHERE slot = ? AND state = 'dispatched'",
                (state, outcome.model_dump_json(), payload, receipt.slot),
            )
            if update.rowcount != 1:
                raise KeenableError("call_transition_conflict")
        return outcome

    def call(self, slot: str) -> tuple[CallReceipt, bytes | None] | None:
        row = self.db.execute("SELECT receipt, payload FROM call WHERE slot = ?", (slot,)).fetchone()
        if row is None:
            return None
        receipt = CallReceipt.model_validate_json(row[0])
        payload = row[1]
        if (digest(payload) if payload is not None else None) != receipt.retained_sha256:
            raise KeenableError("retained_payload_integrity")
        return receipt, payload


class SlotIdentity(Record):
    slot: str
    target: str
    request_sha256: Digest


def verify_lock(ledger: Ledger, lock: CaptureLock, declaration: Declaration) -> dict[str, bytes]:
    ledger.require_healthy()
    if ledger.artifact("lock") != lock.model_dump_json().encode():
        raise KeenableError("capture_lock_binding_mismatch")
    catalog = ledger.artifact("catalog")
    if catalog is None or digest(catalog) != lock.catalog_sha256:
        raise KeenableError("catalog_binding_mismatch")
    if declaration.declaration_sha256 != lock.declaration_sha256:
        raise KeenableError("capture_authorization_mismatch")
    declaration.check_current(datetime.now(UTC))
    if tuple((call.target, call.retained_sha256) for call in lock.rights_calls) != declaration.raw_licenses:
        raise KeenableError("rights_receipt_binding_mismatch")
    for call in lock.rights_calls:
        prior = ledger.call(call.slot)
        if prior is None or prior[0] != call or prior[1] is None or call.state != "succeeded":
            raise KeenableError("rights_receipt_binding_mismatch")
    selected = {entry.lead.url: entry for entry in CatalogReceipt.model_validate_json(catalog).selected}
    bodies: dict[str, bytes] = {}
    for page in lock.pages:
        case = page.case or PAGE_CASES.get(page.url)
        entry = selected.get(page.url)
        permission = declaration.authority(page.url, case) if case is not None else None
        if (
            permission is None
            or (
                page.rights_evidence_sha256,
                page.origin_id,
                page.media_type,
            )
            != (
                permission.rights_evidence_sha256,
                permission.origin_id,
                permission.media_type,
            )
            or (
                entry is None
                or entry.lead.case != case
                or entry.permission != permission
                or (isinstance(permission, PagePermission) and page.body_sha256 != permission.content_sha256)
                or page.call.target != page.url
                or page.call.kind != "origin"
                or page.call.state != "succeeded"
                or page.call.response_sha256 != page.body_sha256
                or page.call.response_bytes != page.size
            )
        ):
            raise KeenableError("capture_authorization_mismatch")
        bodies[page.url] = _verified_body(ledger, page)
    return bodies


def _verified_body(ledger: Ledger, page: VerifiedPage) -> bytes:
    stored = ledger.call(page.call.slot)
    if stored is None:
        raise KeenableError("origin_body_integrity")
    receipt, body = stored
    if receipt != page.call or body is None or len(body) != page.size or digest(body) != page.body_sha256:
        raise KeenableError("origin_body_integrity")
    return body
