from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from random import uniform
from time import sleep
from typing import Final, Literal

from .keenable_ledger import Ledger, SlotIdentity
from .keenable_policy import Declaration, KeenableError, KeenableSourceAccess
from .keenable_receipts import CallReceipt
from .keenable_transport import DirectResponse, HttpStatusError, Request, transient_request_failure

_MAX_ATTEMPTS: Final = 4
_MAX_WAIT: Final = 30.0


@dataclass(frozen=True, slots=True)
class AcquiredResponse:
    receipt: CallReceipt
    response: DirectResponse


@dataclass(frozen=True, slots=True)
class StoredResponse:
    receipt: CallReceipt
    payload: bytes | None


@dataclass(frozen=True, slots=True)
class KeenableClient:
    ledger: Ledger
    declaration: Declaration | KeenableSourceAccess
    send: Callable[[Request], DirectResponse]

    def acquire(
        self,
        kind: Literal["search", "origin"],
        identity: SlotIdentity,
        make_request: Callable[[], Request],
    ) -> AcquiredResponse | StoredResponse:
        """Replay a completed slot or reserve and send one bounded sequence of attempts."""
        while True:
            self.declaration.check_current(datetime.now(UTC))
            if (prior := self.ledger.call(identity.slot)) is not None:
                receipt, payload = prior
                if (receipt.kind, receipt.slot, receipt.target, receipt.request_sha256) != (
                    kind,
                    identity.slot,
                    identity.target,
                    identity.request_sha256,
                ):
                    raise KeenableError(f"{kind}_receipt_integrity")
                match receipt.state:
                    case "succeeded" if payload is not None:
                        return StoredResponse(receipt, payload)
                    case "withheld" if kind == "origin" and payload is None:
                        return StoredResponse(receipt, None)
                    case "failed" if payload is None and transient_request_failure(
                        receipt.reason, receipt.status
                    ):
                        _backoff(receipt, self.ledger.attempts(identity.slot))
                    case "dispatched" | "succeeded" | "failed" | "withheld":
                        raise KeenableError(f"{kind}_receipt_integrity")
            request = make_request()
            self.declaration.check_current(datetime.now(UTC))
            receipt = self.ledger.reserve(kind, identity)
            try:
                return AcquiredResponse(receipt, self.send(request))
            except KeenableError as error:
                self.ledger.finish(
                    receipt,
                    state="failed",
                    reason=error.code,
                    status=error.status,
                    peer=error.peer if isinstance(error, HttpStatusError) else None,
                    retry_after=error.retry_after if isinstance(error, HttpStatusError) else None,
                )
                if not transient_request_failure(error.code, error.status):
                    raise


def _backoff(receipt: CallReceipt, attempts: int) -> None:
    if not 0 < attempts < _MAX_ATTEMPTS:
        raise KeenableError(receipt.reason or "generation_failed", receipt.status)
    remaining = 0.0
    if receipt.retry_after is not None:
        if receipt.observed_at is None:
            raise KeenableError(f"{receipt.kind}_receipt_integrity")
        elapsed = (datetime.now(UTC) - datetime.fromisoformat(receipt.observed_at)).total_seconds()
        remaining = max(0.0, receipt.retry_after - elapsed)
        if remaining > _MAX_WAIT:
            raise KeenableError(receipt.reason or "generation_failed", receipt.status)
    ceiling = 2.0 ** (attempts - 1)
    sleep(min(_MAX_WAIT, remaining + uniform(ceiling / 2, ceiling)))
