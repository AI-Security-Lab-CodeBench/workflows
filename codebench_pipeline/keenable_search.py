from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from random import uniform
from time import sleep
from typing import Final

from pydantic import ValidationError

from .keenable_catalog import LEADS, search_leads
from .keenable_ledger import Ledger, SlotIdentity
from .keenable_planner import SearchOption
from .keenable_policy import PROVIDER, QUERIES, Case, Declaration, KeenableError, digest
from .keenable_receipts import CallReceipt, Lead, SearchEnvelope
from .keenable_transport import (
    DirectResponse,
    HttpStatusError,
    Request,
    search_payload,
    search_request,
    transient_search_failure,
)

_MAX_ATTEMPTS: Final = 4
_MAX_WAIT: Final = 30.0


@dataclass(frozen=True, slots=True)
class DiscoverySearch:
    ledger: Ledger
    declaration: Declaration
    api_key: str = field(repr=False)
    send: Callable[[Request], DirectResponse]

    def __call__(
        self, case: Case, option: SearchOption | None = None
    ) -> tuple[CallReceipt, tuple[Lead, ...]]:
        slot = f"search:{case if option is None else option.id}"
        payload = (
            search_payload(
                QUERIES[case],
                tuple(
                    sorted(
                        source.url_prefix
                        for source in self.declaration.sources
                        if source.url_prefix is not None and source.allow_discovery and case in source.cases
                    )
                ),
            )
            if option is None
            else option.payload
        )
        identity = SlotIdentity(slot=slot, target=PROVIDER, request_sha256=digest(payload))
        while True:
            if (prior := self.ledger.call(slot)) is not None:
                receipt, hits = prior
                if (
                    receipt.kind != "search"
                    or receipt.slot != identity.slot
                    or receipt.target != identity.target
                    or receipt.request_sha256 != identity.request_sha256
                ):
                    raise KeenableError("search_receipt_integrity")
                match receipt.state:
                    case "succeeded" if hits is not None:
                        return receipt, LEADS.validate_json(hits)
                    case "failed" if hits is None and transient_search_failure(
                        receipt.reason, receipt.status
                    ):
                        _backoff(receipt, self.ledger.search_attempts(slot))
                    case "dispatched" | "succeeded" | "failed" | "withheld":
                        raise KeenableError("search_receipt_integrity")
            request = search_request(case, api_key=self.api_key, payload=payload)
            self.declaration.check_current(datetime.now(UTC))
            receipt = self.ledger.reserve("search", identity)
            response: DirectResponse | None = None
            try:
                response = self.send(request)
                hits = search_leads(
                    self.declaration,
                    case,
                    SearchEnvelope.model_validate_json(response.body),
                    reviewed=option.urls if option is not None else None,
                )
                if option is not None:
                    hits = tuple(hit.model_copy(update={"search_slot": slot}) for hit in hits)
            except (KeenableError, ValidationError, UnicodeError) as error:
                failure = (
                    error
                    if isinstance(error, KeenableError)
                    else KeenableError("provider_response_invalid", 200)
                )
                self.ledger.finish(
                    receipt,
                    state="failed",
                    reason=failure.code,
                    status=failure.status,
                    peer=failure.peer
                    if isinstance(failure, HttpStatusError)
                    else response.peer
                    if response
                    else None,
                    response=response.body if response else None,
                    retry_after=failure.retry_after if isinstance(failure, HttpStatusError) else None,
                )
                if not transient_search_failure(failure.code, failure.status):
                    raise failure from None
                continue
            saved = self.ledger.finish(
                receipt,
                state="succeeded",
                status=200,
                peer=response.peer,
                response=response.body,
                payload=LEADS.dump_json(hits),
            )
            return saved, hits


def _backoff(receipt: CallReceipt, attempts: int) -> None:
    if not 0 < attempts < _MAX_ATTEMPTS:
        raise KeenableError(receipt.reason or "generation_failed", receipt.status)
    remaining = 0.0
    if receipt.retry_after is not None:
        if receipt.observed_at is None:
            raise KeenableError("search_receipt_integrity")
        elapsed = (datetime.now(UTC) - datetime.fromisoformat(receipt.observed_at)).total_seconds()
        remaining = max(0.0, receipt.retry_after - elapsed)
        if remaining > _MAX_WAIT:
            raise KeenableError(receipt.reason or "generation_failed", receipt.status)
    ceiling = 2.0 ** (attempts - 1)
    sleep(min(_MAX_WAIT, remaining + uniform(ceiling / 2, ceiling)))
