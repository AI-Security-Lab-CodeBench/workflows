from dataclasses import dataclass, field

from pydantic import ValidationError

from .keenable_catalog import LEADS, search_leads
from .keenable_client import KeenableClient, StoredResponse
from .keenable_ledger import SlotIdentity
from .keenable_planner import SearchOption
from .keenable_policy import PROVIDER, QUERIES, Case, Declaration, KeenableError, digest
from .keenable_receipts import CallReceipt, Lead, SearchEnvelope
from .keenable_transport import search_payload, search_request


@dataclass(frozen=True, slots=True)
class DiscoverySearch:
    client: KeenableClient
    declaration: Declaration
    api_key: str = field(repr=False)

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
        acquired = self.client.acquire(
            "search", identity, lambda: search_request(case, api_key=self.api_key, payload=payload)
        )
        if isinstance(acquired, StoredResponse):
            assert acquired.payload is not None
            return acquired.receipt, LEADS.validate_json(acquired.payload)
        receipt, response = acquired.receipt, acquired.response
        try:
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
                error if isinstance(error, KeenableError) else KeenableError("provider_response_invalid", 200)
            )
            self.client.ledger.finish(
                receipt,
                state="failed",
                reason=failure.code,
                status=failure.status,
                peer=response.peer,
                response=response.body,
            )
            raise failure from None
        saved = self.client.ledger.finish(
            receipt,
            state="succeeded",
            status=200,
            peer=response.peer,
            response=response.body,
            payload=LEADS.dump_json(hits),
        )
        return saved, hits
