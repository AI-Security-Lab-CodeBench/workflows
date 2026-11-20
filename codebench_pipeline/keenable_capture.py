from dataclasses import dataclass
from datetime import UTC, datetime

from scripts.dataset_schema import SECRET_PATTERNS

from .keenable_client import KeenableClient, StoredResponse
from .keenable_ledger import SlotIdentity
from .keenable_observed import LicensedContent, licensed_main
from .keenable_policy import (
    COMMANDS,
    PAGE_CASES,
    KeenableError,
    PagePermission,
    Permission,
    canonical_url,
    digest,
)
from .keenable_receipts import CallReceipt, ObservedPageLicense
from .keenable_transport import Request


@dataclass(frozen=True, slots=True)
class CaptureTarget:
    url: str
    permission: Permission | None = None
    rights_sha256: str | None = None
    inspect: bool = False


def capture_bytes(
    client: KeenableClient, target: CaptureTarget, api_key: str
) -> tuple[CallReceipt, bytes | None, ObservedPageLicense | None]:
    url, permission = target.url, target.permission
    if canonical_url(url) != url:
        raise KeenableError("origin_not_allowlisted")
    pinned = permission.content_sha256 if isinstance(permission, PagePermission) else target.rights_sha256
    kind = "rights" if target.rights_sha256 is not None else "origin"
    media_type = (
        permission.media_type if permission is not None else "text/html" if target.inspect else "text/plain"
    )
    identity = SlotIdentity(
        slot=f"{kind}:{digest(url.encode())}",
        target=url,
        request_sha256=digest(f"GET\n{url}\n{media_type}".encode()),
    )
    acquired = client.acquire("origin", identity, lambda: Request(url, media_type), inspect=target.inspect)
    if isinstance(acquired, StoredResponse):
        receipt, body = acquired.receipt, acquired.payload
        if body is not None and target.inspect:
            retained, evidence = LicensedContent.replay(receipt, body)
            return receipt, retained, evidence
        if body is not None and (
            receipt.response_sha256 != digest(body)
            or receipt.response_bytes != len(body)
            or (pinned is not None and digest(body) != pinned)
        ):
            raise KeenableError("origin_body_integrity")
        return receipt, body, None
    receipt, response = acquired.receipt, acquired.response
    ledger = client.ledger
    try:
        if pinned is not None and digest(response.body) != pinned:
            raise KeenableError("origin_pin_mismatch", 200)
        text = response.body.decode("utf-8")
        client.declaration.check_current(datetime.now(UTC))
        sensitive = any(pattern.search(text) for pattern in SECRET_PATTERNS) or bool(
            api_key and api_key in text
        )
        notice = (
            permission.page_notice
            if permission is not None and not isinstance(permission, PagePermission)
            else None
        )
        if notice is not None and notice.casefold() not in text.casefold():
            return (
                ledger.finish(
                    receipt,
                    state="withheld",
                    reason="license_notice_missing",
                    status=200,
                    peer=response.peer,
                    response=response.body,
                ),
                None,
                None,
            )
        if sensitive or (
            isinstance(permission, PagePermission)
            and any(
                response.body[span.start : span.end].decode("utf-8", errors="replace")
                not in COMMANDS[PAGE_CASES[url]]
                or span.end > len(response.body)
                for span in permission.spans
            )
        ):
            return (
                ledger.finish(
                    receipt,
                    state="withheld",
                    reason="content_policy",
                    status=200,
                    peer=response.peer,
                    response=response.body,
                ),
                None,
                None,
            )
        if target.inspect:
            licensed = licensed_main(text, url)
            if licensed is None:
                return (
                    ledger.finish(
                        receipt,
                        state="withheld",
                        reason="unverified_page_license",
                        status=200,
                        peer=response.peer,
                        response=response.body,
                    ),
                    None,
                    None,
                )
            return (
                ledger.finish(
                    receipt,
                    state="succeeded",
                    status=200,
                    peer=response.peer,
                    response=response.body,
                    payload=licensed.model_dump_json().encode(),
                ),
                licensed.content.encode(),
                licensed.license,
            )
    except (KeenableError, UnicodeDecodeError) as error:
        failure = error if isinstance(error, KeenableError) else KeenableError("origin_encoding_invalid", 200)
        saved = ledger.finish(
            receipt,
            state="withheld" if target.inspect and isinstance(error, UnicodeDecodeError) else "failed",
            reason=failure.code,
            status=failure.status if failure.status is not None else 200,
            peer=response.peer,
            response=response.body,
        )
        if saved.state == "withheld":
            return saved, None, None
        raise failure from None
    saved = ledger.finish(
        receipt,
        state="succeeded",
        status=200,
        peer=response.peer,
        response=response.body,
        payload=response.body,
    )
    return saved, response.body, None
