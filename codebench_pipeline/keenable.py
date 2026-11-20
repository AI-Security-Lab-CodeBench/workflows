import os
from datetime import UTC, datetime
from pathlib import Path
from re import DOTALL, compile

from scripts.dataset_schema import SECRET_PATTERNS

from .keenable_catalog import declared_leads, freeze_catalog
from .keenable_ledger import Ledger, SlotIdentity, open_ledger, verify_lock
from .keenable_planner import GuardedSearchPlanner, SearchSession
from .keenable_policy import (
    COMMANDS,
    PAGE_CASES,
    Case,
    Declaration,
    KeenableDeclaration,
    KeenableError,
    KeenableSourceAccess,
    PagePermission,
    Permission,
    ReviewedSpan,
    canonical_url,
    digest,
    load_declaration,
    record_digest,
)
from .keenable_receipts import (
    AdapterReceipt,
    CallReceipt,
    CandidateReceipt,
    CaptureLock,
    CatalogReceipt,
    VerifiedPage,
)
from .keenable_search import DiscoverySearch
from .keenable_transport import DirectResponse, Request, request_direct

__all__ = ["KeenableCollection", "KeenableDeclaration", "KeenableError", "load_declaration"]


class KeenableCollection:
    """Bounded CAT-3 collection with durable cross-run receipts and inert source bytes."""

    def __init__(
        self,
        root: Path,
        declaration: Declaration | KeenableSourceAccess,
        *,
        planner: GuardedSearchPlanner | None = None,
    ) -> None:
        self.root = root
        self.declaration = declaration
        self.planner = planner

    def discover(self) -> CatalogReceipt:
        return self.discover_with_credential(os.getenv("KEENABLE_API_KEY", ""))

    def discover_with_credential(self, api_key: str) -> CatalogReceipt:
        """Search reviewed administrative cases and freeze the authorized selection."""
        declaration = self.declaration
        if isinstance(declaration, KeenableSourceAccess):
            raise KeenableError("discovery_requires_search_declaration")
        with open_ledger(self.root, declaration) as ledger:
            ledger.require_healthy()
            if (prior := ledger.artifact("catalog")) is not None:
                catalog = CatalogReceipt.model_validate_json(prior)
                if (
                    catalog.plan is not None
                    and catalog.plan.mode == "adaptive"
                    and self.planner is not None
                    and catalog.plan.planner_sha256 != self.planner.identity
                ):
                    raise KeenableError("planner_binding_mismatch")
                return catalog
            leads = declared_leads(declaration)
            searches: list[CallReceipt] = []
            search = DiscoverySearch(ledger, declaration, api_key, request_direct)
            plan = None
            if declaration.search_plan is not None:
                hits, receipts, plan = SearchSession(ledger, declaration, self.planner).run(
                    lambda option: search(option.case, option)
                )
                leads.extend(hits)
                searches.extend(receipts)
            else:
                for case in declaration.search_cases:
                    receipt, hits = search(case)
                    leads.extend(hits)
                    searches.append(receipt)
            declaration.check_current(datetime.now(UTC))
            catalog = freeze_catalog(declaration, leads, searches, plan)
            ledger.publish("catalog", catalog.model_dump_json().encode())
            return catalog

    def capture(self, catalog: CatalogReceipt) -> CaptureLock:
        return self.capture_with_credential(catalog, os.getenv("KEENABLE_API_KEY", ""))

    def capture_with_credential(self, catalog: CatalogReceipt, api_key: str) -> CaptureLock:
        """Capture permitted pinned pages and publish one all-or-nothing lock."""
        with open_ledger(self.root, self.declaration) as ledger:
            ledger.require_healthy()
            if ledger.artifact("catalog") != catalog.model_dump_json().encode():
                raise KeenableError("catalog_binding_mismatch")
            if (prior := ledger.artifact("lock")) is not None:
                lock = CaptureLock.model_validate_json(prior)
                verify_lock(ledger, lock, self.declaration)
                return lock
            pages: list[VerifiedPage] = []
            withheld: list[str] = []
            calls: list[CallReceipt] = []
            rights_calls: list[CallReceipt] = []
            for url, expected in self.declaration.raw_licenses:
                receipt, body = _capture(ledger, url, None, api_key=api_key, rights_sha256=expected)
                if body is None:
                    raise KeenableError("rights_bytes_unavailable")
                rights_calls.append(receipt)
            for selection in catalog.selected:
                permission = selection.permission
                if permission is None:
                    if selection.lead.url is not None:
                        withheld.append(selection.lead.url)
                    continue
                self.declaration.check_current(datetime.now(UTC))
                url = selection.lead.url
                case = selection.lead.case
                if url is None or self.declaration.authority(url, case) != permission:
                    raise KeenableError("origin_not_authorized")
                receipt, body = _capture(ledger, url, permission, api_key=api_key)
                calls.append(receipt)
                if body is None:
                    withheld.append(url)
                    continue
                pages.append(
                    VerifiedPage(
                        url=url,
                        origin_id=permission.origin_id,
                        body_sha256=digest(body),
                        size=len(body),
                        media_type=permission.media_type,
                        rights_evidence_sha256=permission.rights_evidence_sha256,
                        call=receipt,
                        case=None if isinstance(permission, PagePermission) else case,
                    )
                )
            self.declaration.check_current(datetime.now(UTC))
            lock = CaptureLock(
                declaration_sha256=self.declaration.declaration_sha256,
                catalog_sha256=record_digest(catalog),
                pages=tuple(pages),
                withheld=tuple(withheld),
                origin_calls=tuple(calls),
                rights_calls=tuple(rights_calls),
            )
            ledger.publish("lock", lock.model_dump_json().encode())
            return lock

    def adapters(self, lock: CaptureLock) -> tuple[AdapterReceipt, ...]:
        """Bind the reviewed span reader to each verified capture."""
        with open_ledger(self.root, self.declaration) as ledger:
            verify_lock(ledger, lock, self.declaration)
            return self._adapters(lock)

    def read_body(self, lock: CaptureLock, url: str) -> bytes:
        """Resolve a captured page's exact bytes for an authorized receipt."""
        bodies = self.read_bodies(lock)
        if url not in bodies:
            raise KeenableError("origin_not_captured")
        return bodies[url]

    def read_bodies(self, lock: CaptureLock) -> dict[str, bytes]:
        """Read shared verified bytes without a provider or origin request."""
        with open_ledger(self.root, self.declaration) as ledger:
            return verify_lock(ledger, lock, self.declaration)

    def candidates(
        self, lock: CaptureLock, adapters: tuple[AdapterReceipt, ...]
    ) -> tuple[CandidateReceipt, ...]:
        """Emit literal reviewed command spans, without interpreting markup or executing content."""
        with open_ledger(self.root, self.declaration) as ledger:
            bodies = verify_lock(ledger, lock, self.declaration)
            if adapters != self._adapters(lock):
                raise KeenableError("adapter_binding_mismatch")
            candidates: list[CandidateReceipt] = []
            for page in lock.pages:
                case = page.case or PAGE_CASES[page.url]
                permission = self.declaration.authority(page.url, case)
                if permission is None:
                    raise KeenableError("capture_authorization_mismatch")
                body = bodies[page.url]
                spans = (
                    permission.spans
                    if isinstance(permission, PagePermission)
                    else _source_spans(body, case)
                    if permission.permitted_use == "fetch_retain_extract_quote"
                    else ()
                )
                for span in spans:
                    literal = body[span.start : span.end]
                    candidates.append(
                        CandidateReceipt(
                            candidate_id=digest(
                                f"{page.url}\n{page.body_sha256}\n{span.start}:{span.end}".encode()
                            ),
                            origin_id=permission.origin_id,
                            url=page.url,
                            body_sha256=page.body_sha256,
                            start=span.start,
                            end=span.end,
                            span_sha256=digest(literal),
                            command=literal.decode("utf-8"),
                            case=case,
                            attribution=permission.attribution,
                            rights_evidence_sha256=permission.rights_evidence_sha256,
                        )
                    )
            return tuple(candidates)

    def _adapters(self, lock: CaptureLock) -> tuple[AdapterReceipt, ...]:
        adapters: list[AdapterReceipt] = []
        for page in lock.pages:
            permission = self.declaration.authority(page.url, page.case or PAGE_CASES[page.url])
            if permission is None:
                raise KeenableError("capture_authorization_mismatch")
            adapters.append(
                AdapterReceipt(
                    url=page.url,
                    body_sha256=page.body_sha256,
                    permission_sha256=record_digest(permission),
                    parser="literal-code-blocks-v2" if page.case else "reviewed-byte-spans-v1",
                )
            )
        return tuple(adapters)


_CODE_BLOCK = compile(
    rb"<pre(?:\s[^>]*)?>(?P<html>.*?)</pre\s*>|"
    rb"(?m:^[ \t]*```(?:bash|shell|sh|console)?[ \t]*\r?\n)(?P<markdown>.*?)(?m:^[ \t]*```[ \t]*\r?$)",
    flags=DOTALL,
)


def _source_spans(body: bytes, case: Case) -> tuple[ReviewedSpan, ...]:
    allowed = {command.encode() for command in COMMANDS[case]}
    spans: list[ReviewedSpan] = []
    for block in _CODE_BLOCK.finditer(body):
        group = "html" if block["html"] is not None else "markdown"
        payload = block[group]
        start = block.start(group)
        if group == "html" and payload.startswith(b"<code>") and payload.endswith(b"</code>"):
            payload = payload[6:-7]
            start += 6
        for line in payload.splitlines(keepends=True):
            literal = line.strip().removeprefix(b"$ ")
            offset = start + line.find(literal)
            if literal in allowed:
                spans.append(ReviewedSpan(start=offset, end=offset + len(literal)))
                if len(spans) == 20:
                    return tuple(spans)
            start += len(line)
    return tuple(spans)


def _capture(
    ledger: Ledger,
    url: str,
    permission: Permission | None,
    *,
    api_key: str,
    rights_sha256: str | None = None,
) -> tuple[CallReceipt, bytes | None]:
    if canonical_url(url) != url:
        raise KeenableError("origin_not_allowlisted")
    pinned = permission.content_sha256 if isinstance(permission, PagePermission) else rights_sha256
    kind = "rights" if permission is None else "origin"
    media_type = permission.media_type if permission is not None else "text/plain"
    slot = f"{kind}:{digest(url.encode())}"
    if (prior := ledger.call(slot)) is not None:
        receipt, body = prior
        if receipt.state == "succeeded" and (
            body is None or receipt.response_sha256 != digest(body) or (pinned and digest(body) != pinned)
        ):
            raise KeenableError("origin_body_integrity")
        return receipt, body
    receipt = ledger.reserve(
        "origin",
        SlotIdentity(slot=slot, target=url, request_sha256=digest(f"GET\n{url}\n{media_type}".encode())),
    )
    response: DirectResponse | None = None
    try:
        response = request_direct(Request(url, media_type))
        if pinned is not None and digest(response.body) != pinned:
            raise KeenableError("origin_pin_mismatch", 200)
        text = response.body.decode("utf-8")
        if permission is not None and permission.valid_until <= datetime.now(UTC):
            raise KeenableError("authorization_expired", 200)
        sensitive = any(pattern.search(text) for pattern in SECRET_PATTERNS) or bool(
            api_key and api_key in text
        )
        notice = (
            permission.page_notice
            if permission is not None and not isinstance(permission, PagePermission)
            else None
        )
        if notice is not None and notice.casefold() not in text.casefold():
            return ledger.finish(
                receipt,
                state="withheld",
                reason="license_notice_missing",
                status=200,
                peer=response.peer,
                response=response.body,
            ), None
        if sensitive or (
            isinstance(permission, PagePermission)
            and any(
                response.body[span.start : span.end].decode("utf-8", errors="replace")
                not in COMMANDS[PAGE_CASES[url]]
                or span.end > len(response.body)
                for span in permission.spans
            )
        ):
            return ledger.finish(
                receipt,
                state="withheld",
                reason="content_policy",
                status=200,
                peer=response.peer,
                response=response.body,
            ), None
    except (KeenableError, UnicodeDecodeError) as error:
        failure = error if isinstance(error, KeenableError) else KeenableError("origin_encoding_invalid", 200)
        ledger.finish(
            receipt,
            state="failed",
            reason=failure.code,
            status=failure.status,
            peer=response.peer if response else None,
            response=response.body if response else None,
        )
        raise failure from None
    saved = ledger.finish(
        receipt,
        state="succeeded",
        status=200,
        peer=response.peer,
        response=response.body,
        payload=response.body,
    )
    return saved, response.body
