from __future__ import annotations

from collections import deque
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from logging import getLogger
from re import fullmatch
from threading import Lock
from time import monotonic
from typing import Final, Literal

import anyio
import httpx
from anyio import sleep
from mcp.shared.exceptions import McpError
from pydantic import ValidationError

from .keenable_admission import ORG_PARALLEL, ORG_RATE, SLOT_WAIT, KeenableAdmissionLedger, KeenableSlot
from .keenable_policy import KeenableError, Record
from .keenable_transport import retry_after_seconds

type KeenableTool = Literal["search", "fetch"]
type KeenableProviderCode = Literal[
    "provider_http_status",
    "provider_timeout",
    "provider_transport_failed",
    "provider_search_failed",
    "provider_fetch_failed",
    "provider_response_invalid",
    "provider_response_budget",
    "sensitive_provider_response",
    "provider_admission_unavailable",
]

_MAX_ATTEMPTS: Final = 4
_MAX_RETRY_WAIT: Final = 30
_WAVE_LIMIT: Final = 5
_MAX_NUMBERED_KEYS: Final = 64
_LOG: Final = getLogger(__name__)


class KeenableProviderError(Record):
    code: KeenableProviderCode
    status: int | None = None
    retry_after_seconds: int | None = None


class KeenableTransportError(KeenableError):
    def __init__(self, detail: KeenableProviderError) -> None:
        super().__init__(detail.code, detail.status)
        self.detail = detail


class KeenableIncident(Record):
    tool: KeenableTool
    attempt: int
    observed_at: str
    error: KeenableProviderError


@dataclass(frozen=True, slots=True, repr=False)
class _Organization:
    identity: str
    keys: tuple[str, ...]


@dataclass(frozen=True, slots=True, repr=False)
class KeenableCredentials:
    organizations: tuple[_Organization, ...]

    @classmethod
    def bind(cls, primary: str | None, lookup: Callable[[str], str | None]) -> KeenableCredentials:
        """Bind numbered keys (1–64); matching KEENABLE_ORG suffix values share one pool."""
        keys: dict[str, str | None] = {}
        for index in range(_MAX_NUMBERED_KEYS + 1):
            suffix = f"_{index}" if index else ""
            key = lookup(f"KEENABLE_API_KEY{suffix}") if index else primary
            if not key:
                continue
            if not fullmatch(r"[!-~]{1,512}", key):
                raise KeenableError("provider_credential_unavailable")
            org = lookup(f"KEENABLE_ORG{suffix}") or None
            previous = keys.get(key)
            if previous and org and previous != org:
                raise KeenableError("provider_organization_conflict")
            keys[key] = previous or org

        groups: dict[str, list[str]] = {}
        for key, org in keys.items():
            identity = sha256(f"org:{org}".encode() if org else f"key:{key}".encode()).hexdigest()
            groups.setdefault(identity, []).append(key)
        return cls(tuple(_Organization(identity, tuple(values)) for identity, values in groups.items()))

    def contains(self, text: str) -> bool:
        return any(key in text for org in self.organizations for key in org.keys)


@dataclass(slots=True)
class _OrgCapacity:
    completions: deque[float] = field(default_factory=deque)
    active: int = 0
    pending: int = 0
    resume_at: float = 0.0
    last_selected: int = 0
    selections: int = 0

    def delay(self, now: float) -> float:
        while self.completions and now - self.completions[0] >= 1:
            self.completions.popleft()
        rate_delay = (
            self.completions[0] + 1 - now if len(self.completions) + self.pending >= ORG_RATE else 0.0
        )
        return max(0.0, rate_delay, self.resume_at - now)


@dataclass(slots=True)
class _Permit:
    org: _Organization
    key: str = field(repr=False)
    slot: KeenableSlot | None = None
    dispatched: bool = False


class KeenableAdmission:
    """Bound the process-wide wave and delegate organization limits to the bound ledger."""

    def __init__(self) -> None:
        self.lock = Lock()
        self.organizations: dict[str, _OrgCapacity] = {}
        self.active = 0
        self.selections = 0

    async def acquire(
        self, organizations: tuple[_Organization, ...], ledger: KeenableAdmissionLedger | None
    ) -> _Permit:
        while True:
            delay = SLOT_WAIT
            with self.lock:
                now = monotonic()
                if self.active < _WAVE_LIMIT:
                    waits: list[float] = []
                    choices = [
                        (org, self.organizations.setdefault(org.identity, _OrgCapacity()))
                        for org in organizations
                    ]
                    for org, capacity in sorted(choices, key=lambda pair: pair[1].last_selected):
                        if capacity.active >= ORG_PARALLEL:
                            waits.append(SLOT_WAIT)
                            continue
                        slot = None
                        if ledger is None:
                            wait = capacity.delay(now)
                            if wait > 0:
                                waits.append(wait)
                                continue
                        else:
                            slot, wait = ledger.claim_slot(org.identity)
                            if slot is None:
                                waits.append(wait)
                                continue
                        key = org.keys[capacity.selections % len(org.keys)]
                        self.selections += 1
                        self.active += 1
                        capacity.last_selected = self.selections
                        capacity.selections += 1
                        capacity.active += 1
                        return _Permit(org, key, slot)
                    delay = min(waits)
            await sleep(delay)

    async def dispatch(self, permit: _Permit, ledger: KeenableAdmissionLedger | None) -> None:
        while True:
            if permit.dispatched:
                raise KeenableError("provider_admission_unavailable")
            if ledger is None:
                with self.lock:
                    capacity = self.organizations[permit.org.identity]
                    now = monotonic()
                    delay = capacity.delay(now)
                    if delay == 0:
                        capacity.pending += 1
            else:
                assert permit.slot is not None
                delay = ledger.dispatch_delay(permit.org.identity, permit.slot)
            if delay == 0:
                permit.dispatched = True
                return
            await sleep(delay)

    def release(self, permit: _Permit, ledger: KeenableAdmissionLedger | None, retry_after: float) -> None:
        try:
            if ledger is not None and permit.slot is not None:
                ledger.release(
                    permit.org.identity, permit.slot, dispatched=permit.dispatched, retry_after=retry_after
                )
        finally:
            with self.lock:
                capacity = self.organizations[permit.org.identity]
                capacity.active -= 1
                self.active -= 1
                if ledger is None:
                    now = monotonic()
                    if permit.dispatched:
                        capacity.pending -= 1
                        capacity.completions.append(now)
                    if retry_after:
                        capacity.resume_at = max(capacity.resume_at, now + retry_after)


_ADMISSION: Final = KeenableAdmission()
_BEFORE_DISPATCH: Final[ContextVar[Callable[[], Awaitable[None]] | None]] = ContextVar(
    "keenable_before_dispatch", default=None
)


async def admit_tool_dispatch() -> None:
    """Charge one search or fetch attempt just before sending its MCP tool request."""
    if (admit := _BEFORE_DISPATCH.get()) is not None:
        await admit()


@dataclass(frozen=True, slots=True)
class _Failure:
    detail: KeenableProviderError
    retryable: bool = False
    http_errors: tuple[KeenableProviderError, ...] = ()


def _failure(error: Exception) -> _Failure:
    match error:
        case KeenableTransportError(detail=detail):
            return _Failure(detail)
        case httpx.HTTPStatusError(response=response):
            status = response.status_code
            detail = KeenableProviderError(
                code="provider_http_status",
                status=status,
                retry_after_seconds=retry_after_seconds(response.headers.get("Retry-After", "")),
            )
            return _Failure(detail, status in (408, 429) or 500 <= status < 600, (detail,))
        case TimeoutError() | httpx.TimeoutException():
            return _Failure(KeenableProviderError(code="provider_timeout"), retryable=True)
        case httpx.ConnectError() | httpx.ReadError() | httpx.WriteError() | httpx.RemoteProtocolError():
            return _Failure(KeenableProviderError(code="provider_transport_failed"), retryable=True)
        case ValidationError():
            return _Failure(KeenableProviderError(code="provider_response_invalid"))
        case KeenableError(
            code=(
                "provider_search_failed"
                | "provider_fetch_failed"
                | "provider_response_invalid"
                | "provider_response_budget"
                | "provider_admission_unavailable"
            ) as code
        ):
            return _Failure(KeenableProviderError(code=code))
        case ExceptionGroup(exceptions=children):
            first, *rest = (_failure(child) for child in children)
            http_errors = first.http_errors + tuple(detail for child in rest for detail in child.http_errors)
            if all(first.detail == child.detail for child in rest):
                retryable = first.retryable and all(child.retryable for child in rest)
                return _Failure(first.detail, retryable, http_errors)
            return _Failure(KeenableProviderError(code="provider_transport_failed"), http_errors=http_errors)
        case _:
            return _Failure(KeenableProviderError(code="provider_transport_failed"))


@dataclass(frozen=True, slots=True)
class KeenableTransport:
    credentials: KeenableCredentials = field(repr=False)
    ledger: KeenableAdmissionLedger | None = field(default=None, repr=False)

    async def call[T](
        self,
        tool: KeenableTool,
        operation: Callable[[str], Awaitable[T]],
        *,
        audit: Callable[[KeenableIncident], None],
        on_dispatch: Callable[[], None] | None = None,
    ) -> T:
        """Retry in the same organization, and expose only the terminal redacted error."""
        organizations = self.credentials.organizations
        if not organizations:
            raise KeenableError("provider_credential_unavailable")
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                permit = await _ADMISSION.acquire(organizations, self.ledger)
            except KeenableError:
                raise KeenableTransportError(
                    KeenableProviderError(code="provider_admission_unavailable")
                ) from None
            organizations = (permit.org,)
            cooldown = 0.0

            async def before_dispatch() -> None:
                await _ADMISSION.dispatch(permit, self.ledger)
                if on_dispatch is not None:
                    on_dispatch()

            dispatch_token = _BEFORE_DISPATCH.set(before_dispatch)
            try:
                return await operation(permit.key)
            except (
                KeenableError,
                McpError,
                httpx.HTTPError,
                TimeoutError,
                ValidationError,
                ExceptionGroup,
            ) as exc:
                failure = _failure(exc)
                wait = failure.detail.retry_after_seconds
                delay = float(wait) if wait is not None else 0.25 * 2 ** (attempt - 1)
                for error in failure.http_errors:
                    status = error.status
                    if status is not None and 500 <= status < 600:
                        _LOG.warning(
                            "keenable.server_error status=%d attempt=%d",
                            status,
                            attempt,
                            extra={
                                "event": "keenable.server_error",
                                "http_status": status,
                                "attempt": attempt,
                            },
                        )
                        audit(
                            KeenableIncident(
                                tool=tool,
                                attempt=attempt,
                                observed_at=datetime.now(UTC).isoformat(),
                                error=error,
                            )
                        )
                    if status == 429:
                        cooldown = max(
                            cooldown,
                            error.retry_after_seconds if error.retry_after_seconds is not None else delay,
                        )
            finally:
                _BEFORE_DISPATCH.reset(dispatch_token)
                try:
                    _ADMISSION.release(permit, self.ledger, cooldown)
                except KeenableError:
                    raise KeenableTransportError(
                        KeenableProviderError(code="provider_admission_unavailable")
                    ) from None
            if (
                not failure.retryable
                or attempt == _MAX_ATTEMPTS
                or delay > _MAX_RETRY_WAIT
                or anyio.current_time() + delay >= anyio.current_effective_deadline()
            ):
                raise KeenableTransportError(failure.detail) from None
            await sleep(delay)
        raise AssertionError("Keenable retry loop reached an impossible state")
