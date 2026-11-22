from collections import deque
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from threading import BoundedSemaphore, Lock
from time import monotonic_ns
from typing import Final, Literal
from urllib.parse import urlsplit

import anyio
import httpx
from mcp.shared.exceptions import McpError
from mcp.types import TextContent
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from codebench_pipeline.grounding_tools import (
    DocumentationQuery,
    GroundingCredentials,
    GroundingFiles,
    GroundingOrigins,
    LiveGroundingConfig,
)
from codebench_pipeline.keenable_mcp import search_session, search_snippets
from codebench_pipeline.keenable_policy import KeenableError, canonical_url
from codebench_pipeline.storage import fingerprint
from scripts.dataset_schema import SECRET_PATTERNS


class ToolInput(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    candidate_ids: list[str] = Field(min_length=1, max_length=50)


class QueryInput(ToolInput):
    query: str = Field(min_length=1, max_length=500)


class PageFetchInput(ToolInput):
    url: str = Field(min_length=1, max_length=2048)


class _Page(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, hide_input_in_errors=True)

    url: str
    content: str = Field(min_length=1)


class LivePage(_Page):
    request_index: int = Field(ge=0)
    request_url: str
    candidate_ids: tuple[str, ...] = Field(min_length=1, max_length=50)
    fetched_at: str
    response_sha256: str = Field(pattern="^[0-9a-f]{64}$")

    @property
    def identity(self) -> str:
        return fingerprint(self.model_dump(mode="json"))


class LiveHit(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str
    snippet: str


class LiveReceipt(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    index: int
    tool: Literal["query", "page_fetch"]
    request_sha256: str
    request: QueryInput | PageFetchInput | None = None
    discovery: tuple[LiveHit, ...] = ()
    page_id: str | None = None
    response_sha256: str | None = None
    error: str | None = None
    provider_started_monotonic_ns: int | None = None
    provider_finished_monotonic_ns: int | None = None


type _Tool = Literal["query", "page_fetch"]
type _Request = QueryInput | PageFetchInput
type _Response = tuple[LiveHit, ...] | tuple[_Page, str, int]
type _GroundingError = (
    KeenableError | ValidationError | TimeoutError | McpError | httpx.HTTPError | ExceptionGroup
)
_GROUNDING_ERRORS = (KeenableError, ValidationError, TimeoutError, McpError, httpx.HTTPError, ExceptionGroup)
_PROVIDER_SLOTS: Final = BoundedSemaphore(5)
_STARTS_LOCK: Final = Lock()
_STARTS: Final[deque[int]] = deque()
_SECOND_NS: Final = 1_000_000_000


@dataclass(slots=True)
class _Operation:
    request: _Request
    started: int | None = None
    finished: int | None = None
    response: _Response | _GroundingError | None = None

    @asynccontextmanager
    async def provider_call(self) -> AsyncGenerator[None]:
        while True:
            with _STARTS_LOCK:
                now = monotonic_ns()
                while _STARTS and now - _STARTS[0] >= _SECOND_NS:
                    _STARTS.popleft()
                if len(_STARTS) < 10:
                    _STARTS.append(now)
                    self.started = now
                    break
                delay = (_STARTS[0] + _SECOND_NS - now) / _SECOND_NS
            await anyio.sleep(delay)
        try:
            yield
        finally:
            self.finished = monotonic_ns()


def _failed(receipt: LiveReceipt, error: _GroundingError) -> LiveReceipt:
    match error:
        case KeenableError(code=code):
            return receipt.model_copy(update={"error": code})
        case ValidationError():
            return receipt.model_copy(update={"error": "invalid_grounding_arguments_or_response"})
        case TimeoutError() | McpError() | httpx.HTTPError() | ExceptionGroup():
            return receipt.model_copy(update={"error": "provider_transport_failed"})


def trusted_live_url(config: GroundingOrigins, url: str) -> bool:
    try:
        if canonical_url(url) != url:
            return False
    except ValueError:
        return False
    base = url.split("?", 1)[0].rstrip("/")
    return urlsplit(url).hostname in {domain.lower() for domain in config.allowed_domains} or any(
        base == prefix.rstrip("/") or base.startswith(prefix.rstrip("/") + "/")
        for prefix in config.trusted_url_prefixes
    )


async def _retrieve(key: str, operation: _Operation, maximum: int) -> _Response:
    async with search_session(key) as session:
        match operation.request:
            case QueryInput(query=query):
                async with operation.provider_call():
                    results = await search_snippets(session, query, (8, 500))
                return tuple(LiveHit(url=hit.url, snippet=hit.snippet) for hit in results.results)
            case PageFetchInput(url=url):
                async with operation.provider_call():
                    reply = await session.call_tool(
                        "fetch_page_content",
                        arguments={
                            "url": url,
                            "max_chars": min(20_000, maximum),
                            "live": True,
                            "output_format": "json",
                        },
                    )
                if reply.isError:
                    raise KeenableError("provider_fetch_failed")
                match reply.content:
                    case [TextContent(text=text)]:
                        body = text.encode()
                        if len(body) > maximum:
                            raise KeenableError("provider_response_budget")
                        return _Page.model_validate_json(body), sha256(body).hexdigest(), len(body)
                    case _:
                        raise KeenableError("provider_response_invalid")


@dataclass(slots=True)
class LiveGrounding:
    config: LiveGroundingConfig
    credentials: GroundingCredentials
    files: GroundingFiles
    limit: int
    history: list[LiveReceipt] = field(default_factory=list)
    blocked: int = 0
    queries: list[DocumentationQuery] = field(default_factory=list)
    pages: dict[str, LivePage] = field(default_factory=dict)
    response_bytes: int = 0

    def call(self, tool: _Tool, arguments: str, pending: set[str]) -> LiveReceipt:
        return self.call_many([(tool, arguments)], pending)[0]

    execute = call

    def call_many(self, calls: list[tuple[_Tool, str]], pending: set[str]) -> list[LiveReceipt]:
        """Run one row's retrieval wave; admit responses against its byte budget in request order."""
        receipts: list[LiveReceipt] = []
        operations: dict[int, _Operation] = {}
        start = len(self.history)
        remaining_slots = max(0, self.limit - start)
        maximum = min(
            self.config.maximum_response_bytes, self.config.maximum_total_response_bytes - self.response_bytes
        )
        key = self.credentials.keenable
        for offset, (tool, arguments) in enumerate(calls):
            index = start + min(offset, remaining_slots)
            receipt = LiveReceipt(index=index, tool=tool, request_sha256=fingerprint(arguments))
            if offset >= remaining_slots:
                self.blocked += 1
                receipts.append(receipt.model_copy(update={"error": "grounding_budget_exhausted"}))
                continue
            self.files.write(f"operation-{index:03}/request.json", receipt.model_dump_json().encode())
            try:
                request = self._prepare(receipt, arguments, pending)
                if not key:
                    raise KeenableError("provider_credential_unavailable")
                if maximum <= 0:
                    raise KeenableError("provider_response_budget")
                operations[index] = _Operation(request)
            except _GROUNDING_ERRORS as error:
                receipt = _failed(receipt, error)
            receipts.append(receipt)

        async def run(operation: _Operation, credential: str) -> None:
            await anyio.to_thread.run_sync(_PROVIDER_SLOTS.acquire)
            try:
                with anyio.fail_after(self.config.timeout):
                    operation.response = await _retrieve(credential, operation, maximum)
            except _GROUNDING_ERRORS as error:
                operation.response = error
            finally:
                _PROVIDER_SLOTS.release()

        async def wave(credential: str) -> None:
            async with anyio.create_task_group() as group:
                for operation in operations.values():
                    group.start_soon(run, operation, credential)

        if operations and key:
            anyio.run(wave, key)
        for offset, receipt in enumerate(receipts[:remaining_slots]):
            if (operation := operations.get(receipt.index)) is not None:
                receipt = receipt.model_copy(
                    update={
                        "provider_started_monotonic_ns": operation.started,
                        "provider_finished_monotonic_ns": operation.finished,
                    }
                )
                try:
                    receipt = self._retain(receipt, operation)
                except _GROUNDING_ERRORS as error:
                    receipt = _failed(receipt, error)
            receipts[offset] = receipt
            self.history.append(receipt)
            self.files.write(f"operation-{receipt.index:03}/receipt.json", receipt.model_dump_json().encode())
        return receipts

    def _prepare(self, receipt: LiveReceipt, arguments: str, pending: set[str]) -> _Request:
        request = (
            QueryInput.model_validate_json(arguments)
            if receipt.tool == "query"
            else PageFetchInput.model_validate_json(arguments)
        )
        ids = request.candidate_ids
        if len(ids) != len(set(ids)) or not set(ids) <= pending:
            raise KeenableError("invalid_candidate_scope")
        if self.credentials.contains(arguments) or any(
            pattern.search(arguments) for pattern in SECRET_PATTERNS
        ):
            raise KeenableError("sensitive_grounding_arguments")
        match request:
            case QueryInput(query=query):
                if not query.strip():
                    raise KeenableError("invalid_empty_query")
                query_record = DocumentationQuery(action="keenable_search", query=query, candidate_ids=ids)
            case PageFetchInput(url=url):
                if not trusted_live_url(self.config, url):
                    raise KeenableError("untrusted_fetch_request_url")
                query_record = DocumentationQuery(
                    action="keenable_fetch", query="Fetch operation documentation", url=url, candidate_ids=ids
                )
        self.queries.append(query_record)
        self.files.write(f"operation-{receipt.index:03}/arguments.json", request.model_dump_json().encode())
        return request

    def _charge(self, size: int) -> None:
        if (
            size > self.config.maximum_response_bytes
            or self.response_bytes + size > self.config.maximum_total_response_bytes
        ):
            raise KeenableError("provider_response_budget")
        self.response_bytes += size

    def _retain(self, receipt: LiveReceipt, operation: _Operation) -> LiveReceipt:
        response, request = operation.response, operation.request
        assert response is not None
        if isinstance(response, _GROUNDING_ERRORS):
            raise response
        receipt = receipt.model_copy(update={"request": request})
        match response:
            case (_Page() as page, str() as response_digest, int() as size):
                self._charge(size)
                if not trusted_live_url(self.config, page.url):
                    raise KeenableError("untrusted_fetch_response_url")
                if self.credentials.contains(page.model_dump_json()) or self.credentials.contains(
                    page.content
                ):
                    raise KeenableError("sensitive_provider_response")
                if any(pattern.search(page.content) for pattern in SECRET_PATTERNS):
                    raise KeenableError("sensitive_provider_response")
                assert isinstance(request, PageFetchInput)
                saved = LivePage(
                    **page.model_dump(),
                    request_index=receipt.index,
                    request_url=request.url,
                    candidate_ids=tuple(request.candidate_ids),
                    fetched_at=datetime.now(UTC).isoformat(),
                    response_sha256=response_digest,
                )
                self.pages[saved.identity] = saved
                self.files.write(f"pages/{saved.identity}.json", saved.model_dump_json().encode())
                return receipt.model_copy(
                    update={"page_id": saved.identity, "response_sha256": response_digest}
                )
            case hits:
                body = "[" + ",".join(hit.model_dump_json() for hit in hits) + "]"
                self._charge(len(body.encode()))
                safe = tuple(
                    hit.model_copy(update={"url": canonical})
                    for hit in hits
                    if (canonical := canonical_url(hit.url)) is not None
                    and trusted_live_url(self.config, canonical)
                    and not self.credentials.contains(hit.model_dump_json())
                    and not any(pattern.search(hit.model_dump_json()) for pattern in SECRET_PATTERNS)
                )
                return receipt.model_copy(update={"discovery": safe})
