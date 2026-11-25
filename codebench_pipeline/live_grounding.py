from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from time import monotonic_ns
from typing import Literal
from urllib.parse import urlsplit

import anyio
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.agent_trace import CallOutcome, payloads_enabled, tool_call
from codebench_pipeline.grounding_tools import (
    DocumentationQuery,
    GroundingCredentials,
    GroundingFiles,
    GroundingOrigins,
    LiveGroundingConfig,
)
from codebench_pipeline.keenable_mcp import search_session, search_snippets
from codebench_pipeline.keenable_policy import KeenableError, canonical_url
from codebench_pipeline.keenable_provider import (
    KeenableCredentials,
    KeenableIncident,
    KeenableProviderError,
    KeenableTransport,
    KeenableTransportError,
)
from codebench_pipeline.producer_trace import trace_json, trace_payload_secrets, trace_value
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
    provider_error: KeenableProviderError | None = None
    provider_started_monotonic_ns: int | None = None
    provider_finished_monotonic_ns: int | None = None


type _Tool = Literal["query", "page_fetch"]
type _Request = QueryInput | PageFetchInput
type _Response = tuple[LiveHit, ...] | tuple[_Page, str, int]
type _GroundingError = KeenableError | ValidationError
_GROUNDING_ERRORS = (KeenableError, ValidationError)
_INCIDENTS = TypeAdapter(tuple[KeenableIncident, ...])


@dataclass(slots=True)
class _Operation:
    request: _Request
    arguments: str
    tool_call_id: str | None
    started: int | None = None
    finished: int | None = None
    response: _Response | _GroundingError | None = None
    incidents: list[KeenableIncident] = field(default_factory=list)


def _failed(receipt: LiveReceipt, error: _GroundingError) -> LiveReceipt:
    match error:
        case KeenableTransportError(detail=detail):
            return receipt.model_copy(update={"error": detail.code, "provider_error": detail})
        case KeenableError(code=code):
            return receipt.model_copy(update={"error": code})
        case ValidationError():
            return receipt.model_copy(update={"error": "invalid_grounding_arguments_or_response"})


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


async def _retrieve(
    key: str,
    operation: _Operation,
    maximum: int,
    observe: Callable[[CallToolResult], None] | None,
) -> _Response:
    async with search_session(key) as session:
        match operation.request:
            case QueryInput(query=query):
                results = (
                    await search_snippets(session, query, (8, 500), observe=observe)
                    if observe is not None
                    else await search_snippets(session, query, (8, 500))
                )
                return tuple(LiveHit(url=hit.url, snippet=hit.snippet) for hit in results.results)
            case PageFetchInput(url=url):
                reply = await session.call_tool(
                    "fetch_page_content",
                    arguments={
                        "url": url,
                        "max_chars": min(20_000, maximum),
                        "live": True,
                        "output_format": "json",
                    },
                )
                if observe is not None:
                    observe(reply)
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
    incidents: dict[int, tuple[KeenableIncident, ...]] = field(default_factory=dict)
    trace_calls: bool = field(default=False, kw_only=True)

    def call(self, tool: _Tool, arguments: str, pending: set[str]) -> LiveReceipt:
        return self.call_many([(tool, arguments)], pending)[0]

    execute = call

    def call_many(
        self,
        calls: list[tuple[_Tool, str]],
        pending: set[str],
        *,
        tool_call_ids: list[str] | None = None,
    ) -> list[LiveReceipt]:
        """Run one row's retrieval wave; admit responses against its byte budget in request order."""
        receipts: list[LiveReceipt] = []
        operations: dict[int, _Operation] = {}
        start = len(self.history)
        remaining_slots = max(0, self.limit - start)
        maximum = min(
            self.config.maximum_response_bytes, self.config.maximum_total_response_bytes - self.response_bytes
        )
        credentials = self.credentials.keenable_pool
        if credentials is None:
            credentials = KeenableCredentials.bind(self.credentials.keenable, lambda _: None)
        transport = KeenableTransport(credentials, ledger=self.credentials.keenable_ledger)
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
                if not credentials.organizations:
                    raise KeenableError("provider_credential_unavailable")
                if maximum <= 0:
                    raise KeenableError("provider_response_budget")
                operations[index] = _Operation(
                    request, arguments, tool_call_ids[offset] if tool_call_ids is not None else None
                )
            except _GROUNDING_ERRORS as error:
                receipt = _failed(receipt, error)
            receipts.append(receipt)

        async def run(index: int, operation: _Operation) -> None:
            tool = "search" if isinstance(operation.request, QueryInput) else "fetch"
            call_name = "query" if tool == "search" else "page_fetch"
            stack = ExitStack()
            measured: CallOutcome | None = None
            output: dict[str, JsonValue] = {}
            capture_payloads = self.trace_calls and payloads_enabled()

            def capture(reply: CallToolResult) -> None:
                if measured is not None:
                    raw = reply.model_dump_json(by_alias=True, exclude_unset=True).encode()
                    output["response"] = trace_json(raw)
                    measured.output(output.copy())

            async def retrieve(key: str) -> _Response:
                nonlocal measured
                measured = None
                output.clear()
                with stack:
                    try:
                        return await _retrieve(key, operation, maximum, capture if capture_payloads else None)
                    except anyio.get_cancelled_exc_class():
                        if measured is not None:
                            measured.output({**output, "error": "provider_cancelled"})
                        raise

            def started(request: bytes | None, attempt: int) -> None:
                nonlocal measured
                if operation.started is None:
                    operation.started = monotonic_ns()
                if capture_payloads:
                    inputs: dict[str, JsonValue] = {
                        "receipt_index": index,
                        "attempt": attempt,
                        "arguments": operation.arguments,
                    }
                    if operation.tool_call_id is not None:
                        inputs["tool_call_id"] = operation.tool_call_id
                    if request is not None:
                        inputs["request"] = trace_json(request)
                    measured = stack.enter_context(tool_call(call_name, inputs=trace_value(inputs)))
                elif self.trace_calls:
                    stack.enter_context(tool_call(call_name))

            def failed(error: KeenableProviderError) -> None:
                if measured is not None:
                    output["error"] = error.code
                    if error.status is not None:
                        output["http_status"] = error.status
                    measured.output(output.copy())

            try:
                with anyio.fail_after(self.config.timeout):
                    operation.response = await transport.call(
                        tool,
                        retrieve,
                        audit=operation.incidents.append,
                        on_dispatch=started,
                        on_failure=failed,
                    )
            except TimeoutError:
                operation.response = KeenableTransportError(KeenableProviderError(code="provider_timeout"))
            except _GROUNDING_ERRORS as error:
                operation.response = error
            finally:
                if operation.started is not None:
                    operation.finished = monotonic_ns()

        async def wave() -> None:
            async with anyio.create_task_group() as group:
                for index, operation in operations.items():
                    group.start_soon(run, index, operation)

        if operations:
            with trace_payload_secrets(self.credentials.contains):
                anyio.run(wave)
        for offset, receipt in enumerate(receipts[:remaining_slots]):
            if (operation := operations.get(receipt.index)) is not None:
                if operation.incidents:
                    incidents = tuple(operation.incidents)
                    self.files.write(
                        f"operation-{receipt.index:03}/provider-incidents.json",
                        _INCIDENTS.dump_json(incidents),
                    )
                    self.incidents[receipt.index] = incidents
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
        values = (arguments, *ids, request.query if isinstance(request, QueryInput) else request.url)
        if any(self._sensitive(value) for value in values):
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

    def _sensitive(self, text: str) -> bool:
        return self.credentials.contains(text) or any(pattern.search(text) for pattern in SECRET_PATTERNS)

    def _check_sensitive_response(self, *values: str) -> None:
        if any(self._sensitive(value) for value in values):
            raise KeenableTransportError(KeenableProviderError(code="sensitive_provider_response"))

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
                self._check_sensitive_response(page.url, page.content, page.model_dump_json())
                if not trusted_live_url(self.config, page.url):
                    raise KeenableError("untrusted_fetch_response_url")
                assert isinstance(request, PageFetchInput)
                saved = LivePage(
                    **page.model_dump(),
                    request_index=receipt.index,
                    request_url=request.url,
                    candidate_ids=tuple(request.candidate_ids),
                    fetched_at=datetime.now(UTC).isoformat(),
                    response_sha256=response_digest,
                )
                body = saved.model_dump_json()
                self._check_sensitive_response(body)
                self.pages[saved.identity] = saved
                self.files.write(f"pages/{saved.identity}.json", body.encode())
                return receipt.model_copy(
                    update={"page_id": saved.identity, "response_sha256": response_digest}
                )
            case hits:
                body = "[" + ",".join(hit.model_dump_json() for hit in hits) + "]"
                self._charge(len(body.encode()))
                safe: list[LiveHit] = []
                for hit in hits:
                    canonical = canonical_url(hit.url)
                    if canonical is None or not trusted_live_url(self.config, canonical):
                        continue
                    normalized = hit.model_copy(update={"url": canonical})
                    if not any(
                        self._sensitive(value)
                        for value in (
                            hit.url,
                            canonical,
                            hit.snippet,
                            hit.model_dump_json(),
                            normalized.model_dump_json(),
                        )
                    ):
                        safe.append(normalized)
                return receipt.model_copy(update={"discovery": tuple(safe)})
