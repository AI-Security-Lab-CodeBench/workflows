from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal
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
    key: str, request: QueryInput | PageFetchInput, maximum: int
) -> tuple[LiveHit, ...] | tuple[_Page, str, int]:
    async with search_session(key) as session:
        match request:
            case QueryInput(query=query):
                results = await search_snippets(session, query, (8, 500))
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

    def execute(self, tool: Literal["query", "page_fetch"], arguments: str, pending: set[str]) -> LiveReceipt:
        index = len(self.history)
        if index >= self.limit:
            self.blocked += 1
            return LiveReceipt(
                index=index,
                tool=tool,
                request_sha256=fingerprint(arguments),
                error="grounding_budget_exhausted",
            )
        receipt = LiveReceipt(index=index, tool=tool, request_sha256=fingerprint(arguments))
        self.files.write(f"operation-{index:03}/request.json", receipt.model_dump_json().encode())
        try:
            receipt = self._execute(receipt, arguments, pending)
        except KeenableError as error:
            receipt = receipt.model_copy(update={"error": error.code})
        except ValidationError:
            receipt = receipt.model_copy(update={"error": "invalid_grounding_arguments_or_response"})
        except (TimeoutError, McpError, httpx.HTTPError, ExceptionGroup):
            receipt = receipt.model_copy(update={"error": "provider_transport_failed"})
        self.history.append(receipt)
        self.files.write(f"operation-{index:03}/receipt.json", receipt.model_dump_json().encode())
        return receipt

    def _execute(self, receipt: LiveReceipt, arguments: str, pending: set[str]) -> LiveReceipt:
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
        key = self.credentials.keenable
        if not key:
            raise KeenableError("provider_credential_unavailable")
        remaining = self.config.maximum_total_response_bytes - self.response_bytes
        maximum = min(remaining, self.config.maximum_response_bytes)
        if maximum <= 0:
            raise KeenableError("provider_response_budget")

        async def retrieve() -> tuple[LiveHit, ...] | tuple[_Page, str, int]:
            with anyio.fail_after(self.config.timeout):
                return await _retrieve(key, request, maximum)

        response = anyio.run(retrieve)
        receipt = receipt.model_copy(update={"request": request})
        match response:
            case (_Page() as page, str() as response_digest, int() as size):
                self.response_bytes += size
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
                    candidate_ids=tuple(ids),
                    fetched_at=datetime.now(UTC).isoformat(),
                    response_sha256=response_digest,
                )
                self.pages[saved.identity] = saved
                self.files.write(f"pages/{saved.identity}.json", saved.model_dump_json().encode())
                return receipt.model_copy(
                    update={"page_id": saved.identity, "response_sha256": response_digest}
                )
            case hits:
                safe = tuple(
                    hit.model_copy(update={"url": canonical})
                    for hit in hits
                    if (canonical := canonical_url(hit.url)) is not None
                    and trusted_live_url(self.config, canonical)
                    and not self.credentials.contains(hit.model_dump_json())
                    and not any(pattern.search(hit.model_dump_json()) for pattern in SECRET_PATTERNS)
                )
                body = "[" + ",".join(hit.model_dump_json() for hit in hits) + "]"
                size = len(body.encode())
                if size > maximum:
                    raise KeenableError("provider_response_budget")
                self.response_bytes += size
                return receipt.model_copy(update={"discovery": safe})
