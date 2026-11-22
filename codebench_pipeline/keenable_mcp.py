import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from re import fullmatch
from typing import Final

import httpx
from anyio import fail_after
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import TextContent
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .keenable_policy import RESPONSE_BYTES, KeenableError
from .keenable_provider import admit_tool_dispatch
from .keenable_transport import TIMEOUT

_ENDPOINT: Final = "https://api.keenable.ai/mcp"


class SearchHit(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, hide_input_in_errors=True)

    url: str
    snippet: str
    description: str = ""


class SearchResult(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, hide_input_in_errors=True)

    results: tuple[SearchHit, ...] = Field(max_length=50)


async def _before_request(request: httpx.Request) -> None:
    if request.method == "POST":
        message: object = json.loads(request.content)
        match message:
            case {"method": "tools/call"}:
                await admit_tool_dispatch()
            case _:
                return


@asynccontextmanager
async def search_session(api_key: str) -> AsyncGenerator[ClientSession]:
    """Connect to the fixed Keenable MCP endpoint without forwarding the key to a proxy."""
    if not fullmatch(r"[!-~]{1,512}", api_key):
        raise KeenableError("provider_credential_unavailable")
    async with httpx.AsyncClient(
        headers={"X-API-Key": api_key},
        timeout=TIMEOUT,
        trust_env=False,
    ) as http:
        http.event_hooks["request"].append(_before_request)
        async with (
            streamable_http_client(_ENDPOINT, http_client=http) as (reader, writer, _),
            ClientSession(reader, writer, read_timeout_seconds=timedelta(seconds=TIMEOUT)) as session,
        ):
            with fail_after(TIMEOUT):
                await session.initialize()
            yield session


async def search_snippets(session: ClientSession, query: str, limits: tuple[int, int]) -> SearchResult:
    """Search the index; Keenable returns the indexed excerpt inside one MCP text block."""
    max_results, snippet_max_length = limits
    with fail_after(TIMEOUT):
        result = await session.call_tool(
            "search_web_pages",
            arguments={
                "query": query,
                "max_results": max_results,
                "snippet_max_length": snippet_max_length,
                "output_format": "json",
                "disable_cards": True,
            },
        )
    if result.isError:
        raise KeenableError("provider_search_failed")
    match result.content:
        case [TextContent(text=text)] if len(text.encode()) <= RESPONSE_BYTES:
            try:
                envelope = SearchResult.model_validate_json(text)
            except ValidationError:
                raise KeenableError("provider_response_invalid") from None
        case _:
            raise KeenableError("provider_response_invalid")
    if len(envelope.results) > max_results or any(
        len(hit.snippet) > snippet_max_length for hit in envelope.results
    ):
        raise KeenableError("provider_response_budget")
    return envelope
