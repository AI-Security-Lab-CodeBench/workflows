from __future__ import annotations

import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client
from mcp.types import CallToolResult, TextContent, Tool
from pydantic import JsonValue, TypeAdapter

from scripts.dataset_schema import SECRET_PATTERNS

from .discovery_models import DiscoveryError, is_http_url, save_model


@asynccontextmanager
async def connect_keenable() -> AsyncGenerator[ClientSession]:
    command = os.environ.get("CODEBENCH_KEENABLE_COMMAND", "")
    url = os.environ.get("CODEBENCH_KEENABLE_URL", "")
    if command and url:
        raise DiscoveryError("Configure exactly one Keenable stdio command or HTTP URL")
    if not command and not url:
        url = "https://api.keenable.ai/mcp"
    if command:
        args = TypeAdapter(list[str]).validate_json(
            os.environ.get("CODEBENCH_KEENABLE_ARGS", "[]"), strict=True
        )
        environment = TypeAdapter(dict[str, str]).validate_json(
            os.environ.get("CODEBENCH_KEENABLE_ENV", "{}"),
            strict=True,
        )
        parameters = StdioServerParameters(command=command, args=args, env=dict(os.environ, **environment))
        async with stdio_client(parameters) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                await session.initialize()
                yield session
    else:
        if not is_http_url(url):
            raise DiscoveryError("Keenable HTTP endpoint must be an HTTP(S) URL without credentials")
        headers = TypeAdapter(dict[str, str]).validate_json(
            os.environ.get("CODEBENCH_KEENABLE_HEADERS", "{}"),
            strict=True,
        )
        async with create_mcp_http_client(headers=headers) as client:
            async with streamable_http_client(url, http_client=client) as (reader, writer, _):
                async with ClientSession(reader, writer) as session:
                    await session.initialize()
                    yield session


def find_tool(tools: list[Tool], name: str) -> Tool:
    matches = [tool for tool in tools if tool.name == name or tool.name.endswith("_" + name)]
    if len(matches) != 1:
        raise DiscoveryError(f"Expected exactly one Keenable {name} tool")
    return matches[0]


def result_text(result: CallToolResult) -> str:
    chunks: list[str] = []
    for part in result.content:
        if isinstance(part, TextContent):
            chunks.append(part.text)
        else:
            raise DiscoveryError("Unexpected non-text Keenable content")
    if result.structuredContent is not None:
        chunks.append(json.dumps(result.structuredContent, ensure_ascii=False))
    return "\n".join(chunks)


def search_urls(result: CallToolResult) -> list[str]:
    values: list[JsonValue] = []
    if result.structuredContent is not None:
        values.append(TypeAdapter(JsonValue).validate_python(result.structuredContent))
    for part in result.content:
        if isinstance(part, TextContent):
            values.append(TypeAdapter(JsonValue).validate_json(part.text, strict=True))
    urls: list[str] = []
    for value in values:
        if isinstance(value, dict):
            records = value.get("results")
            if isinstance(records, list):
                for record in records:
                    if isinstance(record, dict):
                        url = record.get("url")
                        if isinstance(url, str) and is_http_url(url) and url not in urls:
                            urls.append(url)
    return urls


async def call_tool(
    session: ClientSession,
    tool: Tool,
    arguments: dict[str, JsonValue],
    evidence: Path,
    timeout: int,
) -> CallToolResult:
    properties = TypeAdapter(dict[str, JsonValue]).validate_python(tool.inputSchema).get("properties", {})
    if not isinstance(properties, dict) or not set(arguments) <= set(properties):
        raise DiscoveryError(f"Unsupported input schema for {tool.name}")
    with anyio.fail_after(timeout):
        result = await session.call_tool(tool.name, arguments=arguments)
    if any(pattern.search(result.model_dump_json()) for pattern in SECRET_PATTERNS):
        raise DiscoveryError("Sensitive-value indicator in tool result; source content withheld")
    save_model(evidence, result)
    if result.isError:
        raise DiscoveryError(f"Keenable tool {tool.name} returned an error; inspect {evidence.name}")
    return result
