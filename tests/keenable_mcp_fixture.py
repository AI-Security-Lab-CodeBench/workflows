import json
import os
from collections.abc import Generator
from contextlib import contextmanager
from unittest.mock import patch

from httpx import AsyncClient, MockTransport, Request, Response
from pydantic import JsonValue
from test_keenable import PLAN

SNIPPET = "Preview the changes:\n\n```bash\nterraform plan\n```"
ASYNC_CLIENT = "codebench_pipeline.keenable_mcp.httpx.AsyncClient"


@contextmanager
def keenable_mcp_fixture(*, empty: bool = False, failed: bool = False) -> Generator[list[str]]:
    queries: list[str] = []

    def respond(request: Request) -> Response:
        assert str(request.url) == "https://api.keenable.ai/mcp"
        assert request.headers["X-API-Key"] == "fixture-key"
        message = json.loads(request.content)
        result: dict[str, JsonValue]
        match message["method"]:
            case "initialize":
                result = {
                    "protocolVersion": message["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fixture", "version": "1"},
                }
            case "notifications/initialized":
                return Response(202)
            case "tools/list":
                result = {"tools": []}
            case "tools/call":
                assert message["params"]["name"] == "search_web_pages"
                queries.append(message["params"]["arguments"]["query"])
                hits = [] if empty else [{"url": PLAN, "description": "", "snippet": SNIPPET}]
                result = {
                    "isError": failed,
                    "content": [{"type": "text", "text": json.dumps({"results": hits})}],
                }
            case _:
                raise AssertionError(f"Unexpected MCP method: {message['method']}")
        return Response(200, json={"jsonrpc": "2.0", "id": message["id"], "result": result})

    def client(*, headers: dict[str, str], timeout: float, trust_env: bool) -> AsyncClient:
        return AsyncClient(
            headers=headers, timeout=timeout, trust_env=trust_env, transport=MockTransport(respond)
        )

    with (
        patch.dict(
            os.environ,
            {
                **{key: value for key, value in os.environ.items() if not key.startswith("KEENABLE_")},
                "KEENABLE_API_KEY": "fixture-key",
            },
            clear=True,
        ),
        patch(ASYNC_CLIENT, side_effect=client),
        patch("codebench_pipeline.keenable.request_direct", side_effect=AssertionError("Origin transport")),
    ):
        yield queries
