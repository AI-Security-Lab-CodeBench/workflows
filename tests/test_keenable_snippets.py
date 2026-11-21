import json
from dataclasses import dataclass, field
from hashlib import sha256
from unittest import TestCase
from unittest.mock import patch

from httpx import AsyncClient, MockTransport, Request, Response
from pydantic import JsonValue

from codebench_pipeline.keenable_policy import KeenableSnippetSettings
from codebench_pipeline.keenable_snippets import (
    SnippetCollectionReceipt,
    provider_snippet_candidates,
)

SNIPPET_URL = "https://example.org/post#inventory"
SNIPPET_KEY = "synthetic-keenable-key"
SNIPPET_SETTINGS = KeenableSnippetSettings(queries=("Find operator inventory examples",))


@dataclass(slots=True)
class KeenableWire:
    results: list[dict[str, JsonValue]]
    calls: list[str] = field(default_factory=list)

    def respond(self, request: Request) -> Response:
        if str(request.url) != "https://api.keenable.ai/mcp" or request.headers["X-API-Key"] != SNIPPET_KEY:
            raise AssertionError("Unexpected MCP destination or credential")
        message = json.loads(request.content)
        method = message["method"]
        self.calls.append(method)
        match method:
            case "initialize":
                return Response(
                    200,
                    json={
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "result": {
                            "protocolVersion": message["params"]["protocolVersion"],
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "fixture", "version": "1"},
                        },
                    },
                )
            case "notifications/initialized":
                return Response(202)
            case "tools/call":
                if (
                    message["params"]["name"] != "search_web_pages"
                    or message["params"]["arguments"]["query"] != SNIPPET_SETTINGS.queries[0]
                    or message["params"]["arguments"]["output_format"] != "json"
                ):
                    raise AssertionError("Expected the configured snippet search")
                return Response(
                    200,
                    json={
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "result": {
                            "content": [{"type": "text", "text": json.dumps({"results": self.results})}]
                        },
                    },
                )
            case "tools/list":
                return Response(
                    200,
                    json={"jsonrpc": "2.0", "id": message["id"], "result": {"tools": []}},
                )
            case _:
                raise AssertionError(f"Unexpected MCP method: {method}")


class KeenableSnippetTests(TestCase):
    def test_same_page_can_contribute_distinct_literal_snippets(self) -> None:
        first = "Préface\r\n```bash\r\n  $ docker container ls --format '{{.Names}}'\r\n```\r\n"
        second = "Show disk use.\n```console\n$ docker system df\nTYPE TOTAL ACTIVE\n```"
        wire = KeenableWire(
            [
                {"url": SNIPPET_URL, "snippet": first, "description": "List container names."},
                {"url": SNIPPET_URL, "snippet": second, "description": ""},
            ]
        )
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client):
            frame = provider_snippet_candidates(SNIPPET_SETTINGS, SNIPPET_KEY)

        commands = ("docker container ls --format '{{.Names}}'", "docker system df")
        self.assertEqual(frame.command.tolist(), list(commands))
        self.assertEqual(frame.context.tolist(), ["List container names.", "Show disk use."])
        self.assertEqual(frame.source_url.tolist(), [SNIPPET_URL, SNIPPET_URL])
        self.assertEqual(wire.calls.count("tools/call"), 1)
        receipt = SnippetCollectionReceipt.model_validate_json(frame.attrs["keenable_receipt_json"])
        for key, original, command in zip(frame.candidate_id, (first, second), commands, strict=True):
            evidence = receipt.candidates[key]
            body = original.encode()
            sha = sha256(body).hexdigest()
            start = body.index(command.encode())
            self.assertEqual(evidence.body_sha256, sha)
            self.assertEqual(receipt.read_body(sha), body)
            self.assertEqual((evidence.start, evidence.end), (start, start + len(command.encode())))
            self.assertEqual((evidence.source_type, evidence.license), ("web_page", "keenable"))

    def test_prose_unclosed_fences_and_sensitive_hits_do_not_create_candidates(self) -> None:
        wire = KeenableWire(
            [
                {"url": SNIPPET_URL, "snippet": "Try `docker system df` or:\n```bash\ndocker system df\n"},
                {"url": SNIPPET_URL, "snippet": f"```shell\n# {SNIPPET_KEY}\ndocker system df\n```"},
                {"url": SNIPPET_URL, "snippet": "```python\nprint('docker system df')\n```"},
                {"url": SNIPPET_URL, "snippet": "```bash\n/var/lib/postgresql/\n├── data/\n```"},
            ]
        )
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client):
            frame = provider_snippet_candidates(SNIPPET_SETTINGS, SNIPPET_KEY)

        self.assertTrue(frame.empty)
        serialized = frame.attrs["keenable_receipt_json"]
        receipt = SnippetCollectionReceipt.model_validate_json(serialized)
        self.assertEqual(
            [hit.outcome for hit in receipt.searches[0].hits],
            ["no_commands", "sensitive", "no_commands", "no_commands"],
        )
        self.assertEqual(receipt.snippets, {})
        self.assertNotIn(SNIPPET_KEY, serialized)
