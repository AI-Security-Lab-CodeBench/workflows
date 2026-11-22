import json
from dataclasses import dataclass, field, replace
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from httpx import AsyncClient, MockTransport, Request, Response
from pydantic import JsonValue

from codebench_pipeline.dataframe_io import frame_bytes
from codebench_pipeline.keenable_decisions import DiscoveryModel
from codebench_pipeline.keenable_inspection import AuthoredInspection, InspectionTaxonomy
from codebench_pipeline.keenable_policy import KeenableError, KeenableSnippetSettings
from codebench_pipeline.keenable_provider import KeenableCredentials, KeenableTransport
from codebench_pipeline.keenable_snippets import (
    DiscoveryRun,
    SnippetCollectionReceipt,
    provider_snippet_candidates,
)
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse

SNIPPET_URL = "https://example.org/post#inventory"
SNIPPET_KEY = "synthetic-keenable-key"
SNIPPET_SETTINGS = KeenableSnippetSettings(seeds=("docker_inventory",))


@dataclass(slots=True)
class KeenableWire:
    results: list[dict[str, JsonValue]]
    keys: tuple[str, ...] = (SNIPPET_KEY,)
    fail_first_search: bool = False
    calls: list[str] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)
    search_keys: list[str] = field(default_factory=list)

    def client(self, *, headers: dict[str, str], timeout: float, trust_env: bool) -> AsyncClient:
        return AsyncClient(
            headers=headers, timeout=timeout, trust_env=trust_env, transport=MockTransport(self.respond)
        )

    def respond(self, request: Request) -> Response:
        key = request.headers["X-API-Key"]
        if str(request.url) != "https://api.keenable.ai/mcp" or key not in self.keys:
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
                    or message["params"]["arguments"]["output_format"] != "json"
                ):
                    raise AssertionError("Expected the configured snippet search")
                self.queries.append(message["params"]["arguments"]["query"])
                self.search_keys.append(key)
                if self.fail_first_search and len(self.queries) == 1:
                    return Response(500, text=" ".join(self.keys), headers={"Retry-After": "0"})
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
    def setUp(self) -> None:
        self.evidence = Path(self.enterContext(TemporaryDirectory()))
        self.access = DiscoveryRun(self.evidence, snippet_transport())

    def test_replay_a_serialized_extracted_description_cannot_echo_a_numbered_key(self) -> None:
        key = 'synthetic-retained-description-marker"'
        unsafe = {
            "url": SNIPPET_URL,
            "description": "",
            "snippet": "```sh\n# synthetic-retained-description-marker\ndocker system df\n```",
        }
        for value in (*unsafe.values(), json.dumps(unsafe, separators=(",", ":"))):
            self.assertNotIn(key, value)
        safe = {"url": SNIPPET_URL, "snippet": "```sh\ndocker system df\n```"}
        transport = KeenableTransport(KeenableCredentials.bind(SNIPPET_KEY, {"KEENABLE_API_KEY_1": key}.get))
        wire = KeenableWire([{**unsafe}, {**safe}], keys=(SNIPPET_KEY, key))
        with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", side_effect=wire.client):
            frame = provider_snippet_candidates(SNIPPET_SETTINGS, DiscoveryRun(self.evidence, transport))
        result = self.evidence / "discovery-01-result.json"
        for raw in (result.read_bytes(), frame.attrs["keenable_receipt_json"].encode()):
            self.assertNotIn(key.encode(), raw)
            receipt = SnippetCollectionReceipt.model_validate_json(raw)
            self.assertEqual([hit.outcome for hit in receipt.searches[0].hits], ["sensitive", "extracted"])
            (candidate,) = receipt.candidates.values()
            self.assertEqual(receipt.read_body(candidate.body_sha256), safe["snippet"].encode())
        self.assertEqual(frame.command.tolist(), ["docker system df"])
        self.assertNotIn(key.encode(), frame_bytes(frame))

    def test_derived_feedback_and_frame_values_do_not_escape_in_safe_sibling_outputs(self) -> None:
        feedback_key = 'storage"]'
        position_only = "```sh\n# Fixture comment\n docker system df\n```"
        start = position_only.index("docker")
        frame_key = f"bytes:{start}:{start + len('docker system df')}"
        transport = KeenableTransport(
            KeenableCredentials.bind(
                SNIPPET_KEY, {"KEENABLE_API_KEY_1": feedback_key, "KEENABLE_API_KEY_2": frame_key}.get
            )
        )
        shared_url = SNIPPET_URL
        wire = KeenableWire(
            [
                {"url": shared_url, "snippet": "```sh\ndocker ps\n```", "description": "VOLUMES STORAGE"},
                {"url": shared_url, "snippet": position_only},
                {"url": shared_url, "snippet": "```sh\ndocker system df\n```", "description": "image sizes"},
            ],
            keys=(SNIPPET_KEY, feedback_key, frame_key),
        )
        requests: list[bytes] = []

        def reply(_url: str, body: bytes, _key: str | None, _timeout: float) -> HTTPResponse:
            requests.append(body)
            return model_reply(
                {"action": "query", "seed": "docker_inventory", "context": "disk usage"}
                if len(requests) == 1
                else {"action": "finalize", "seed": None}
            )

        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", side_effect=wire.client),
            patch("codebench_pipeline.keenable_decisions.complete", side_effect=reply),
        ):
            frame = provider_snippet_candidates(
                *discovery_settings(2, DiscoveryRun(self.evidence, transport))
            )
        feedback = json.loads(json.loads(requests[1])["messages"][1]["content"])["previous"][0]
        self.assertEqual(feedback["excerpt_context"], ["image sizes"])
        self.assertEqual(frame.command.tolist(), ["docker system df"])
        raw = frame.attrs["keenable_receipt_json"].encode()
        receipt = SnippetCollectionReceipt.model_validate_json(raw)
        self.assertEqual(
            [hit.outcome for hit in receipt.searches[0].hits], ["sensitive", "sensitive", "extracted"]
        )
        outputs = [
            raw,
            frame_bytes(frame),
            *requests,
            *(path.read_bytes() for path in self.evidence.glob("*.json")),
        ]
        for key in (feedback_key, frame_key):
            self.assertTrue(all(key.encode() not in output for output in outputs))

    def test_final_receipt_is_checked_even_when_every_hit_was_safe(self) -> None:
        key = '"stop":"budget"'
        transport = KeenableTransport(KeenableCredentials.bind(SNIPPET_KEY, {"KEENABLE_API_KEY_1": key}.get))
        wire = KeenableWire([], keys=(SNIPPET_KEY, key))
        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", side_effect=wire.client),
            self.assertRaisesRegex(KeenableError, "^sensitive_discovery_output$"),
        ):
            provider_snippet_candidates(SNIPPET_SETTINGS, DiscoveryRun(self.evidence, transport))
        self.assertEqual(len(wire.queries), 1)
        self.assertTrue(all(key.encode() not in path.read_bytes() for path in self.evidence.glob("*.json")))

    def test_nested_model_feedback_json_is_checked_before_dispatch(self) -> None:
        key = '"available":'
        transport = KeenableTransport(KeenableCredentials.bind(SNIPPET_KEY, {"KEENABLE_API_KEY_1": key}.get))
        with (
            patch("codebench_pipeline.keenable_decisions.complete") as model,
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient") as mcp,
            self.assertRaisesRegex(KeenableError, "^sensitive_discovery_output$"),
        ):
            provider_snippet_candidates(*discovery_settings(2, DiscoveryRun(self.evidence, transport)))
        model.assert_not_called()
        mcp.assert_not_called()
        self.assertEqual(list(self.evidence.iterdir()), [])

    def test_same_page_can_contribute_distinct_literal_snippets(self) -> None:
        first = "Préface\r\n```bash\r\n  $ docker container ls --all\r\n```\r\n"
        second = "Show disk use.\n```console\n$ docker system df\nTYPE TOTAL ACTIVE\n```"
        wire = KeenableWire(
            [
                {"url": SNIPPET_URL, "snippet": first, "description": "List container names."},
                {"url": SNIPPET_URL, "snippet": second, "description": ""},
            ]
        )
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client):
            frame = provider_snippet_candidates(SNIPPET_SETTINGS, self.access)

        commands = ("docker container ls --all", "docker system df")
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
            frame = provider_snippet_candidates(SNIPPET_SETTINGS, self.access)

        self.assertTrue(frame.empty)
        serialized = frame.attrs["keenable_receipt_json"]
        receipt = SnippetCollectionReceipt.model_validate_json(serialized)
        self.assertEqual(
            [hit.outcome for hit in receipt.searches[0].hits],
            ["no_commands", "sensitive", "no_commands", "no_commands"],
        )
        self.assertEqual(receipt.snippets, {})
        self.assertNotIn(SNIPPET_KEY, serialized)

    def test_serialized_key_echo_excludes_an_otherwise_valid_collection_hit(self) -> None:
        key = r"synthetic\"collection-key"
        decoded_description = 'synthetic"collection-key'
        hit: dict[str, JsonValue] = {
            "url": SNIPPET_URL,
            "snippet": "```sh\ndocker system df\n```",
            "description": decoded_description,
        }
        self.assertIn(key, json.dumps(hit))
        self.assertNotIn(key, decoded_description)
        credentials = KeenableCredentials.bind(SNIPPET_KEY, {"KEENABLE_API_KEY_1": key}.get)
        access = replace(self.access, transport=KeenableTransport(credentials))
        wire = KeenableWire([hit], keys=(SNIPPET_KEY, key))
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client):
            frame = provider_snippet_candidates(SNIPPET_SETTINGS, access)

        self.assertTrue(frame.empty)
        serialized = frame.attrs["keenable_receipt_json"]
        self.assertNotIn(key, serialized)
        receipt = SnippetCollectionReceipt.model_validate_json(serialized)
        (observation,) = receipt.searches[0].hits
        self.assertEqual(observation.outcome, "sensitive")
        self.assertEqual(receipt.snippets, {})

    def test_discovery_rejects_out_of_scope_commands_even_from_a_successful_search(self) -> None:
        lines = (
            "docker system df",
            "docker system prune",
            "docker system df > report.txt",
            "docker ps --format $(example)",
            "kubectl get pods",
            "docker system df; docker ps",
        )
        wire = KeenableWire(
            [{"url": SNIPPET_URL, "snippet": f"```bash\n{command}\n```"} for command in lines]
        )
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        settings = SNIPPET_SETTINGS.model_copy(update={"max_results": len(lines)})
        with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client):
            frame = provider_snippet_candidates(settings, self.access)
        self.assertEqual(frame.command.tolist(), ["docker system df"])
        receipt = SnippetCollectionReceipt.model_validate_json(frame.attrs["keenable_receipt_json"])
        self.assertEqual(
            [hit.outcome for hit in receipt.searches[0].hits], ["extracted"] + ["outside_inspection"] * 5
        )
        self.assertEqual(list(receipt.snippets.values()), ["```bash\ndocker system df\n```"])

    def test_model_can_finalize_using_counts_without_receiving_snippet_text(self) -> None:
        wire = KeenableWire([{"url": SNIPPET_URL, "snippet": "Private fixture prose\n```sh\ndocker ps\n```"}])
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        requests: list[bytes] = []

        def reply(url: str, body: bytes, key: str | None, timeout: float) -> HTTPResponse:
            requests.append(body)
            choice: dict[str, JsonValue] = (
                {"action": "query", "seed": "docker_inventory", "context": "disk usage"}
                if len(requests) == 1
                else {"action": "finalize", "seed": None}
            )
            return model_reply(choice)

        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client),
            patch("codebench_pipeline.keenable_decisions.complete", side_effect=reply),
        ):
            frame = provider_snippet_candidates(*discovery_settings(2, self.access))
        receipt = SnippetCollectionReceipt.model_validate_json(frame.attrs["keenable_receipt_json"])
        self.assertEqual(receipt.stop, "finalized")
        self.assertEqual(len(wire.queries), 1)
        feedback_request = json.loads(json.loads(requests[1])["messages"][1]["content"])
        self.assertEqual(feedback_request["remaining_searches"], 1)
        self.assertEqual(feedback_request["previous"][0]["new_candidates"], 1)
        self.assertNotIn(b"Private fixture prose", requests[1])
        partial = SnippetCollectionReceipt.model_validate_json(
            (self.evidence / "discovery-01-result.json").read_bytes()
        )
        self.assertEqual([row.command for row in partial.candidates.values()], ["docker ps"])
        self.assertEqual(partial.searches, receipt.searches)

    def test_exhausted_search_budget_does_not_call_the_model_again(self) -> None:
        wire = KeenableWire([])
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client),
            patch(
                "codebench_pipeline.keenable_decisions.complete",
                side_effect=[
                    model_reply({"action": "query", "seed": "docker_inventory", "context": "disk usage"})
                ],
            ),
        ):
            frame = provider_snippet_candidates(*discovery_settings(1, self.access))
        receipt = SnippetCollectionReceipt.model_validate_json(frame.attrs["keenable_receipt_json"])
        self.assertEqual(receipt.stop, "budget")
        self.assertEqual(len(wire.queries), 1)

    def test_model_cannot_replace_the_query_with_free_text(self) -> None:
        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient") as network,
            patch(
                "codebench_pipeline.keenable_decisions.complete",
                return_value=model_reply(
                    {"action": "query", "seed": "docker_inventory", "query": "different search"}
                ),
            ),
            self.assertRaisesRegex(KeenableError, "discovery_model_response_invalid"),
        ):
            provider_snippet_candidates(*discovery_settings(2, self.access))
        network.assert_not_called()

    def test_repeating_the_same_query_fails_without_losing_previous_search_evidence(self) -> None:
        wire = KeenableWire([{"url": SNIPPET_URL, "snippet": "```bash\ndocker ps\n```"}])
        client = AsyncClient(headers={"X-API-Key": SNIPPET_KEY}, transport=MockTransport(wire.respond))
        repeated = model_reply({"action": "query", "seed": "docker_inventory", "context": "disk usage"})
        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", return_value=client),
            patch("codebench_pipeline.keenable_decisions.complete", return_value=repeated),
            self.assertRaisesRegex(KeenableError, "discovery_query_already_used"),
        ):
            provider_snippet_candidates(*discovery_settings(2, self.access))
        self.assertEqual(len(wire.queries), 1)
        partial = SnippetCollectionReceipt.model_validate_json(
            (self.evidence / "discovery-01-result.json").read_bytes()
        )
        self.assertEqual([row.command for row in partial.candidates.values()], ["docker ps"])

    def test_context_outside_the_selected_seed_cannot_reach_search(self) -> None:
        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient") as network,
            patch(
                "codebench_pipeline.keenable_decisions.complete",
                return_value=model_reply(
                    {"action": "query", "seed": "docker_inventory", "context": "delete volumes"}
                ),
            ),
            self.assertRaisesRegex(KeenableError, "discovery_context_outside_inspection"),
        ):
            provider_snippet_candidates(*discovery_settings(2, self.access))
        network.assert_not_called()

    def test_archived_raw_queries_cannot_dispatch_searches(self) -> None:
        archived = KeenableSnippetSettings.model_validate_json('{"queries":["historical search"]}')
        with (
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient") as network,
            self.assertRaisesRegex(KeenableError, "raw_discovery_queries_unsupported"),
        ):
            provider_snippet_candidates(archived, self.access)
        network.assert_not_called()


def snippet_transport() -> KeenableTransport:
    return KeenableTransport(KeenableCredentials.bind(SNIPPET_KEY, lambda _: None))


def discovery_settings(
    max_search_calls: int, access: DiscoveryRun
) -> tuple[KeenableSnippetSettings, DiscoveryRun]:
    seed = AuthoredInspection(
        id="docker_inventory", instruction="Find read-only inventory.", risk_id="CAT-3.3", severity="low"
    )
    return KeenableSnippetSettings(seeds=(seed,), max_search_calls=max_search_calls), replace(
        access,
        model=DiscoveryModel(
            ModelConfig(
                provider="openai_compatible",
                model="fixture-discovery",
                base_url="https://example.org/v1",
                max_output_tokens=150,
                timeout_seconds=10,
            ),
            None,
            (
                InspectionTaxonomy(
                    seed=seed,
                    risk_definition="Container cleanup",
                    risk_guidance="Check actual operation",
                    severity_definition="No material effects",
                ),
            ),
        ),
    )


def model_reply(choice: dict[str, JsonValue]) -> HTTPResponse:
    return HTTPResponse(
        json.dumps(
            {
                "id": "fixture-response",
                "model": "fixture-discovery",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": json.dumps(choice)},
                    }
                ],
            }
        ).encode(),
        None,
    )
