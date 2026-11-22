from __future__ import annotations

import json
import tempfile
import unittest
from collections.abc import AsyncGenerator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event, Lock
from time import monotonic_ns
from typing import Literal
from unittest.mock import patch

import anyio
import httpx
from httpx import AsyncClient
from mcp.types import CallToolResult, TextContent
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline import keenable_admission, keenable_provider, live_grounding
from codebench_pipeline.grounding_tools import GroundingCredentials, GroundingFiles, LiveGroundingConfig
from codebench_pipeline.keenable_admission import KeenableAdmissionLedger
from codebench_pipeline.keenable_policy import KeenableError
from codebench_pipeline.keenable_provider import (
    KeenableAdmission,
    KeenableIncident,
    KeenableTransport,
    admit_tool_dispatch,
)
from codebench_pipeline.keenable_snippets import DiscoveryRun
from codebench_pipeline.live_grounding import LiveGrounding, LiveReceipt

_KEY = "synthetic-provider-key"
_SECOND_KEY = "secondary-provider-key"
_URL = "https://example.org/docs"
_SCOPE = {"first", "second"}


def _query(value: str, candidate: str = "first") -> tuple[Literal["query"], str]:
    return "query", json.dumps({"query": value, "candidate_ids": [candidate]})


def _fetch(url: str, candidate: str = "second") -> tuple[Literal["page_fetch"], str]:
    return "page_fetch", json.dumps({"url": url, "candidate_ids": [candidate]})


def _page(url: str) -> str:
    return json.dumps({"url": url, "content": "Operation reference text"})


def _interval(receipt: LiveReceipt) -> tuple[int, int]:
    start, end = receipt.provider_started_monotonic_ns, receipt.provider_finished_monotonic_ns
    assert start is not None and end is not None and start < end, receipt
    return start, end


def _mcp_client(
    body: str, *, tool_error: bool, statuses: tuple[int, ...]
) -> Callable[..., httpx.AsyncClient]:
    responses = iter(statuses)

    def respond(request: httpx.Request) -> httpx.Response:
        message = json.loads(request.content)
        match message["method"]:
            case "initialize":
                result = {
                    "protocolVersion": message["params"]["protocolVersion"],
                    "capabilities": {},
                    "serverInfo": {"name": "synthetic", "version": "1"},
                }
            case "notifications/initialized":
                return httpx.Response(202)
            case "tools/list":
                result = {
                    "tools": [
                        {"name": name, "inputSchema": {"type": "object"}}
                        for name in ("search_web_pages", "fetch_page_content")
                    ]
                }
            case "tools/call":
                status = next(responses)
                if status != 200:
                    return httpx.Response(
                        status, headers={"Retry-After": "0"}, text=f"raw provider error for {_KEY}"
                    )
                result = {"content": [{"type": "text", "text": body}], "isError": tool_error}
            case _:
                raise AssertionError("Unexpected synthetic MCP request")
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": message["id"], "result": result})

    def client(*, headers: dict[str, str], timeout: float, trust_env: bool) -> httpx.AsyncClient:
        return AsyncClient(
            headers=headers, timeout=timeout, trust_env=trust_env, transport=httpx.MockTransport(respond)
        )

    return client


class _Provider:
    def __init__(self, respond: Callable[[str, str], Awaitable[str]]) -> None:
        self.respond = respond

    async def call_tool(self, name: str, arguments: dict[str, str | int | bool]) -> CallToolResult:
        await admit_tool_dispatch()
        value = str(arguments["query" if name == "search_web_pages" else "url"])
        return CallToolResult(content=[TextContent(type="text", text=await self.respond(name, value))])

    @asynccontextmanager
    async def session(self, key: str) -> AsyncGenerator[_Provider]:
        assert key in {_KEY, _SECOND_KEY}
        yield self


class ParallelGroundingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(patch.object(keenable_provider, "_ADMISSION", KeenableAdmission()))

    def broker(
        self, name: str, *, slots: int = 20, total_bytes: int = 4096, key: str = _KEY
    ) -> LiveGrounding:
        return LiveGrounding(
            LiveGroundingConfig(
                allowed_domains=["example.org"], maximum_total_response_bytes=total_bytes, timeout=5
            ),
            GroundingCredentials(keenable=key),
            GroundingFiles(self.root / name),
            slots,
        )

    def provider(self, respond: Callable[[str, str], Awaitable[str]]) -> None:
        self.enterContext(patch.object(live_grounding, "search_session", _Provider(respond).session))

    def test_mcp_task_groups_preserve_known_failure_codes(self) -> None:
        marker = "synthetic confidential provider content claiming HTTP 500 and 429 with Retry-After"
        oversized_hit = json.dumps({"results": [{"url": _URL, "snippet": marker + "x" * 501}]})
        cases = (
            (_query("provider error"), marker, True, 200, "provider_search_failed"),
            (_query("invalid search response"), marker, False, 200, "provider_response_invalid"),
            (_query("oversized search response"), oversized_hit, False, 200, "provider_response_budget"),
            (
                _fetch(_URL),
                json.dumps({"url": _URL, "content": [marker]}),
                False,
                200,
                "provider_response_invalid",
            ),
            (_fetch(_URL), marker, True, 200, "provider_fetch_failed"),
            (_query("http failure"), marker, False, 429, "provider_http_status"),
        )
        for index, ((tool, arguments), body, tool_error, status, expected) in enumerate(cases):
            with self.subTest(expected=expected):
                broker = self.broker(str(index))
                client = _mcp_client(body, tool_error=tool_error, statuses=(status,) * 4)
                with patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", side_effect=client):
                    receipt = broker.call(tool, arguments, _SCOPE)
                path = self.root / str(index) / "operation-000/receipt.json"
                persisted = path.read_text()
                self.assertEqual(receipt.error, expected)
                assert receipt.provider_error is not None
                self.assertEqual(receipt.provider_error.code, expected)
                self.assertEqual(receipt.provider_error.status, 429 if status == 429 else None)
                self.assertEqual(LiveReceipt.model_validate_json(persisted).error, expected)
                self.assertNotIn(marker, persisted)
                self.assertEqual((broker.response_bytes, broker.pages), (0, {}))
                self.assertEqual(broker.incidents, {})

    def test_recovered_http_500_keeps_redacted_audit_separate_from_success(self) -> None:
        broker = self.broker("recovered")
        marker = "private-page-marker"
        client = _mcp_client(_page(_URL), tool_error=False, statuses=(500, 200))
        tool, arguments = _fetch(f"{_URL}/{marker}")
        with (
            self.assertLogs("codebench_pipeline.keenable_provider", level="WARNING") as logs,
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", side_effect=client),
        ):
            receipt = broker.call(tool, arguments, _SCOPE)

        self.assertEqual((receipt.error, receipt.provider_error), (None, None))
        self.assertIsNotNone(receipt.page_id)
        operation = self.root / "recovered/operation-000"
        persisted = (operation / "provider-incidents.json").read_text()
        self.assertNotIn(marker, persisted)
        self.assertNotIn(_URL, persisted)
        self.assertNotIn(_KEY, persisted)
        (incident,) = [KeenableIncident.model_validate(record) for record in json.loads(persisted)]
        self.assertEqual(broker.incidents, {0: (incident,)})
        self.assertEqual((incident.tool, incident.attempt, incident.error.status), ("fetch", 1, 500))
        (event,) = logs.records
        self.assertEqual(event.getMessage(), "keenable.server_error status=500 attempt=1")
        self.assertIsNone(event.exc_info)
        self.assertNotIn("keenable.server_error", (operation / "receipt.json").read_text())

    def test_mixed_group_does_not_guess_a_single_known_cause(self) -> None:
        async def respond(_name: str, _query_value: str) -> str:
            raise ExceptionGroup(
                "synthetic private group text",
                [KeenableError("provider_response_budget"), httpx.ReadTimeout("synthetic private leaf text")],
            )

        self.provider(respond)
        tool, arguments = _query("mixed provider errors")
        receipt = self.broker("mixed").call(tool, arguments, _SCOPE)
        self.assertEqual(receipt.error, "provider_transport_failed")

    def test_numbered_credential_is_rejected_in_arguments_and_responses(self) -> None:
        additional = 'extra"provider-key'

        async def respond(_name: str, _query_value: str) -> str:
            return json.dumps({"results": [{"url": _URL, "snippet": additional}]})

        self.provider(respond)
        broker = self.broker("numbered")
        broker.credentials = GroundingCredentials.bind(
            broker.config, {"KEENABLE_API_KEY": _KEY, "KEENABLE_API_KEY_1": additional}.get
        )
        blocked, filtered = broker.call_many(
            [_query(f"search {additional}"), _query("safe question")], _SCOPE
        )

        self.assertEqual(blocked.error, "sensitive_grounding_arguments")
        self.assertEqual((filtered.error, filtered.discovery), (None, ()))
        for path in (self.root / "numbered").rglob("*.json"):
            self.assertNotIn("extra", path.read_text())

    def test_query_calls_overlap_and_receipts_bind_in_request_order(self) -> None:
        first_started, second_started = anyio.Event(), anyio.Event()

        async def respond(_name: str, query: str) -> str:
            if query == "first call":
                first_started.set()
                with anyio.fail_after(2):
                    await second_started.wait()
            else:
                with anyio.fail_after(2):
                    await first_started.wait()
                second_started.set()
            return json.dumps({"results": [{"url": _URL, "snippet": query}]})

        self.provider(respond)
        broker = self.broker("reverse")
        first, second = broker.call_many([_query("first call"), _query("second call")], _SCOPE)

        self.assertEqual([first.index, second.index], [0, 1])
        self.assertEqual([first.error, second.error], [None, None])
        self.assertEqual(
            [first.discovery[0].snippet, second.discovery[0].snippet], ["first call", "second call"]
        )
        first_start, first_end = _interval(first)
        second_start, second_end = _interval(second)
        self.assertLess(max(first_start, second_start), min(first_end, second_end))
        self.assertLess(second_end, first_end)
        for index, receipt in enumerate((first, second)):
            path = self.root / f"reverse/operation-{index:03}/receipt.json"
            self.assertEqual(LiveReceipt.model_validate_json(path.read_bytes()), receipt)

    def test_failed_calls_do_not_cancel_siblings_or_bypass_scope_and_slots(self) -> None:
        async def respond(_name: str, query: str) -> str:
            if query == "transport failure":
                raise httpx.ReadTimeout("must not appear in receipts")
            return _page(query)

        self.provider(respond)
        broker = self.broker("failures", slots=4)
        missing, sensitive, transport, fetched, blocked = broker.call_many(
            [
                _query("scope", "outside"),
                _query(f"search {_KEY}"),
                _query("transport failure"),
                _fetch(_URL),
                _query("extra"),
            ],
            _SCOPE,
        )
        self.assertEqual(
            [receipt.error for receipt in (missing, sensitive, transport, fetched, blocked)],
            [
                "invalid_candidate_scope",
                "sensitive_grounding_arguments",
                "provider_timeout",
                None,
                "grounding_budget_exhausted",
            ],
        )
        self.assertEqual([receipt.index for receipt in broker.history], [0, 1, 2, 3])
        self.assertEqual(broker.blocked, 1)
        self.assertEqual([r.provider_started_monotonic_ns for r in (missing, sensitive, blocked)], [None] * 3)
        _interval(transport)
        assert fetched.page_id is not None
        page = broker.pages[fetched.page_id]
        self.assertEqual((page.request_index, page.candidate_ids, page.url), (3, ("second",), _URL))
        page_path = self.root / f"failures/pages/{page.identity}.json"
        self.assertNotIn("monotonic_ns", page_path.read_text())
        for path in (self.root / "failures").rglob("*.json"):
            self.assertNotIn(_KEY, path.read_text())
            self.assertNotIn("must not appear in receipts", path.read_text())

    def test_total_budget_admits_only_the_first_page_and_blocks_next_wave(self) -> None:
        async def respond(_name: str, url: str) -> str:
            return _page(url)

        self.provider(respond)
        budget = len(_page(f"{_URL}/a").encode())
        broker = self.broker("budget", total_bytes=budget)
        first, second = broker.call_many([_fetch(f"{_URL}/a"), _fetch(f"{_URL}/b")], _SCOPE)
        tool, arguments = _fetch(f"{_URL}/c")
        third = broker.call(tool, arguments, _SCOPE)

        self.assertEqual(
            [receipt.error for receipt in (first, second, third)],
            [None, "provider_response_budget", "provider_response_budget"],
        )
        self.assertEqual(broker.response_bytes, budget)
        self.assertEqual([page.url for page in broker.pages.values()], [f"{_URL}/a"])
        _interval(second)
        self.assertIsNone(third.provider_started_monotonic_ns)

    def test_inflight_limit_is_shared_across_concurrent_rows(self) -> None:
        five_started, six_started, release = Event(), Event(), Event()
        guard = Lock()
        starts = 0

        async def respond(_name: str, _query_value: str) -> str:
            nonlocal starts
            with guard:
                starts += 1
                if starts == 5:
                    five_started.set()
                if starts == 6:
                    six_started.set()
            assert await anyio.to_thread.run_sync(release.wait, 5)
            return '{"results":[]}'

        self.provider(respond)
        first, second = self.broker("capacity-a"), self.broker("capacity-b", key=_SECOND_KEY)
        with ThreadPoolExecutor(2) as threads:
            wave_a = threads.submit(first.call_many, [_query(str(i)) for i in range(5)], _SCOPE)
            try:
                self.assertTrue(five_started.wait(3), "five calls never reached the provider")
                wave_b = threads.submit(second.call_many, [_query("other row")], _SCOPE)
                self.assertFalse(six_started.wait(0.2), "another row exceeded the shared in-flight limit")
            finally:
                release.set()
            first_receipts, second_receipts = wave_a.result(), wave_b.result()
        self.assertEqual([r.error for r in first_receipts + second_receipts], [None] * 6)
        self.assertTrue(six_started.is_set())

    def test_rolling_start_rate_is_shared_across_concurrent_rows(self) -> None:
        async def respond(_name: str, _query_value: str) -> str:
            return '{"results":[]}'

        self.provider(respond)
        brokers = [self.broker(f"rate-{index}") for index in range(3)]
        with ThreadPoolExecutor(3) as threads:
            waves = [
                threads.submit(broker.call_many, [_query(str(i)) for i in range(5)], _SCOPE)
                for broker in brokers
            ]
            receipts = [receipt for wave in waves for receipt in wave.result()]

        self.assertEqual([r.error for r in receipts], [None] * 15)
        starts = sorted(_interval(receipt)[0] for receipt in receipts)
        self.assertLess(starts[9] - starts[0], 1_000_000_000)
        self.assertTrue(all(later - earlier >= 1_000_000_000 for earlier, later in zip(starts, starts[10:])))

    def test_mock_wire_starts_follow_shared_rate_despite_nonuniform_mcp_handshakes(self) -> None:
        self._assert_mock_wire_window(staggered=True)

    def test_mock_wire_rate_survives_forward_wall_clock_step(self) -> None:
        self._assert_mock_wire_window(staggered=False, wall_step=2_000_000_000)

    def _assert_mock_wire_window(self, *, staggered: bool, wall_step: int = 0) -> None:
        ledger = KeenableAdmissionLedger(self.root / "ledger")
        broker = self.broker("wire")
        config = broker.config.model_copy(update={"timeout": 8})
        broker.credentials = GroundingCredentials.bind(
            config, {"KEENABLE_API_KEY": _KEY}.get, keenable_ledger=ledger
        )
        broker.config = config
        credentials = broker.credentials.keenable_pool
        assert credentials is not None
        collection = DiscoveryRun(self.root, KeenableTransport(credentials, ledger=ledger))
        wire: list[tuple[int, str]] = []
        lock = Lock()
        first_five_initializing = Event()
        initializations = 0
        wall_offset = 0
        obj = TypeAdapter(dict[str, JsonValue])
        string = TypeAdapter(str)

        async def respond(request: httpx.Request) -> httpx.Response:
            nonlocal initializations, wall_offset
            message = obj.validate_json(request.content)
            result: dict[str, JsonValue]
            match message["method"]:
                case "initialize":
                    with lock:
                        index = initializations
                        initializations += 1
                        if initializations == 5:
                            first_five_initializing.set()
                    if staggered and index < 5:
                        await anyio.sleep(0.68 + index * 0.02)
                    result = {
                        "protocolVersion": obj.validate_python(message["params"])["protocolVersion"],
                        "capabilities": {},
                        "serverInfo": {"name": "synthetic", "version": "1"},
                    }
                case "notifications/initialized":
                    return httpx.Response(202)
                case "tools/list":
                    result = {
                        "tools": [
                            {"name": name, "inputSchema": {"type": "object"}}
                            for name in ("search_web_pages", "fetch_page_content")
                        ]
                    }
                case "tools/call":
                    params = obj.validate_python(message["params"])
                    payload = obj.validate_python(params["arguments"])
                    name = string.validate_python(params["name"])
                    operation = string.validate_python(payload.get("query", name))
                    with lock:
                        wire.append((monotonic_ns(), operation))
                        if len(wire) == 10:
                            wall_offset = wall_step
                    text = '{"results":[]}' if name == "search_web_pages" else _page(_URL)
                    result = {"content": [{"type": "text", "text": text}]}
                case _:
                    raise AssertionError("Unexpected synthetic MCP request")
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": message["id"], "result": result})

        def client(*, headers: dict[str, str], timeout: float, trust_env: bool) -> AsyncClient:
            return AsyncClient(
                headers=headers, timeout=timeout, trust_env=trust_env, transport=httpx.MockTransport(respond)
            )

        actions: list[tuple[Literal["query", "page_fetch"], str]] = [
            *(_query(f"labeling {i}") for i in range(13)),
            _fetch(_URL),
        ]
        with (
            patch.object(keenable_admission, "time_ns", lambda: monotonic_ns() + wall_offset, create=True),
            patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", side_effect=client),
            ThreadPoolExecutor(2) as threads,
        ):
            wave = threads.submit(broker.call_many, actions, _SCOPE)
            self.assertTrue(first_five_initializing.wait(5))
            discovered = threads.submit(anyio.run, collection.search, "collection", (8, 500), 1)
            receipts = wave.result()
            self.assertEqual(discovered.result().results, ())

        self.assertEqual([r.error for r in receipts], [None] * 14)
        self.assertEqual(len(wire), 15)
        self.assertIn("collection", [operation for _, operation in wire])
        self.assertIn("fetch_page_content", [operation for _, operation in wire])
        starts = sorted(start for start, _ in wire)
        self.assertGreaterEqual(
            min(later - earlier for earlier, later in zip(starts, starts[10:])),
            1_000_000_000,
            f"Rolling window exceeded; wire offsets in seconds: {[(start - starts[0]) / 1e9 for start in starts]}",
        )

    def test_sdk_serialized_credential_echo_is_not_retained_in_page_or_search_receipt(self) -> None:
        key = r"synthetic\"serialized-field-marker"
        decoded = 'synthetic"serialized-field-marker'
        returned_url = f"{_URL}/{decoded}"
        cases = (
            (_fetch(_URL), {"url": returned_url, "content": "Synthetic page"}),
            (_query("synthetic query"), {"results": [{"url": _URL, "snippet": decoded}]}),
        )
        for index, ((tool, arguments), body) in enumerate(cases):
            with self.subTest(tool=tool):
                envelope = json.dumps(body)
                self.assertIn(key, envelope)
                self.assertNotIn(key, decoded)
                name = f"serialized-{index}"
                broker = self.broker(name)
                broker.credentials = GroundingCredentials.bind(
                    broker.config, {"KEENABLE_API_KEY": _KEY, "KEENABLE_API_KEY_1": key}.get
                )
                client = _mcp_client(envelope, tool_error=False, statuses=(200,))
                with (
                    patch("codebench_pipeline.keenable_mcp.httpx.AsyncClient", side_effect=client),
                    self.assertNoLogs("codebench_pipeline.keenable_provider", level="WARNING"),
                ):
                    receipt = broker.call(tool, arguments, _SCOPE)

                self.assertEqual(
                    (receipt.page_id, receipt.response_sha256, receipt.discovery), (None, None, ())
                )
                if tool == "page_fetch":
                    assert receipt.provider_error is not None
                    self.assertEqual(receipt.provider_error.code, "sensitive_provider_response")
                else:
                    self.assertIsNone(receipt.error)
                self.assertEqual(broker.pages, {})
                artifact_dir = self.root / name
                self.assertEqual(list(artifact_dir.glob("pages/*.json")), [])
                for path in artifact_dir.rglob("*.json"):
                    self.assertNotIn(key.encode(), path.read_bytes(), path.name)
