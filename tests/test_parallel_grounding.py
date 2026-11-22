from __future__ import annotations

import json
import tempfile
import unittest
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event, Lock
from typing import Literal
from unittest.mock import patch

import anyio
import httpx
from mcp.types import CallToolResult, TextContent

from codebench_pipeline import live_grounding
from codebench_pipeline.grounding_tools import GroundingCredentials, GroundingFiles, LiveGroundingConfig
from codebench_pipeline.live_grounding import LiveGrounding, LiveReceipt

_KEY = "synthetic-provider-key"
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


class _Provider:
    def __init__(self, respond: Callable[[str, str], Awaitable[str]]) -> None:
        self.respond = respond

    async def call_tool(self, name: str, arguments: dict[str, str | int | bool]) -> CallToolResult:
        value = str(arguments["query" if name == "search_web_pages" else "url"])
        return CallToolResult(content=[TextContent(type="text", text=await self.respond(name, value))])

    @asynccontextmanager
    async def session(self, key: str) -> AsyncGenerator[_Provider]:
        assert key == _KEY
        yield self


class ParallelGroundingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(patch.object(live_grounding, "_STARTS", deque[int]()))

    def broker(self, name: str, *, slots: int = 20, total_bytes: int = 4096) -> LiveGrounding:
        return LiveGrounding(
            LiveGroundingConfig(
                allowed_domains=["example.org"], maximum_total_response_bytes=total_bytes, timeout=5
            ),
            GroundingCredentials(keenable=_KEY),
            GroundingFiles(self.root / name),
            slots,
        )

    def provider(self, respond: Callable[[str, str], Awaitable[str]]) -> None:
        self.enterContext(patch.object(live_grounding, "search_session", _Provider(respond).session))

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
                "provider_transport_failed",
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
        first, second = self.broker("capacity-a"), self.broker("capacity-b")
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
