from __future__ import annotations

import io
import json
import tempfile
import unittest
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from queue import Queue
from threading import Event, Thread
from types import TracebackType
from typing import Self
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request

from dagster import Config, DagsterInvalidInvocationError
from pydantic import JsonValue, ValidationError

from codebench_pipeline.agent_events import parse_json
from codebench_pipeline.inference_capacity import (
    CapacityConfig,
    CapacityQuarantined,
    CapacityUnavailable,
    ContextBudgetExceeded,
    guarded_complete,
)
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.typed_config import TypedConfig

BASE = "http://capacity.example:8080/v1"
MODEL = "MiniMaxAI/MiniMax-M3-MXFP8"
WEIGHTS = "/opt/zai-org/GLM-5.3-Flash"
CAPACITY = 871296
CONTEXT = 262144
CLOCK = 1794952800.0


class _NestedConfig(TypedConfig):
    capacity: CapacityConfig


def _json(value: JsonValue) -> bytes:
    return json.dumps(value, allow_nan=False).encode("utf-8")


def _object(raw: bytes) -> dict[str, JsonValue]:
    value = parse_json(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise AssertionError("Expected fixture object")
    return value


@dataclass(slots=True)
class _Clock:
    """Advance deterministic time only when a production wait or fake I/O occurs."""

    now: float = 0.0
    sleeps: list[float] = field(default_factory=list)
    on_sleep: Callable[[float], None] | None = None

    def monotonic(self) -> float:
        return self.now

    def monotonic_ns(self) -> int:
        return int(self.now * 1_000_000_000)

    def sleep(self, seconds: float) -> None:
        if self.on_sleep is not None:
            self.on_sleep(seconds)
        self.sleeps.append(seconds)
        self.now += seconds


@dataclass(frozen=True, slots=True)
class _Observation:
    used: int = 66944
    running: int = 2
    capacity: int = CAPACITY
    slots: int = 32
    queued: int = 0
    age: float = 0.0
    server_time: float = CLOCK
    rank_used: tuple[int, ...] = ()
    rank_running: tuple[int, ...] = ()
    rank_capacity: tuple[int, ...] = ()
    extra: tuple[tuple[str, float], ...] = ()
    missing: str | None = None
    missing_rank: int | None = None
    metric_model: str = MODEL
    metric_age: float | None = None
    inconsistent_usage: bool = False
    leader_only_metrics: bool = False
    rounded_usage: bool = False
    queue_history: bool = False

    def loads(self) -> JsonValue:
        row: dict[str, JsonValue] = {
            "timestamp": self.server_time - self.age,
            "dp_rank": 0,
            "num_running_reqs": self.running,
            "num_waiting_reqs": self.queued,
            "num_used_tokens": self.used,
            "num_total_tokens": self.used,
            "max_total_num_tokens": self.capacity,
            "max_running_requests": self.slots,
            "token_usage": self.used / self.capacity,
            **dict(self.extra),
        }
        return {
            "timestamp": datetime.fromtimestamp(self.server_time, UTC).isoformat(),
            "version": "fixture-v1",
            "loads": [row],
        }

    def metrics(self) -> bytes:
        lines: list[str] = []
        for rank in range(8):
            used = self.rank_used[rank] if self.rank_used else self.used
            running = self.rank_running[rank] if self.rank_running else self.running
            capacity = self.rank_capacity[rank] if self.rank_capacity else self.capacity
            auxiliary = dict(self.extra)
            pool_usage = max(
                used / capacity,
                auxiliary.get("full_token_usage", 0),
                auxiliary.get("swa_token_usage", 0),
                auxiliary.get("mamba_usage", 0),
            )
            values = {
                "num_running_reqs": running,
                "num_queue_reqs": self.queued,
                "num_used_tokens": used,
                "max_total_num_tokens": capacity,
                "token_usage": (
                    0.0
                    if self.inconsistent_usage
                    else round(pool_usage, 2)
                    if self.rounded_usage
                    else pool_usage
                ),
                **dict(self.extra),
            }
            for name, value in values.items():
                if self.leader_only_metrics and rank != 0 and name != "max_total_num_tokens":
                    continue
                if name == self.missing or rank == self.missing_rank:
                    continue
                labels = f'model_name="{self.metric_model}",tp_rank="{rank}",dp_rank="0"'
                stamp = "" if self.metric_age is None else f" {(self.server_time - self.metric_age) * 1000}"
                lines.append(f"sglang:{name}{{{labels}}} {value}{stamp}")
        if self.queue_history:
            labels = f'model_name="{self.metric_model}",tp_rank="0",dp_rank="0"'
            lines.extend(
                [
                    "# TYPE sglang:queue_time_seconds histogram",
                    f'sglang:queue_time_seconds_bucket{{{labels},le="+Inf"}} 1000',
                    f"sglang:queue_time_seconds_sum{{{labels}}} 42",
                    f"sglang:queue_time_seconds_count{{{labels}}} 1000",
                    f"sglang:queue_time_seconds_created{{{labels}}} {CLOCK}",
                ]
            )
        return "\n".join(lines).encode()


@dataclass(frozen=True, slots=True)
class _Response:
    payload: bytes
    headers: Message = field(default_factory=Message)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


@dataclass(slots=True)
class _Backend:
    """A wire-boundary fake with scripted scheduler state and exact token counts."""

    clock: _Clock
    observations: list[_Observation] = field(default_factory=lambda: [_Observation()])
    info: dict[str, JsonValue] = field(
        default_factory=lambda: {
            "model_path": WEIGHTS,
            "tokenizer_path": WEIGHTS,
            "served_model_name": MODEL,
            "context_length": CONTEXT,
            "max_running_requests": 32,
            "tp_size": 8,
            "dp_size": 1,
            "version": "fixture-v1",
            "enable_metrics_for_all_schedulers": True,
            "internal_states": [{"memory_usage": {"token_capacity_swa": None}}],
        }
    )
    snapshot_index: int = 0
    active: _Observation = field(default_factory=_Observation)
    paths: list[str] = field(default_factory=list)
    generations: list[dict[str, JsonValue]] = field(default_factory=list)
    tokenizations: list[dict[str, JsonValue]] = field(default_factory=list)
    token_count: int = 24
    tokenizer_limit: int = 1048576
    token_count_delta: int = 0
    finish_reason: str = "stop"
    completion_error: OSError | HTTPError | None = None
    crash: bool = False
    abort_response: bool = False
    echoed_secret: str = ""
    network_seconds: float = 0.0
    entered: Event | None = None
    release: Event | None = None

    def complete(self, url: str, body: bytes, key: str | None, timeout: float) -> HTTPResponse:
        response = self.open(Request(url, data=body, method="POST"), timeout)
        return HTTPResponse(response.read(), None)

    def open(self, request: Request, timeout: float) -> _Response:
        if timeout <= 0:
            raise AssertionError("Unbounded or exhausted timeout")
        self.clock.now += self.network_seconds
        path = urlsplit(request.full_url).path
        self.paths.append(path)
        if request.get_method() == "GET":
            if path == "/get_server_info":
                return _Response(_json(self.info))
            if path == "/v1/loads":
                if urlsplit(request.full_url).query != "include=core":
                    raise AssertionError("Missing scheduler-core observation")
                self.active = self.observations[min(self.snapshot_index, len(self.observations) - 1)]
                self.snapshot_index += 1
                return _Response(_json(self.active.loads()))
            if path == "/metrics":
                return _Response(self.active.metrics())
            raise AssertionError(f"Unexpected telemetry path: {path}")
        if request.data is None:
            raise AssertionError("Missing POST body")
        payload = _object(request.data)
        if path == "/v1/tokenize":
            self.tokenizations.append(payload)
            count = self.token_count
            for name, weight in (("tools", 7), ("tool_choice", 3), ("chat_template_kwargs", 5)):
                if name in payload:
                    count += weight
            return _Response(
                _json(
                    {
                        "count": count + self.token_count_delta,
                        "max_model_len": self.tokenizer_limit,
                        "tokens": list(range(count)),
                    }
                )
            )
        if path != "/v1/chat/completions":
            raise AssertionError(f"Unexpected generation path: {path}")
        self.generations.append(payload)
        if self.entered is not None:
            self.entered.set()
        if self.release is not None and not self.release.wait(timeout=5):
            raise AssertionError("Generation fixture was not released")
        if self.crash:
            raise SystemExit("fixture worker died after send")
        if self.completion_error is not None:
            raise self.completion_error
        if self.abort_response:
            return _Response(_json({"success": True}))
        return _Response(
            _json(
                {
                    "id": "chat-fixture",
                    "model": MODEL,
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": self.finish_reason,
                            "message": {"role": "assistant", "content": self.echoed_secret},
                        }
                    ],
                }
            )
        )


@contextmanager
def _wire(backend: _Backend) -> Generator[None]:
    with (
        patch("codebench_pipeline.inference_capacity.build_opener", return_value=backend),
        patch("codebench_pipeline.inference_capacity.complete", side_effect=backend.complete),
        patch("codebench_pipeline.inference_capacity.time", backend.clock),
    ):
        yield


class InferenceCapacityTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = CapacityConfig(
            state_directory=str(self.root / "state"),
            expected_model_path=WEIGHTS,
            poll_seconds=1,
            maximum_wait_seconds=3,
        )
        self.clock = _Clock()
        self.backend = _Backend(self.clock)
        self.body: dict[str, JsonValue] = {
            "model": MODEL,
            "messages": [{"role": "user", "content": "inert fixture prompt"}],
            "max_completion_tokens": 100,
        }

    def call(self, *, base: str = BASE, key: str | None = None, out: str = "out") -> HTTPResponse:
        return guarded_complete(base, _json(self.body), key, 30, self.config, self.root / out)

    def ledger(self) -> dict[str, JsonValue]:
        paths = list((self.root / "state").glob("*.json"))
        self.assertEqual(len(paths), 1)
        return _object(paths[0].read_bytes())

    def events(self) -> list[dict[str, JsonValue]]:
        return [_object(path.read_bytes()) for path in self.root.glob("out/capacity-*/*.json")]

    def test_config_is_strict_frozen_and_serializes_as_nested_dagster_config(self) -> None:
        nested = _NestedConfig(capacity=self.config)
        self.assertIsInstance(nested.capacity, Config)
        serialized = nested._convert_to_config_dictionary()
        capacity = serialized["capacity"]
        self.assertEqual(capacity["state_directory"], str(self.root / "state"))
        self.assertEqual(capacity["max_fraction"], 0.5)
        with self.assertRaises(ValidationError):
            self.config.model_validate({**self.config.model_dump(), "max_fraction": 0.500001})
        with self.assertRaises(ValidationError):
            self.config.model_validate({**self.config.model_dump(), "context_length": "262144"})
        with self.assertRaises(DagsterInvalidInvocationError):
            self.config.max_fraction = 0.25

    def test_replicated_tp_metrics_do_not_sum_and_terminal_length_releases(self) -> None:
        self.backend.finish_reason = "length"
        with _wire(self.backend):
            response = self.call()
            self.call(out="next-turn")
        self.assertEqual(_object(response.body)["model"], MODEL)
        ledger = self.ledger()
        self.assertEqual(
            (ledger["state"], ledger["reservation"], ledger["reason"]), ("terminal", 0, "length")
        )
        admissions = [event for event in self.events() if event["event"] == "admission"]
        self.assertEqual(admissions[0]["observed_tokens"], 66944)
        self.assertEqual(admissions[0]["observed_running"], 2)
        self.assertEqual(admissions[0]["reservation"], CONTEXT)
        self.assertEqual(len(self.backend.generations), 2)
        self.assertEqual(self.backend.snapshot_index, 4)
        first_rid = self.backend.generations[0]["rid"]
        self.assertIsInstance(first_rid, str)
        self.assertNotEqual(first_rid, self.backend.generations[1]["rid"])
        self.assertEqual(str(uuid.UUID(str(first_rid))), first_rid)

    def test_exact_half_token_and_slot_boundaries_admit_but_one_more_blocks(self) -> None:
        for used, running, allowed in (
            (173504, 15, True),
            (173505, 15, False),
            (173504, 16, False),
        ):
            with self.subTest(used=used, running=running), tempfile.TemporaryDirectory() as temporary:
                config = self.config.model_copy(update={"state_directory": str(Path(temporary) / "state")})
                backend = _Backend(_Clock(), observations=[_Observation(used=used, running=running)])
                with _wire(backend):
                    if allowed:
                        guarded_complete(BASE, _json(self.body), None, 30, config, Path(temporary) / "out")
                    else:
                        with self.assertRaises(CapacityUnavailable):
                            guarded_complete(
                                BASE, _json(self.body), None, 30, config, Path(temporary) / "out"
                            )
                self.assertEqual(len(backend.generations), int(allowed))

    def test_leader_only_scheduler_metrics_use_reported_mode_without_summing_capacity(self) -> None:
        self.backend.info["enable_metrics_for_all_schedulers"] = False
        self.backend.observations = [_Observation(leader_only_metrics=True)]
        with _wire(self.backend):
            self.call()
        admission = next(event for event in self.events() if event["event"] == "admission")
        self.assertEqual(admission["capacity"], CAPACITY)
        self.assertEqual(admission["observed_running"], 2)
        self.assertEqual(len(self.backend.generations), 1)

    def test_rounded_token_usage_remains_conservative_against_exact_token_count(self) -> None:
        self.backend.observations = [_Observation(rounded_usage=True)]
        with _wire(self.backend):
            self.call()
        admission = next(event for event in self.events() if event["event"] == "admission")
        self.assertEqual(admission["observed_tokens"], 69704)

    def test_historical_queue_histograms_are_not_current_side_load(self) -> None:
        self.backend.observations = [_Observation(queue_history=True)]
        with _wire(self.backend):
            self.call()
        self.assertEqual(len(self.backend.generations), 1)

    def test_explicitly_inactive_swa_pool_can_report_zero_usage_without_capacity_gauge(self) -> None:
        self.backend.observations = [_Observation(extra=(("swa_token_usage", 0.0), ("mamba_usage", 0.0)))]
        with _wire(self.backend):
            self.call()
        self.assertEqual(len(self.backend.generations), 1)

    def test_hybrid_aggregate_usage_includes_live_mamba_states_without_requiring_idle_server(self) -> None:
        self.backend.info["enable_metrics_for_all_schedulers"] = False
        self.backend.observations = [
            _Observation(
                used=10000,
                leader_only_metrics=True,
                rounded_usage=True,
                extra=(("full_token_usage", 10000 / CAPACITY), ("mamba_usage", 8 / 230)),
            )
        ]
        with _wire(self.backend):
            self.call()
        admission = next(event for event in self.events() if event["event"] == "admission")
        self.assertEqual(admission["observed_tokens"], 30306)
        self.assertEqual(len(self.backend.generations), 1)

    def test_worst_rank_pressure_minimum_capacity_and_queues_block(self) -> None:
        for observation in (
            _Observation(rank_used=(0, 0, 0, 0, 0, 0, 0, 173505)),
            _Observation(rank_running=(0, 0, 0, 0, 0, 0, 0, 16)),
            _Observation(rank_capacity=(CAPACITY,) * 7 + (524288,)),
            _Observation(queued=1),
        ):
            with self.subTest(observation=observation):
                backend = _Backend(_Clock(), observations=[observation])
                with _wire(backend), self.assertRaises(CapacityUnavailable):
                    self.call()
                self.assertEqual(backend.generations, [])

    def test_high_load_waits_then_resumes_with_fresh_safe_metrics(self) -> None:
        self.backend.observations = [
            _Observation(used=250000),
            _Observation(used=250000),
            _Observation(),
        ]
        with _wire(self.backend):
            self.call()
        self.assertEqual(self.clock.sleeps, [1])
        self.assertEqual(len(self.backend.generations), 1)
        waits = [event for event in self.events() if event["event"] == "wait"]
        self.assertEqual(len(waits), 1)
        self.assertEqual(waits[0]["reason"], "observed_pressure")

    def test_transiently_inconsistent_gauges_wait_for_consistent_snapshot_before_generation(self) -> None:
        self.backend.observations = [_Observation(inconsistent_usage=True), _Observation()]
        with _wire(self.backend):
            self.call()
        self.assertEqual(self.clock.sleeps, [1])
        self.assertEqual(len(self.backend.generations), 1)
        waits = [event for event in self.events() if event["event"] == "wait"]
        self.assertEqual(len(waits), 1)
        self.assertEqual(waits[0]["reason"], "inconsistent_token_metrics")

    def test_frozen_scheduler_is_stale_despite_advancing_handler_clock(self) -> None:
        self.backend.observations = [
            _Observation(age=0, server_time=CLOCK),
            _Observation(age=20, server_time=CLOCK + 20),
        ]
        with _wire(self.backend), self.assertRaises(CapacityUnavailable) as raised:
            self.call()
        self.assertEqual(raised.exception.reason, "stale_snapshot")
        self.assertEqual(self.backend.generations, [])
        self.assertNotIn("/get_load", self.backend.paths)

    def test_client_request_duration_is_added_to_server_clock_snapshot_age(self) -> None:
        self.backend.observations = [_Observation(age=8)]
        self.backend.network_seconds = 1
        with _wire(self.backend), self.assertRaises(CapacityUnavailable) as raised:
            self.call()
        self.assertEqual(raised.exception.reason, "stale_snapshot")
        self.assertEqual(self.backend.generations, [])

    def test_missing_inconsistent_and_stale_metric_families_fail_closed(self) -> None:
        for observation in (
            _Observation(missing="num_used_tokens"),
            _Observation(missing_rank=7),
            _Observation(metric_model="another-model"),
            _Observation(metric_age=11),
            _Observation(inconsistent_usage=True),
        ):
            with self.subTest(observation=observation):
                backend = _Backend(_Clock(), observations=[observation])
                with _wire(backend), self.assertRaises(CapacityUnavailable):
                    self.call()
                self.assertEqual(backend.generations, [])

    def test_auxiliary_queues_pending_and_pool_pressure_block_admission(self) -> None:
        for extra in (
            (("num_grammar_queue_reqs", 1.0),),
            (("num_prefill_inflight_queue_reqs", 1.0),),
            (("pending_prealloc_token_usage", 0.01),),
            (("full_token_usage", 0.3),),
            (("mamba_usage", 0.51),),
            (("swa_token_usage", 0.1), ("max_total_num_tokens_swa", 300000.0)),
            (("swa_token_usage", 0.1),),
        ):
            with self.subTest(extra=extra):
                backend = _Backend(_Clock(), observations=[_Observation(extra=extra)])
                with _wire(backend), self.assertRaises(CapacityUnavailable):
                    self.call()
                self.assertEqual(backend.generations, [])

    def test_backend_tokenizer_and_request_alias_mismatches_fail_before_generation(self) -> None:
        for field_name in ("model_path", "tokenizer_path", "served_model_name"):
            with self.subTest(field=field_name):
                backend = _Backend(_Clock())
                backend.info[field_name] = "unexpected-backend"
                with _wire(backend), self.assertRaises(CapacityUnavailable):
                    self.call()
                self.assertEqual(backend.generations, [])
                self.assertEqual(backend.tokenizations, [])

    def test_tokenizer_counts_tools_and_template_and_enforces_smaller_server_limit(self) -> None:
        self.body.update(
            {
                "tools": [
                    {"type": "function", "function": {"name": "submit", "parameters": {"type": "object"}}}
                ],
                "tool_choice": {"type": "function", "function": {"name": "submit"}},
                "chat_template_kwargs": {"enable_thinking": True},
                "max_completion_tokens": CONTEXT - 39,
            }
        )
        with _wire(self.backend):
            self.call()
            self.body["max_completion_tokens"] = CONTEXT - 38
            with self.assertRaises(ContextBudgetExceeded) as raised:
                self.call(out="too-large")
        self.assertEqual(str(raised.exception), "context_budget_exceeded")
        self.assertNotIsInstance(raised.exception, CapacityUnavailable)
        self.assertEqual(len(self.backend.generations), 1)
        for name in ("messages", "tools", "tool_choice", "chat_template_kwargs"):
            self.assertEqual(self.backend.tokenizations[0][name], self.body[name])
        admission = next(event for event in self.events() if event["event"] == "admission")
        self.assertEqual(admission["prompt_tokens"], 39)
        self.assertEqual(admission["tokenizer_max_model_len"], 1048576)

    def test_missing_output_budget_or_inconsistent_tokenization_never_generates(self) -> None:
        with _wire(self.backend):
            self.body.pop("max_completion_tokens")
            with self.assertRaises(CapacityUnavailable):
                self.call()
            for budget in (None, 0, True, "100"):
                with self.subTest(budget=budget):
                    self.body["max_completion_tokens"] = budget
                    with self.assertRaises(CapacityUnavailable):
                        self.call()
            self.body["max_completion_tokens"] = 100
            self.backend.token_count_delta = 1
            with self.assertRaises(CapacityUnavailable):
                self.call()
        self.assertEqual(self.backend.generations, [])

    def test_tokenizer_smaller_limit_is_respected(self) -> None:
        self.backend.tokenizer_limit = 123
        with _wire(self.backend), self.assertRaises(ContextBudgetExceeded) as raised:
            self.call()
        self.assertEqual(str(raised.exception), "context_budget_exceeded")
        self.assertEqual(self.backend.generations, [])

    def test_timeout_followed_by_idle_keeps_full_quarantined_reservation(self) -> None:
        self.backend.completion_error = TimeoutError("fixture credential must not be persisted")
        with _wire(self.backend):
            with self.assertRaises(CapacityQuarantined):
                self.call()
            self.backend.completion_error = None
            self.backend.observations = [_Observation(used=0, running=0)]
            before = len(self.backend.paths)
            self.clock.now += 100000
            with self.assertRaises(CapacityQuarantined):
                self.call(base=BASE + "/", out="idle")
        self.assertEqual(len(self.backend.paths), before)
        self.assertEqual(len(self.backend.generations), 1)
        self.assertEqual((self.ledger()["state"], self.ledger()["reservation"]), ("quarantined", CONTEXT))

    def test_crashed_submitted_ledger_survives_worker_death(self) -> None:
        self.backend.crash = True
        with _wire(self.backend):
            with self.assertRaises(SystemExit):
                self.call()
            self.assertEqual(self.ledger()["state"], "submitted")
            self.backend.crash = False
            with self.assertRaises(CapacityQuarantined):
                self.call(out="after-crash")
        self.assertEqual(len(self.backend.generations), 1)
        self.assertEqual(self.ledger()["reservation"], CONTEXT)

    def test_http_errors_and_generic_abort_success_do_not_release_reservation(self) -> None:
        for status in (400, 401, 429, 500, 503, None):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary:
                backend = _Backend(_Clock())
                if status is None:
                    backend.abort_response = True
                else:
                    backend.completion_error = HTTPError(
                        BASE,
                        status,
                        "provider-secret",
                        Message(),
                        io.BytesIO(b"provider-secret"),
                    )
                config = self.config.model_copy(update={"state_directory": str(Path(temporary) / "state")})
                with _wire(backend), self.assertRaises(CapacityQuarantined):
                    guarded_complete(BASE, _json(self.body), None, 30, config, Path(temporary) / "out")
                ledger_path = next((Path(temporary) / "state").glob("*.json"))
                ledger = _object(ledger_path.read_bytes())
                self.assertEqual((ledger["state"], ledger["reservation"]), ("quarantined", CONTEXT))
                for receipt in (Path(temporary) / "out").rglob("*.json"):
                    self.assertNotIn("provider-secret", receipt.read_text())

    def test_receipts_do_not_store_prompt_response_or_credentials(self) -> None:
        secret = "fixture-sensitive-value"
        self.body["messages"] = [{"role": "user", "content": secret}]
        self.backend.echoed_secret = secret
        with _wire(self.backend):
            self.call(key=secret)
        for path in self.root.rglob("*.json"):
            self.assertNotIn(secret, path.read_text())
        self.assertEqual(self.ledger()["identity"], self.backend.info)

    def test_simultaneous_callers_cannot_overlap_owned_generation(self) -> None:
        entered, release = Event(), Event()
        self.backend.entered, self.backend.release = entered, release
        results: Queue[HTTPResponse | BaseException] = Queue()

        def first() -> None:
            try:
                results.put(self.call())
            except BaseException as error:
                results.put(error)

        def blocked_wait(seconds: float) -> None:
            raise CapacityUnavailable("fixture_observed_lock_contention")

        thread = Thread(target=first, name="capacity-owner-fixture")
        with _wire(self.backend):
            thread.start()
            try:
                self.assertTrue(entered.wait(timeout=5), "Owner never submitted")
                self.assertEqual(self.ledger()["state"], "submitted")
                self.clock.on_sleep = blocked_wait
                try:
                    with self.assertRaises(CapacityUnavailable) as raised:
                        self.call(base="http://CAPACITY.EXAMPLE:8080/v1/", out="contender")
                finally:
                    self.clock.on_sleep = None
                self.assertEqual(raised.exception.reason, "fixture_observed_lock_contention")
                self.assertEqual(len(self.backend.generations), 1)
                self.assertEqual(self.ledger()["reservation"], CONTEXT)
            finally:
                release.set()
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive(), "Owner did not terminate")
            result = results.get(timeout=5)
            if isinstance(result, BaseException):
                raise result
        self.assertEqual(self.ledger()["state"], "terminal")


if __name__ == "__main__":
    unittest.main()
