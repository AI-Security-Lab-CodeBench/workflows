from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from http.client import HTTPException
from pathlib import Path
from typing import Final, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, build_opener

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, JsonValue, ValidationError

from .agent_events import InferenceError, parse_json
from .openai_transport import HTTPResponse, NoRedirect, complete
from .typed_config import TypedConfig

_LOG: Final = logging.getLogger(__name__)
_CORE: Final = frozenset(
    {
        "num_running_reqs",
        "num_queue_reqs",
        "num_used_tokens",
        "max_total_num_tokens",
        "token_usage",
    }
)
_SAMPLE: Final = re.compile(
    r"(?P<name>sglang:[a-zA-Z_][a-zA-Z_0-9]*)(?:\{(?P<labels>.*)\})?"
    r"\s+(?P<value>\S+)(?:\s+(?P<timestamp>\S+))?"
)
_LABEL: Final = re.compile(r'([a-zA-Z_][a-zA-Z_0-9]*)="((?:[^"\\]|\\["\\n])*)"(?:,|$)')


class CapacityUnavailable(InferenceError):
    """Admission failed before generation; no submission is authorized."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class CapacityQuarantined(CapacityUnavailable):
    """A durable reservation may still own server work and cannot be reused."""


class ContextBudgetExceeded(InferenceError):
    """Exact prompt plus output tokens exceed the effective conversation limit."""


class CapacityConfig(TypedConfig):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    state_directory: str = Field(min_length=1, pattern=r"\S")
    expected_model_path: str = Field(min_length=1, pattern=r"\S")
    context_length: int = Field(default=262144, gt=0)
    max_fraction: float = Field(default=0.5, gt=0, le=0.5, allow_inf_nan=False)
    max_snapshot_age_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)
    poll_seconds: float = Field(default=5, gt=0, allow_inf_nan=False)
    maximum_wait_seconds: float = Field(default=900, gt=0, allow_inf_nan=False)


class _MemoryPools(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    token_capacity_swa: int | None = Field(ge=0)


class _SchedulerState(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    memory_usage: _MemoryPools


class _Server(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    model_path: str = Field(min_length=1)
    tokenizer_path: str = Field(min_length=1)
    served_model_name: str = Field(min_length=1)
    context_length: int = Field(gt=0)
    max_running_requests: int = Field(gt=0)
    tp_size: int = Field(gt=0, validation_alias=AliasChoices("tp_size", "tp"))
    dp_size: int = Field(gt=0, validation_alias=AliasChoices("dp_size", "dp"))
    version: str = Field(min_length=1)
    enable_metrics_for_all_schedulers: bool
    internal_states: list[_SchedulerState] = Field(min_length=1)


class _Load(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="allow")

    timestamp: float = Field(gt=0, allow_inf_nan=False)
    dp_rank: int = Field(ge=0)
    num_running_reqs: int = Field(ge=0)
    num_waiting_reqs: int = Field(ge=0)
    num_used_tokens: int = Field(ge=0)
    num_total_tokens: int = Field(ge=0)
    max_total_num_tokens: int = Field(gt=0)
    max_running_requests: int = Field(gt=0)
    token_usage: float = Field(ge=0, le=1, allow_inf_nan=False)


class _Loads(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    timestamp: str
    version: str
    loads: list[_Load] = Field(min_length=1)


class _Chat(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="allow")

    model: str = Field(min_length=1)
    messages: list[dict[str, JsonValue]] = Field(min_length=1)
    max_completion_tokens: int = Field(gt=0)
    max_tokens: None = None
    stream: Literal[False] = False
    n: Literal[1] = 1


class _Tokenization(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    count: int = Field(ge=0)
    max_model_len: int = Field(gt=0)
    tokens: list[int]


class _Choice(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    index: Literal[0]
    finish_reason: Literal["stop", "length", "tool_calls", "function_call", "content_filter"]
    message: dict[str, JsonValue]


class _Terminal(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    id: str = Field(min_length=1)
    model: str = Field(min_length=1)
    choices: list[_Choice] = Field(min_length=1, max_length=1)
    error: None = None
    status: Literal["completed"] | None = None


class _Ledger(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    endpoint: str
    rid: str
    state: Literal["reserved", "submitted", "terminal", "quarantined"]
    reservation: int = Field(ge=0)
    identity: _Server
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _Metric:
    name: str
    rank: tuple[int, int]
    value: float
    timestamp: float | None


@dataclass(frozen=True, slots=True)
class _Snapshot:
    identity: _Server
    tokens: float
    capacity: float
    running: float
    slots: int
    queued: bool
    auxiliary_blocked: bool
    age: float
    started: float

    def admits(self, config: CapacityConfig) -> bool:
        return (
            not self.queued
            and not self.auxiliary_blocked
            and self.tokens + config.context_length <= config.max_fraction * self.capacity
            and self.running + 1 <= math.floor(config.max_fraction * self.slots)
        )


def _decode(raw: bytes) -> JsonValue:
    try:
        return parse_json(raw.decode("utf-8"))
    except (UnicodeError, ValueError, InferenceError) as error:
        raise CapacityUnavailable("invalid_json") from error


def _redact(value: JsonValue, key: str | None) -> JsonValue:
    match value:
        case str():
            return value.replace(key, "[REDACTED]") if key else value
        case list():
            return [_redact(item, key) for item in value]
        case dict():
            return {name: _redact(item, key) for name, item in value.items()}
        case _:
            return value


def _durable(path: Path, value: JsonValue, key: str | None) -> None:
    temporary = path.with_name(path.name + "." + str(uuid.uuid4()) + ".tmp")
    data = json.dumps(_redact(value, key), allow_nan=False, sort_keys=True).encode("utf-8")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _receipt(directory: Path, event: str, values: dict[str, JsonValue], key: str | None) -> None:
    _durable(
        directory / f"{time.monotonic_ns()}-{uuid.uuid4()}-{event}.json", {"event": event, **values}, key
    )


def _endpoint(base_url: str) -> tuple[str, str]:
    try:
        parts = urlsplit(base_url)
        port = parts.port
    except ValueError as error:
        raise CapacityUnavailable("invalid_base_url") from error
    if (
        parts.scheme not in {"http", "https"}
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or parts.path.rstrip("/") != "/v1"
    ):
        raise CapacityUnavailable("invalid_base_url")
    host = parts.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    root = f"{parts.scheme}://{host}:{port or (443 if parts.scheme == 'https' else 80)}"
    return root + "/v1", root


def _get(url: str, key: str | None, timeout: float) -> bytes:
    headers = {"Accept": "application/json, text/plain"}
    if key is not None:
        headers["Authorization"] = "Bearer " + key
    try:
        with build_opener(NoRedirect()).open(Request(url, headers=headers), timeout=timeout) as response:
            result: bytes = response.read()
            return result
    except HTTPError as error:
        status = error.code
        error.close()
        raise CapacityUnavailable(f"telemetry_http_{status}") from None
    except (OSError, URLError, HTTPException) as error:
        raise CapacityUnavailable("telemetry_transport_error") from error


def _number(value: JsonValue) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise CapacityUnavailable("invalid_pressure")
    return float(value)


def _metrics(raw: bytes, server: _Server) -> list[_Metric]:
    result: list[_Metric] = []
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as error:
        raise CapacityUnavailable("invalid_metrics") from error
    metric_types = {
        parts[2]: parts[3] for line in lines if line.startswith("# TYPE ") and len(parts := line.split()) == 4
    }
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith("sglang:"):
            continue
        sample = _SAMPLE.fullmatch(line)
        if sample is None:
            raise CapacityUnavailable("invalid_metrics")
        name = sample["name"].removeprefix("sglang:")
        family_name = re.sub(r"_(?:bucket|sum|count|created)$", "", sample["name"])
        metric_type = metric_types.get(sample["name"], metric_types.get(family_name))
        if metric_type in {"counter", "histogram", "summary"} or family_name != sample["name"]:
            if name in _CORE:
                raise CapacityUnavailable("invalid_metric_type")
            continue
        relevant = (
            name in _CORE
            or "queue" in name
            or "pending" in name
            or name.endswith("_usage")
            or name.startswith("max_total_num_tokens")
        )
        if not relevant:
            continue
        labels: dict[str, str] = {}
        text = sample["labels"] or ""
        position = 0
        while position < len(text):
            label = _LABEL.match(text, position)
            if label is None or label[1] in labels:
                raise CapacityUnavailable("invalid_metric_labels")
            labels[label[1]] = json.loads('"' + label[2] + '"')
            position = label.end()
        models = [labels[label] for label in ("model_name", "model") if label in labels]
        if not models or any(model not in {server.model_path, server.served_model_name} for model in models):
            raise CapacityUnavailable("metric_model_mismatch")
        try:
            tp = int(labels.get("tp_rank", "0" if server.tp_size == 1 else "-1"))
            dp = int(labels.get("dp_rank", "0" if server.dp_size == 1 else "-1"))
            value = float(sample["value"])
            stamp = float(sample["timestamp"]) / 1000 if sample["timestamp"] else None
        except ValueError as error:
            raise CapacityUnavailable("invalid_metrics") from error
        if (
            not 0 <= tp < server.tp_size
            or not 0 <= dp < server.dp_size
            or not math.isfinite(value)
            or value < 0
            or (stamp is not None and not math.isfinite(stamp))
        ):
            raise CapacityUnavailable("invalid_metrics")
        result.append(_Metric(name, (tp, dp), value, stamp))
    expected = {(tp, dp) for tp in range(server.tp_size) for dp in range(server.dp_size)}
    leaders = {(0, dp) for dp in range(server.dp_size)}
    for name in {metric.name for metric in result} | _CORE:
        family = [metric for metric in result if metric.name == name]
        reported = {metric.rank for metric in family}
        if reported != expected and (server.enable_metrics_for_all_schedulers or reported != leaders):
            raise CapacityUnavailable("missing_metric_family")
        if name in _CORE - {"num_queue_reqs"} and len(family) != len(reported):
            raise CapacityUnavailable("duplicate_metric_family")
        if name in _CORE - {"token_usage"} and any(not metric.value.is_integer() for metric in family):
            raise CapacityUnavailable("noninteger_metric")
    return result


def _pressure(values: dict[str, JsonValue], config: CapacityConfig, capacity: float) -> tuple[bool, bool]:
    queued = False
    blocked = False
    for name, value in values.items():
        if "queue" in name or "waiting" in name or "pending" in name:
            queued |= _number(value) != 0
        elif name.endswith("_usage") and name != "token_usage":
            usage = _number(value)
            if usage > 1:
                raise CapacityUnavailable("invalid_pool_ratio")
            pool_capacity = capacity
            if "swa" in name:
                reported_capacity = values.get("max_total_num_tokens_swa")
                pool_capacity = _number(reported_capacity)
                if pool_capacity == 0:
                    blocked |= usage != 0
                    continue
            elif "mamba" in name:
                blocked |= usage != 0
                continue
            elif name not in {"full_token_usage", "prealloc_full_token_usage"}:
                blocked |= usage != 0
                continue
            blocked |= usage + config.context_length / pool_capacity > config.max_fraction
    return queued, blocked


def _snapshot(
    root: str, key: str | None, timeout: float, config: CapacityConfig, deadline: float
) -> _Snapshot:
    started = time.monotonic()

    def get(path: str) -> bytes:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise CapacityUnavailable("maximum_wait_exceeded")
        return _get(root + path, key, min(timeout, remaining))

    try:
        server = _Server.model_validate(_decode(get("/get_server_info")))
        if (
            server.model_path != config.expected_model_path
            or server.tokenizer_path != config.expected_model_path
        ):
            raise CapacityUnavailable("backend_mismatch")
        if server.context_length != config.context_length:
            raise CapacityUnavailable("context_configuration_mismatch")
        loads = _Loads.model_validate(_decode(get("/v1/loads?include=core")))
        server_time = datetime.fromisoformat(loads.timestamp)
        if server_time.tzinfo is None:
            raise CapacityUnavailable("missing_server_timezone")
        clock = server_time.timestamp()
        metrics = _metrics(get("/metrics"), server)
    except (ValidationError, ValueError, OverflowError) as error:
        raise CapacityUnavailable("invalid_snapshot") from error
    if loads.version != server.version:
        raise CapacityUnavailable("serving_version_mismatch")
    if len(loads.loads) != server.dp_size or {load.dp_rank for load in loads.loads} != set(
        range(server.dp_size)
    ):
        raise CapacityUnavailable("missing_scheduler_rank")
    ages = [clock - load.timestamp for load in loads.loads]
    ages.extend(clock - metric.timestamp for metric in metrics if metric.timestamp is not None)
    elapsed = time.monotonic() - started
    if any(age < 0 or age + elapsed > config.max_snapshot_age_seconds for age in ages):
        raise CapacityUnavailable("stale_snapshot")
    capacities = [float(load.max_total_num_tokens) for load in loads.loads]
    capacities.extend(metric.value for metric in metrics if metric.name == "max_total_num_tokens")
    capacity = min(capacities)
    if capacity <= 0:
        raise CapacityUnavailable("invalid_capacity")
    tokens = max(
        [float(max(load.num_used_tokens, load.num_total_tokens)) for load in loads.loads]
        + [metric.value for metric in metrics if metric.name == "num_used_tokens"]
    )
    usage = max(
        [load.token_usage for load in loads.loads]
        + [metric.value for metric in metrics if metric.name == "token_usage"]
    )
    if usage > 1 or tokens > capacity:
        raise CapacityUnavailable("inconsistent_token_metrics")
    for load in loads.loads:
        if abs(load.token_usage - load.num_used_tokens / load.max_total_num_tokens) > 0.001:
            raise CapacityUnavailable("inconsistent_token_metrics")
    for rank in {metric.rank for metric in metrics}:
        rank_values = {metric.name: metric.value for metric in metrics if metric.rank == rank}
        if not {"token_usage", "num_used_tokens"} <= rank_values.keys():
            continue
        if (
            abs(
                rank_values["token_usage"]
                - rank_values["num_used_tokens"] / rank_values["max_total_num_tokens"]
            )
            > 0.005000001
        ):
            raise CapacityUnavailable("inconsistent_token_metrics")
    values: dict[str, JsonValue] = {}
    sources: list[dict[str, JsonValue]] = [load.model_dump(mode="json") for load in loads.loads]
    sources.extend({metric.name: metric.value} for metric in metrics)
    for source in sources:
        for name, value in source.items():
            if not (
                "queue" in name
                or "waiting" in name
                or "pending" in name
                or name.endswith("_usage")
                or name.startswith("max_total_num_tokens")
            ):
                continue
            pressure = _number(value)
            previous = values.get(name)
            if previous is not None:
                pressure = (
                    min(_number(previous), pressure)
                    if name.startswith("max_total_num_tokens")
                    else max(_number(previous), pressure)
                )
            values[name] = pressure
    declared_swa = min(state.memory_usage.token_capacity_swa or 0 for state in server.internal_states)
    reported_swa = values.get("max_total_num_tokens_swa")
    values["max_total_num_tokens_swa"] = (
        min(declared_swa, _number(reported_swa)) if reported_swa is not None else declared_swa
    )
    queued, auxiliary = _pressure(values, config, capacity)
    running = max(
        [float(load.num_running_reqs) for load in loads.loads]
        + [metric.value for metric in metrics if metric.name == "num_running_reqs"]
    )
    slots = min([server.max_running_requests] + [load.max_running_requests for load in loads.loads])
    return _Snapshot(
        server,
        max(tokens, math.ceil(math.nextafter(usage * capacity, -math.inf))),
        capacity,
        running,
        slots,
        queued,
        auxiliary,
        max(ages),
        started,
    )


def _tokenize(
    base: str,
    payload: dict[str, JsonValue],
    chat: _Chat,
    server: _Server,
    key: str | None,
    timeout: float,
) -> _Tokenization:
    if chat.model != server.served_model_name:
        raise CapacityUnavailable("request_alias_mismatch")
    if any(
        name in payload
        for name in (
            "prompt",
            "input_ids",
            "chat_template",
            "documents",
            "continue_final_message",
            "add_generation_prompt",
        )
    ):
        raise CapacityUnavailable("unsupported_template_override")
    token_body = {
        name: payload[name]
        for name in (
            "model",
            "messages",
            "tools",
            "tool_choice",
            "chat_template_kwargs",
        )
        if name in payload
    }
    try:
        response = complete(base + "/tokenize", json.dumps(token_body).encode(), key, timeout)
        result = _Tokenization.model_validate(_decode(response.body))
    except HTTPError as error:
        status = error.code
        error.close()
        raise CapacityUnavailable(f"tokenization_http_{status}") from None
    except (OSError, URLError, HTTPException, ValidationError) as error:
        raise CapacityUnavailable("tokenization_failed") from error
    if len(result.tokens) != result.count or any(token < 0 for token in result.tokens):
        raise CapacityUnavailable("inconsistent_tokenization")
    if result.count + chat.max_completion_tokens > min(server.context_length, result.max_model_len):
        raise ContextBudgetExceeded("context_budget_exceeded")
    return result


def _wait(directory: Path, reason: str, deadline: float, config: CapacityConfig, key: str | None) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CapacityUnavailable("maximum_wait_exceeded")
    _receipt(directory, "wait", {"reason": reason, "remaining_seconds": remaining}, key)
    _LOG.info("Capacity admission waiting: %s", reason)
    time.sleep(min(config.poll_seconds, remaining))


def guarded_complete(
    base_url: str, body: bytes, key: str | None, timeout: float, config: CapacityConfig, out: Path
) -> HTTPResponse:
    """Admit one nonstreaming chat turn against half-capacity and durable ownership.

    Every caller of an endpoint must share state_directory. Ambiguous submissions
    remain quarantined permanently; this API intentionally provides no reset.
    This bounds admission against observed tokens and slots, not GPU compute share.
    ContextBudgetExceeded signals an oversized conversation, not capacity waiting.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise CapacityUnavailable("invalid_timeout")
    base, root = _endpoint(base_url)
    payload = _decode(body)
    if not isinstance(payload, dict):
        raise CapacityUnavailable("invalid_chat_request")
    try:
        chat = _Chat.model_validate(payload)
    except ValidationError as error:
        raise CapacityUnavailable("invalid_chat_request") from error
    rid = str(uuid.uuid4())
    payload = {**payload, "rid": rid}
    directory = out / ("capacity-" + rid)
    directory.mkdir(parents=True, mode=0o700)
    state_directory = Path(config.state_directory)
    state_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    name = hashlib.sha256(base.encode()).hexdigest()
    ledger_path = state_directory / (name + ".json")
    lock_path = state_directory / (name + ".lock")
    deadline = time.monotonic() + config.maximum_wait_seconds
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "a+b") as lock:
        while True:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                _wait(directory, "owned_request", deadline, config, key)
        submitted = False
        try:
            if ledger_path.exists():
                try:
                    previous = _Ledger.model_validate(_decode(ledger_path.read_bytes()))
                except (OSError, ValidationError, CapacityUnavailable) as error:
                    raise CapacityQuarantined("invalid_ledger") from error
                if previous.endpoint != base or previous.state != "terminal" or previous.reservation != 0:
                    _receipt(directory, "quarantined", {"reason": "unresolved_ledger"}, key)
                    raise CapacityQuarantined("unresolved_ledger")
            tokenization: _Tokenization | None = None
            identity: _Server | None = None
            while True:
                try:
                    snapshot = _snapshot(root, key, timeout, config, deadline)
                except CapacityUnavailable as error:
                    if error.reason != "inconsistent_token_metrics":
                        raise
                    _wait(directory, error.reason, deadline, config, key)
                    continue
                if identity is not None and snapshot.identity != identity:
                    raise CapacityUnavailable("identity_changed")
                if snapshot.identity.served_model_name != chat.model:
                    raise CapacityUnavailable("request_alias_mismatch")
                identity = snapshot.identity
                if tokenization is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise CapacityUnavailable("maximum_wait_exceeded")
                    tokenization = _tokenize(base, payload, chat, identity, key, min(timeout, remaining))
                    continue
                if snapshot.age + time.monotonic() - snapshot.started > config.max_snapshot_age_seconds:
                    raise CapacityUnavailable("stale_snapshot")
                if snapshot.admits(config):
                    break
                _wait(directory, "observed_pressure", deadline, config, key)
            ledger = _Ledger(
                endpoint=base, rid=rid, state="reserved", reservation=config.context_length, identity=identity
            )
            _durable(ledger_path, ledger.model_dump(mode="json"), key)
            _receipt(
                directory,
                "admission",
                {
                    "state": "reserved",
                    "rid": rid,
                    "identity": identity.model_dump(mode="json"),
                    "observed_tokens": snapshot.tokens,
                    "capacity": snapshot.capacity,
                    "observed_running": snapshot.running,
                    "slots": snapshot.slots,
                    "reservation": config.context_length,
                    "max_fraction": config.max_fraction,
                    "prompt_tokens": tokenization.count,
                    "output_tokens": chat.max_completion_tokens,
                    "tokenizer_max_model_len": tokenization.max_model_len,
                },
                key,
            )
            ledger = ledger.model_copy(update={"state": "submitted"})
            _durable(ledger_path, ledger.model_dump(mode="json"), key)
            submitted = True
            _receipt(
                directory,
                "submitted",
                {"state": "submitted", "rid": rid, "reservation": config.context_length},
                key,
            )
            reason = "nonterminal_response"
            status: int | None = None
            response: HTTPResponse | None = None
            try:
                if snapshot.age + time.monotonic() - snapshot.started > config.max_snapshot_age_seconds:
                    raise CapacityQuarantined("stale_before_submission")
                if time.monotonic() >= deadline:
                    raise CapacityQuarantined("maximum_wait_exceeded")
                response = complete(
                    base + "/chat/completions",
                    json.dumps(payload).encode("utf-8"),
                    key,
                    timeout,
                )
                terminal = _Terminal.model_validate(_decode(response.body))
                if terminal.model != chat.model or terminal.choices[0].message.get("role") != "assistant":
                    raise CapacityQuarantined("response_identity_mismatch")
            except HTTPError as error:
                status = error.code
                reason = "http_error_ambiguous"
                error.close()
            except (OSError, URLError, HTTPException):
                reason = "transport_error_ambiguous"
            except (ValidationError, CapacityUnavailable):
                reason = "nonterminal_response"
            else:
                ledger = ledger.model_copy(
                    update={
                        "state": "terminal",
                        "reservation": 0,
                        "reason": terminal.choices[0].finish_reason,
                    }
                )
                _receipt(
                    directory,
                    "response",
                    {
                        "state": "terminal",
                        "finish_reason": terminal.choices[0].finish_reason,
                        "response_sha256": hashlib.sha256(response.body).hexdigest(),
                    },
                    key,
                )
                _durable(ledger_path, ledger.model_dump(mode="json"), key)
                return response
            ledger = ledger.model_copy(update={"state": "quarantined", "reason": reason})
            _durable(ledger_path, ledger.model_dump(mode="json"), key)
            _receipt(
                directory,
                "response",
                {
                    "state": "quarantined",
                    "reason": reason,
                    "http_status": status,
                    "response_sha256": hashlib.sha256(response.body).hexdigest() if response else None,
                },
                key,
            )
            raise CapacityQuarantined(reason) from None
        except OSError as error:
            if submitted:
                raise CapacityQuarantined("coordination_io_error") from error
            raise CapacityUnavailable("coordination_io_error") from error
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
