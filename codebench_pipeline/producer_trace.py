import json
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from http.client import HTTPException, IncompleteRead
from typing import Final
from urllib.error import HTTPError

from pydantic import JsonValue

from scripts.dataset_schema import SECRET_PATTERNS

from .agent_events import InferenceError, parse_json
from .agent_trace import CallOutcome, llm_call, payloads_enabled
from .openai_transport import HTTPResponse

_EXTRA_SECRETS: Final = ContextVar[Callable[[str], bool] | None]("trace_payload_secrets", default=None)


@contextmanager
def trace_payload_secrets(contains: Callable[[str], bool]) -> Generator[None]:
    parent = _EXTRA_SECRETS.get()

    def bound(text: str) -> bool:
        return contains(text) or (parent is not None and parent(text))

    token = _EXTRA_SECRETS.set(bound)
    try:
        yield
    finally:
        _EXTRA_SECRETS.reset(token)


def _sensitive(text: str, key: str | None) -> bool:
    extra = _EXTRA_SECRETS.get()
    return (
        bool(key and key in text)
        or (extra is not None and extra(text))
        or any(pattern.search(text) for pattern in SECRET_PATTERNS)
    )


def _contains_secret(value: JsonValue, key: str | None) -> bool:
    match value:
        case str():
            return _sensitive(value, key)
        case dict():
            return any(_sensitive(name, key) or _contains_secret(item, key) for name, item in value.items())
        case list():
            return any(_contains_secret(item, key) for item in value)
        case _:
            return False


def trace_json(raw: bytes, key: str | None = None) -> JsonValue:
    if not payloads_enabled():
        return None
    try:
        text = raw.decode()
        if _sensitive(text, key):
            return {"omitted": "sensitive_payload"}
        value = parse_json(text)
        if _contains_secret(value, key):
            return {"omitted": "sensitive_payload"}
    except (InferenceError, ValueError, RecursionError):
        return {"omitted": "invalid_json", "bytes": len(raw)}
    return value


def trace_value(value: JsonValue, key: str | None = None) -> JsonValue:
    if not payloads_enabled():
        return None
    return trace_json(json.dumps(value, ensure_ascii=False).encode(), key)


@contextmanager
def completion_span(request: bytes, key: str | None) -> Generator[CallOutcome]:
    with llm_call(inputs=trace_json(request, key)) as measured:
        try:
            yield measured
        except HTTPError as error:
            measured.output({"error": "http_status", "http_status": error.code})
            raise
        except IncompleteRead as error:
            measured.output({"error": "incomplete_read", "partial_bytes": len(error.partial)})
            raise
        except (HTTPException, OSError) as error:
            measured.output({"error": type(error).__name__})
            raise


def capture_completion(measured: CallOutcome, response: HTTPResponse, key: str | None) -> None:
    if not payloads_enabled():
        return
    output: dict[str, JsonValue] = {"response": trace_json(response.body, key)}
    if (request_id := response.request_id) is not None and not _sensitive(request_id, key):
        output["provider_request_id"] = request_id
    measured.output(output)
