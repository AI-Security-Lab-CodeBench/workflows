from __future__ import annotations

import json
from typing import Final, NoReturn

from pydantic import JsonValue, TypeAdapter

JSON: Final = TypeAdapter(JsonValue)


class InferenceError(RuntimeError):
    """Native inference violated its structured isolation contract."""


def _unique(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise InferenceError("Duplicate JSON field")
        result[key] = value
    return result


def _nonfinite(value: str) -> NoReturn:
    raise InferenceError(f"Nonfinite JSON value: {value}")


def parse_json(text: str) -> JsonValue:
    return JSON.validate_python(json.loads(text, object_pairs_hook=_unique, parse_constant=_nonfinite))


def _reject_tools(value: JsonValue) -> None:
    match value:
        case dict():
            kind, role = value.get("type"), value.get("role")
            if isinstance(kind, str) and (
                "tool" in kind.lower() or kind in {"function_call", "function_result"}
            ):
                raise InferenceError("Tool output in isolated inference")
            if role in ("tool", "function"):
                raise InferenceError("Tool message in isolated inference")
            for child in value.values():
                _reject_tools(child)
        case list():
            for child in value:
                _reject_tools(child)
        case str() | bool() | int() | float() | None:
            return


def assistant_result(events: str, requested: str) -> tuple[str, str]:
    messages: list[dict[str, JsonValue]] = []
    for line in events.splitlines():
        if not line.strip():
            continue
        event = parse_json(line)
        _reject_tools(event)
        if not isinstance(event, dict):
            raise InferenceError("Non-object CLI event")
        message = event.get("message")
        if (
            event.get("type") == "message_end"
            and isinstance(message, dict)
            and message.get("role") == "assistant"
        ):
            messages.append(message)
    if not messages:
        raise InferenceError("CLI returned no assistant message")
    for message in messages:
        if message.get("stopReason") not in {"stop", "end_turn"}:
            raise InferenceError("Incomplete or unknown assistant stop reason")
        provider, model = message.get("provider"), message.get("model")
        if not isinstance(provider, str) or not isinstance(model, str) or provider + "/" + model != requested:
            raise InferenceError("Observed inference model differs from requested model")
        content = message.get("content")
        if not isinstance(content, list) or not content:
            raise InferenceError("Missing assistant content")
        for part in content:
            if not isinstance(part, dict) or part.get("type") not in {"text", "thinking"}:
                raise InferenceError("Unexpected assistant content block")
    content = messages[-1]["content"]
    if not isinstance(content, list):
        raise InferenceError("Missing final assistant content")
    text = ""
    for part in content:
        if isinstance(part, dict) and part.get("type") == "text":
            chunk = part.get("text")
            if not isinstance(chunk, str):
                raise InferenceError("Non-text assistant output")
            text += chunk
    text = text.strip()
    if text.startswith("```json\n") and text.endswith("\n```"):
        text = text[8:-4]
    return text, requested
