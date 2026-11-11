from __future__ import annotations

from dataclasses import dataclass
from email.message import Message
from typing import IO
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from .agent_events import InferenceError, parse_json


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> Request | None:
        return None


class _Message(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    role: str
    content: str
    refusal: JsonValue = None
    tool_calls: JsonValue = None
    function_call: JsonValue = None


class _Choice(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    index: int
    finish_reason: str
    message: _Message


class Completion(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    id: str = Field(min_length=1)
    model: str = Field(min_length=1)
    choices: list[_Choice] = Field(min_length=1, max_length=1)
    error: JsonValue = None
    status: str | None = None
    incomplete_details: JsonValue = None

    def text(self) -> str:
        choice = self.choices[0]
        message = choice.message
        if (
            self.error is not None
            or self.incomplete_details is not None
            or self.status not in {None, "completed"}
            or choice.index != 0
            or choice.finish_reason != "stop"
            or message.role != "assistant"
            or message.refusal is not None
            or message.tool_calls not in (None, [])
            or message.function_call is not None
            or not self.model.strip()
            or not self.id.strip()
        ):
            raise InferenceError("Provider returned an incomplete, refused, tool or error response")
        return message.content


@dataclass(frozen=True, slots=True)
class HTTPResponse:
    body: bytes
    request_id: str | None


def complete(url: str, body: bytes, key: str | None, timeout: float) -> HTTPResponse:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if key is not None:
        headers["Authorization"] = "Bearer " + key
    request = Request(url, data=body, headers=headers, method="POST")
    with build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
        payload: bytes = response.read()
        request_id: str | None = response.headers.get("x-request-id")
    return HTTPResponse(payload, request_id)


def parse_completion(text: str) -> Completion:
    return Completion.model_validate(parse_json(text), strict=True)


def strict_schema(value: JsonValue) -> JsonValue:
    """Constrain object keys recursively without rewriting enum/default data."""
    if not isinstance(value, dict):
        raise InferenceError("Structured output schema must be an object")
    result = value.copy()
    result.pop("default", None)
    for key in ("properties", "$defs", "definitions", "patternProperties"):
        children = result.get(key)
        if isinstance(children, dict):
            result[key] = {name: strict_schema(child) for name, child in children.items()}
    for key in ("anyOf", "allOf", "oneOf", "prefixItems"):
        alternatives = result.get(key)
        if isinstance(alternatives, list):
            result[key] = [strict_schema(child) for child in alternatives]
    items = result.get("items")
    if isinstance(items, dict):
        result["items"] = strict_schema(items)
    if result.get("type") == "object":
        properties = result.get("properties", {})
        if not isinstance(properties, dict) or result.get("additionalProperties", False) is not False:
            raise InferenceError("Strict structured output does not support open object keys")
        result["properties"] = properties
        result["required"] = list(properties)
        result["additionalProperties"] = False
    return result
