from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from datetime import UTC, datetime
from http.client import HTTPException
from pathlib import Path
from typing import Final, TypeVar
from urllib.error import HTTPError, URLError

from pydantic import BaseModel, JsonValue, TypeAdapter

from .agent_events import InferenceError, assistant_result, parse_json
from .model_config import ModelConfig
from .openai_transport import complete, parse_completion, strict_schema

T = TypeVar("T", bound=BaseModel)
JSON: Final = TypeAdapter(JsonValue)


def _json(value: JsonValue) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _save(path: Path, text: str, key: str | None) -> None:
    if key:
        for representation in (key, json.dumps(key, ensure_ascii=False)[1:-1], json.dumps(key)[1:-1]):
            text = text.replace(representation, "[REDACTED]")
    with path.open("x", encoding="utf-8") as stream:
        _ = stream.write(text)


def _output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _native(system: str, user: str, config: ModelConfig, out: Path, receipt: dict[str, JsonValue]) -> str:
    with tempfile.TemporaryDirectory(prefix="codebench-typed-infer-") as temporary:
        command = [
            os.environ.get("CODEBENCH_OMO_EXECUTABLE", "omo"),
            "--print",
            "--mode",
            "json",
            "--model",
            config.model,
            "--thinking",
            "low",
            "--no-tools",
            "--no-extensions",
            "--no-skills",
            "--no-prompt-templates",
            "--no-context-files",
            "--no-session",
            "--offline",
            "--system-prompt",
            system,
            "-",
        ]
        receipt["command"] = list(command)
        receipt["structured_output_enforcement"] = "prompt_and_local_validation"
        receipt["generation_controls"] = "native_cli_defaults"
        try:
            process = subprocess.run(
                command,
                input=user,
                text=True,
                capture_output=True,
                cwd=temporary,
                env=dict(
                    os.environ,
                    TMPDIR=temporary,
                    NODE_COMPILE_CACHE=str(Path(temporary) / "node-cache"),
                    BUN_RUNTIME_TRANSPILER_CACHE_PATH=str(Path(temporary) / "bun-cache"),
                ),
                timeout=config.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            _save(out / "stdout.jsonl", _output(error.stdout), None)
            _save(out / "stderr.txt", _output(error.stderr), None)
            raise InferenceError("Native inference timed out") from None
    _save(out / "stdout.jsonl", process.stdout, None)
    _save(out / "stderr.txt", process.stderr, None)
    receipt["exit_code"] = process.returncode
    receipt["stdout_sha256"] = _hash(process.stdout)
    if process.returncode:
        raise InferenceError("Native inference process failed")
    _, actual = assistant_result(process.stdout, config.model)
    receipt["actual_model"] = actual
    text = ""
    for line in process.stdout.splitlines():
        if not line.strip():
            continue
        event = parse_json(line)
        if not isinstance(event, dict) or event.get("type") != "message_end":
            continue
        message = event.get("message")
        if not isinstance(message, dict) or message.get("role") != "assistant":
            continue
        content = message.get("content")
        if isinstance(content, list):
            text = ""
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    chunk = part.get("text")
                    if isinstance(chunk, str):
                        text += chunk
    return text


def typed_request(schema: type[BaseModel], system: str, prompt: str, config: ModelConfig) -> str:
    response_schema = JSON.validate_python(schema.model_json_schema())
    user = (
        prompt + "\nReturn only one complete JSON value conforming to this schema:\n" + _json(response_schema)
    )
    response_format: JsonValue = {"type": "json_object"}
    if config.provider == "openai_compatible" and config.structured_output == "json_schema":
        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "typed_response",
                "strict": True,
                "schema": strict_schema(response_schema),
            },
        }
    request: dict[str, JsonValue] = {
        "model": config.model,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "response_format": response_format,
        "max_completion_tokens": config.max_output_tokens,
    }
    if config.temperature is not None:
        request["temperature"] = config.temperature
    return _json(request)


def infer_typed(schema: type[T], system: str, prompt: str, config: ModelConfig, out: Path) -> T:
    """Run one isolated invocation; ``out`` must not already exist.

    OpenAI-compatible providers enforce the selected response format and local
    strict validation. Native uses CLI generation defaults and prompted schemas,
    matching the existing native runner, with its own configured process timeout.
    """
    out.mkdir(parents=True, exist_ok=False)
    receipt: dict[str, JsonValue] = {
        "invocation_id": str(uuid.uuid4()),
        "provider_config": JSON.validate_json(config.model_dump_json()),
        "requested_model": config.model,
        "actual_model": None,
        "started_at": datetime.now(UTC).isoformat(),
        "status": "failed",
        "tools_enabled": False,
        "session_saved": False,
    }
    key: str | None = None
    try:
        if config.api_key_env is not None:
            key = os.environ.get(config.api_key_env)
            if not key or not key.strip() or "\r" in key or "\n" in key:
                raise InferenceError("Configured API key environment variable is missing or invalid")
        response_schema = JSON.validate_python(schema.model_json_schema())
        schema_text = _json(response_schema)
        user = prompt + "\nReturn only one complete JSON value conforming to this schema:\n" + schema_text
        receipt["schema_sha256"] = _hash(schema_text)
        receipt["input_sha256"] = _hash(_json({"system": system, "prompt": prompt}))
        _save(out / "schema.json", schema_text + "\n", key)
        _save(out / "input.json", _json({"system": system, "prompt": prompt}) + "\n", key)
        body = typed_request(schema, system, prompt, config)
        receipt["request_sha256"] = _hash(body)
        _save(out / "request.json", body + "\n", key)
        match config.provider:
            case "native":
                text = _native(system, user, config, out, receipt)
            case "openai_compatible":
                if config.base_url is None:
                    raise InferenceError("OpenAI-compatible base URL is required")
                url = config.base_url.rstrip("/") + "/chat/completions"
                response = complete(url, body.encode("utf-8"), key, config.timeout_seconds)
                receipt["provider_request_id"] = response.request_id
                receipt["raw_response_sha256"] = hashlib.sha256(response.body).hexdigest()
                _save(out / "raw_response.json", response.body.decode("utf-8", errors="replace"), key)
                completion = parse_completion(response.body.decode("utf-8"))
                receipt["provider_invocation_id"] = completion.id
                receipt["actual_model"] = completion.model
                text = completion.text()
        _save(out / "response.txt", text, key)
        _ = parse_json(text)
        result = schema.model_validate_json(text, strict=True, extra="forbid")
        _save(out / "response.json", result.model_dump_json() + "\n", key)
        receipt["status"] = "validated"
        return result
    except HTTPError as error:
        receipt["http_status"] = error.code
        with error:
            payload = error.read()
        _save(out / "raw_response.txt", payload.decode("utf-8", errors="replace"), key)
        receipt["error"] = "Provider HTTP request failed"
        raise InferenceError("Provider HTTP request failed") from None
    except (OSError, URLError, HTTPException, ValueError, InferenceError) as error:
        receipt["error_type"] = type(error).__name__
        receipt["error"] = str(error)
        raise InferenceError("Typed inference failed; inspect the redacted evidence receipt") from None
    finally:
        receipt["ended_at"] = datetime.now(UTC).isoformat()
        _save(out / "receipt.json", _json(receipt) + "\n", key)
