import hashlib
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Final, Literal

from pydantic import JsonValue, TypeAdapter, ValidationError

from scripts.dataset_schema import SECRET_PATTERNS

PAYLOAD_DIR: Final = "CODEBENCH_TRACE_PAYLOAD_DIR"
type CaptureFailure = Literal["not_json", "storage_error"]
_JSON: Final = TypeAdapter(JsonValue)
_PRIVATE_FIELDS: Final = frozenset({
    "authorization", "proxy_authorization", "api_key", "apikey", "x_api_key", "password",
    "access_token", "refresh_token", "cookie", "set_cookie", "reasoning_content", "reasoning_details",
})


def _redact(value: JsonValue) -> JsonValue:
    if isinstance(value, dict):
        return {
            key: "[redacted]" if key.lower().replace("-", "_") in _PRIVATE_FIELDS else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str) and any(pattern.search(value) for pattern in SECRET_PATTERNS):
        return "[redacted: credential pattern]"
    return value


def save_payload(directory: Path, value: object) -> str:
    try:
        parsed = _JSON.validate_json(value) if isinstance(value, bytes) else _JSON.validate_python(value)
        data = json.dumps(_redact(parsed), separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, RecursionError):
        return "not_json"
    digest = hashlib.sha256(data).hexdigest()
    path = directory / digest
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not path.exists():
            with NamedTemporaryFile(dir=directory, delete_on_close=False) as temporary:
                temporary.write(data)
                temporary.close()
                try:
                    os.link(temporary.name, path)
                except FileExistsError:
                    pass
    except OSError:
        return "storage_error"
    return digest


def load_payload(directory: Path | None, reference: str) -> str:
    if reference in ("not_json", "storage_error"):
        return json.dumps({"capture_status": reference})
    if directory is not None:
        try:
            data = (directory / reference).read_bytes()
            if hashlib.sha256(data).hexdigest() == reference:
                _JSON.validate_json(data)
                return data.decode()
        except (OSError, ValidationError, UnicodeError):
            pass
    return json.dumps({"capture_status": "payload_unavailable", "sha256": reference})
