import json
import os
import runpy
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Final, TypedDict
from urllib.parse import urlsplit

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.agent_events import InferenceError, parse_json
from codebench_pipeline.inference_capacity import CapacityConfig, guarded_complete
from codebench_pipeline.openai_transport import complete, parse_completion, strict_schema
from codebench_pipeline.synthetic_pilot_policy import (
    OPERATIONS,
    BackendMode,
    Candidates,
    PilotError,
    Seed,
    VendorConfig,
)

ENV_PREFIX: Final = "CODEBENCH_SYNTHETIC_PILOT_"
_JSON: Final = TypeAdapter(JsonValue)


@dataclass(frozen=True, slots=True)
class Runtime:
    endpoint: str
    model: str
    vendor_dir: Path
    output_dir: Path
    capacity: CapacityConfig | None = None
    api_key: str | None = field(default=None, repr=False)

    def child_environment(self) -> dict[str, str]:
        values = {
            "ENDPOINT": self.endpoint,
            "MODEL": self.model,
            "VENDOR_DIR": str(self.vendor_dir),
            "OUTPUT_DIR": str(self.output_dir),
        }
        if self.capacity is not None:
            values.update(
                MODEL_PATH=self.capacity.expected_model_path,
                LEDGER_DIR=self.capacity.state_directory,
            )
        if self.api_key is not None:
            values["API_KEY"] = self.api_key
        return {ENV_PREFIX + name: value for name, value in values.items()}

    @classmethod
    def from_environment(cls, mode: BackendMode) -> "Runtime":
        settings = ("ENDPOINT", "MODEL", "VENDOR_DIR", "OUTPUT_DIR")
        if mode == "sglang_guarded":
            settings += ("MODEL_PATH", "LEDGER_DIR")
        values = {name: os.environ.get(ENV_PREFIX + name, "").strip() for name in settings}
        missing = [ENV_PREFIX + name for name, value in values.items() if not value]
        if missing:
            raise PilotError(f"Missing pilot environment: {', '.join(missing)}")
        endpoint = values["ENDPOINT"]
        try:
            parts = urlsplit(endpoint)
            port = parts.port
        except ValueError:
            raise PilotError("Pilot endpoint must be an HTTP(S) OpenAI base URL ending in /v1") from None
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or port == 0
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or parts.path.rstrip("/") != "/v1"
            or any(character.isspace() for character in endpoint)
        ):
            raise PilotError(
                "Pilot endpoint must be an HTTP(S) OpenAI base URL ending in /v1 without credentials"
            )
        match mode:
            case "plain":
                capacity = None
            case "sglang_guarded":
                ledger_dir = Path(values["LEDGER_DIR"])
                if not ledger_dir.is_absolute():
                    raise PilotError(f"{ENV_PREFIX}LEDGER_DIR must be absolute")
                ledger_dir = ledger_dir.resolve()
                if not ledger_dir.is_dir():
                    raise PilotError("The shared inference ledger directory must already exist")
                capacity = CapacityConfig(
                    state_directory=str(ledger_dir), expected_model_path=values["MODEL_PATH"]
                )
        for name in ("VENDOR_DIR", "OUTPUT_DIR"):
            if not Path(values[name]).is_absolute():
                raise PilotError(f"{ENV_PREFIX + name} must be absolute")
        vendor_dir = Path(values["VENDOR_DIR"]).resolve()
        if not all((vendor_dir / "cgen" / name).is_file() for name in ("__main__.py", "generate.py")):
            raise PilotError("The vendor directory does not contain the cgen CLI")
        return cls(
            endpoint=endpoint,
            model=values["MODEL"],
            capacity=capacity,
            vendor_dir=vendor_dir,
            output_dir=Path(values["OUTPUT_DIR"]).resolve(),
            api_key=os.environ.get(ENV_PREFIX + "API_KEY") or None,
        )


class Message(TypedDict):
    role: str
    content: str


class ScopedMessages(list[Message]):
    def __init__(self, messages: list[Message], seed: Seed) -> None:
        self.commands = frozenset(command for command, risk in OPERATIONS.items() if risk == seed.risk)
        super().__init__(messages)


@dataclass(frozen=True, slots=True)
class PilotBackend:
    runtime: Runtime
    config: VendorConfig
    receipts: Path

    def complete(self, messages: ScopedMessages, call_seed: int) -> tuple[str, dict[str, JsonValue]]:
        """Perform one turn and only return candidates that match reviewed command forms."""
        settings = self.config.sampling
        mode = self.config.backend.mode
        response_format: JsonValue = {"type": "json_object"}
        if mode == "plain":
            response_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": "pilot_candidates",
                    "strict": True,
                    "schema": strict_schema(_JSON.validate_python(Candidates.model_json_schema())),
                },
            }
        body = {
            "model": self.runtime.model,
            "messages": messages,
            "temperature": settings.temperature,
            "top_p": settings.top_p,
            "seed": call_seed,
            ("max_tokens" if mode == "plain" else "max_completion_tokens"): settings.max_tokens,
            "response_format": response_format,
            "stream": False,
        }
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        timeout = self.config.backend.timeout
        try:
            match mode:
                case "plain":
                    response = complete(
                        self.runtime.endpoint.rstrip("/") + "/chat/completions",
                        payload,
                        self.runtime.api_key,
                        timeout,
                    )
                case "sglang_guarded":
                    capacity = self.runtime.capacity
                    if capacity is None:
                        raise PilotError("Guarded pilot inference requires shared capacity configuration")
                    response = guarded_complete(
                        self.runtime.endpoint, payload, self.runtime.api_key, timeout, capacity, self.receipts
                    )
            completion = parse_completion(response.body.decode("utf-8"))
            if completion.model != self.runtime.model:
                raise InferenceError("Pilot response model differs from the configured model")
            content = completion.text()
            selected = Candidates.model_validate(parse_json(content))
        except (InferenceError, OSError, ValueError) as error:
            raise SystemExit(f"Pilot stopped: {mode} turn failed ({type(error).__name__})") from None
        for candidate in selected.candidates:
            if (
                candidate.command not in messages.commands
                or candidate.util != candidate.command.split(" ", 1)[0]
            ):
                raise SystemExit("Pilot stopped: command outside the reviewed inspection forms")
        return selected.model_dump_json(by_alias=True), {}


def run_vendor(config_path: Path) -> None:
    """Run the unchanged CLI with its generation backend replaced inside this process."""
    config = VendorConfig.model_validate_json(config_path.read_bytes())
    runtime = Runtime.from_environment(config.backend.mode)
    backend = PilotBackend(runtime, config, config_path.parent / "capacity")
    sys.path.append(str(runtime.vendor_dir))
    generator = import_module("cgen.generate")
    if Path(generator.__file__ or "").resolve() != runtime.vendor_dir / "cgen" / "generate.py":
        raise PilotError("cgen resolved outside the configured vendor directory")
    build_messages: Callable[..., list[Message]] = getattr(generator, "build_messages")

    def scoped_messages(
        template: dict[str, str], seed_row: dict[str, JsonValue], *settings: dict[str, JsonValue]
    ) -> ScopedMessages:
        seed = Seed.model_validate(seed_row)
        return ScopedMessages(build_messages(template, seed_row, *settings), seed)

    def make_backend(_config: dict[str, JsonValue]) -> PilotBackend:
        return backend

    setattr(generator, "build_messages", scoped_messages)
    setattr(generator, "make_backend", make_backend)
    sys.argv[1:] = ["run", "--config", str(config_path)]
    runpy.run_module("cgen", run_name="__main__", alter_sys=True)
