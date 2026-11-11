from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from queue import Queue
from threading import Event, Thread
from unittest.mock import patch

from dagster import Config, DagsterInvalidInvocationError
from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError

from codebench_pipeline.agent_events import InferenceError, parse_json
from codebench_pipeline.inference import infer_typed
from codebench_pipeline.model_config import ModelConfig, ModelRoles


class Answer(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    count: int


class NestedAnswer(BaseModel):
    answer: Answer
    note: str | None = None


@dataclass(frozen=True, slots=True)
class Captured:
    path: str
    authorization: str | None
    body: JsonValue


@dataclass(frozen=True, slots=True)
class Endpoint:
    url: str
    requests: Queue[Captured]


def completion(
    content: str = '{"count":7}',
    *,
    finish: str = "stop",
    message_extra: dict[str, JsonValue] | None = None,
) -> str:
    message: dict[str, JsonValue] = {"role": "assistant", "content": content}
    message.update(message_extra or {})
    return json.dumps(
        {
            "id": "completion-fixture",
            "model": "observed-revision",
            "choices": [{"index": 0, "finish_reason": finish, "message": message}],
        }
    )


@contextmanager
def endpoint(
    body: str, *, status: int = 200, location: str | None = None, release: Event | None = None
) -> Generator[Endpoint]:
    requests: Queue[Captured] = Queue()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            requests.put(Captured(self.path, self.headers.get("Authorization"), parse_json(payload.decode())))
            if release is not None and not release.wait(timeout=5):
                raise RuntimeError("HTTP fixture response was not released")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("x-request-id", "request-fixture")
                if location is not None:
                    self.send_header("Location", location)
                response = body.encode()
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                _ = self.wfile.write(response)
            except BrokenPipeError:
                if release is None:
                    raise

        def log_request(self, code: int | str = "-", size: int | str = "-") -> None:
            return

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, name="inference-http-fixture")
        thread.start()
        try:
            yield Endpoint(f"http://127.0.0.1:{server.server_port}/v1", requests)
        finally:
            server.shutdown()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("HTTP fixture did not terminate")


def read_json(path: Path) -> JsonValue:
    return parse_json(path.read_text(encoding="utf-8"))


class ModelConfigTests(unittest.TestCase):
    def test_roles_are_required_strict_dagster_configs(self) -> None:
        native = ModelConfig(provider="native", model="fixture/small")
        roles = ModelRoles(selector=native, adapter=native, labeler=native, reviewer=native)
        self.assertIsInstance(roles, Config)
        self.assertIsInstance(roles.selector, Config)
        with self.assertRaises(ValidationError):
            _ = ModelRoles.model_validate({"selector": native, "adapter": native, "labeler": native})
        with self.assertRaises(ValidationError):
            _ = ModelRoles.model_validate({**roles.model_dump(), "unknown": True})
        with self.assertRaises(ValidationError):
            _ = ModelConfig.model_validate({"provider": "native", "model": "small", "unknown": True})
        with self.assertRaises(DagsterInvalidInvocationError):
            native.model = "changed"

    def test_invalid_provider_fields_urls_and_numbers_are_rejected(self) -> None:
        cases: tuple[dict[str, JsonValue], ...] = (
            {"model": "small"},
            {"provider": "native", "model": " "},
            {"provider": "native", "model": "small", "base_url": "http://localhost"},
            {"provider": "native", "model": "small", "api_key_env": "TOKEN"},
            {"provider": "openai_compatible", "model": "small"},
            {"provider": "openai_compatible", "model": "small", "base_url": "ftp://localhost"},
            {"provider": "openai_compatible", "model": "small", "base_url": "http://user:pass@localhost"},
            {"provider": "openai_compatible", "model": "small", "base_url": "http://localhost:bad"},
            {"provider": "openai_compatible", "model": "small", "base_url": "http://localhost?key=secret"},
            {"provider": "openai_compatible", "model": "small", "base_url": "http://localhost/#fragment"},
            {
                "provider": "openai_compatible",
                "model": "small",
                "base_url": "http://localhost",
                "api_key_env": "BAD-NAME",
            },
            {"provider": "native", "model": "small", "timeout_seconds": 0},
            {"provider": "native", "model": "small", "timeout_seconds": "1"},
            {"provider": "native", "model": "small", "max_output_tokens": True},
            {"provider": "native", "model": "small", "max_output_tokens": 0},
            {"provider": "native", "model": "small", "temperature": "cold"},
            {"provider": "native", "model": "small", "structured_output": "auto"},
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                _ = ModelConfig.model_validate(value)


class DatasetInferenceTests(unittest.TestCase):
    def test_roles_route_to_distinct_endpoints_and_models_with_bound_evidence(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temporary,
            endpoint(completion('{"count":1}')) as first,
            endpoint(completion('{"count":2}')) as second,
        ):
            roles = ModelRoles(
                selector=ModelConfig(provider="openai_compatible", model="selector", base_url=first.url),
                adapter=ModelConfig(provider="openai_compatible", model="adapter", base_url=second.url),
                labeler=ModelConfig(provider="openai_compatible", model="labeler", base_url=first.url),
                reviewer=ModelConfig(provider="openai_compatible", model="reviewer", base_url=second.url),
            )
            for name, config, expected, server in (
                ("selector", roles.selector, 1, first),
                ("adapter", roles.adapter, 2, second),
                ("labeler", roles.labeler, 1, first),
                ("reviewer", roles.reviewer, 2, second),
            ):
                with self.subTest(role=name):
                    out = Path(temporary) / name
                    result = infer_typed(Answer, "policy", "inert input", config, out)
                    self.assertEqual(result.count, expected)
                    captured = server.requests.get(timeout=5)
                    self.assertEqual(captured.path, "/v1/chat/completions")
                    self.assertIsNone(captured.authorization)
                    request = captured.body
                    assert isinstance(request, dict)
                    self.assertEqual(request["model"], name)
                    self.assertNotIn("tools", request)
                    self.assertEqual(request["max_completion_tokens"], 4096)
                    messages = request["messages"]
                    assert isinstance(messages, list)
                    self.assertEqual(len(messages), 2)
                    receipt = read_json(out / "receipt.json")
                    assert isinstance(receipt, dict)
                    self.assertEqual(receipt["status"], "validated")
                    self.assertEqual(receipt["actual_model"], "observed-revision")
                    self.assertEqual(receipt["provider_invocation_id"], "completion-fixture")
                    self.assertEqual(receipt["provider_request_id"], "request-fixture")
                    for filename, field in (
                        ("request.json", "request_sha256"),
                        ("schema.json", "schema_sha256"),
                        ("input.json", "input_sha256"),
                    ):
                        raw = (out / filename).read_text().rstrip("\n")
                        self.assertEqual(receipt[field], hashlib.sha256(raw.encode()).hexdigest())
                    self.assertTrue(receipt["started_at"])
                    self.assertTrue(receipt["ended_at"])

    def test_strict_schema_handles_nested_models_and_nullable_defaults(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temporary,
            endpoint(completion('{"answer":{"count":7},"note":null}')) as server,
        ):
            config = ModelConfig(provider="openai_compatible", model="alias", base_url=server.url + "/")
            answer = infer_typed(NestedAnswer, "policy", "input", config, Path(temporary) / "out")
            self.assertEqual(answer.answer.count, 7)
            captured = server.requests.get(timeout=5).body
            assert isinstance(captured, dict)
            response_format = captured["response_format"]
            assert isinstance(response_format, dict)
            self.assertEqual(response_format["type"], "json_schema")
            definition = response_format["json_schema"]
            assert isinstance(definition, dict)
            self.assertIs(definition["strict"], True)
            schema = definition["schema"]
            assert isinstance(schema, dict)
            self.assertEqual(schema["required"], ["answer", "note"])
            self.assertIs(schema["additionalProperties"], False)
            definitions = schema["$defs"]
            assert isinstance(definitions, dict)
            child = definitions["Answer"]
            assert isinstance(child, dict)
            self.assertIs(child["additionalProperties"], False)

    def test_json_object_auth_and_echoed_secret_are_not_persisted(self) -> None:
        secret = "fixture-secret-never-persist"
        envelope = parse_json(completion())
        assert isinstance(envelope, dict)
        envelope["echo"] = secret
        body = json.dumps(envelope)
        with (
            tempfile.TemporaryDirectory() as temporary,
            endpoint(body) as server,
            patch.dict(os.environ, {"INFERENCE_TEST_KEY": secret}),
        ):
            config = ModelConfig(
                provider="openai_compatible",
                model="alias",
                base_url=server.url,
                api_key_env="INFERENCE_TEST_KEY",
                structured_output="json_object",
                temperature=0.25,
                max_output_tokens=128,
            )
            out = Path(temporary) / "out"
            _ = infer_typed(Answer, "policy", secret, config, out)
            captured = server.requests.get(timeout=5)
            self.assertEqual(captured.authorization, "Bearer " + secret)
            request = captured.body
            assert isinstance(request, dict)
            self.assertEqual(request["response_format"], {"type": "json_object"})
            self.assertEqual(request["temperature"], 0.25)
            self.assertEqual(request["max_completion_tokens"], 128)
            for path in out.iterdir():
                self.assertNotIn(secret, path.read_text())
            self.assertIn("INFERENCE_TEST_KEY", (out / "receipt.json").read_text())

    def test_invalid_complete_response_keeps_failure_evidence_without_fallback(self) -> None:
        cases = (
            completion('{"count":"7"}'),
            completion('{"count":7,"extra":1}'),
            completion('{"count":7,"count":8}'),
            completion('{"count":NaN}'),
            completion('{"count":7} trailing'),
            completion('{"count":7} {"count":8}'),
            completion('```json\n{"count":7}\n```'),
            completion('{"count":'),
            completion(finish="length"),
            completion(finish="content_filter"),
            completion(message_extra={"refusal": "refused"}),
            completion(message_extra={"tool_calls": [{"type": "function"}]}),
            completion(message_extra={"function_call": {"name": "run"}}),
            '{"error":{"message":"provider failed"}}',
            "not JSON",
        )
        with tempfile.TemporaryDirectory() as temporary:
            for index, body in enumerate(cases):
                with self.subTest(index=index), endpoint(body) as server:
                    config = ModelConfig(provider="openai_compatible", model="alias", base_url=server.url)
                    out = Path(temporary) / str(index)
                    with self.assertRaises(InferenceError):
                        _ = infer_typed(Answer, "policy", "input", config, out)
                    receipt = read_json(out / "receipt.json")
                    assert isinstance(receipt, dict)
                    self.assertEqual(receipt["status"], "failed")
                    self.assertFalse((out / "response.json").exists())
                    self.assertEqual((out / "raw_response.json").read_text(), body)
                    self.assertEqual(server.requests.qsize(), 1)

    def test_http_errors_redact_provider_echoes_and_do_not_retry(self) -> None:
        secret = "fixture-secret-http-error"
        with (
            tempfile.TemporaryDirectory() as temporary,
            endpoint(json.dumps({"error": secret}), status=401) as server,
            patch.dict(os.environ, {"INFERENCE_TEST_KEY": secret}),
        ):
            config = ModelConfig(
                provider="openai_compatible",
                model="alias",
                base_url=server.url,
                api_key_env="INFERENCE_TEST_KEY",
            )
            out = Path(temporary) / "out"
            with self.assertRaises(InferenceError) as raised:
                _ = infer_typed(Answer, "policy", "input", config, out)
            self.assertNotIn(secret, str(raised.exception))
            for path in out.iterdir():
                self.assertNotIn(secret, path.read_text())
            receipt = read_json(out / "receipt.json")
            assert isinstance(receipt, dict)
            self.assertEqual(receipt["http_status"], 401)
            self.assertEqual(server.requests.qsize(), 1)

    def test_redirect_does_not_forward_credentials(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temporary,
            endpoint(completion()) as target,
            endpoint("", status=307, location=target.url + "/chat/completions") as source,
            patch.dict(os.environ, {"INFERENCE_TEST_KEY": "fixture-redirect-key"}),
        ):
            config = ModelConfig(
                provider="openai_compatible",
                model="alias",
                base_url=source.url,
                api_key_env="INFERENCE_TEST_KEY",
            )
            with self.assertRaises(InferenceError):
                _ = infer_typed(Answer, "policy", "input", config, Path(temporary) / "out")
            self.assertTrue(target.requests.empty())

    def test_missing_key_fails_before_network_with_receipt(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temporary,
            endpoint(completion()) as server,
            patch.dict(os.environ, {}, clear=True),
        ):
            config = ModelConfig(
                provider="openai_compatible",
                model="alias",
                base_url=server.url,
                api_key_env="MISSING_TEST_KEY",
            )
            out = Path(temporary) / "out"
            with self.assertRaises(InferenceError):
                _ = infer_typed(Answer, "policy", "input", config, out)
            self.assertTrue((out / "receipt.json").exists())
            self.assertTrue(server.requests.empty())

    def test_native_timeout_uses_config_without_mutating_environment(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.dict(os.environ, {"CODEBENCH_AGENT_TIMEOUT": "999"}),
        ):
            config = ModelConfig(provider="native", model="fixture/small", timeout_seconds=0.25)
            out = Path(temporary) / "out"
            with patch(
                "codebench_pipeline.inference.subprocess.run",
                side_effect=subprocess.TimeoutExpired(["omo"], 0.25, output=b"partial", stderr=b"timeout"),
            ) as process:
                with self.assertRaises(InferenceError):
                    _ = infer_typed(Answer, "policy", "input", config, out)
            self.assertEqual(process.call_args.kwargs["timeout"], 0.25)
            self.assertEqual(os.environ["CODEBENCH_AGENT_TIMEOUT"], "999")
            self.assertFalse(Path(process.call_args.kwargs["cwd"]).exists())
            self.assertEqual((out / "stdout.jsonl").read_text(), "partial")
            self.assertEqual((out / "stderr.txt").read_text(), "timeout")

    def test_http_timeout_terminates_without_retry_or_provider_fallback(self) -> None:
        release = Event()
        with tempfile.TemporaryDirectory() as temporary, endpoint(completion(), release=release) as server:
            config = ModelConfig(
                provider="openai_compatible", model="alias", base_url=server.url, timeout_seconds=0.05
            )
            out = Path(temporary) / "out"
            try:
                with self.assertRaises(InferenceError):
                    _ = infer_typed(Answer, "policy", "input", config, out)
                self.assertEqual(server.requests.get(timeout=5).path, "/v1/chat/completions")
            finally:
                release.set()
            receipt = read_json(out / "receipt.json")
            assert isinstance(receipt, dict)
            self.assertEqual(receipt["status"], "failed")
            self.assertEqual(receipt["error_type"], "TimeoutError")
            self.assertFalse((out / "response.json").exists())

    def test_native_validates_exact_json_and_keeps_process_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            for index, content in enumerate(('{"count":7}', '```json\n{"count":7}\n```')):
                events = json.dumps(
                    {
                        "type": "message_end",
                        "message": {
                            "role": "assistant",
                            "provider": "fixture",
                            "model": "small",
                            "stopReason": "stop",
                            "content": [{"type": "text", "text": content}],
                        },
                    }
                )
                out = Path(temporary) / str(index)
                config = ModelConfig(provider="native", model="fixture/small")
                with patch(
                    "codebench_pipeline.inference.subprocess.run",
                    return_value=subprocess.CompletedProcess(["omo"], 0, events, ""),
                ):
                    if index == 0:
                        self.assertEqual(infer_typed(Answer, "policy", "input", config, out).count, 7)
                    else:
                        with self.assertRaises(InferenceError):
                            _ = infer_typed(Answer, "policy", "input", config, out)
                self.assertEqual((out / "stdout.jsonl").read_text(), events)
                receipt = read_json(out / "receipt.json")
                assert isinstance(receipt, dict)
                self.assertEqual(receipt["actual_model"], "fixture/small")
