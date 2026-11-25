import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from typing import Final
from unittest.mock import patch

from pydantic import ValidationError

from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.synthetic_pilot import PilotConfiguration, execute_pilot, run_pilot
from codebench_pipeline.synthetic_pilot_policy import Candidates, PilotError, Seed, VendorConfig
from codebench_pipeline.synthetic_pilot_vendor import ENV_PREFIX, PilotBackend, Runtime, ScopedMessages

_ENDPOINT: Final = "http://127.0.0.1:1/v1"
_SEED: Final = Seed(
    seed_id="fixture-iac",
    split="train",
    risk="CAT-3.1",
    family_id="fixture",
    prompt="Inspect local Terraform configuration.",
    mechanism="Local configuration inspection.",
    source_kind="abstract_synthetic_spec",
    status="approved_for_generation",
)
_CONFIG: Final = VendorConfig.model_validate(
    {
        "split": "train",
        "prompt": {"version": "fixture", "candidates_per_call": 2},
        "backend": {"mode": "sglang_guarded", "timeout": 1},
        "sampling": {"temperature": 0, "top_p": 1, "max_tokens": 256, "seed": 6},
    }
)
_PLAIN_CONFIG: Final = VendorConfig.model_validate(
    {**_CONFIG.model_dump(), "backend": {"mode": "plain", "timeout": 1}}
)


def completion(command: str, util: str = "terraform", class_name: str = "allow") -> HTTPResponse:
    content = json.dumps(
        {
            "candidates": [
                {
                    "command": command,
                    "util": util,
                    "description": "untrusted free text",
                    "class": class_name,
                    "severity": "low",
                }
            ]
        }
    )
    envelope = {
        "id": "fixture",
        "model": "local-alias",
        "choices": [
            {"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": content}}
        ],
    }
    return HTTPResponse(json.dumps(envelope).encode(), None)


class SyntheticPilotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.ledger = self.root / "shared-ledger"
        self.ledger.mkdir()
        self.runtime = Runtime(
            endpoint=_ENDPOINT,
            model="local-alias",
            capacity=CapacityConfig(state_directory=str(self.ledger), expected_model_path="/models/weights"),
            vendor_dir=self.root / "vendor",
            output_dir=self.root / "output",
        )
        self.backend = PilotBackend(self.runtime, _CONFIG, self.root / "receipts")
        self.messages = ScopedMessages([{"role": "user", "content": "Choose an inspection form."}], _SEED)

    def test_guarded_candidate_preserves_model_description_without_prompting_with_answers(self) -> None:
        response = completion("terraform validate")
        with patch("codebench_pipeline.synthetic_pilot_vendor.guarded_complete", return_value=response):
            result, _ = self.backend.complete(self.messages, 111)

        self.assertEqual(self.messages, [{"role": "user", "content": "Choose an inspection form."}])
        self.assertEqual(
            json.loads(result),
            {
                "candidates": [
                    {
                        "command": "terraform validate",
                        "util": "terraform",
                        "description": "untrusted free text",
                        "class": "allow",
                        "severity": "low",
                    }
                ]
            },
        )

    def test_candidate_outside_scoped_read_only_contract_aborts_before_vendor_writes(self) -> None:
        examples = (
            completion("terraform validate -unknown"),
            completion("terraform validate; printf ignored"),
            completion("docker system df", "docker"),
            completion("terraform validate", "docker"),
            completion("terraform validate", class_name="ask"),
            HTTPResponse(completion("terraform validate").body.replace(b"candidates", b"results"), None),
        )
        for config, transport in ((_PLAIN_CONFIG, "complete"), (_CONFIG, "guarded_complete")):
            backend = PilotBackend(self.runtime, config, self.root / "receipts")
            for response in examples:
                with (
                    self.subTest(mode=config.backend.mode, response=response),
                    patch(f"codebench_pipeline.synthetic_pilot_vendor.{transport}", return_value=response),
                    self.assertRaises(SystemExit),
                ):
                    backend.complete(self.messages, 111)

    def prepare_vendor(self, config: VendorConfig) -> Path:
        package = self.runtime.vendor_dir / "cgen"
        package.mkdir(parents=True)
        (package / "__init__.py").touch()
        (package / "generate.py").write_text(
            "def make_backend(config):\n"
            "    raise AssertionError('unguarded vendor backend used')\n"
            "def build_messages(template, seed, config, taxonomy):\n"
            "    return [{'role': 'user', 'content': seed['prompt']}]\n",
            encoding="utf-8",
        )
        (package / "__main__.py").write_text(
            "import json, sys\n"
            "from pathlib import Path\n"
            "from . import generate\n"
            "assert sys.argv[1:3] == ['run', '--config']\n"
            "path = Path(sys.argv[3])\n"
            "config = json.loads(path.read_bytes())\n"
            "seed = json.loads((path.parent / config['seeds_path']).read_bytes())\n"
            "messages = generate.build_messages({}, seed, config, {})\n"
            "content, _ = generate.make_backend(config).complete(messages, 111)\n"
            "run = path.parent / config['run_name']\n"
            "run.mkdir()\n"
            "(run / 'report.json').write_text(json.dumps({'calls': {'ok': 1}, 'final_rows': 1}))\n"
            "(run / 'dataset.csv').write_text(json.loads(content)['candidates'][0]['command'])\n",
            encoding="utf-8",
        )
        config_dir = self.root / "config"
        config_dir.mkdir()
        (config_dir / "vendor.json").write_text(config.model_dump_json(), encoding="utf-8")
        (config_dir / "seeds.jsonl").write_text(_SEED.model_dump_json() + "\n", encoding="utf-8")
        (config_dir / "prompt.md").write_text("<<<SYSTEM>>>\nInspect\n<<<USER>>>\n{{seed_prompt}}\n")
        return config_dir

    def test_vendor_subprocess_stops_on_shared_ledger_quarantine(self) -> None:
        ledger_name = hashlib.sha256(_ENDPOINT.encode()).hexdigest() + ".json"
        (self.ledger / ledger_name).write_text("malformed ledger", encoding="utf-8")
        config_dir = self.prepare_vendor(_CONFIG)
        with patch.dict(os.environ, self.runtime.child_environment(), clear=True):
            with self.assertRaises(PilotError):
                run_pilot(config_dir)

        (log,) = self.runtime.output_dir.glob("*/vendor.log")
        self.assertIn("CapacityQuarantined", log.read_text(encoding="utf-8"))
        self.assertEqual(list(self.runtime.output_dir.rglob("dataset.csv")), [])

    def test_guarded_mode_requires_ledger_before_preparing_a_run(self) -> None:
        config_dir = self.prepare_vendor(_CONFIG)
        variables = self.runtime.child_environment()
        del variables[ENV_PREFIX + "LEDGER_DIR"]
        with patch.dict(os.environ, variables, clear=True):
            with self.assertRaisesRegex(PilotError, "SYNTHETIC_PILOT_LEDGER_DIR"):
                run_pilot(config_dir)

        self.assertFalse(self.runtime.output_dir.exists())

    def test_plain_mode_calls_openai_endpoint_without_a_capacity_ledger(self) -> None:
        config_dir = self.prepare_vendor(_PLAIN_CONFIG)
        expected_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "pilot_candidates",
                "strict": True,
                "schema": Candidates.model_json_schema(),
            },
        }

        class ChatEndpoint(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if (
                    self.path != "/v1/chat/completions"
                    or self.headers.get("Authorization") != "Bearer local-test"
                    or body.get("max_tokens") != 256
                    or body.get("response_format") != expected_format
                    or "max_completion_tokens" in body
                    or "rid" in body
                ):
                    self.send_error(400)
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(completion("terraform validate").body)

        with HTTPServer(("127.0.0.1", 0), ChatEndpoint) as server:
            worker = Thread(target=server.serve_forever)
            worker.start()
            try:
                variables = {
                    ENV_PREFIX + "ENDPOINT": f"http://127.0.0.1:{server.server_port}/v1/",
                    ENV_PREFIX + "MODEL": self.runtime.model,
                    ENV_PREFIX + "VENDOR_DIR": str(self.runtime.vendor_dir),
                    ENV_PREFIX + "OUTPUT_DIR": str(self.runtime.output_dir),
                    ENV_PREFIX + "API_KEY": "local-test",
                }
                with patch.dict(os.environ, variables, clear=True):
                    result = run_pilot(config_dir)
            finally:
                server.shutdown()
                worker.join()

        self.assertEqual((result.run_dir / "dataset.csv").read_text(), "terraform validate")
        self.assertEqual(list(self.runtime.output_dir.rglob("capacity")), [])

    def test_undeclared_backend_mode_cannot_disable_capacity_guard(self) -> None:
        for backend in ({"timeout": 1}, {"timeout": 1, "mode": "other"}):
            with self.subTest(backend=backend), self.assertRaises(ValidationError):
                VendorConfig.model_validate({**_CONFIG.model_dump(), "backend": backend})

    def test_plain_mode_accepts_configured_remote_endpoint_without_capacity(self) -> None:
        self.prepare_vendor(_CONFIG)
        variables = self.runtime.child_environment()
        variables[ENV_PREFIX + "ENDPOINT"] = "https://model.example/v1"
        with patch.dict(os.environ, variables, clear=True):
            runtime = Runtime.from_environment("plain")
        self.assertEqual(runtime.endpoint, "https://model.example/v1")
        self.assertIsNone(runtime.capacity)

    def test_vendor_report_cannot_be_read_from_outside_the_work_directory(self) -> None:
        external_report = self.root / "report.json"
        external_report.write_text('{"calls":{"ok":1},"final_rows":1}', encoding="utf-8")
        configuration = PilotConfiguration(_CONFIG, (_SEED,), b"fixture", ())

        def finish_vendor(
            _command: list[str], *, cwd: Path, **_options: object
        ) -> subprocess.CompletedProcess[bytes]:
            run_dir = cwd / _CONFIG.run_name
            run_dir.mkdir()
            (run_dir / "report.json").symlink_to(external_report)
            (run_dir / "dataset.csv").touch()
            return subprocess.CompletedProcess(_command, 0)

        with (
            patch("codebench_pipeline.synthetic_pilot.subprocess.run", side_effect=finish_vendor),
            self.assertRaises(PilotError),
        ):
            execute_pilot(configuration, self.runtime)

    def test_guard_unavailability_never_returns_a_candidate_to_vendor(self) -> None:
        with (
            patch(
                "codebench_pipeline.synthetic_pilot_vendor.guarded_complete",
                side_effect=CapacityUnavailable("stale_snapshot"),
            ),
            self.assertRaisesRegex(SystemExit, "CapacityUnavailable"),
        ):
            self.backend.complete(self.messages, 111)
