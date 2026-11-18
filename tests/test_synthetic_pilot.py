import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Final
from unittest.mock import patch

from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.synthetic_pilot import run_pilot
from codebench_pipeline.synthetic_pilot_policy import PilotError, Seed, VendorConfig
from codebench_pipeline.synthetic_pilot_vendor import ENV_PREFIX, GuardedBackend, Runtime, ScopedMessages

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
        "backend": {"timeout": 1},
        "sampling": {"temperature": 0, "top_p": 1, "max_tokens": 256, "seed": 6},
    }
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
        self.backend = GuardedBackend(self.runtime, _CONFIG, self.root / "receipts")
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
            ("terraform validate -unknown", "terraform", "allow"),
            ("terraform validate; printf ignored", "terraform", "allow"),
            ("docker system df", "docker", "allow"),
            ("terraform validate", "docker", "allow"),
            ("terraform validate", "terraform", "ask"),
        )
        for command, util, class_name in examples:
            with (
                self.subTest(command=command, util=util, class_name=class_name),
                patch(
                    "codebench_pipeline.synthetic_pilot_vendor.guarded_complete",
                    return_value=completion(command, util, class_name),
                ),
                self.assertRaises(SystemExit),
            ):
                self.backend.complete(self.messages, 111)

    def test_vendor_subprocess_stops_on_shared_ledger_quarantine(self) -> None:
        ledger_name = hashlib.sha256(_ENDPOINT.encode()).hexdigest() + ".json"
        (self.ledger / ledger_name).write_text("malformed ledger", encoding="utf-8")
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
            "generate.make_backend(config).complete(messages, 111)\n"
            "raise AssertionError('a quarantined request completed')\n",
            encoding="utf-8",
        )
        config_dir = self.root / "config"
        config_dir.mkdir()
        (config_dir / "vendor.json").write_text(_CONFIG.model_dump_json(), encoding="utf-8")
        (config_dir / "seeds.jsonl").write_text(_SEED.model_dump_json() + "\n", encoding="utf-8")
        (config_dir / "prompt.md").write_text("<<<SYSTEM>>>\nInspect\n<<<USER>>>\n{{seed_prompt}}\n")
        variables = {
            "ENDPOINT": _ENDPOINT,
            "MODEL": self.runtime.model,
            "MODEL_PATH": self.runtime.capacity.expected_model_path,
            "LEDGER_DIR": str(self.ledger),
            "VENDOR_DIR": str(self.runtime.vendor_dir),
            "OUTPUT_DIR": str(self.runtime.output_dir),
        }

        with patch.dict(os.environ, {ENV_PREFIX + key: value for key, value in variables.items()}):
            with self.assertRaises(PilotError):
                run_pilot(config_dir)

        (log,) = self.runtime.output_dir.glob("*/vendor.log")
        self.assertIn("CapacityQuarantined", log.read_text(encoding="utf-8"))
        self.assertEqual(list(self.runtime.output_dir.rglob("dataset.csv")), [])

    def test_missing_configuration_stops_before_preparing_a_run(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(PilotError, "SYNTHETIC_PILOT_LEDGER_DIR"):
                run_pilot(self.root)

        self.assertFalse(self.runtime.output_dir.exists())

    def test_guard_unavailability_never_returns_a_candidate_to_vendor(self) -> None:
        with (
            patch(
                "codebench_pipeline.synthetic_pilot_vendor.guarded_complete",
                side_effect=CapacityUnavailable("stale_snapshot"),
            ),
            self.assertRaisesRegex(SystemExit, "CapacityUnavailable"),
        ):
            self.backend.complete(self.messages, 111)
