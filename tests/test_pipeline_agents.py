from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import BaseModel, ValidationError

from codebench_pipeline.agents import InferenceError, infer, propose_adapter, run_packets

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from label_contract import make_packet


class Answer(BaseModel):
    count: int


def event(text: str, model: str = "small", kind: str = "text", stop: str = "stop") -> str:
    return json.dumps(
        {
            "type": "message_end",
            "message": {
                "role": "assistant",
                "provider": "fixture",
                "model": model,
                "stopReason": stop,
                "content": [{"type": kind, "text": text}],
            },
        }
    )


class PipelineAgentTests(unittest.TestCase):
    def test_infer_keeps_bound_request_response_and_observed_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary) / "inference"
            events = event('{"count":7}')
            with patch(
                "codebench_pipeline.agents.subprocess.run",
                return_value=subprocess.CompletedProcess(["omo"], 0, events, ""),
            ) as process:
                answer = infer(Answer, "policy", "$(never execute)", out, "fixture/small")
            self.assertEqual(answer.count, 7)
            request = json.loads((out / "request.json").read_text())
            digest = request.pop("request_sha256")
            serialized = json.dumps(
                request, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            )
            self.assertEqual(digest, hashlib.sha256(serialized.encode()).hexdigest())
            receipt = json.loads((out / "receipt.json").read_text())
            self.assertEqual((receipt["actual_model"], receipt["status"]), ("fixture/small", "validated"))
            self.assertEqual(json.loads((out / "response.json").read_text()), {"count": 7})
            self.assertEqual((out / "stdout.jsonl").read_text(), events)
            self.assertFalse(Path(process.call_args.kwargs["cwd"]).exists())
            command = process.call_args.args[0]
            for flag in (
                "--no-tools",
                "--no-session",
                "--no-context-files",
                "--no-extensions",
                "--no-skills",
                "--no-prompt-templates",
            ):
                self.assertIn(flag, command)
            self.assertNotIn("$(never execute)", command)

    def test_invalid_structured_outputs_keep_failed_evidence(self) -> None:
        cases = (
            (event('{"count":"7"}'), ValidationError),
            (event('{"count":7,"extra":true}'), ValidationError),
            (event("{}"), ValidationError),
            (event('{"count":7,"count":8}'), InferenceError),
            (event('{"count":NaN}'), InferenceError),
            (event('{"count":7}', kind="toolCall"), InferenceError),
            (
                json.dumps({"type": "tool_execution_start", "toolName": "bash"})
                + "\n"
                + event('{"count":7}'),
                InferenceError,
            ),
            (event('{"count":7}', model="fallback"), InferenceError),
            (event('{"count":7}', stop="length"), InferenceError),
        )
        for events, error in cases:
            with self.subTest(events=events), tempfile.TemporaryDirectory() as temporary:
                out = Path(temporary) / "inference"
                with patch(
                    "codebench_pipeline.agents.subprocess.run",
                    return_value=subprocess.CompletedProcess(["omo"], 0, events, ""),
                ):
                    with self.assertRaises(error):
                        infer(Answer, "policy", "input", out, "fixture/small")
                self.assertEqual(json.loads((out / "receipt.json").read_text())["status"], "failed")
                self.assertFalse((out / "response.json").exists())
                self.assertEqual((out / "stdout.jsonl").read_text(), events)

    def test_process_failures_and_timeouts_preserve_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            for name, failure in (
                ("exit", subprocess.CompletedProcess(["omo"], 1, "", "provider failed")),
                ("timeout", subprocess.TimeoutExpired(["omo"], 1, output=b"partial", stderr=b"timed out")),
            ):
                with self.subTest(name=name):
                    out = Path(temporary) / name
                    with patch("codebench_pipeline.agents.subprocess.run") as process:
                        if isinstance(failure, subprocess.TimeoutExpired):
                            process.side_effect = failure
                        else:
                            process.return_value = failure
                        with self.assertRaises(InferenceError):
                            infer(Answer, "policy", "input", out, "fixture/small")
                    self.assertTrue((out / "stderr.txt").read_text())
                    self.assertEqual(json.loads((out / "receipt.json").read_text())["status"], "failed")

    def test_adapter_rejects_executable_field_expressions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = json.dumps(
                {
                    "format": "jsonl",
                    "text_field": "__import__('os').system('false')",
                    "context_field": "",
                    "rationale": "fixture",
                }
            )
            with patch(
                "codebench_pipeline.agents.subprocess.run",
                return_value=subprocess.CompletedProcess(["omo"], 0, event(output), ""),
            ):
                with self.assertRaises(InferenceError):
                    propose_adapter("inert sample", Path(temporary) / "adapter", "fixture/small")

    def test_cached_packet_responses_are_validated_not_blindly_skipped(self) -> None:
        for invalid in (False, True):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                packets, responses = root / "packets", root / "responses"
                packets.mkdir()
                responses.mkdir()
                packet = make_packet("review", ["candidate-1"], {})
                (packets / "review-00001.json").write_text(json.dumps(packet))
                envelope = {
                    "packet_sha256": "0" * 64 if invalid else packet["packet_sha256"],
                    "model": "fixture/small",
                    "responded_at": "2026-11-09T00:00:00Z",
                    "result": {
                        "reviews": [
                            {
                                "candidate_id": "candidate-1",
                                "verdict": "approve",
                                "rationale": "checked",
                            }
                        ]
                    },
                }
                response = responses / "review-00001.json"
                response.write_text(json.dumps(envelope))
                original = response.read_bytes()
                with patch.dict(os.environ, {"CODEBENCH_OMO_EXECUTABLE": "/nonexistent/native-omo"}):
                    if invalid:
                        with self.assertRaises(InferenceError):
                            run_packets(packets, responses, "fixture/small")
                    else:
                        run_packets(packets, responses, "fixture/small")
                self.assertEqual(response.read_bytes(), original)
                receipts = sorted((responses / "evidence/pipeline-runs").glob("*/*.receipt.json"))
                self.assertEqual(len(receipts), 1 if invalid else 3)
                self.assertEqual(json.loads(receipts[0].read_text())["exit_code"], 1 if invalid else 0)
