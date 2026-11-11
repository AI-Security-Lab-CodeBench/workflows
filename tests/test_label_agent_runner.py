import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from label_contract import DatasetError
from run_label_agents import assistant_result


class LabelAgentRunnerTests(unittest.TestCase):
    def event(self, content, model="fixture-small", stop="stop"):
        return json.dumps({"type": "message_end", "message": {
            "role": "assistant", "provider": "fixture", "model": model,
            "stopReason": stop, "content": content,
        }})

    def test_reads_final_json_with_observed_model_identity(self):
        events = self.event([{"type": "thinking", "thinking": "analysis"},
                             {"type": "text", "text": '{"labels":[]}'}])
        result, model = assistant_result(events, "fixture/fixture-small")
        self.assertEqual(result, {"labels": []})
        self.assertEqual(model, "fixture/fixture-small")

    def test_rejects_fallback_truncation_and_tools(self):
        for event in (
            self.event([{"type": "text", "text": "{}"}], model="unexpected"),
            self.event([{"type": "text", "text": "{}"}], stop="length"),
            self.event([{"type": "toolCall", "name": "bash"}]),
        ):
            with self.subTest(event=event), self.assertRaises(DatasetError):
                assistant_result(event, "fixture/fixture-small")
