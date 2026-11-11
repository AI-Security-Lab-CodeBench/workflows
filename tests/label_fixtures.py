import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import label_contract as contract
import label_pipeline as pipeline
import pipeline_export
import validate_dataset as validator


class LabelPipelineFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.sources = [{
            "source_id": "fixture-source", "url": "https://example.org/source",
            "locator": "examples 1-2", "revision": "fixture-v1", "retrieved_at": "2026-11-04",
            "source_type": "documentation", "risks": [validator.RISKS[0]],
            "accessibility": "accessible", "relevance": "Inert exchange fixture",
            "commands_present": True, "license": "CC0-1.0",
            "license_url": "https://example.org/license", "reuse_decision": "permitted",
            "notes": "Inert test data; not semantic safety evidence",
        }]
        self.candidates = []
        for index, (role, command) in enumerate((
            ("dangerous", "terraform destroy -auto-approve"),
            ("counterexample", "terraform validate"),
        )):
            self.candidates.append({
                "candidate_id": f"CAT-3.1-fixture-{index}", "row": {
                    "command": command, "description": "Предыдущее описание.",
                    "util": "terraform", "severity": "medium", "category": validator.CATEGORY,
                    "risk": validator.RISKS[0], "source": "real", "seed": "-", "class": "deny",
                },
                "source_ids": ["fixture-source"], "source_locator": "examples 1-2",
                "evidence_context": "Actual fixture text includes " + command,
                "transformations": [], "documentation_urls": ["https://example.org/docs"],
                "tool_version": "fixture-v1", "label_rationale": "Previous verdict",
                "scenario_id": "fixture-scenario", "role": role,
                "paired_candidate_ids": [f"CAT-3.1-fixture-{1 - index}"],
                "synthetic_model": None, "status": "accepted",
            })
        self.docs = [{
            "utility": "terraform", "subcommands": ["destroy", "validate"],
            "url": "https://example.org/docs", "retrieved_at": "2026-11-04",
            "snippet": "Inert fixture describing both operations.", "version_limits": "fixture-v1 only",
        }]
        self.write_jsonl("candidates.jsonl", self.candidates)
        self.write_jsonl("sources.jsonl", self.sources)
        self.write_jsonl("semantics.jsonl", self.docs)
        pipeline.prepare(self.root / "candidates.jsonl", self.root / "sources.jsonl",
                         self.root / "semantics.jsonl", self.root / "label", batch_size=1)
        self.packet_dir = self.root / "label/packets"
        self.response_dir = self.root / "responses"
        self.response_dir.mkdir()
        for path in sorted(self.packet_dir.glob("*.json")):
            packet = contract.read_json(path)
            labels = []
            for key in packet["candidate_ids"]:
                safe = key.endswith("-1")
                labels.append({
                    "candidate_id": key, "decision": "accept", "category": validator.CATEGORY,
                    "risk": validator.RISKS[0], "severity": "low" if safe else "critical",
                    "class": "allow" if safe else "ask", "description": "Проверяет конфигурацию."
                    if safe else "Удаляет инфраструктуру без подтверждения.",
                    "util": "terraform", **{field: 0.8 for field in contract.CONFIDENCES},
                    "rationale": "Fresh fixture reasoning", "documentation_urls": ["https://example.org/docs"],
                })
            self.write_json(f"responses/{path.name}", self.envelope(packet, {"labels": labels}))

    def envelope(self, packet, result):
        return {"packet_sha256": packet["packet_sha256"], "model": "fixture-isolated-alias",
                "responded_at": "2026-11-04T00:00:00Z", "result": result}

    def write_json(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")

    def write_jsonl(self, name, values):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(value, ensure_ascii=False) + "\n" for value in values),
                        encoding="utf-8")

    def prepare_reviews(self):
        pipeline.review_prepare(self.packet_dir, self.response_dir, self.root / "independent")
        for path in (self.root / "independent/packets").glob("*.json"):
            packet = contract.read_json(path)
            result = {"reviews": [{"candidate_id": key, "verdict": "approve",
                                   "rationale": "Independent fixture review"}
                                  for key in packet["candidate_ids"]]}
            envelope = self.envelope(packet, result)
            envelope["responded_at"] = "2026-11-04T00:01:00Z"
            self.write_json(f"reviews/{path.name}", envelope)

    def export(self, limit=250):
        return pipeline_export.export(
            self.root / "candidates.jsonl", self.root / "sources.jsonl", self.packet_dir,
            self.response_dir, self.root / "independent/packets", self.root / "reviews",
            self.root / "bundle", limit_per_risk=limit,
        )
