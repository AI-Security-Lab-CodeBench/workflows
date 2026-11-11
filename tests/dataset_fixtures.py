import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import integrate_dataset as integrator
import validate_dataset as validator


class DatasetFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.candidates = self.make_pair("alpha")
        self.sources = [{
            "source_id": "fixture-source",
            "url": "https://example.org/fixture",
            "locator": "fixture example",
            "revision": "fixture-revision",
            "retrieved_at": "2026-10-29",
            "source_type": "documentation",
            "risks": [validator.RISKS[0]],
            "accessibility": "accessible",
            "relevance": "Inert test fixture",
            "commands_present": True,
            "license": "CC0-1.0",
            "license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
            "reuse_decision": "permitted",
            "notes": "Local fixture; no network access",
        }]
        self.pending = []
        self.assertEqual(self.errors(), [])

    def make_pair(self, name):
        result = []
        for role, label, command in (
            ("dangerous", "deny", f"echo {name}-danger"),
            ("counterexample", "allow", f"printf '{name}-safe'"),
        ):
            other_role = "counterexample" if role == "dangerous" else "dangerous"
            result.append({
                "candidate_id": f"CAT-3.1-{name}-{role}",
                "row": {
                    "command": command,
                    "description": "Инертный пример для проверки обработки данных",
                    "util": "echo" if role == "dangerous" else "printf",
                    "severity": "high" if role == "dangerous" else "low",
                    "category": validator.CATEGORY,
                    "risk": validator.RISKS[0],
                    "source": "documentation",
                    "seed": "-",
                    "class": label,
                },
                "source_ids": ["fixture-source"],
                "source_locator": "fixture example",
                "evidence_context": "Inert command text for parser tests only",
                "transformations": [],
                "documentation_urls": ["https://example.org/fixture"],
                "tool_version": "fixture-version",
                "label_rationale": "Fixture labels exercise standalone validation, not shell safety",
                "scenario_id": "fixture-scenario",
                "role": role,
                "paired_candidate_ids": [f"CAT-3.1-{name}-{other_role}"],
                "synthetic_model": None,
                "status": "accepted",
            })
        return result

    def errors(self):
        rows, provenance = integrator.prepare(self.candidates)
        return validator.validate_bundle(rows, provenance, self.candidates, self.sources, self.pending)

    def write_jsonl(self, relative, records):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records), encoding="utf-8")

    def write_inputs(self):
        self.write_jsonl("data/candidates/CAT-3.1/candidates.jsonl", self.candidates)
        self.write_jsonl("research/sources.jsonl", self.sources)
        self.write_jsonl("data/review/pending.jsonl", self.pending)

    def run_script(self, name):
        return subprocess.run(
            [sys.executable, str(SCRIPTS / name), str(self.root)],
            cwd=self.root,
            capture_output=True,
            text=True,
            check=False,
        )
