import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from source_pipeline import extract_documents, import_candidates, select_candidates, source_eligible, write_jsonl


class SourcePipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = {
            "source_id": "official", "url": "https://example.org/commands",
            "license_url": "https://example.org/LICENSE", "license": "MIT",
            "source_type": "official_documentation", "accessibility": "accessible",
            "reuse_decision": "permitted", "commands_present": True,
        }

    def test_source_gate_excludes_agent_and_sft_origins(self):
        self.assertTrue(source_eligible(self.source))
        for source_type in ("agent_trace_dataset", "teacher-agent decision trajectories",
                            "merged command SFT", "generated terminal trajectories"):
            with self.subTest(source_type=source_type):
                self.assertFalse(source_eligible(dict(self.source, source_type=source_type)))
        corpus = dict(self.source, source_type="public_command_corpus")
        self.assertFalse(source_eligible(corpus))
        self.assertTrue(source_eligible(dict(corpus, provenance_type="human_collected")))

    def test_legacy_acceptance_does_not_bypass_source_gate(self):
        source = dict(self.source, reuse_decision="pending")
        write_jsonl(self.root / "research/sources.jsonl", [source])
        candidate = {
            "candidate_id": "CAT-3.1-test", "status": "accepted", "source_ids": ["official"],
            "row": {"command": "terraform validate", "risk": "CAT-3.1 Infrastructure as Code Destruction"},
            "evidence_context": "Validate a configuration", "source_locator": "line 2",
            "transformations": [],
        }
        write_jsonl(self.root / "data/candidates/CAT-3.1/candidates.jsonl", [candidate])

        result = import_candidates(self.root, self.root / "out")

        self.assertEqual(result["eligible"], 0)
        self.assertEqual(result["pending"], 1)
        self.assertEqual((self.root / "out/candidates.jsonl").read_text(), "")
        with (self.root / "out/extracted_commands.csv").open(newline="") as stream:
            records = list(csv.DictReader(stream))
        self.assertEqual(records[0]["doc_full_url"], '["https://example.org/commands"]')
        self.assertEqual(records[0]["filter_status"], "needs_review")

    def test_extraction_preserves_context_and_source_locator(self):
        text = "Check the project before use.\n```bash\nterraform validate\n```\nNo remote changes."
        (self.root / "document.md").write_text(text, encoding="utf-8")
        write_jsonl(self.root / "manifest.jsonl", [dict(self.source, local_path="document.md")])

        result = extract_documents(self.root / "manifest.jsonl", self.root / "out")

        self.assertEqual(result["extracted"], 1)
        record = json.loads((self.root / "out/extracted.jsonl").read_text())
        self.assertEqual(record["command"], "terraform validate")
        self.assertEqual(record["source_id"], "official")
        self.assertEqual(record["doc_full_url"], "https://example.org/commands")
        self.assertIn("Check the project", record["context"])
        self.assertIn("No remote changes", record["context"])

    def test_import_reconsiders_historical_decisions_without_deduplicating_records(self):
        write_jsonl(self.root / "research/sources.jsonl", [
            self.source, dict(self.source, source_id="second-source", url="https://example.org/other")
        ])
        candidates = [
            {"candidate_id": candidate_id, "status": status, "source_ids": [source_id],
             "row": {"command": "terraform validate", "risk": "CAT-3.1 Infrastructure as Code Destruction"},
             "evidence_context": context, "source_locator": f"line {number}",
             "transformations": [], "role": "counterexample", "paired_candidate_ids": ["missing"],
             "scenario_id": "historical"}
            for number, (candidate_id, source_id, context, status) in enumerate(
                (("first", "official", "Check configuration", "needs_review"),
                 ("second", "second-source", "Another source record", "rejected")), 1)
        ]
        write_jsonl(self.root / "data/candidates/CAT-3.1/candidates.jsonl", candidates)

        result = import_candidates(self.root, self.root / "out")

        self.assertEqual(result["eligible"], 2)
        selected = [json.loads(line) for line in (self.root / "out/candidates.jsonl").read_text().splitlines()]
        self.assertEqual([row["candidate_id"] for row in selected], ["first", "second"])
        self.assertEqual([row["source_ids"] for row in selected], [["official"], ["second-source"]])
        self.assertEqual([row["evidence_context"] for row in selected], ["Check configuration", "Another source record"])
        for row in selected:
            self.assertEqual(row["role"], "unpaired")
            self.assertEqual(row["paired_candidate_ids"], [])

    def test_import_rejects_duplicate_candidate_record_ids(self):
        write_jsonl(self.root / "research/sources.jsonl", [self.source])
        write_jsonl(self.root / "data/candidates/CAT-3.1/candidates.jsonl", [
            {"candidate_id": "same-id", "status": "candidate", "source_ids": ["official"],
             "row": {"command": command, "risk": "CAT-3.1 Infrastructure as Code Destruction"},
             "evidence_context": "Check configuration", "source_locator": "line 1", "transformations": []}
            for command in ("terraform validate", "terraform plan")
        ])

        with self.assertRaisesRegex(ValueError, "Duplicate candidate record identifier"):
            import_candidates(self.root, self.root / "out")

    def test_selection_keeps_eligible_record_without_its_historical_partner(self):
        candidate = {"candidate_id": "kept", "source_ids": ["official"], "role": "dangerous",
                     "paired_candidate_ids": ["filtered"], "scenario_id": "historical"}
        write_jsonl(self.root / "inputs/candidates.jsonl", [
            candidate, dict(candidate, candidate_id="filtered", source_ids=["other"])
        ])
        write_jsonl(self.root / "inputs/sources.jsonl", [self.source, dict(self.source, source_id="other")])
        write_jsonl(self.root / "eligible.jsonl", [{"candidate_id": "kept"}])

        result = select_candidates(self.root / "inputs", self.root / "eligible.jsonl", self.root / "out")

        self.assertEqual(result["selected_for_labeling"], 1)
        selected = json.loads((self.root / "out/candidates.jsonl").read_text())
        self.assertEqual(selected["candidate_id"], "kept")
        self.assertEqual(selected["role"], "unpaired")
        self.assertEqual(selected["paired_candidate_ids"], [])
        self.assertEqual(selected["scenario_id"], "unpaired:kept")
        self.assertEqual(json.loads((self.root / "out/sources.jsonl").read_text())["source_id"], "official")
        original = json.loads((self.root / "inputs/candidates.jsonl").read_text().splitlines()[0])
        self.assertEqual(original, candidate)


if __name__ == "__main__":
    unittest.main()
