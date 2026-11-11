import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from prepare_review_candidates import prepare
from source_pipeline import write_jsonl


class PrepareReviewCandidatesTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.args = argparse.Namespace(
            inputs=self.root / "inputs", eligible=self.root / "eligible.jsonl",
            documents=self.root / "documents.jsonl", out=self.root / "out",
        )
        write_jsonl(self.args.inputs / "sources.jsonl", [])
        write_jsonl(self.args.inputs / "candidates.jsonl", [])
        write_jsonl(self.args.documents, [{
            "source_id": "official", "url": "https://example.org/commands",
            "license_url": "https://example.org/LICENSE", "license": "MIT",
            "source_type": "official_documentation", "accessibility": "accessible",
            "reuse_decision": "permitted", "commands_present": True,
            "local_path": "commands.md", "revision": "v1", "retrieved_at": "2026-11-05",
        }])

    def test_preparation_clears_historical_roles_and_keeps_identical_commands(self):
        historical = {
            "candidate_id": "legacy", "row": {"command": "terraform validate"},
            "role": "counterexample", "paired_candidate_ids": ["missing"], "scenario_id": "historical",
        }
        write_jsonl(self.args.inputs / "candidates.jsonl", [historical])
        write_jsonl(self.args.eligible, [
            {"raw_id": raw_id, "source_id": "official", "risk_candidates": ["CAT-3.1"],
             "command": "terraform validate", "doc_full_url": "https://example.org/commands",
             "doc_pos": f"line {number}", "context": context, "transformations": []}
            for number, (raw_id, context) in enumerate(
                (("first-record", "First example"), ("second-record", "Second example")), 1)
        ])

        result = prepare(self.args)

        self.assertEqual(result, {"candidates": 3, "new_unpaired_candidates": 2})
        records = [json.loads(line) for line in (self.args.out / "candidates.jsonl").read_text().splitlines()]
        self.assertEqual([row["candidate_id"] for row in records],
                         ["legacy", "CAT-3.1-raw-first-record", "CAT-3.1-raw-second-record"])
        self.assertEqual([row["row"]["command"] for row in records], ["terraform validate"] * 3)
        self.assertEqual([row["evidence_context"] for row in records[1:]], ["First example", "Second example"])
        for row in records:
            self.assertEqual(row["role"], "unpaired")
            self.assertEqual(row["paired_candidate_ids"], [])
            self.assertEqual(row["scenario_id"], "unpaired:" + row["candidate_id"])
        self.assertEqual(json.loads((self.args.inputs / "candidates.jsonl").read_text()), historical)

    def test_preparation_rejects_duplicate_raw_ids_before_skipping_legacy_rows(self):
        write_jsonl(self.args.eligible, [{"raw_id": "same-id", "candidate_id": "legacy"}] * 2)

        with self.assertRaisesRegex(ValueError, "Duplicate raw record identifier"):
            prepare(self.args)

    def test_preparation_rejects_duplicate_candidate_ids(self):
        write_jsonl(self.args.inputs / "candidates.jsonl", [{"candidate_id": "same-id"}] * 2)

        with self.assertRaisesRegex(ValueError, "Duplicate candidate record identifier"):
            prepare(self.args)


if __name__ == "__main__":
    unittest.main()
