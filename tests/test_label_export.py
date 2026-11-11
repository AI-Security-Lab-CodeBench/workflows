import csv
import json
import subprocess
import sys
import unittest

from label_fixtures import LabelPipelineFixture, SCRIPTS

import label_contract as contract
import label_pipeline as pipeline
import validate_dataset as validator


class LabelExportTests(LabelPipelineFixture):
    def test_cli_exports_independently_approved_records_and_valid_bundle(self):
        self.prepare_reviews()
        command = [
            sys.executable, "-B", str(SCRIPTS / "label_pipeline.py"), "export",
            "--candidates", str(self.root / "candidates.jsonl"), "--sources", str(self.root / "sources.jsonl"),
            "--packets", str(self.packet_dir), "--responses", str(self.response_dir),
            "--review-packets", str(self.root / "independent/packets"), "--reviews", str(self.root / "reviews"),
            "--out", str(self.root / "bundle"),
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        stats = json.loads(result.stdout)
        self.assertEqual(stats["target_per_risk"], 250)
        self.assertEqual(stats["by_risk"][validator.RISKS[0]]["deficit"], 248)
        root = self.root / "bundle"
        rows = validator.read_csv(root / "commands.csv")
        self.assertEqual([row["command"] for row in rows],
                         [item["row"]["command"] for item in self.candidates])
        self.assertEqual([row["class"] for row in rows], ["ask", "allow"])
        self.assertEqual(validator.validate_bundle(
            rows, validator.load_jsonl(root / "data/processed/provenance.jsonl"),
            validator.load_candidates(root), validator.load_jsonl(root / "research/sources.jsonl"),
            validator.load_jsonl(root / "data/review/pending.jsonl")), [])
        with (root / "review/commands.csv").open(encoding="utf-8", newline="") as stream:
            review_rows = list(csv.DictReader(stream))
        self.assertEqual([row["csv_record"] for row in review_rows], ["1", "2"])
        self.assertEqual(review_rows[0]["review_rationale"], "Independent fixture review")
        self.assertEqual(validator.load_jsonl(self.root / "candidates.jsonl"), self.candidates)

    def test_identical_command_text_with_distinct_records_survives_export(self):
        self.candidates[1]["row"]["command"] = self.candidates[0]["row"]["command"]
        self.candidates[1]["evidence_context"] = self.candidates[0]["evidence_context"]
        self.write_jsonl("candidates.jsonl", self.candidates)
        pipeline.prepare(self.root / "candidates.jsonl", self.root / "sources.jsonl",
                         self.root / "semantics.jsonl", self.root / "duplicates", batch_size=1)
        self.packet_dir = self.root / "duplicates/packets"
        for path in self.packet_dir.glob("*.json"):
            envelope = contract.read_json(self.response_dir / path.name)
            envelope["packet_sha256"] = contract.read_json(path)["packet_sha256"]
            envelope["result"]["labels"][0].update(
                severity="critical", **{"class": "ask"},
                description="Удаляет инфраструктуру без подтверждения.")
            self.write_json(f"responses/{path.name}", envelope)
        self.prepare_reviews()
        self.assertEqual(self.export()["records"], 2)
        rows = validator.read_csv(self.root / "bundle/commands.csv")
        self.assertEqual([row["command"] for row in rows], [self.candidates[0]["row"]["command"]] * 2)


if __name__ == "__main__":
    unittest.main()
