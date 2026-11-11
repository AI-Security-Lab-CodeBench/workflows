import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from postfilter import postfilter
from prefilter import prefilter
from source_pipeline import write_jsonl


class FilterStageTests(unittest.TestCase):
    def test_existing_audit_directories_are_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for stage in ("pre", "post"):
                target = root / stage
                target.mkdir()
                with self.subTest(stage=stage), self.assertRaises(ValueError):
                    if stage == "pre":
                        prefilter(root / "unused.jsonl", target)
                    else:
                        postfilter([root / "unused.jsonl"], target)

    def test_broad_extraction_keeps_noise_until_auditable_postfilter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = "Example:\n```bash\nterraform validate\nprintf harmless\npsql -c 'unterminated\n```\n"
            (root / "doc.md").write_text(document)
            source = {
                "source_id": "official", "url": "https://example.org/doc", "local_path": "doc.md",
                "format": "markdown", "source_type": "official_documentation", "license": "MIT",
                "license_url": "https://example.org/LICENSE", "accessibility": "accessible",
                "reuse_decision": "permitted", "commands_present": True,
            }
            write_jsonl(root / "manifest.jsonl", [source])

            raw_report = prefilter(root / "manifest.jsonl", root / "pre")
            report = postfilter([root / "pre/raw_commands.jsonl"], root / "post")

            self.assertEqual(raw_report["raw_candidates"], 3)
            self.assertEqual(report["eligible"], 1)
            self.assertEqual(report["rejected_or_needs_review"], 2)
            accepted = json.loads((root / "post/eligible.jsonl").read_text())
            self.assertEqual(accepted["command"], "terraform validate")
            self.assertEqual(accepted["doc_full_url"], "https://example.org/doc")
            rejected = [json.loads(line) for line in (root / "post/rejected.jsonl").read_text().splitlines()]
            self.assertEqual({row["raw_command"] for row in rejected}, {"printf harmless", "psql -c 'unterminated"})

    def test_structured_source_rows_keep_record_locators(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_jsonl(root / "commands.jsonl", [
                {"shell": "terraform validate", "description": "Before deployment"},
                {"shell": "terraform validate", "description": "Repeated example"},
            ])
            source = {
                "source_id": "human", "url": "https://example.org/corpus", "local_path": "commands.jsonl",
                "format": "jsonl", "text_field": "shell", "context_field": "description",
                "source_type": "public_command_corpus", "provenance_type": "human_authored",
                "license": "MIT", "license_url": "https://example.org/LICENSE",
                "accessibility": "accessible", "reuse_decision": "permitted", "commands_present": True,
            }
            write_jsonl(root / "manifest.jsonl", [source])

            prefilter(root / "manifest.jsonl", root / "pre")
            report = postfilter([root / "pre/raw_commands.jsonl"], root / "post")

            self.assertEqual(report["eligible"], 2)
            records = [json.loads(line) for line in (root / "post/eligible.jsonl").read_text().splitlines()]
            self.assertNotEqual(records[0]["raw_id"], records[1]["raw_id"])
            self.assertEqual([row["command"] for row in records], ["terraform validate", "terraform validate"])
            self.assertIn("record 2; field shell", records[1]["doc_pos"])
            self.assertIn("Repeated example", records[1]["context"])
            self.assertEqual((root / "post/rejected.jsonl").read_text(), "")

    def test_trivial_command_variants_survive_postfilter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = ["aws s3 ls s3://example-bucket-1", "aws s3 ls s3://example-bucket-2"]
            write_jsonl(root / "raw.jsonl", [
                {"raw_id": str(number), "source_id": "official", "command": command,
                 "doc_full_url": "https://example.org/doc", "doc_pos": f"line {number}"}
                for number, command in enumerate(commands, 1)
            ])

            postfilter([root / "raw.jsonl"], root / "post")

            records = [json.loads(line) for line in (root / "post/eligible.jsonl").read_text().splitlines()]
            self.assertEqual([row["command"] for row in records], commands)

    def test_duplicate_raw_record_ids_fail_even_with_different_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_jsonl(root / "raw.jsonl", [
                {"raw_id": "same-id", "source_id": "official", "command": command,
                 "doc_full_url": "https://example.org/doc", "doc_pos": "line 1"}
                for command in ("terraform validate", "terraform plan")
            ])

            with self.assertRaisesRegex(ValueError, "Duplicate raw record identifier"):
                postfilter([root / "raw.jsonl"], root / "post")


if __name__ == "__main__":
    unittest.main()
