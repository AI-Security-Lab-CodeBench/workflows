import copy
import csv
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# isort: split
from label_fixtures import LabelPipelineFixture

# isort: split
import label_contract as contract
import label_pipeline as pipeline
import validate_dataset as validator
from category_config import parse_script_policy


class LabelPipelineTests(LabelPipelineFixture):
    def test_distinct_compiled_policies_bind_packets_without_cross_invocation_state(self):
        variant_input = json.loads(self.policy.to_json())
        variant_input["rules"]["category_name"] = "Alternative fixture category"
        variant_input["base"] = "Alternative fixture instructions"
        variant = parse_script_policy(json.dumps(variant_input))

        outputs = []
        for name, policy in (("baseline", self.policy), ("variant", variant), ("repeat", self.policy)):
            out = self.root / name
            pipeline.prepare(
                self.root / "candidates.jsonl",
                self.root / "sources.jsonl",
                self.root / "semantics.jsonl",
                out,
                batch_size=1,
                policy=policy,
            )
            outputs.append((out / "packets/label-00001.json").read_bytes())

        self.assertEqual(outputs[0], outputs[2])
        packet = json.loads(outputs[1])
        self.assertTrue(packet["system_prompt"].startswith("Alternative fixture instructions"))
        self.assertEqual(
            packet["response_schema"]["properties"]["labels"]["items"]["properties"]["category"]["enum"],
            ["CAT-3 Alternative fixture category"],
        )
        with self.assertRaisesRegex(validator.DatasetError, "Altered response schema"):
            contract.validate_packet(packet, "label", self.taxonomy)

    def test_compiled_policy_subprocess_and_active_import_work_without_authored_files(self):
        isolated = self.root / "isolated"
        isolated.mkdir()
        authored = Path(__file__).resolve().parents[1] / "business-configs"
        (isolated / "sitecustomize.py").write_text(
            "import os, sys\n"
            f"authored = {str(authored)!r} + os.sep\n"
            "def forbid_authored(event, arguments):\n"
            "    if event == 'open' and isinstance(arguments[0], (str, bytes)):\n"
            "        if os.path.abspath(os.fsdecode(arguments[0])).startswith(authored):\n"
            "            raise FileNotFoundError('authored inputs unavailable in this subprocess')\n"
            "sys.addaudithook(forbid_authored)\n",
            encoding="utf-8",
        )

        with patch.dict(os.environ, {"PYTHONPATH": str(isolated)}):
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    "from codebench_pipeline.dataset_documentation_models import command_words\n"
                    "assert command_words(['terraform', 'validate']) == ['terraform', 'validate']\n"
                    "from scripts.category_config import load_taxonomy\n"
                    "try:\n"
                    "    load_taxonomy()\n"
                    "except FileNotFoundError as error:\n"
                    "    assert 'authored inputs unavailable' in str(error)\n"
                    "else:\n"
                    "    raise AssertionError('authored files were readable')\n",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            out = self.root / "compiled"
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(authored.parent / "scripts" / "label_pipeline.py"),
                    "--compiled-policy",
                    "prepare",
                    "--candidates",
                    str(self.root / "candidates.jsonl"),
                    "--sources",
                    str(self.root / "sources.jsonl"),
                    "--semantics",
                    str(self.root / "semantics.jsonl"),
                    "--out",
                    str(out),
                    "--batch-size",
                    "1",
                ],
                input=self.policy.to_json(),
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

        self.assertEqual(
            {path.name: path.read_bytes() for path in (out / "packets").glob("*.json")},
            {path.name: path.read_bytes() for path in self.packet_dir.glob("*.json")},
        )

    def test_prepare_exposes_standalone_facts_without_previous_verdicts_or_links(self):
        packet = contract.read_json(self.packet_dir / "label-00001.json")
        facts = packet["input"]["candidates"][0]
        self.assertEqual(
            facts["row"],
            {key: self.candidates[0]["row"][key] for key in ("command", "util", "source", "seed")},
        )
        self.assertNotIn("label_rationale", facts)
        self.assertNotIn("status", facts)
        self.assertEqual(set(packet["input"]), {"candidates"})
        for field in ("role", "scenario_id", "paired_candidate_ids"):
            self.assertNotIn(field, facts)
        self.assertEqual(facts["source_evidence"], self.sources)
        self.assertEqual(facts["documentation"], self.docs)
        result_schema = packet["response_schema"]["properties"]["labels"]
        self.assertEqual(result_schema["minItems"], 1)
        self.assertEqual(result_schema["maxItems"], 1)
        self.assertEqual(result_schema["items"]["properties"]["candidate_id"]["enum"], ["CAT-3.1-fixture-0"])
        self.assertEqual(
            result_schema["items"]["properties"]["documentation_urls"]["items"]["enum"],
            ["https://example.org/docs"],
        )

    def test_confidences_reject_boolean_nonfinite_and_out_of_range(self):
        path = self.response_dir / "label-00001.json"
        original = contract.read_json(path)
        for field in contract.CONFIDENCES:
            for value in (True, False, float("nan"), float("inf"), -0.01, 1.01, "0.8", None):
                with self.subTest(field=field, value=value):
                    envelope = copy.deepcopy(original)
                    envelope["result"]["labels"][0][field] = value
                    self.write_json(f"responses/{path.name}", envelope)
                    with self.assertRaises(validator.DatasetError):
                        pipeline.review_prepare(
                            self.packet_dir, self.response_dir, self.root / "bad", taxonomy=self.taxonomy
                        )
                    self.assertFalse((self.root / "bad").exists())

    def test_result_ids_hashes_enums_and_rationales_fail_closed(self):
        path = self.response_dir / "label-00001.json"
        original = contract.read_json(path)
        variants = []
        missing = copy.deepcopy(original)
        missing["result"]["labels"] = []
        variants.append(missing)
        duplicate = copy.deepcopy(original)
        duplicate["result"]["labels"] *= 2
        variants.append(duplicate)
        for field, value in (
            ("candidate_id", "unknown"),
            ("decision", "accepted"),
            ("severity", "extreme"),
            ("class", "modify"),
            ("rationale", "  "),
            ("rationale", "x" * 401),
            ("documentation_urls", ["https://example.org/not-provided"]),
            ("risk", "CAT-3.1"),
            ("category", "CAT-3"),
        ):
            bad = copy.deepcopy(original)
            bad["result"]["labels"][0][field] = value
            variants.append(bad)
        forged = copy.deepcopy(original)
        forged["packet_sha256"] = "0" * 64
        variants.append(forged)
        for index, value in enumerate(variants):
            with self.subTest(index=index):
                self.write_json(f"responses/{path.name}", value)
                with self.assertRaises(validator.DatasetError):
                    pipeline.review_prepare(
                        self.packet_dir, self.response_dir, self.root / "bad", taxonomy=self.taxonomy
                    )
                self.assertFalse((self.root / "bad").exists())

    def test_missing_response_file_and_altered_packet_fail_closed(self):
        path = self.response_dir / "label-00001.json"
        original = path.read_text(encoding="utf-8")
        path.unlink()
        with self.assertRaises(validator.DatasetError):
            pipeline.review_prepare(
                self.packet_dir, self.response_dir, self.root / "missing", taxonomy=self.taxonomy
            )
        path.write_text(original, encoding="utf-8")
        packet_path = self.packet_dir / path.name
        packet = contract.read_json(packet_path)
        packet["input"]["candidates"][0]["row"]["command"] = "altered inert text"
        self.write_json(f"label/packets/{path.name}", packet)
        with self.assertRaisesRegex(validator.DatasetError, "hash"):
            pipeline.review_prepare(
                self.packet_dir, self.response_dir, self.root / "altered", taxonomy=self.taxonomy
            )

    def test_repair_preserves_valid_responses_and_repackages_only_missing(self):
        (self.response_dir / "label-00002.json").unlink()
        original = (self.response_dir / "label-00001.json").read_bytes()
        report = pipeline.repair(
            self.packet_dir, self.response_dir, self.root / "repaired", batch_size=1, taxonomy=self.taxonomy
        )
        self.assertEqual((report["preserved"], report["retry_packets"]), (1, 1))
        self.assertEqual((self.root / "repaired/responses/label-00001.json").read_bytes(), original)
        retry = contract.read_json(self.root / "repaired/packets/label-00002-retry-001.json")
        self.assertEqual(retry["candidate_ids"], ["CAT-3.1-fixture-1"])
        self.assertFalse((self.root / "repaired/responses/label-00002-retry-001.json").exists())

    def test_malformed_semantics_and_source_evidence_fail_before_output(self):
        for field, value in (("snippet", ""), ("subcommands", "destroy"), ("url", "file:///tmp/docs")):
            with self.subTest(field=field):
                docs = copy.deepcopy(self.docs)
                docs[0][field] = value
                self.write_jsonl("malformed.jsonl", docs)
                with self.assertRaises(validator.DatasetError):
                    pipeline.prepare(
                        self.root / "candidates.jsonl",
                        self.root / "sources.jsonl",
                        self.root / "malformed.jsonl",
                        self.root / "bad",
                        policy=self.policy,
                    )
        sources = copy.deepcopy(self.sources)
        sources[0]["commands_present"] = "true"
        self.write_jsonl("malformed-sources.jsonl", sources)
        with self.assertRaises(validator.DatasetError):
            pipeline.prepare(
                self.root / "candidates.jsonl",
                self.root / "malformed-sources.jsonl",
                self.root / "semantics.jsonl",
                self.root / "bad",
                policy=self.policy,
            )
        self.assertFalse((self.root / "bad").exists())

    def test_unrelated_same_utility_documentation_cannot_fill_a_gap(self):
        docs = [dict(self.docs[0], subcommands=["fmt"])]
        self.write_jsonl("unrelated.jsonl", docs)
        with self.assertRaises(validator.DatasetError):
            pipeline.prepare(
                self.root / "candidates.jsonl",
                self.root / "sources.jsonl",
                self.root / "unrelated.jsonl",
                self.root / "bad",
                policy=self.policy,
            )
        self.assertFalse((self.root / "bad").exists())

    def test_default_command_documents_do_not_cover_explicit_subcommands(self):
        for utility, command, count in (
            ("vercel", "vercel --prod", 1),
            ("vercel", "vercel rollback status", 0),
            ("redis-cli", "redis-cli -c", 1),
            ("redis-cli", "redis-cli -c INFO", 0),
            ("dropdb", "dropdb demo", 1),
        ):
            with self.subTest(command=command):
                candidate = {"row": {"command": command, "util": utility}, "documentation_urls": []}
                docs = [dict(self.docs[0], utility=utility, subcommands=[utility])]
                self.assertEqual(len(pipeline.documentation_for(candidate, docs)), count)

    def test_pending_review_does_not_exclude_another_approved_record(self):
        self.prepare_reviews()
        path = self.root / "reviews/label-00002.json"
        envelope = contract.read_json(path)
        envelope["result"]["reviews"][0]["verdict"] = "needs_review"
        envelope["result"]["reviews"][0]["rationale"] = (
            "The supplied documentation does not establish this flag's behavior."
        )
        self.write_json("reviews/label-00002.json", envelope)
        stats = self.export()
        self.assertEqual(stats["records"], 1)
        pending = validator.load_jsonl(self.root / "bundle/data/review/pending.jsonl")
        self.assertEqual({item["candidate_id"] for item in pending}, {self.candidates[1]["candidate_id"]})
        self.assertIn("independent review", pending[0]["issue"])
        with (self.root / "bundle/review/pending.csv").open(encoding="utf-8", newline="") as stream:
            pending_row = next(csv.DictReader(stream))
        self.assertNotIn("evidence", pending_row)
        self.assertEqual(pending_row["command"], self.candidates[1]["row"]["command"])
        self.assertEqual(pending_row["llm_rationale"], envelope["result"]["reviews"][0]["rationale"])
        self.assertEqual(pending_row["source_urls"], self.sources[0]["url"])
        self.assertEqual(pending_row["evidence_record"], "1")

    def test_cap_applies_to_individual_records_without_pair_closure(self):
        self.prepare_reviews()
        stats = self.export(limit=1)
        self.assertEqual(stats["records"], 1)
        pending = validator.load_jsonl(self.root / "bundle/data/review/pending.jsonl")
        self.assertEqual(len(pending), 1)
        self.assertTrue(all("cap" in item["issue"] for item in pending))

    def test_wrong_utility_withholds_only_the_affected_record(self):
        path = self.response_dir / "label-00002.json"
        envelope = contract.read_json(path)
        envelope["result"]["labels"][0]["util"] = "tofu"
        self.write_json(f"responses/{path.name}", envelope)
        self.prepare_reviews()
        self.assertEqual(self.export()["records"], 1)

    def test_subcommand_qualified_utility_keeps_reviewed_records(self):
        path = self.response_dir / "label-00001.json"
        envelope = contract.read_json(path)
        envelope["result"]["labels"][0]["util"] = "terraform destroy"
        self.write_json(f"responses/{path.name}", envelope)
        self.prepare_reviews()
        self.assertEqual(self.export()["records"], 2)
        rows = validator.read_csv(self.root / "bundle/commands.csv")
        self.assertEqual([row["util"] for row in rows], ["terraform", "terraform"])

    def test_unpaired_source_candidate_is_accepted_after_independent_approval(self):
        self.candidates[0]["role"] = "unpaired"
        self.candidates[0]["paired_candidate_ids"] = []
        self.write_jsonl("candidates.jsonl", self.candidates)
        pipeline.prepare(
            self.root / "candidates.jsonl",
            self.root / "sources.jsonl",
            self.root / "semantics.jsonl",
            self.root / "unpaired",
            batch_size=1,
            policy=self.policy,
        )
        self.packet_dir = self.root / "unpaired/packets"
        for path in self.packet_dir.glob("*.json"):
            envelope = contract.read_json(self.response_dir / path.name)
            envelope["packet_sha256"] = contract.read_json(path)["packet_sha256"]
            self.write_json(f"responses/{path.name}", envelope)
        self.prepare_reviews()
        self.assertEqual(self.export()["records"], 2)

    def test_optional_invocation_receipts_are_preserved_in_review_exchange(self):
        path = self.response_dir / "label-00001.json"
        envelope = contract.read_json(path)
        envelope["invocation_id"] = "native-label-invocation-1"
        self.write_json(f"responses/{path.name}", envelope)
        self.prepare_reviews()
        packet = contract.read_json(self.root / "independent/packets" / path.name)
        self.assertEqual(packet["input"]["label_response"]["invocation_id"], "native-label-invocation-1")
        review = contract.read_json(self.root / "reviews" / path.name)
        review["invocation_id"] = "native-review-invocation-2"
        self.write_json(f"reviews/{path.name}", review)
        self.assertEqual(self.export()["records"], 2)

    def test_review_uses_identical_hashed_system_instructions(self):
        self.prepare_reviews()
        original = contract.read_json(self.packet_dir / "label-00001.json")
        review = contract.read_json(self.root / "independent/packets/label-00001.json")
        self.assertEqual(review["system_prompt"], original["system_prompt"])
        review["system_prompt"] += "\nAltered policy"
        with self.assertRaisesRegex(validator.DatasetError, "hash"):
            contract.validate_packet(review, "review", self.taxonomy)

    def test_export_rejects_metadata_drift_and_review_forgery_before_writing(self):
        self.prepare_reviews()
        for field, value in (("source_locator", "changed locator"), ("evidence_context", "changed context")):
            with self.subTest(field=field):
                candidates = copy.deepcopy(self.candidates)
                candidates[0][field] = value
                self.write_jsonl("candidates.jsonl", candidates)
                with self.assertRaisesRegex(validator.DatasetError, "drift"):
                    self.export()
                self.assertFalse((self.root / "bundle").exists())
        self.write_jsonl("candidates.jsonl", self.candidates)
        path = self.root / "reviews/label-00001.json"
        envelope = contract.read_json(path)
        envelope["packet_sha256"] = "0" * 64
        self.write_json("reviews/label-00001.json", envelope)
        with self.assertRaisesRegex(validator.DatasetError, "hash"):
            self.export()
        self.assertFalse((self.root / "bundle").exists())

    def test_risk_reassignment_stays_pending_for_source_metadata_repair(self):
        for path in self.response_dir.glob("*.json"):
            envelope = contract.read_json(path)
            envelope["result"]["labels"][0]["risk"] = self.taxonomy.risk_names[1]
            self.write_json(f"responses/{path.name}", envelope)
        self.prepare_reviews()
        stats = self.export()
        self.assertEqual(stats["records"], 0)
        pending = validator.load_jsonl(self.root / "bundle/data/review/pending.jsonl")
        self.assertTrue(all("risk reassignment" in item["issue"] for item in pending))


if __name__ == "__main__":
    unittest.main()
