import copy
import unittest

from dataset_fixtures import DatasetFixture

import integrate_dataset as integrator
import validate_dataset as validator


class DatasetPipelineTests(DatasetFixture):
    def test_summary_uses_one_aggregate_target_at_boundaries(self):
        template = self.candidates[0]["row"]
        for total, expected_deficit in ((0, 250), (249, 1), (250, 0), (251, 0)):
            with self.subTest(total=total):
                rows = [
                    dict(template, risk=validator.RISKS[index % len(validator.RISKS)])
                    for index in range(total)
                ]
                report = validator.summary(rows, [])
                self.assertEqual(report["target"], 250)
                self.assertEqual(report["records"], total)
                self.assertEqual(report["deficit"], expected_deficit)
                self.assertEqual(sum(item["total"] for item in report["by_risk"].values()), total)
                for risk_report in report["by_risk"].values():
                    self.assertEqual(set(risk_report), {"total", "class", "severity", "utilities"})
                    self.assertLess(risk_report["total"], 250)
                if total == 250:
                    self.assertEqual([item["total"] for item in report["by_risk"].values()], [50] * 5)

    def test_summary_validity_is_independent_of_target(self):
        rows, provenance = integrator.prepare(self.candidates)
        errors = validator.validate_bundle(rows, provenance, self.candidates, self.sources, [])
        report = validator.summary(rows, errors)
        self.assertTrue(report["valid"])
        self.assertEqual(report["deficit"], 248)
        self.assertEqual(report["errors"], [])

        errors = ["Fixture schema failure"]
        report = validator.summary([rows[0]] * 250, errors)
        self.assertFalse(report["valid"])
        self.assertEqual(report["deficit"], 0)
        self.assertEqual(report["errors"], errors)

    def test_csv_roundtrip_preserves_embedded_newlines_quotes_and_unicode(self):
        self.candidates[0]["row"]["command"] = 'printf \'первая строка\n"вторая", строка\''
        self.candidates[0]["row"]["description"] = 'Описание, с "кавычками"\r\nи новой строкой'
        rows, provenance = integrator.prepare(self.candidates)
        self.assertEqual(self.errors(), [])
        csv_text, provenance_text = integrator.serialize(rows, provenance)
        integrator.persist(self.root, csv_text, provenance_text)
        self.assertEqual(validator.read_csv(self.root / "commands.csv"), rows)
        self.assertEqual(validator.load_jsonl(self.root / "data/processed/provenance.jsonl"), provenance)

    def test_csv_rejects_malformed_quoting(self):
        header = ",".join(validator.FIELDS) + "\n"
        for malformed in (
            'echo "broken,description,echo,high,category,risk,source,-,deny\n',
            '"echo"unexpected,description,echo,high,category,risk,source,-,deny\n',
            '"echo,description,echo,high,category,risk,source,-,deny\n',
        ):
            with self.subTest(record=malformed):
                path = self.root / "commands.csv"
                path.write_text(header + malformed, encoding="utf-8")
                with self.assertRaises(validator.DatasetError):
                    validator.read_csv(path)

    def test_rejects_unknown_provenance_record_indices(self):
        rows, original = integrator.prepare(self.candidates)
        for index in (0, -1, len(rows) + 1, True, "1"):
            with self.subTest(index=index):
                provenance = copy.deepcopy(original)
                provenance[0]["csv_record"] = index
                self.assertTrue(validator.validate_bundle(rows, provenance, self.candidates, self.sources, []))

    def test_rejects_provenance_for_unknown_candidate(self):
        rows, provenance = integrator.prepare(self.candidates)
        provenance[0]["candidate_id"] = "CAT-3.1-missing"
        self.assertTrue(validator.validate_bundle(rows, provenance, self.candidates, self.sources, []))

    def test_accepts_historical_dangling_links_without_resolving_them(self):
        self.candidates[0]["paired_candidate_ids"] = ["CAT-3.1-missing"]
        rows, provenance = integrator.prepare(self.candidates)
        self.assertEqual([item["paired_csv_records"] for item in provenance], [[], []])
        provenance[0]["paired_csv_records"] = [len(rows) + 1]
        self.assertEqual(validator.validate_bundle(rows, provenance, self.candidates, self.sources, []), [])

    def test_accepts_historical_asymmetric_same_role_cross_scenario_links(self):
        self.candidates.extend(self.make_pair("beta"))
        self.candidates[2]["scenario_id"] = "other-scenario"
        self.candidates[0]["paired_candidate_ids"].append(self.candidates[2]["candidate_id"])
        rows, provenance = integrator.prepare(self.candidates)
        indices = {item["candidate_id"]: item["csv_record"] for item in provenance}
        for item in provenance:
            if item["candidate_id"] == self.candidates[0]["candidate_id"]:
                item["paired_csv_records"] = [indices[self.candidates[2]["candidate_id"]]]
        self.assertEqual(validator.validate_bundle(rows, provenance, self.candidates, self.sources, []), [])

    def test_accepts_standalone_classes_independently_of_historical_role(self):
        self.candidates = [self.candidates[0]]
        self.candidates[0]["paired_candidate_ids"] = []
        for role in ("unpaired", "dangerous", "counterexample"):
            for label in ("allow", "ask", "deny"):
                with self.subTest(role=role, label=label):
                    self.candidates[0]["role"] = role
                    self.candidates[0]["row"]["class"] = label
                    self.assertEqual(self.errors(), [])

    def test_rejects_invalid_historical_metadata_schema(self):
        for field, value in (("role", "unknown"), ("paired_candidate_ids", None),
                             ("paired_candidate_ids", [""]), ("paired_candidate_ids", "CAT-3.1-missing")):
            with self.subTest(field=field, value=value):
                candidates = copy.deepcopy(self.candidates)
                candidates[0][field] = value
                if field == "paired_candidate_ids":
                    with self.assertRaises(validator.DatasetError):
                        integrator.prepare(candidates)
                else:
                    rows, provenance = integrator.prepare(candidates)
                    self.assertTrue(validator.validate_bundle(rows, provenance, candidates, self.sources, []))

    def test_rejects_duplicate_candidate_and_source_identities(self):
        rows, provenance = integrator.prepare(self.candidates)
        candidates = self.candidates + [copy.deepcopy(self.candidates[0])]
        with self.assertRaises(validator.DatasetError):
            integrator.prepare(candidates)
        self.assertTrue(validator.validate_bundle(rows, provenance, candidates, self.sources, []))
        sources = self.sources + [copy.deepcopy(self.sources[0])]
        self.assertTrue(validator.validate_bundle(rows, provenance, self.candidates, sources, []))

    def test_rejects_duplicate_csv_and_provenance_candidate_identities(self):
        rows, original = integrator.prepare(self.candidates)
        for field in ("csv_record", "candidate_id"):
            with self.subTest(field=field):
                provenance = copy.deepcopy(original)
                provenance[1][field] = provenance[0][field]
                self.assertTrue(validator.validate_bundle(rows, provenance, self.candidates, self.sources, []))

    def test_rejects_provenance_tampering(self):
        rows, original = integrator.prepare(self.candidates)
        for field, value in (("source_ids", ["other-source"]), ("source_locator", "other locator"),
                             ("transformations", ["unrecorded change"]),
                             ("documentation_urls", ["https://example.org/other"]),
                             ("label_rationale", "Changed rationale"),
                             ("scenario_id", "other-scenario"), ("role", "unpaired")):
            with self.subTest(field=field):
                provenance = copy.deepcopy(original)
                provenance[0][field] = value
                self.assertTrue(validator.validate_bundle(rows, provenance, self.candidates, self.sources, []))
        rows[0] = dict(rows[0], command="echo tampered")
        self.assertTrue(validator.validate_bundle(rows, original, self.candidates, self.sources, []))

    def test_rejects_unknown_source_and_unpermitted_reuse(self):
        self.candidates[0]["source_ids"] = ["missing-source"]
        self.assertTrue(self.errors())
        self.candidates[0]["source_ids"] = ["fixture-source"]
        for decision in ("pending", "forbidden", ""):
            with self.subTest(reuse_decision=decision):
                self.sources[0]["reuse_decision"] = decision
                self.assertTrue(self.errors())

    def test_synthetic_requires_seed_and_model(self):
        candidate = self.candidates[0]
        candidate["row"]["source"] = "synthetic"
        candidate["row"]["seed"] = "fixture-seed"
        candidate["synthetic_model"] = "fixture-model"
        self.assertEqual(self.errors(), [])
        candidate["row"]["seed"] = "-"
        self.assertTrue(self.errors())
        for seed in ("", "   "):
            with self.subTest(seed=seed):
                candidate["row"]["seed"] = seed
                with self.assertRaises(validator.DatasetError):
                    integrator.prepare(self.candidates)
        candidate["row"]["seed"] = "fixture-seed"
        for model in (None, "", "   "):
            with self.subTest(model=model):
                candidate["synthetic_model"] = model
                self.assertTrue(self.errors())

    def test_nonsynthetic_rejects_seed_or_generation_model(self):
        candidate = self.candidates[0]
        candidate["row"]["seed"] = "fixture-seed"
        self.assertTrue(self.errors())
        candidate["row"]["seed"] = "-"
        candidate["synthetic_model"] = "fixture-model"
        self.assertTrue(self.errors())

    def test_pending_accepted_candidate_blocks_integration(self):
        self.pending = [{"candidate_id": self.candidates[0]["candidate_id"], "status": "pending"}]
        self.assertTrue(self.errors())
        self.write_inputs()
        result = self.run_script("integrate_dataset.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "commands.csv").exists())
        self.assertFalse((self.root / "data/processed/provenance.jsonl").exists())
        self.pending[0]["status"] = "resolved"
        self.assertEqual(self.errors(), [])

    def test_integration_excludes_all_nonaccepted_candidates(self):
        expected_rows, expected_provenance = integrator.prepare(self.candidates)
        for status in ("pending", "rejected", "excluded"):
            pair = self.make_pair(status)
            for candidate in pair:
                candidate["status"] = status
            self.candidates.extend(pair)
        self.write_inputs()
        result = self.run_script("integrate_dataset.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(validator.read_csv(self.root / "commands.csv"), expected_rows)
        self.assertEqual(validator.load_jsonl(self.root / "data/processed/provenance.jsonl"), expected_provenance)
        validation = self.run_script("validate_dataset.py")
        self.assertEqual(validation.returncode, 0, validation.stdout + validation.stderr)

    def test_integration_accepts_standalone_with_link_to_nonaccepted_candidate(self):
        self.candidates[1]["status"] = "pending"
        self.write_inputs()
        result = self.run_script("integrate_dataset.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(validator.read_csv(self.root / "commands.csv"), [self.candidates[0]["row"]])
        provenance = validator.load_jsonl(self.root / "data/processed/provenance.jsonl")
        self.assertEqual([item["candidate_id"] for item in provenance], [self.candidates[0]["candidate_id"]])
        self.assertEqual(provenance[0]["paired_csv_records"], [])
        validation = self.run_script("validate_dataset.py")
        self.assertEqual(validation.returncode, 0, validation.stdout + validation.stderr)

    def test_integration_preserves_duplicate_command_text_and_near_variants(self):
        self.candidates.extend(self.make_pair("beta"))
        commands = ("echo example-resource-1", "echo example-resource-1",
                    "echo example-resource-2", "echo  example-resource-2")
        for candidate, command in zip(self.candidates, commands):
            candidate["row"]["command"] = command
            candidate["paired_candidate_ids"] = []
            candidate["role"] = "unpaired"
        expected = sorted(self.candidates, key=lambda candidate: candidate["candidate_id"])
        self.write_inputs()
        result = self.run_script("integrate_dataset.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(validator.read_csv(self.root / "commands.csv"), [item["row"] for item in expected])
        provenance = validator.load_jsonl(self.root / "data/processed/provenance.jsonl")
        self.assertEqual([item["candidate_id"] for item in provenance], [item["candidate_id"] for item in expected])
        self.assertEqual([item["paired_csv_records"] for item in provenance], [[], [], [], []])
        validation = self.run_script("validate_dataset.py")
        self.assertEqual(validation.returncode, 0, validation.stdout + validation.stderr)


if __name__ == "__main__":
    unittest.main()
