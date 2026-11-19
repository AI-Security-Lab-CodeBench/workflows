from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from business_fixture import write_business
from pydantic import ValidationError
from test_dataset_annotation import documentation
from test_dataset_bundle import write_bundle
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.business_config import load_business_rules
from codebench_pipeline.dataset_annotation import request
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_records import validate_label
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.storage import fingerprint
from scripts.category_config import load_labeling_prompt, load_taxonomy


class SharedLabelingPolicyTests(unittest.TestCase):
    def test_two_categories_share_policy_but_keep_local_routing_and_severity_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = write_business(root)
            other = make_rules("CAT-9")
            inspection = other.risks[0].model_copy(update={"command_patterns": [r"^probe\s+inspect\b"]})
            other = other.model_copy(
                update={"risks": [inspection, other.risks[1]], "policy": "Check archive context."}
            )
            second = write_business(root, other, allowed_severities=["low", "medium"])
            policy_file = root / "labeling" / "policy.json"
            policy = json.loads(policy_file.read_bytes())
            policy["description_language"] = "French"
            policy["classes"][0]["definition"] = "The supplied evidence supports allowing the command."
            policy_file.write_text(json.dumps(policy), encoding="utf-8")

            for path, expected_risks, expected_severities in (
                (first, ("CAT-8.1", "CAT-8.2"), policy["severities"]),
                (second, ("CAT-9.1", "CAT-9.2"), policy["severities"][:2]),
            ):
                with self.subTest(category=path.parent.name):
                    rules = load_business_rules(path)
                    candidate = make_candidate(risk=expected_risks[0])
                    system, payload = request(candidate, documentation(), rules)
                    pipeline_policy = json.loads(system.rsplit("\n\n", 1)[1])
                    _, _, standalone_policy, standalone_examples = load_labeling_prompt(
                        [candidate.command], path
                    ).split("\n\n")
                    for compiled in (pipeline_policy, json.loads(standalone_policy)):
                        self.assertEqual(compiled["classes"], policy["classes"])
                        self.assertEqual(compiled["severities"], expected_severities)
                        self.assertEqual(compiled["description_language"], "French")
                    self.assertEqual(load_taxonomy(path).risk_ids, expected_risks)
                    self.assertEqual(load_taxonomy(path).description_language, "French")
                    self.assertEqual(payload["allowed_risk_ids"], [expected_risks[0]])
                    self.assertEqual(
                        [example["risk_id"] for example in json.loads(standalone_examples)["examples"]],
                        [expected_risks[0]],
                    )

            first_rules, second_rules = load_business_rules(first), load_business_rules(second)
            self.assertEqual(first_rules.matching_risks("probe erase example"), ["CAT-8.1"])
            self.assertEqual(second_rules.matching_risks("probe erase example"), [])
            self.assertEqual(second_rules.policy, "Check archive context.")
            self.assertIsNotNone(first_rules.shared_policy_digest)
            self.assertEqual(first_rules.shared_policy_digest, second_rules.shared_policy_digest)
            self.assertNotEqual(first_rules.category_policy_digest, second_rules.category_policy_digest)
            urls = {doc.url for doc in documentation()}
            candidate = make_candidate(risk="CAT-8.1")
            label = make_label(candidate, first_rules).model_copy(update={"severity": "high"})
            validate_label(candidate, label, first_rules, urls)
            candidate = make_candidate(risk="CAT-9.1")
            label = make_label(candidate, second_rules).model_copy(update={"severity": "high"})
            with self.assertRaisesRegex(ValueError, "Severity"):
                validate_label(candidate, label, second_rules, urls)

    def test_split_category_rejects_global_overrides_and_unknown_severity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            category = write_business(Path(temporary))
            authored = json.loads(category.read_bytes())
            rules = make_rules().model_dump(mode="json")
            for field, value in (
                ("classes", rules["classes"]),
                ("severities", rules["severities"]),
                ("description_language", "French"),
                ("allowed_severities", ["low", "extreme"]),
            ):
                with self.subTest(field=field):
                    overridden = {"rules": {**authored["rules"], field: value}}
                    category.write_text(json.dumps(overridden), encoding="utf-8")
                    with self.assertRaisesRegex(ValidationError, field):
                        load_business_rules(category)

    def test_re_registration_compiles_current_policy_and_replays_old_snapshot_offline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            business = home / "business"
            category = write_business(business)
            bundle = write_bundle(home)
            registry = DatasetRegistry(home)
            document = json.loads(bundle.read_bytes())
            finalization = document["finalization"]
            historical_rules = make_rules().model_copy(update={"base_prompt": "Historical instructions."})
            finalization["rules"] = historical_rules.model_dump(mode="json")
            bundle.write_text(json.dumps(document), encoding="utf-8")
            historical = load_build_config(bundle, home)
            registry.register(historical)
            directory = registry.root / historical.dataset_id
            old_revision = fingerprint(
                historical.model_dump_json(
                    exclude={"rules": {"shared_policy_digest", "category_policy_digest"}}
                )
            )
            self.assertEqual((directory / "current").read_text(encoding="ascii"), old_revision)
            old_file = directory / f"{old_revision}.json"
            old_bytes = old_file.read_bytes()

            finalization.pop("rules")
            finalization["business_config"] = category.relative_to(home).as_posix()
            bundle.write_text(json.dumps(document), encoding="utf-8")
            registry.register(load_build_config(bundle, home))
            initial = registry.get_snapshot(historical.dataset_id)
            initial_revision = (directory / "current").read_text(encoding="ascii")
            initial_file = directory / f"{initial_revision}.json"
            initial_bytes = initial_file.read_bytes()

            policy_path = business / "labeling" / "policy.json"
            policy = json.loads(policy_path.read_bytes())
            policy["description_language"] = "French"
            policy_path.write_text(json.dumps(policy), encoding="utf-8")
            revised_instructions = "Revised shared instructions."
            (policy_path.parent / "instructions.md").write_text(revised_instructions, encoding="utf-8")
            self.assertEqual(registry.get(historical.dataset_id), initial)
            registry.register(load_build_config(bundle, home))
            self.assertNotEqual((directory / "current").read_text(encoding="ascii"), initial_revision)

            shutil.rmtree(business)
            replayed = registry.get_snapshot(historical.dataset_id, revision=old_revision)
            prior = registry.get_snapshot(historical.dataset_id, revision=initial_revision)
            current = registry.get(historical.dataset_id)
            self.assertEqual(replayed, historical)
            self.assertEqual(prior, initial)
            self.assertEqual(old_file.read_bytes(), old_bytes)
            self.assertEqual(initial_file.read_bytes(), initial_bytes)
            self.assertEqual(current.model_dump(exclude={"rules"}), initial.model_dump(exclude={"rules"}))
            self.assertEqual(current.rules.description_language, "French")
            self.assertEqual(current.rules.base_prompt, revised_instructions)
            self.assertEqual(current.rules.examples, initial.rules.examples)
            self.assertNotEqual(current.rules.shared_policy_digest, initial.rules.shared_policy_digest)
            self.assertEqual(current.rules.category_policy_digest, initial.rules.category_policy_digest)
