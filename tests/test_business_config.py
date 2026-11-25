from __future__ import annotations

import copy
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from business_fixture import write_business
from pydantic import ValidationError
from test_dataset_annotation import config, documentation
from test_dataset_bundle import write_declared_bundle
from test_dataset_rules import make_candidate, make_label

from codebench_pipeline.business_config import DEFAULT_BUSINESS_CONFIG, load_business_rules
from codebench_pipeline.category_rules import CategoryRules, CategoryRulesConfig
from codebench_pipeline.dataset_annotation import annotate, request
from codebench_pipeline.dataset_cli import main
from codebench_pipeline.dataset_config import DatasetBuildSpec, load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.labeling_stage_settings import LabelingRules
from codebench_pipeline.storage import fingerprint
from scripts.category_config import load_script_policy, load_taxonomy


class BusinessConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = write_business(self.root / "business")
        self.shared = self.root / "business" / "labeling"

    def test_compilation_needs_no_collection_manifest_and_roundtrips_complete_snapshot(self) -> None:
        rules = load_business_rules(self.path)
        config = CategoryRulesConfig.model_validate_json(rules.model_dump_json())
        self.path.unlink()
        (self.shared / "policy.json").unlink()
        (self.shared / "instructions.md").unlink()
        self.assertEqual(CategoryRules.model_validate_json(config.model_dump_json()), rules)
        self.assertEqual(len(rules.examples), 2)
        self.assertEqual(
            json.loads(rules.examples[0].examples_json)[0]["candidate"]["candidate_id"], "example-0"
        )

    def test_hints_select_examples_without_narrowing_risks_or_changing_documentation(self) -> None:
        file = self.path.parent / "examples.json"
        families = json.loads(file.read_text())
        additional = copy.deepcopy(families[0])
        additional["operation_family"] = "another-operation-family"
        additional["examples"][0]["candidate"]["candidate_id"] = "another-example"
        additional["examples"][0]["expected"]["candidate_id"] = "another-example"
        file.write_text(json.dumps([*families, additional]))
        rules = load_business_rules(self.path)
        _, payload = request(make_candidate(), documentation(), rules)
        self.assertEqual([family["risk_id"] for family in payload["examples"]], ["CAT-8.1", "CAT-8.1"])
        self.assertEqual(
            [family["operation_family"] for family in payload["examples"]],
            ["family-0", "another-operation-family"],
        )
        self.assertEqual(payload["documentation"], [doc.model_dump(mode="json") for doc in documentation()])
        self.assertEqual(payload["allowed_risk_ids"], ["CAT-8.1", "CAT-8.2", "other"])

    def test_shipped_examples_preserve_cross_hint_and_rejection_contrasts(self) -> None:
        rules = LabelingRules.from_rules(load_business_rules(DEFAULT_BUSINESS_CONFIG))
        examples = [example for family in rules.examples for example in family.examples]
        self.assertEqual(
            {example.expected.risk_id for example in examples if example.expected.decision == "reject"},
            {"other"},
        )
        self.assertTrue(
            any(
                example.expected.decision == "accept"
                and example.expected.risk_id not in example.candidate.risk_candidates
                for example in examples
            )
        )

    def test_example_documentation_never_substitutes_for_candidate_docs_or_authorizes_citations(self) -> None:
        rules, candidate = load_business_rules(self.path), make_candidate()
        unrelated = documentation()[0].model_copy(update={"utility": "other"})
        for docs in ([], [unrelated]):
            with self.subTest(docs=docs), patch("codebench_pipeline.dataset_annotation.infer_typed") as infer:
                with self.assertRaises(ValueError):
                    annotate([candidate], docs, rules, config(), self.root / "missing")
                infer.assert_not_called()
        actual = documentation()[0].model_copy(update={"url": "https://example.org/actual"})
        with patch(
            "codebench_pipeline.dataset_annotation.infer_typed",
            return_value=make_label(candidate, rules),
        ):
            with self.assertRaises(ValueError):
                annotate([candidate], [actual], rules, config(), self.root / "foreign-citation")

    def test_invalid_example_contracts_and_incomplete_coverage_fail_at_loading(self) -> None:
        original = json.loads((self.path.parent / "examples.json").read_text())
        variants = []
        empty = copy.deepcopy(original)
        empty[0]["examples"] = []
        variants.append(empty)
        variants.append(original[:1])
        variants.append([*original, original[0]])
        for change in (
            {"risk_id": "CAT-8.2"},
            {"risk_id": "other"},
            {"risk_id": "other", "decision": "needs_review"},
            {"risk_id": "other", "decision": "reject", "documentation_urls": []},
            {"risk_id": "other", "decision": "reject"},
        ):
            changed = copy.deepcopy(original)
            changed[0]["examples"][0]["expected"].update(change)
            variants.append(changed)
        for hints in (["CAT-8.99"], ["other"]):
            changed = copy.deepcopy(original)
            changed[0]["examples"][0]["candidate"]["risk_candidates"] = hints
            variants.append(changed)
        for field, value in (
            ("risk_id", "CAT-9.1"),
            ("candidate_id", "example-1"),
            ("documentation", []),
            ("documentation_urls", ["https://example.org/foreign"]),
            ("class", "modify"),
            ("category_id", "CAT-9"),
            ("utility", "other"),
        ):
            changed = copy.deepcopy(original)
            example = changed[0]["examples"][0]
            if field == "risk_id":
                changed[0][field] = value
            elif field == "candidate_id":
                example["candidate"][field] = value
                example["expected"][field] = value
            elif field == "documentation":
                example[field] = value
            elif field == "utility":
                example["documentation"][0][field] = value
            else:
                example["expected"][field] = value
            variants.append(changed)
        for variant in variants:
            with self.subTest(variant=variant):
                (self.path.parent / "examples.json").write_text(json.dumps(variant))
                with self.assertRaises((ValueError, ValidationError)):
                    load_business_rules(self.path)

    def test_category_identity_and_missing_or_empty_business_files_fail_closed(self) -> None:
        with self.assertRaises(ValueError):
            load_business_rules(self.path, "CAT-99")
        for file in (
            self.shared / "policy.json",
            self.shared / "instructions.md",
            self.path.parent / "instructions.md",
            self.path.parent / "examples.json",
        ):
            with self.subTest(file=file):
                original = file.read_bytes()
                file.unlink()
                with self.assertRaises(FileNotFoundError):
                    load_business_rules(self.path)
                file.write_bytes(original)
        (self.path.parent / "instructions.md").write_text(" \n")
        with self.assertRaises(ValueError):
            load_business_rules(self.path)

    def test_dataset_resolves_only_the_category_owned_rules(self) -> None:
        file = write_declared_bundle(self.root / "declared")
        original = json.loads(file.read_bytes())
        for field, value in (("rules", {}), ("business_config", str(self.path)), ("rules", None)):
            with self.subTest(field=field, value=value), self.assertRaises(ValidationError):
                file.write_text(json.dumps({**original, field: value}), encoding="utf-8")
                load_build_config(file, self.root)
        file.write_text(json.dumps(original), encoding="utf-8")
        category = file.parent.parent / "categories" / "cat3" / "category.json"
        expected = load_business_rules(category)
        spec = load_build_config(file, self.root)
        registry = DatasetRegistry(self.root)
        registry.register(spec)
        category.unlink()
        self.assertEqual(registry.get(spec.dataset_id).rules, expected)

    def test_cli_reregistration_refreshes_rules_and_preserves_runtime_settings_and_history(self) -> None:
        file = write_declared_bundle(self.root / "declared")
        original = load_build_config(file, self.root)
        registry = DatasetRegistry(self.root)
        registry.register(original)
        revision = fingerprint(original.model_dump_json())
        raw = json.loads(file.read_bytes())
        for field in ("labeling", "storage"):
            raw[field] = {"from_registration": True}
        file.write_text(json.dumps(raw), encoding="utf-8")
        business = file.parent.parent
        (business / "labeling/instructions.md").write_text("Edited base instructions.")
        category = business / "categories/cat3"
        (category / "instructions.md").write_text("Edited category instructions.")
        self.assertEqual(registry.get(original.dataset_id), original)
        with (
            patch(
                "sys.argv",
                [
                    "dataset_cli",
                    "register",
                    "--config",
                    str(file),
                    "--home",
                    str(self.root),
                ],
            ),
            redirect_stdout(io.StringIO()),
        ):
            main()
        refreshed = registry.get(original.dataset_id)
        self.assertNotEqual(refreshed.rules, original.rules)
        self.assertEqual(
            refreshed.model_dump(exclude={"rules", "config_directory"}),
            original.model_dump(exclude={"rules", "config_directory"}),
        )
        assert refreshed.config_directory is not None and original.config_directory is not None
        self.assertNotEqual(refreshed.config_directory.digest, original.config_directory.digest)
        self.assertEqual(
            refreshed.config_directory.read(Path("categories/cat3/instructions.md")),
            b"Edited category instructions.",
        )
        (category / "category.json").unlink()
        historical = registry.get_snapshot(original.dataset_id, revision=revision)
        self.assertEqual(historical.model_dump_json(), original.model_dump_json())
        self.assertEqual(DatasetBuildSpec.model_validate_json(refreshed.model_dump_json()), refreshed)
        with self.assertRaises(FileNotFoundError):
            load_build_config(file, self.root)

    def test_failed_reregistration_keeps_previous_registered_configuration(self) -> None:
        file = write_declared_bundle(self.root / "declared")
        original = load_build_config(file, self.root)
        registry = DatasetRegistry(self.root)
        registry.register(original)
        (file.parent.parent / "labeling/instructions.md").write_text(" ")
        with self.assertRaises(ValueError):
            registry.register(load_build_config(file, self.root))
        self.assertEqual(registry.get_snapshot(original.dataset_id), original)

    def test_registry_preserves_source_configuration_without_opening_collection_declarations(self) -> None:
        file = write_declared_bundle(self.root / "declared")
        original = load_build_config(file, self.root)
        registry = DatasetRegistry(self.root)
        registry.register(original)
        category = file.parent.parent / "categories/cat3"
        (category / "collection/existing-datasets.json").unlink()
        (category / "collection/documentation-crawl.json").unlink()
        (category / "keenable/declaration.json").unlink()
        self.assertEqual(registry.get(original.dataset_id).declared_collection, original.declared_collection)

    def test_standalone_taxonomy_follows_dataset_reference_without_collection_inputs(self) -> None:
        dataset = self.root / "dataset.json"
        dataset.write_text(
            json.dumps(
                {
                    "business_config": self.path.relative_to(self.root).as_posix(),
                    "seeds": {"manifest": "does-not-exist.jsonl"},
                }
            )
        )
        self.assertEqual(load_taxonomy(dataset).risk_ids, ("CAT-8.1", "CAT-8.2"))
        prompt = load_script_policy(dataset).render_prompt(["probe inspect example"])
        examples = json.loads(prompt.rsplit("\n\n", 1)[1])["examples"]
        self.assertEqual([family["risk_id"] for family in examples], ["CAT-8.1"])
