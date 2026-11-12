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
from configuration_fixture import write_fixture
from pydantic import ValidationError
from test_dataset_annotation import config, documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.business_config import load_business_rules
from codebench_pipeline.category_rules import CategoryRules, CategoryRulesConfig
from codebench_pipeline.dataset_annotation import annotate, request
from codebench_pipeline.dataset_cli import main
from codebench_pipeline.dataset_config import DatasetBuildSpec, DatasetFileConfig, load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from scripts.category_config import load_labeling_prompt, load_taxonomy


class BusinessConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = write_business(self.root / "business")

    def test_compilation_needs_no_collection_manifest_and_roundtrips_complete_snapshot(self) -> None:
        rules = load_business_rules(self.path)
        config = CategoryRulesConfig.model_validate_json(rules.model_dump_json())
        self.path.unlink()
        (self.path.parent / "base.md").unlink()
        self.assertEqual(CategoryRules.model_validate_json(config.model_dump_json()), rules)
        self.assertEqual(len(rules.examples), 2)
        self.assertEqual(json.loads(rules.examples[0].examples_json)[0]["candidate"]["candidate_id"], "example-0")

    def test_only_allowed_risk_examples_are_selected_without_changing_actual_documentation(self) -> None:
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
        self.assertEqual(payload["allowed_risk_ids"], ["CAT-8.1"])

    def test_example_documentation_never_substitutes_for_candidate_docs_or_authorizes_citations(self) -> None:
        rules, candidate = load_business_rules(self.path), make_candidate()
        unrelated = documentation()[0].model_copy(update={"utility": "other"})
        for docs in ([], [unrelated]):
            with self.subTest(docs=docs), patch(
                "codebench_pipeline.dataset_annotation.infer_typed"
            ) as infer:
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
        for name in ("base.md", "instructions.md", "examples.json"):
            with self.subTest(file=name):
                file = self.path.parent / name
                original = file.read_bytes()
                file.unlink()
                with self.assertRaises(FileNotFoundError):
                    load_business_rules(self.path)
                file.write_bytes(original)
        (self.path.parent / "instructions.md").write_text(" \n")
        with self.assertRaises(ValueError):
            load_business_rules(self.path)

    def test_dataset_requires_exactly_one_inline_or_business_rules_source(self) -> None:
        original = json.loads(write_fixture(self.root).read_text())
        for changed in (
            {**original, "business_config": str(self.path)},
            {**original, "rules": None},
        ):
            with self.subTest(changed=changed), self.assertRaises(ValidationError):
                DatasetFileConfig.model_validate(changed)
        original.pop("rules")
        original["business_config"] = "business/category.json"
        original["documentation_sources"] = None
        file = self.root / "dataset.json"
        file.write_text(json.dumps(original))
        spec = load_build_config(file, self.root)
        self.assertEqual(spec.business_config, str(self.path.resolve()))
        self.assertEqual(spec.rules.category_id, "CAT-8")

    def test_cli_binding_and_live_refresh_preserve_runtime_settings_and_historical_snapshot(self) -> None:
        original = load_build_config(write_fixture(self.root), self.root)
        original = original.model_copy(update={"rules": make_rules()})
        registry = DatasetRegistry(self.root)
        registry.register(original)
        with patch("sys.argv", [
            "dataset_cli", "bind-business", "--dataset", original.dataset_id,
            "--business-config", str(self.path), "--home", str(self.root),
        ]), redirect_stdout(io.StringIO()):
            main()
        bound = registry.get_snapshot(original.dataset_id)
        self.assertEqual(
            bound.model_dump(exclude={"rules", "business_config"}),
            original.model_dump(exclude={"rules", "business_config"}),
        )
        persisted = bound.model_dump_json()
        (self.path.parent / "base.md").write_text("Edited base instructions.")
        (self.path.parent / "instructions.md").write_text("Edited category instructions.")
        refreshed = registry.get(original.dataset_id)
        self.assertNotEqual(refreshed.rules, bound.rules)
        self.assertEqual(
            refreshed.model_dump(exclude={"rules"}), bound.model_dump(exclude={"rules"})
        )
        self.assertEqual(registry.get_snapshot(original.dataset_id).model_dump_json(), persisted)
        self.path.unlink()
        self.assertEqual(DatasetBuildSpec.model_validate_json(persisted), bound)
        with self.assertRaises(FileNotFoundError):
            registry.get(original.dataset_id)

    def test_failed_binding_keeps_previous_registered_configuration(self) -> None:
        original = load_build_config(write_fixture(self.root), self.root)
        registry = DatasetRegistry(self.root)
        registry.register(original)
        with self.assertRaises(ValueError):
            registry.bind_business(original.dataset_id, self.path)
        self.assertEqual(registry.get_snapshot(original.dataset_id), original)

    def test_binding_preserves_source_configuration_without_opening_collection_manifests(self) -> None:
        original = load_build_config(write_fixture(self.root), self.root)
        original = original.model_copy(update={
            "rules": make_rules(),
            "catalog": original.catalog.model_copy(update={"manifest": str(self.root / "missing.jsonl")}),
        })
        registry = DatasetRegistry(self.root)
        registry.register(original)
        bound = registry.bind_business(original.dataset_id, self.path)
        self.assertEqual(bound.catalog, original.catalog)
        self.assertEqual(bound.snapshots, original.snapshots)

    def test_standalone_taxonomy_follows_dataset_reference_without_collection_inputs(self) -> None:
        dataset = self.root / "dataset.json"
        dataset.write_text(json.dumps({
            "business_config": "business/category.json",
            "seeds": {"manifest": "does-not-exist.jsonl"},
        }))
        self.assertEqual(load_taxonomy(dataset).risk_ids, ("CAT-8.1", "CAT-8.2"))
        prompt = load_labeling_prompt(["probe inspect example"], dataset)
        examples = json.loads(prompt.rsplit("\n\n", 1)[1])["examples"]
        self.assertEqual([family["risk_id"] for family in examples], ["CAT-8.1"])
