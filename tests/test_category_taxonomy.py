from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from configuration_fixture import example_config
from pandera.errors import SchemaError
from pydantic import JsonValue, ValidationError

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.schemas import FIELDS, BridgeError, validate_commands, validate_export
from scripts.category_config import CategoryConfigError, load_taxonomy
from scripts.command_filter import assess_command, extract_commands
from scripts.validate_dataset import validate_rows

ROOT = Path(__file__).resolve().parents[1]


class CategoryTaxonomyTests(unittest.TestCase):
    def test_taxonomy_consumers_import_in_clean_package_and_standalone_modes(self) -> None:
        environment = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
        modules = (
            "dataset_schema",
            "command_filter",
            "validate_dataset",
            "prefilter",
            "postfilter",
            "prepare_review_candidates",
        )
        for directory, prefix in ((ROOT, "scripts."), (ROOT / "scripts", "")):
            with self.subTest(mode=prefix or "standalone"):
                source = "; ".join(f"import {prefix}{module}" for module in modules)
                result = subprocess.run(
                    [sys.executable, "-B", "-c", source],
                    cwd=directory,
                    env=environment,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="codebench-taxonomy-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.configuration = example_config().model_dump(mode="json")
        original = self.configuration["rules"]
        self.configuration["rules"] = {
            **original,
            "category_id": "CAT-91",
            "category_name": "Archive operations",
            "description_language": "English",
            "risks": [
                {
                    "id": "CAT-91.7",
                    "name": "Archive removal",
                    "definition": "Removal of archive entries.",
                    "command_patterns": [r"^archive-tool\s+remove\b"],
                    "shell_routes": {"archive-tool": r"^remove(?: |$)"},
                    "guidance": "Assess the supplied context.",
                },
                {
                    "id": "CAT-91.19",
                    "name": "Archive inspection",
                    "definition": "Archive inspection commands.",
                    "command_patterns": [r"^archive-tool\s+inspect\b"],
                    "guidance": "Assess the supplied context.",
                },
            ],
        }
        self.path = self.root / "category.json"
        self.path.write_text(json.dumps(self.configuration), encoding="utf-8")
        self.rules = CategoryRules.model_validate(self.configuration["rules"])
        self.taxonomy = load_taxonomy(self.path)

    def eligible(self, risk: str) -> dict[str, JsonValue]:
        return {
            "raw_id": "a" * 64,
            "source_id": "archive-docs",
            "doc_full_url": "https://example.org/archive",
            "doc_pos": "characters 0:24",
            "local_path": "archive.md",
            "format": "markdown",
            "raw_command": "archive-tool remove old",
            "command": "archive-tool remove old",
            "context": "Remove an old archive entry.",
            "extraction_transformations": [],
            "transformations": [],
            "risk_candidates": [risk],
            "filter_status": "eligible",
            "filter_reason": "recognized_family",
            "duplicate_of": "",
        }

    def row(self, risk: str) -> dict[str, str]:
        return dict(
            zip(
                FIELDS,
                (
                    "archive-tool remove old",
                    "Removes an archive entry.",
                    "archive-tool",
                    "high",
                    "CAT-91 Archive operations",
                    risk,
                    "real",
                    "-",
                    "ask",
                ),
                strict=True,
            )
        )

    def test_configured_shell_routes_and_patterns_replace_default_taxonomy(self) -> None:
        for command, risk in (
            ("archive-tool remove old", "CAT-91.7"),
            ("archive-tool inspect old", "CAT-91.19"),
        ):
            with self.subTest(command=command):
                (record,) = extract_commands(command, taxonomy=self.taxonomy)
                self.assertEqual((record.status, record.risk_candidates), ("eligible", (risk,)))
                self.assertEqual(self.rules.matching_risks(command), [risk])
        self.assertEqual(assess_command("terraform destroy", self.taxonomy).risk_candidates, ())

    def test_eligible_boundary_accepts_each_configured_id_without_fixed_enum(self) -> None:
        path = self.root / "eligible.jsonl"
        for risk in ("CAT-91.7", "CAT-91.19"):
            with self.subTest(risk=risk):
                path.write_text(json.dumps(self.eligible(risk)) + "\n", encoding="utf-8")
                records = validate_commands(path, eligible=True, rules=self.rules)
                self.assertEqual(records[0].model_dump()["risk_candidates"], [risk])

    def test_eligible_boundary_rejects_foreign_unknown_and_missing_ids(self) -> None:
        path = self.root / "eligible.jsonl"
        for risks, error in (
            (["CAT-3.1"], BridgeError),
            (["CAT-91.1"], BridgeError),
            ([], ValidationError),
            ([""], ValidationError),
        ):
            with self.subTest(risks=risks):
                record = {**self.eligible("CAT-91.7"), "risk_candidates": risks}
                path.write_text(json.dumps(record) + "\n", encoding="utf-8")
                with self.assertRaises(error):
                    validate_commands(path, eligible=True, rules=self.rules)

    def test_csv_boundaries_accept_only_supplied_canonical_taxonomy(self) -> None:
        path = self.root / "commands.csv"
        for risk, valid in (
            ("CAT-91.7 Archive removal", True),
            ("CAT-91.19 Archive inspection", True),
            ("CAT-3.1 Infrastructure as Code Destruction", False),
            ("CAT-91.1 Unconfigured", False),
            ("CAT-91.7", False),
            ("", False),
        ):
            with self.subTest(risk=risk):
                row = self.row(risk)
                with path.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter[str](stream, fieldnames=FIELDS)
                    writer.writeheader()
                    writer.writerow(row)
                if valid:
                    self.assertEqual(validate_export(path, rules=self.rules).height, 1)
                    self.assertEqual(validate_rows([row], self.taxonomy), [])
                else:
                    with self.assertRaises(SchemaError):
                        validate_export(path, rules=self.rules)
                    self.assertTrue(validate_rows([row], self.taxonomy))

    def test_initial_rules_reject_foreign_and_missing_taxonomy_ids(self) -> None:
        for identifier in ("CAT-3.1", ""):
            with self.subTest(identifier=identifier):
                configuration = json.loads(json.dumps(self.configuration))
                configuration["rules"]["risks"][0]["id"] = identifier
                self.path.write_text(json.dumps(configuration), encoding="utf-8")
                with self.assertRaises(ValidationError):
                    CategoryRules.model_validate(configuration["rules"])
                with self.assertRaises(CategoryConfigError):
                    load_taxonomy(self.path)


if __name__ == "__main__":
    unittest.main()
