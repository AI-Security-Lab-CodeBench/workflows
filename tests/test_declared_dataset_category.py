import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.storage import IntegrityError


class DeclaredDatasetCategoryTests(TestCase):
    def test_loads_synthetic_category_settings_and_checks_seed_taxonomy(self) -> None:
        root = Path("business-configs")
        published = root / "categories/cat3"
        with TemporaryDirectory() as temporary:
            home = Path(temporary)
            category = home / "categories/cat91"
            shutil.copytree(root / "labeling", home / "labeling")
            shutil.copytree(published / "collection", category / "collection")
            (category / "instructions.md").write_text("Use the synthetic validation taxonomy.")
            (category / "category.json").write_text(
                json.dumps(
                    {
                        "rules": {
                            "category_id": "CAT-91",
                            "category_name": "Synthetic Configuration Validation",
                            "version": "fixture",
                            "allowed_severities": ["low"],
                            "risks": [
                                {
                                    "id": "CAT-91.1",
                                    "name": "Local validation",
                                    "definition": "Checks local configuration without applying changes.",
                                    "command_patterns": [r"^terraform validate$"],
                                    "guidance": "Use only the local validation command.",
                                }
                            ],
                        },
                    }
                )
            )
            example = next(
                example
                for family in json.loads((published / "examples.json").read_bytes())
                for example in family["examples"]
                if example["candidate"]["command"] == "terraform validate"
            )
            example["candidate"]["risk_candidates"] = ["CAT-91.1"]
            example["expected"].update(category_id="CAT-91", risk_id="CAT-91.1")
            (category / "examples.json").write_text(
                json.dumps(
                    [
                        {
                            "risk_id": "CAT-91.1",
                            "operation_family": "local validation",
                            "examples": [example],
                        }
                    ]
                )
            )
            declaration = category / "keenable/declaration.json"
            declaration.parent.mkdir()
            seed = {
                "id": "terraform_validation",
                "instruction": "Find examples of local configuration validation.",
                "risk_id": "CAT-91.1",
                "severity": "low",
            }
            source = {
                "schema_version": "4",
                "dataset_id": "synthetic_validation",
                "search": {"seeds": [seed], "max_results": 2},
            }
            declaration.write_text(json.dumps(source))
            bundle = home / "datasets/cat91.json"
            bundle.parent.mkdir()
            bundle.write_text(
                json.dumps(
                    {
                        "dataset_id": "synthetic_validation",
                        "category_id": "CAT-91",
                        "revision_label": "synthetic-validation",
                        "labeling": {
                            "model": {
                                "provider": "openai_compatible",
                                "model": "fixture/model",
                                "base_url": "http://fixture.invalid/v1",
                            },
                            "batch": {
                                "inference_mode": "plain",
                                "backend_revision": "fixture-backend",
                                "grounding": {"allowed_domains": ["example.org"]},
                            },
                        },
                        "storage": {"backend": "s3", "bucket": "fixture-artifacts"},
                    }
                )
            )

            spec = load_build_config(bundle, home=home)

            self.assertEqual(spec.rules.category_name, "Synthetic Configuration Validation")
            self.assertEqual(spec.rules.matching_risks("terraform validate"), ["CAT-91.1"])
            self.assertEqual([rule.name for rule in spec.rules.severities], ["low"])
            assert spec.declared_collection is not None
            search = spec.declared_collection.keenable_use_cases.keenable_search()
            self.assertEqual(search.max_results, 2)
            self.assertEqual([item.risk_id for item in search.authored_seeds], ["CAT-91.1"])

            seed["risk_id"] = "CAT-3.1"
            declaration.write_text(json.dumps(source))
            with self.assertRaisesRegex(IntegrityError, "outside the active taxonomy"):
                load_build_config(bundle, home=home)
