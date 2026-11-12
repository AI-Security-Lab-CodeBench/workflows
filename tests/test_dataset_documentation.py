from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInvalidInvocationError
from dataset_documentation_fixtures import commands, seed
from pydantic import JsonValue, ValidationError
from test_dataset_rules import make_rules

from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_documentation import collect_documentation
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_seed_config import DocumentationSeed
from codebench_pipeline.source_manifests import DocumentationManifest, manifest_documentation
from codebench_pipeline.storage import IntegrityError


class DatasetDocumentationTests(unittest.TestCase):
    def test_configuration_rejects_online_search_and_preserves_strict_bounds(self) -> None:
        supplied = DocumentationConfig(snippets=[seed()]).model_dump(mode="json")
        for changes in (
            {"mode": "keenable"},
            {"search_request": {"searches": [{"risk": "CAT-8.1", "queries": ["probe docs"]}]}},
            {"maximum_bytes": 0}, {"maximum_bytes": 10_485_761},
            {"maximum_bytes": "8000"}, {"unexpected": True},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                DocumentationConfig.model_validate_json(json.dumps({**supplied, **changes}))
        config = DocumentationConfig(snippets=[seed()])
        self.assertEqual(DocumentationConfig.model_validate_json(config.model_dump_json()), config)
        with self.assertRaises((ValidationError, DagsterInvalidInvocationError)):
            config.revision = "changed"

    def test_provided_evidence_retains_rows_and_reports_only_eligible_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch("urllib.request.OpenerDirector.open", side_effect=AssertionError("No source search")),
                patch("codebench_pipeline.inference.subprocess.run", side_effect=AssertionError("No docs model")),
            ):
                table = collect_documentation(
                    commands("probe inspect example && probe erase example"),
                    make_rules(), DocumentationConfig(snippets=[seed()]), root,
                )
            self.assertEqual(frame_records(table), [seed().model_dump(mode="json")])
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            self.assertEqual([row["status"] for row in receipt["coverage"]], ["covered", "gap"])
            self.assertEqual(receipt["coverage"][1]["command_words"], ["erase", "example"])
            self.assertEqual(receipt["use_scope"], "semantic_reference_only")

    def test_empty_or_ineligible_commands_do_not_invent_operation_coverage(self) -> None:
        for frame in (commands(), commands("probe inspect example", decision="needs_review")):
            with self.subTest(rows=len(frame)), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                table = collect_documentation(frame, make_rules(), DocumentationConfig(), root)
                self.assertTrue(table.empty)
                self.assertEqual(json.loads((root / "documentation-receipt.json").read_text())["coverage"], [])

    def test_nested_subcommand_coverage_ignores_global_option_values(self) -> None:
        supplied = seed().model_copy(update={"subcommands": ["system inspect"]})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            collect_documentation(
                commands("probe --context example system inspect target", "probe system erase target"),
                make_rules(), DocumentationConfig(snippets=[supplied]), root,
            )
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            self.assertEqual([row["status"] for row in receipt["coverage"]], ["covered", "gap"])
            self.assertEqual(receipt["coverage"][1]["command_words"], ["system", "erase", "target"])

    def test_empty_semantics_retains_gap_without_fabricating_documentation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            table = collect_documentation(commands("probe inspect example"), make_rules(), DocumentationConfig(), root)
            self.assertTrue(table.empty)
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            self.assertEqual(receipt["coverage"][0]["status"], "gap")
            self.assertEqual(receipt["status"], "partial")

    def test_manifest_retains_full_input_and_category_binding_without_inference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "semantics.json"
            manifest = DocumentationManifest(category_id="CAT-8", revision="provided-v1", snippets=[seed()])
            source.write_text(manifest.model_dump_json(), encoding="utf-8")
            config = DocumentationConfig(manifest=str(source), snippets=manifest_documentation(source, "CAT-8"))
            table = collect_documentation(commands("probe inspect example"), make_rules(), config, root / "evidence")
            self.assertEqual(frame_records(table), [seed().model_dump(mode="json")])
            self.assertEqual((root / "evidence/documentation-manifest.json").read_bytes(), source.read_bytes())
            with self.assertRaises(IntegrityError):
                manifest_documentation(source, "CAT-99")
            with self.assertRaises(FileNotFoundError):
                manifest_documentation(root / "missing.json", "CAT-8")

    def test_malformed_semantic_inputs_are_rejected_at_the_input_boundary(self) -> None:
        changes: list[dict[str, JsonValue]] = [
            {"url": "file:///private/input"}, {"snippet": ""}, {"utility": ""}, {"subcommands": []}
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValidationError):
                DocumentationSeed.model_validate({**seed().model_dump(), **change})

    def test_flag_references_match_equals_options_without_matching_embedded_values(self) -> None:
        supplied = seed().model_copy(update={"subcommands": ["--inspect"]})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            collect_documentation(
                commands("probe --inspect=example", "probe --context=--inspect erase example"),
                make_rules(), DocumentationConfig(snippets=[supplied]), root,
            )
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            self.assertEqual([row["status"] for row in receipt["coverage"]], ["covered", "gap"])

    def test_default_reference_is_accepted_and_covers_only_an_exact_bare_invocation(self) -> None:
        supplied = DocumentationSeed.model_validate({
            **seed().model_dump(),
            "subcommands": ["<default>"],
        })
        manifest = DocumentationManifest(category_id="CAT-8", revision="root-reference", snippets=[supplied])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "documentation.json"
            path.write_text(manifest.model_dump_json())
            snippets = manifest_documentation(path, "CAT-8")
            table = collect_documentation(
                commands(
                    "probe", "probe inspect", "probe -h", "probe --context example",
                    "probe --", 'probe ""', "probe <default>", "other", "probe 'unterminated",
                ),
                make_rules(),
                DocumentationConfig(manifest=str(path), snippets=snippets),
                root / "evidence",
            )
            self.assertEqual(frame_records(table), [supplied.model_dump(mode="json")])
            receipt = json.loads((root / "evidence/documentation-receipt.json").read_text())
            self.assertEqual(
                [row["status"] for row in receipt["coverage"]],
                ["covered", "gap", "gap", "gap", "gap", "gap", "gap", "gap", "gap"],
            )

    def test_byte_limits_count_utf8_and_leave_a_failure_receipt(self) -> None:
        supplied = seed().model_copy(update={"snippet": "\u00e9\u00e9"})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(IntegrityError):
                collect_documentation(commands("probe inspect example"), make_rules(),
                                      DocumentationConfig(snippets=[supplied], maximum_bytes=3), root)
            self.assertEqual(json.loads((root / "documentation-receipt.json").read_text())["status"], "failed")

    def test_manifest_evidence_change_invalidates_the_semantic_stage_signature(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "semantics.json"
            manifest = DocumentationManifest(category_id="CAT-8", revision="first", snippets=[seed()])
            path.write_text(manifest.model_dump_json(), encoding="utf-8")
            config = DocumentationConfig(manifest=str(path), snippets=[seed()])
            before = stage_signature("labeling_documentation", config, {})
            path.write_text(manifest.model_copy(update={"revision": "second"}).model_dump_json(), encoding="utf-8")
            self.assertNotEqual(stage_signature("labeling_documentation", config, {}), before)

    def test_existing_receipt_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frame = commands("probe inspect example")
            collect_documentation(frame, make_rules(), DocumentationConfig(), root)
            previous = (root / "documentation-receipt.json").read_bytes()
            with self.assertRaises(IntegrityError):
                collect_documentation(frame, make_rules(), DocumentationConfig(), root)
            self.assertEqual((root / "documentation-receipt.json").read_bytes(), previous)
