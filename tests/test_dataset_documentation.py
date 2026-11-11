from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInvalidInvocationError
from dataset_documentation_fixtures import commands, discovery, live, proposal, seed
from pydantic import JsonValue, ValidationError
from test_dataset_annotation import native_response
from test_dataset_rules import make_rules

from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_documentation import collect_documentation
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.discovery_models import InspectedSelection
from codebench_pipeline.discovery_selection import gate_selection
from codebench_pipeline.storage import IntegrityError


class DatasetDocumentationTests(unittest.TestCase):
    def test_configuration_enforces_modes_strict_bounds_and_roundtrip(self) -> None:
        supplied = live().model_dump(mode="json")
        for changes in (
            {"mode": "provided_snippets"},
            {"search_request": None},
            {"snippets": [seed().model_dump(mode="json")]},
            {"maximum_bytes": 0},
            {"maximum_bytes": 10_485_761},
            {"maximum_bytes": "8000"},
            {"unexpected": True},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                DocumentationConfig.model_validate_json(json.dumps({**supplied, **changes}))
        config = DocumentationConfig(snippets=[seed()])
        self.assertEqual(DocumentationConfig.model_validate_json(config.model_dump_json()), config)
        self.assertEqual(DocumentationConfig().snippets, [])
        self.assertIsNone(DocumentationConfig.model_validate({**supplied, "model": None}).model)
        with self.assertRaises((ValidationError, DagsterInvalidInvocationError)):
            config.revision = "changed"

    def test_unresolved_live_model_fails_before_network(self) -> None:
        config = DocumentationConfig.model_validate({**live().model_dump(), "model": None})
        with tempfile.TemporaryDirectory() as temporary:
            with patch("codebench_pipeline.dataset_documentation.discover") as network:
                with self.assertRaises(IntegrityError):
                    collect_documentation(
                        commands("probe inspect example"), make_rules(), config, Path(temporary)
                    )
            network.assert_not_called()

    def test_provided_evidence_retains_rows_and_reports_only_eligible_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.dataset_documentation.discover") as network:
                with patch("codebench_pipeline.inference.subprocess.run") as model:
                    table = collect_documentation(
                        commands("probe inspect example && probe erase example"),
                        make_rules(),
                        DocumentationConfig(snippets=[seed()]),
                        root,
                    )
            self.assertEqual(frame_records(table), [seed().model_dump(mode="json")])
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            self.assertEqual([row["status"] for row in receipt["coverage"]], ["covered", "gap"])
            self.assertEqual(receipt["coverage"][1]["command_words"], ["erase", "example"])
            self.assertEqual(receipt["mode"], "provided_snippets")
            network.assert_not_called()
            model.assert_not_called()

    def test_empty_or_ineligible_inputs_skip_live_network_and_model(self) -> None:
        for frame in (commands(), commands("probe inspect example", decision="needs_review")):
            with self.subTest(rows=len(frame)), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch("codebench_pipeline.dataset_documentation.discover") as network:
                    with patch("codebench_pipeline.inference.subprocess.run") as model:
                        table = collect_documentation(frame, make_rules(), live(), root)
                self.assertTrue(table.empty)
                self.assertEqual(
                    json.loads((root / "documentation-receipt.json").read_text())["coverage"], []
                )
                network.assert_not_called()
                model.assert_not_called()

    def test_nested_subcommand_coverage_ignores_global_option_values(self) -> None:
        supplied = seed().model_copy(update={"subcommands": ["system inspect"]})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            collect_documentation(
                commands("probe --context example system inspect target", "probe system erase target"),
                make_rules(),
                DocumentationConfig(snippets=[supplied]),
                root,
            )
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            self.assertEqual([row["status"] for row in receipt["coverage"]], ["covered", "gap"])
            self.assertEqual(receipt["coverage"][1]["command_words"], ["system", "erase", "target"])

    def test_empty_semantic_response_retains_gap_without_fabricated_documentation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.dataset_documentation.discover", return_value=discovery(root)):
                with patch(
                    "codebench_pipeline.inference.subprocess.run",
                    return_value=native_response({"snippets": []}),
                ):
                    table = collect_documentation(
                        commands("probe inspect example"), make_rules(), live(), root
                    )
            self.assertTrue(table.empty)
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            self.assertEqual(receipt["exclusions"][0]["reason"], "no_semantic_excerpt")
            self.assertEqual(receipt["coverage"][0]["status"], "gap")

    def test_live_excerpt_is_grounded_and_hash_and_inference_receipts_are_retained(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            found = discovery(root)
            with patch("codebench_pipeline.dataset_documentation.discover", return_value=found):
                with patch(
                    "codebench_pipeline.inference.subprocess.run",
                    return_value=native_response({"snippets": [proposal()]}),
                ):
                    table = collect_documentation(
                        commands("probe inspect example"), make_rules(), live(), root
                    )
            row = frame_records(table)[0]
            receipt = json.loads((root / "documentation-receipt.json").read_text())
            excerpt = receipt["accepted"][0]
            self.assertEqual(
                row["snippet"], found.inspected[0].text[excerpt["excerpt_start"] : excerpt["excerpt_end"]]
            )
            self.assertEqual(row["retrieved_at"], found.inspected[0].retrieved_at)
            self.assertEqual(
                receipt["pages"][0]["sha256"], hashlib.sha256(found.inspected[0].text.encode()).hexdigest()
            )
            self.assertEqual(receipt["coverage"][0]["status"], "covered")
            self.assertEqual(receipt["accepted"][0]["source_id"], "docs")
            inference = json.loads((root / "snippets/0/receipt.json").read_text())
            self.assertEqual(inference["status"], "validated")
            self.assertFalse(inference["tools_enabled"])

    def test_ungrounded_or_unrelated_model_proposals_leave_gaps(self) -> None:
        changes: list[dict[str, JsonValue]] = [
            {"url": "https://example.org/invented"},
            {"snippet": "Search-only invented text."},
            {"utility": "other"},
            {"subcommands": ["erase"]},
            {"snippet": "MIT license."},
        ]
        for change in changes:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch("codebench_pipeline.dataset_documentation.discover", return_value=discovery(root)):
                    with patch(
                        "codebench_pipeline.inference.subprocess.run",
                        return_value=native_response({"snippets": [proposal(**change)]}),
                    ):
                        table = collect_documentation(
                            commands("probe inspect example"), make_rules(), live(), root
                        )
                receipt = json.loads((root / "documentation-receipt.json").read_text())
                self.assertTrue(table.empty)
                self.assertEqual(len(receipt["exclusions"]), 1)
                self.assertEqual(receipt["coverage"][0]["status"], "gap")

    def test_license_uncertainty_does_not_block_semantics_or_authorize_corpus_reuse(self) -> None:
        for changes in (
            {"license": "unknown", "license_url": "", "license_evidence": "", "reuse_decision": "unknown"},
            {"license_url": "https://example.org/uninspected-license"},
        ):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                found = discovery(root)
                found = found.model_copy(update={"sources": [found.sources[0].model_copy(update=changes)]})
                with (
                    patch("codebench_pipeline.dataset_documentation.discover", return_value=found),
                    patch(
                        "codebench_pipeline.inference.subprocess.run",
                        return_value=native_response({"snippets": [proposal()]}),
                    ),
                ):
                    table = collect_documentation(
                        commands("probe inspect example"), make_rules(), live(), root
                    )
                self.assertEqual(list(table["snippet"]), [seed().snippet])
                receipt = json.loads((root / "documentation-receipt.json").read_text())
                self.assertEqual(receipt["use_scope"], "semantic_reference_only")
                self.assertEqual(receipt["reuse_review"][0]["decision"], "needs_review")
                corpus = gate_selection(InspectedSelection(sources=found.sources), found.inspected)
                self.assertEqual(corpus.sources[0].decision, "needs_review")

    def test_prohibited_or_ungrounded_sources_never_reach_snippet_inference(self) -> None:
        for changes in (
            {"reuse_decision": "prohibited"},
            {"origin": "synthetic"},
            {"origin_evidence": "Invented provenance"},
            {"commands_evidence": "Invented command"},
        ):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                found = discovery(root)
                found = found.model_copy(update={"sources": [found.sources[0].model_copy(update=changes)]})
                with patch("codebench_pipeline.dataset_documentation.discover", return_value=found):
                    with patch("codebench_pipeline.inference.subprocess.run") as model:
                        table = collect_documentation(
                            commands("probe inspect example"), make_rules(), live(), root
                        )
                self.assertTrue(table.empty)
                model.assert_not_called()
                self.assertEqual(
                    len(json.loads((root / "documentation-receipt.json").read_text())["exclusions"]), 1
                )

    def test_byte_limits_count_utf8_and_reject_oversized_inspected_pages(self) -> None:
        supplied = seed().model_copy(update={"snippet": "\u00e9\u00e9"})
        for mode in ("provided", "live"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                found = discovery(root)
                found = found.model_copy(
                    update={"inspected": [found.inspected[0].model_copy(update={"text": "\u00e9" * 4001})]}
                )
                config = (
                    DocumentationConfig(snippets=[supplied], maximum_bytes=3)
                    if mode == "provided"
                    else live()
                )
                with patch("codebench_pipeline.dataset_documentation.discover", return_value=found):
                    with patch("codebench_pipeline.inference.subprocess.run") as model:
                        with self.assertRaises(IntegrityError):
                            collect_documentation(
                                commands("probe inspect example"), make_rules(), config, root
                            )
                self.assertEqual(
                    json.loads((root / "documentation-receipt.json").read_text())["status"], "failed"
                )
                model.assert_not_called()

    def test_insufficient_discovery_budget_records_gap_without_external_calls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.dataset_documentation.discover") as network:
                table = collect_documentation(
                    commands("probe inspect example"), make_rules(), live(3999), root
                )
            self.assertTrue(table.empty)
            self.assertEqual(
                json.loads((root / "documentation-receipt.json").read_text())["coverage"][0]["status"], "gap"
            )
            network.assert_not_called()

    def test_existing_receipt_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frame = commands("probe inspect example")
            collect_documentation(frame, make_rules(), DocumentationConfig(), root)
            path = root / "documentation-receipt.json"
            original = path.read_bytes()
            with self.assertRaises(IntegrityError):
                collect_documentation(frame, make_rules(), DocumentationConfig(snippets=[seed()]), root)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(json.loads(original)["coverage"][0]["reason"], "utility_not_documented")
