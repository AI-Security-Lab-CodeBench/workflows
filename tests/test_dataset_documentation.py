from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from dagster import DagsterInvalidInvocationError
from dataset_documentation_fixtures import commands, seed
from pydantic import JsonValue, ValidationError
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.dataset_documentation_config import BundleDocumentation, DocumentationConfig
from codebench_pipeline.dataset_documentation_models import command_operations
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_seed_config import DocumentationSeed
from codebench_pipeline.source_manifests import DocumentationManifest, manifest_documentation
from codebench_pipeline.stage_configuration import stage_configs


class DatasetDocumentationTests(unittest.TestCase):
    def test_configuration_rejects_online_search_and_preserves_strict_bounds(self) -> None:
        supplied = DocumentationConfig(snippets=[seed()]).model_dump(mode="json")
        for changes in (
            {"mode": "keenable"},
            {"search_request": {"searches": [{"risk": "CAT-8.1", "queries": ["probe docs"]}]}},
            {"maximum_bytes": 0},
            {"maximum_bytes": 10_485_761},
            {"maximum_bytes": "8000"},
            {"unexpected": True},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                DocumentationConfig.model_validate_json(json.dumps({**supplied, **changes}))
        config = DocumentationConfig(snippets=[seed()])
        self.assertEqual(DocumentationConfig.model_validate_json(config.model_dump_json()), config)
        with self.assertRaises((ValidationError, DagsterInvalidInvocationError)):
            config.revision = "changed"

    def test_nested_subcommand_matching_ignores_global_option_values(self) -> None:
        (inspect,) = command_operations("inspect", "probe", "probe --context example system inspect target")
        (erase,) = command_operations("erase", "probe", "probe system erase target")
        self.assertTrue(inspect.matches("probe", "system inspect"))
        self.assertFalse(erase.matches("probe", "system inspect"))
        self.assertEqual(erase.words, ("system", "erase", "target"))

    def test_malformed_semantic_inputs_are_rejected_at_the_input_boundary(self) -> None:
        changes: list[dict[str, JsonValue]] = [
            {"url": "file:///private/input"},
            {"snippet": ""},
            {"utility": ""},
            {"subcommands": []},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValidationError):
                DocumentationSeed.model_validate({**seed().model_dump(), **change})

    def test_archived_manifest_retains_operation_selectors_and_applicability(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "semantics.json"
            path.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "category_id": "CAT-8",
                        "revision": "operation-reference-2",
                        "snippets": [
                            {
                                "utility": "probe",
                                "operation_selectors": ["--inspect"],
                                "url": "https://example.com/probe",
                                "retrieved_at": "2026-01-01",
                                "snippet": "Inspect an example.",
                                "applicability_notes": "Only on supported builds.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            config = BundleDocumentation(manifest=path.name, revision="old-wrapper-revision")

            resolved = config.resolve(root, "CAT-8")
            self.assertEqual(resolved.revision, "operation-reference-2")
            (snippet,) = resolved.snippets
            self.assertEqual(snippet.subcommands, ["--inspect"])
            self.assertEqual(snippet.version_limits, "Only on supported builds.")

    def test_flag_references_match_equals_options_without_matching_embedded_values(self) -> None:
        (direct,) = command_operations("direct", "probe", "probe --inspect=example")
        (embedded,) = command_operations("embedded", "probe", "probe --context=--inspect erase example")
        self.assertTrue(direct.matches("probe", "--inspect"))
        self.assertFalse(embedded.matches("probe", "--inspect"))

    def test_default_reference_is_accepted_and_covers_only_an_exact_bare_invocation(self) -> None:
        supplied = DocumentationSeed.model_validate(
            {
                **seed().model_dump(),
                "subcommands": ["<default>"],
            }
        )
        manifest = DocumentationManifest(category_id="CAT-8", revision="root-reference", snippets=[supplied])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "documentation.json"
            path.write_text(manifest.model_dump_json())
            (snippet,) = manifest_documentation(path, "CAT-8")
            self.assertEqual(snippet, supplied)
            (selector,) = snippet.subcommands
            for command in (
                "probe",
                "probe inspect",
                "probe -h",
                "probe --context example",
                "probe --",
                'probe ""',
                "probe <default>",
                "other",
                "probe 'unterminated",
            ):
                with self.subTest(command=command):
                    (operation,) = command_operations("candidate", "probe", command)
                    self.assertEqual(operation.matches(snippet.utility, selector), command == "probe")

    def test_changed_documentation_capture_invalidates_labeling_without_reopening_authored_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            settings = stage_configs(fixture_spec(home))["labeling_attempts"]
            capture = commands("probe inspect example")
            inputs = {"documentation_capture": capture}
            before = stage_signature("labeling_attempts", settings, inputs)
            (home / "semantics.json").write_text("unregistered", encoding="utf-8")
            self.assertEqual(stage_signature("labeling_attempts", settings, inputs), before)
            capture.attrs["documentation_scope_receipt_json"] = '{"revision":"changed"}'
            self.assertNotEqual(stage_signature("labeling_attempts", settings, inputs), before)
