import json
import tempfile
import unittest
from pathlib import Path

from pydantic import JsonValue, ValidationError

from codebench_pipeline.source_manifests import manifest_sources


class PreparedSourceAuthoringTests(unittest.TestCase):
    def setUp(self) -> None:
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.path = Path(root.name) / "sources.json"
        self.source: dict[str, JsonValue] = {
            "source_id": "sample-source",
            "url": "https://example.com/source",
            "local_path": "source.jsonl",
            "license": "MIT",
            "license_url": "https://example.com/license",
            "source_type": "documentation",
            "accessibility": "accessible",
            "reuse_decision": "permitted",
            "commands_present": True,
            "origin_revision": "upstream-42",
            "retrieved_at": "2026-01-01",
            "format": "jsonl",
        }
        self.transform: dict[str, JsonValue] = {
            "module_path": "reviewed.py",
            "sha256": "1" * 64,
            "entrypoint": "transform",
            "trusted": True,
        }

    def write_manifest(self, source: dict[str, JsonValue], *, version: int = 2) -> None:
        self.path.write_text(
            json.dumps(
                {
                    "version": version,
                    "category_id": "CAT-99",
                    "revision": "manifest-7",
                    "sources": [source],
                }
            ),
            encoding="utf-8",
        )

    def test_object_options_and_prior_provenance_reach_runtime_record(self) -> None:
        self.write_manifest(
            {
                **self.source,
                "raw_format": "ndjson",
                "prior_transformations": ["source_prepared"],
                "provenance_evidence": {"capture_stage": "reviewed", "offsets": [1, 2]},
                "transform": {**self.transform, "options": {"window": 5}, "batch_size": 2},
            }
        )

        [source] = manifest_sources(self.path, "CAT-99")

        self.assertEqual(source.revision, "upstream-42")
        self.assertEqual((source.format, source.raw_format), ("jsonl", "ndjson"))
        self.assertEqual(source.transformations, ["source_prepared"])
        self.assertEqual(
            json.loads(source.provenance_evidence_json), {"capture_stage": "reviewed", "offsets": [1, 2]}
        )
        self.assertIsNotNone(source.transform)
        assert source.transform is not None
        self.assertEqual(json.loads(source.transform.options_json), {"window": 5})
        self.assertEqual(source.transform.batch_size, 2)
        self.assertEqual(Path(source.transform.module_path), self.path.parent / "reviewed.py")

    def test_current_schema_rejects_obsolete_or_ineffective_options(self) -> None:
        for overrides in (
            {"untrusted_text": True},
            {"preserve_all_context": False},
            {"context_field": "metadata.description"},
            {"format": "text", "text_field": "$"},
            {"format": "markdown", "context_field": "description"},
            {"context_field": "description", "transform": self.transform},
            {"format": "markdown", "transform": {**self.transform, "batch_size": 2}},
            {"transform": {**self.transform, "options_json": "{}"}},
        ):
            with self.subTest(overrides=overrides):
                self.write_manifest({**self.source, **overrides})
                with self.assertRaises(ValidationError):
                    manifest_sources(self.path, "CAT-99")

    def test_historical_schema_preserves_inert_metadata_and_dotted_context(self) -> None:
        historic = dict(self.source)
        historic["revision"] = historic.pop("origin_revision")
        self.write_manifest(
            {
                **historic,
                "context_field": "metadata.description",
                "untrusted_text": False,
                "preserve_all_context": True,
            },
            version=1,
        )

        [source] = manifest_sources(self.path, "CAT-99")

        self.assertEqual(source.context_field, "metadata.description")
        self.assertFalse(source.untrusted_text)
        self.assertTrue(source.preserve_all_context)
