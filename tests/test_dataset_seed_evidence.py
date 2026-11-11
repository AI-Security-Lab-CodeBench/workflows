from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from configuration_fixture import example_config, source_bytes
from pydantic import JsonValue, ValidationError

from codebench_pipeline.dataset_seed_config import DocumentationSeed, SourceSeed
from codebench_pipeline.storage import IntegrityError


class DatasetSeedEvidenceTests(unittest.TestCase):
    def test_source_seeds_preserve_eligibility_and_sensitive_metadata_boundaries(self) -> None:
        inventory = example_config().documentation_sources
        assert inventory is not None
        original = inventory.documents[0].model_dump()
        alterations: list[dict[str, JsonValue]] = [
            {"source_type": "agent_results"},
            {"source_type": "sft_dataset"},
            {"source_type": "command_dataset", "provenance_type": "unknown"},
            {"commands_present": False},
            {"commands_present": 1},
            {"reuse_decision": "unknown"},
            {"accessibility": "inaccessible"},
            {"local_path": ""},
            {"sha256": "not-a-checksum"},
            {"unexpected": True},
            {"provenance_evidence_json": "[]"},
            {"provenance_evidence_json": '{"notes":"https://user:password@example.org"}'},
            {"attribution": "https://user:password@example.org"},
        ]
        for alteration in alterations:
            with self.subTest(alteration=alteration), self.assertRaises(ValidationError):
                SourceSeed.model_validate({**original, **alteration})
        with self.assertRaises(ValidationError):
            SourceSeed.model_validate({**original, "local_path": Path("source.md")})
        snippet = example_config().documentation.snippets[0].model_dump()
        for alteration in ({"extra": True}, {"snippet": "https://user:password@example.org"}):
            with self.subTest(alteration=alteration), self.assertRaises(ValidationError):
                DocumentationSeed.model_validate({**snippet, **alteration})

    def test_source_conversion_preserves_nested_provenance_and_rejects_changed_bytes(self) -> None:
        inventory = example_config().documentation_sources
        assert inventory is not None
        original = inventory.documents[0]
        raw = source_bytes()
        provenance: dict[str, JsonValue] = {"inspection": {"lines": [1, 3], "permitted": True, "notes": None}}
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.md"
            path.write_bytes(raw)
            seed = SourceSeed.model_validate(
                {
                    **original.model_dump(),
                    "local_path": str(path),
                    "provenance_evidence_json": json.dumps(provenance),
                    "upstream_sha256": "a" * 64,
                    "transformations": ["Preserved source text"],
                    "inventory_id": "inspected-inventory",
                }
            )

            document = seed.to_source_document()

            self.assertEqual(document.provenance_evidence, provenance)
            self.assertEqual(document.upstream_sha256, "a" * 64)
            self.assertEqual(document.transformations, ["Preserved source text"])
            self.assertEqual(document.inventory_id, "inspected-inventory")
            self.assertEqual(document.attribution, original.attribution)
            self.assertEqual(document.source_locator, original.source_locator)
            path.write_bytes(raw + b"\n")
            with self.assertRaises(IntegrityError):
                seed.to_source_document()
