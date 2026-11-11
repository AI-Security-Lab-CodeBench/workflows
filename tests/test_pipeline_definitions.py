from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from codebench_pipeline.engine import DEPENDENCIES, implementation_inputs
from codebench_pipeline.legacy_definitions import make_asset
from codebench_pipeline.storage import digest


class DagsterContractTests(unittest.TestCase):
    def test_helper_changes_invalidate_consuming_stage_versions(self) -> None:
        before = implementation_inputs("filtering")

        def changed_digest(path: Path) -> str:
            return "a" * 64 if path.name == "replay.py" else digest(path)

        with patch("codebench_pipeline.engine.digest", side_effect=changed_digest):
            after = implementation_inputs("filtering")
        self.assertNotEqual(before, after)

    def test_partitioned_asset_edges_use_checked_string_references(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for stage in DEPENDENCIES:
                with self.subTest(stage=stage):
                    definition = make_asset(stage, root)
                    for input_definition in definition.op.input_defs:
                        if input_definition.name in DEPENDENCIES[stage]:
                            self.assertEqual(input_definition.dagster_type.key, "String")
                    self.assertEqual(definition.op.output_defs[0].dagster_type.key, "String")
