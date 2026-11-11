from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from codebench_pipeline.publication import publish
from codebench_pipeline.schemas import validate_export
from codebench_pipeline.storage import IntegrityError


class PublicationTests(unittest.TestCase):
    def test_empty_release_is_valid_and_cached_bytes_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            published = publish(root, {})
            self.assertEqual(validate_export(published / "bundle/commands.csv").height, 0)
            self.assertEqual(publish(root, {}), published)
            (published / "bundle/README.md").write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(IntegrityError, "release content changed"):
                publish(root, {})
