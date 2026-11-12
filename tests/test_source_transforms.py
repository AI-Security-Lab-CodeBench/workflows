import hashlib
import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from codebench_pipeline.source_transform_contract import (
    SourceTransformConfig,
    TransformContext,
)
from codebench_pipeline.source_transforms import source_rows, transformed_rows
from codebench_pipeline.storage import IntegrityError


class SourceTransformationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.context = TransformContext(
            "fixture", "https://example.org/source", "a" * 64, {"prefix": "retained:"}
        )

    def module(self, code: str, *, kind: str = "function") -> SourceTransformConfig:
        path = self.root / "transform.py"
        path.write_text(code, encoding="utf-8")
        return SourceTransformConfig.model_validate({
            "module_path": str(path),
            "sha256": hashlib.sha256(code.encode()).hexdigest(),
            "entrypoint": "Mapper" if kind == "class" else "transform",
            "kind": kind,
            "trusted": True,
            "batch_size": 2,
        })

    def test_functions_and_classes_transform_every_row_in_bounded_batches(self) -> None:
        body = (
            "if len(rows) > 2:\n"
            "    raise ValueError('batch exceeded')\n"
            "return [{'command': row.value['cmd'], 'source_position': row.position,\n"
            "         'context': context.options['prefix'] + row.value['description'],\n"
            "         'transformations': ['projected command field']} for row in rows]\n"
        )
        records = source_rows(
            '[{"cmd":"probe one","description":"first"},{"cmd":"probe two","description":"second"},'
            '{"cmd":"probe three","description":"third"}]',
            "json",
        )
        for kind in ("function", "class"):
            with self.subTest(kind=kind):
                header = "def transform(rows, context):\n" if kind == "function" else (
                    "class Mapper:\n    def transform(self, rows, context):\n"
                )
                indent = "    " if kind == "function" else "        "
                config = self.module(header + "".join(indent + line + "\n" for line in body.splitlines()),
                                     kind=kind)
                result = list(transformed_rows(config, records, self.context))
                self.assertEqual(
                    [(row.command, row.context, row.source_position) for row in result],
                    [
                        ("probe one", "retained:first", "record 1"),
                        ("probe two", "retained:second", "record 2"),
                        ("probe three", "retained:third", "record 3"),
                    ],
                )

    def test_changed_code_is_rejected_before_any_import_side_effect(self) -> None:
        marker = self.root / "imported"
        config = self.module(
            f"from pathlib import Path\nPath({str(marker)!r}).touch()\n"
            "def transform(rows, context):\n    return []\n"
        )
        Path(config.module_path).write_text("raise RuntimeError('changed code')\n", encoding="utf-8")
        with self.assertRaises(IntegrityError):
            list(transformed_rows(config, source_rows("inert", "text"), self.context))
        self.assertFalse(marker.exists())

    def test_import_matching_code_cannot_bypass_canonical_output_validation(self) -> None:
        config = self.module(
            "def transform(rows, context):\n"
            "    return [{'command': '', 'source_position': 'record 1'}]\n"
        )
        with self.assertRaises(ValidationError):
            list(transformed_rows(config, source_rows("inert", "text"), self.context))

    def test_manifest_requires_explicit_boolean_trust_and_named_entrypoint(self) -> None:
        config = self.module("def transform(rows, context):\n    return []\n")
        for value in (False, 1, "true", None):
            with self.subTest(trusted=value), self.assertRaises(ValidationError):
                SourceTransformConfig.model_validate({**config.model_dump(), "trusted": value})
        with self.assertRaises(ValidationError):
            SourceTransformConfig.model_validate({**config.model_dump(), "entrypoint": "transform()"})

    def test_csv_and_jsonl_preserve_complete_rows_and_original_positions(self) -> None:
        csv_rows = source_rows('cmd,note\n"probe a","line one\nline two"\n', "csv")
        self.assertEqual(csv_rows[0].value, {"cmd": "probe a", "note": "line one\nline two"})
        json_rows = source_rows('\n{"cmd":"probe a","extra":7}\n\n{"cmd":"probe b"}\n', "jsonl")
        self.assertEqual([row.position for row in json_rows], ["line 2", "line 4"])
        self.assertEqual(json_rows[0].value, {"cmd": "probe a", "extra": 7})
