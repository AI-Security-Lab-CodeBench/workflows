import os
import shlex
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from dagster import AssetKey, DagsterInvalidConfigError, validate_run_config
from jsonschema import Draft202012Validator
from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.artifact_schemas import declared_artifact_schemas
from codebench_pipeline.bashguard_audit import BashGuardAnalyzer, audit_candidates
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, frame_records, validated_frame
from codebench_pipeline.dataset_finalization import merge_candidates
from codebench_pipeline.definitions import build_definitions

AUDIT = AssetKey(["datasets", "bashguard_syntax_audit"])


def collected(*commands: str) -> DataFrame[CommandCandidatesTable]:
    rows: list[dict[str, JsonValue]] = [
        {
            "candidate_id": f"row:{index}",
            "command": command,
            "util": "printf",
            "context": "",
            "source_id": "original:source",
            "source_url": "https://example.org/source",
            "source_position": f"line:{index + 1}",
            "transformations": [],
            "risk_candidates": [],
        }
        for index, command in enumerate(commands)
    ]
    return validated_frame(rows, CommandCandidatesTable)


class BashGuardAuditTests(TestCase):
    def test_parsed_and_unparsed_rows_keep_original_evidence_and_never_run(self) -> None:
        with TemporaryDirectory() as temporary:
            sentinel = Path(temporary) / "never-created"
            candidates = merge_candidates(
                collected(
                    f'LABEL=π printf "%s" "$(touch {shlex.quote(str(sentinel))})" | cat > /dev/null',
                    "printf '$(literal) | >'",
                    "if",
                ),
                collected("# comment only", "x" * 65537, "printf '%s' $value"),
            )

            table, metrics = audit_candidates(candidates, BashGuardAnalyzer.from_environment())

            self.assertFalse(sentinel.exists())
            self.assertEqual(
                [
                    (
                        row["candidate_id"],
                        row["original_candidate_id"],
                        row["original_source_id"],
                        row["source_position"],
                        row["status"],
                        row["flags"],
                    )
                    for row in frame_records(table)
                ],
                [
                    (
                        "dataset:row:0",
                        "row:0",
                        "original:source",
                        "line:1",
                        "parsed",
                        ["assignment", "command_substitution", "pipeline", "redirection"],
                    ),
                    ("dataset:row:1", "row:1", "original:source", "line:2", "parsed", []),
                    ("dataset:row:2", "row:2", "original:source", "line:3", "parse", []),
                    ("documentation:row:0", "row:0", "original:source", "line:1", "empty", []),
                    ("documentation:row:1", "row:1", "original:source", "line:2", "size", []),
                    (
                        "documentation:row:2",
                        "row:2",
                        "original:source",
                        "line:3",
                        "parsed",
                        ["parameter_expansion"],
                    ),
                ],
            )
            self.assertEqual(
                metrics.model_dump(),
                {
                    "candidates": 6,
                    "status_counts": {"parsed": 3, "parse": 1, "empty": 1, "size": 1},
                    "flag_counts": {
                        "assignment": 1,
                        "command_substitution": 1,
                        "pipeline": 1,
                        "redirection": 1,
                        "parameter_expansion": 1,
                    },
                },
            )
            self.assertNotIn("command", table.columns)

    def test_empty_merge_returns_typed_empty_table_without_starting_analyzer(self) -> None:
        with patch.dict(os.environ, {"CODEBENCH_BASHGUARD_AUDIT_BINARY": "/missing-audit-binary"}):
            frame, metrics = audit_candidates(
                merge_candidates(collected(), collected()), BashGuardAnalyzer.from_environment()
            )

        self.assertTrue(frame.empty)
        self.assertEqual(metrics.model_dump(), {"candidates": 0, "status_counts": {}, "flag_counts": {}})

    def test_audit_job_rejects_analyzer_binary_override(self) -> None:
        with TemporaryDirectory() as temporary:
            home = Path(temporary)
            definitions = build_definitions(home)
            job = definitions.resolve_job_def("audit_bashguard_syntax")
            validate_run_config(job, {})
            with self.assertRaisesRegex(DagsterInvalidConfigError, "binary"):
                validate_run_config(
                    job, {"resources": {"bashguard_analyzer": {"config": {"binary": "/tmp/parser"}}}}
                )

    def test_native_schema_distinguishes_flag_arrays_from_scalar_text(self) -> None:
        with TemporaryDirectory() as temporary:
            definitions = build_definitions(Path(temporary))
            schema = declared_artifact_schemas(definitions)[tuple(AUDIT.path)]
        validator = Draft202012Validator(schema)
        record = {
            "candidate_id": "dataset:row:0",
            "branch": "dataset",
            "original_candidate_id": "row:0",
            "original_source_id": "original:source",
            "source_position": "line:1",
            "status": "parsed",
            "flags": ["pipeline"],
        }

        self.assertTrue(validator.is_valid(record))
        self.assertFalse(validator.is_valid({**record, "flags": "pipeline"}))
