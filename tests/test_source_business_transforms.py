import hashlib
import unittest
from pathlib import Path

from test_dataset_rules import make_candidate, make_rules

from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_extract import filter_candidates
from codebench_pipeline.source_transform_contract import (
    CanonicalSourceRow,
    SourceTransformConfig,
    TransformContext,
)
from codebench_pipeline.source_transforms import source_rows, transformed_rows


class SourceBusinessTransformationTests(unittest.TestCase):
    def transform(self, filename: str, entrypoint: str, text: str) -> list[CanonicalSourceRow]:
        path = Path(__file__).resolve().parents[1] / "transforms" / filename
        config = SourceTransformConfig(
            module_path=str(path),
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            entrypoint=entrypoint,
            kind="function" if entrypoint == "transform" else "class",
            trusted=True,
        )
        context = TransformContext("fixture", "https://example.org/source", "a" * 64, {})
        return list(transformed_rows(config, source_rows(text, "text"), context))

    def test_tldr_examples_remain_literal_and_keep_original_positions(self) -> None:
        text = "# probe\n\n- Inspect:\n\n`probe inspect {{resource}}`\n"
        rows = self.transform("tldr.py", "transform", text)
        self.assertEqual([row.command for row in rows], ["probe inspect {{resource}}"])
        start = text.index("probe inspect")
        self.assertEqual(rows[0].source_position, f"document; characters {start}:{start + 26}")
        self.assertEqual(self.transform("tldr.py", "transform", ""), [])

    def test_cheatsheet_continuations_preserve_quoted_whitespace(self) -> None:
        text = "# example\nprobe inspect \\\n  --value '  kept  '\nprobe inspect \\\\\n"
        rows = self.transform("cheatsheets.py", "Cheatsheet", text)
        self.assertEqual(
            [row.command for row in rows],
            ["probe inspect \\\n  --value '  kept  '", "probe inspect \\\\"],
        )
        self.assertEqual(
            [row.source_position for row in rows], ["document; lines 2:3", "document; lines 4:4"]
        )

    def test_aws_rst_keeps_continuations_and_quoted_values_without_output_rows(self) -> None:
        text = (
            "Example::\n\n"
            "    aws cloudformation delete-stack \\\n"
            "        --stack-name '  example  '\n\n"
            'Output::\n\n    {"StackId": "example"}\n'
        )
        rows = self.transform("aws_rst.py", "AwsExamples", text)
        self.assertEqual(
            [row.command for row in rows],
            ["aws cloudformation delete-stack \\\n    --stack-name '  example  '"],
        )
        self.assertEqual(rows[0].source_position, "document; lines 3:4")
        self.assertEqual(rows, self.transform("aws_rst.py", "AwsExamples", text))

    def test_docopt_placeholders_are_withheld_without_rewriting_sql_comparisons(self) -> None:
        examples = [
            make_candidate("placeholder").model_copy(update={"command": "probe inspect <resource_name>"}),
            make_candidate("comparison").model_copy(update={"command": "probe inspect --query 'a<b && c>d'"}),
        ]
        frame = validated_frame([row.model_dump(mode="json") for row in examples], CommandCandidatesTable)
        filtered = filter_candidates(frame, make_rules(), FilteringConfig())
        self.assertEqual(list(filtered["filter_decision"]), ["needs_review", "eligible"])
        self.assertEqual(list(filtered["filter_reason"]), ["unresolved_template", ""])
        self.assertEqual(list(filtered["command"]), [row.command for row in examples])

    def test_json_object_closers_are_not_template_markers(self) -> None:
        cases = [
            ("""probe inspect --data '{"filter":{"enabled":true}}'""", "eligible"),
            ("""probe inspect --data='{"filter":{"enabled":true}}'""", "eligible"),
            ("""probe inspect --data='{"filter":{"enabled":true}}' 'tail}}'""", "needs_review"),
            ("""probe inspect --data '[{"name":{"id":1}}]' 'tail}}'""", "needs_review"),
            ("""probe inspect --data '{"filter":{"name":"value}}"}}'""", "needs_review"),
            ("""probe inspect --data '{"filter":{"name":"<resource>"}}'""", "needs_review"),
            ("""probe inspect --data '{"invalid":}}'""", "needs_review"),
        ]
        rows = [
            make_candidate(str(index)).model_copy(update={"command": command}).model_dump(mode="json")
            for index, (command, _) in enumerate(cases)
        ]
        result = filter_candidates(
            validated_frame(rows, CommandCandidatesTable), make_rules(), FilteringConfig()
        )
        self.assertEqual(list(result["filter_decision"]), [decision for _, decision in cases])
        self.assertEqual(
            list(result["filter_reason"]),
            ["" if decision == "eligible" else "unresolved_template" for _, decision in cases],
        )
