import unittest

from pandera.typing import DataFrame
from pydantic import JsonValue
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    ProposedAnnotationsTable,
    validated_frame,
)
from codebench_pipeline.dataset_finalization import deduplicate_annotations, merge_candidates


def reviewed_row(identity: str, **changes: JsonValue) -> dict[str, JsonValue]:
    candidate = make_candidate(identity)
    label = make_label(candidate, make_rules())
    return {
        **candidate.model_dump(mode="json"),
        **label.model_dump(mode="json", by_alias=True),
        **changes,
    }


def annotations(*rows: dict[str, JsonValue]) -> DataFrame[ProposedAnnotationsTable]:
    return validated_frame(list(rows), ProposedAnnotationsTable)


class UnifiedDeduplicationTests(unittest.TestCase):
    def test_branch_collisions_retain_original_identity_and_duplicate_mapping(self) -> None:
        merged = merge_candidates(
            validated_frame([make_candidate("one").model_dump(mode="json")], CommandCandidatesTable),
            validated_frame(
                [make_candidate("one").model_dump(mode="json")], CommandCandidatesTable
            ),
        )
        self.assertEqual(merged["candidate_id"].tolist(), ["dataset:one", "documentation:one"])
        self.assertEqual(
            merged["source_id"].tolist(),
            ["dataset:maintainer-reference", "documentation:maintainer-reference"],
        )
        reviewed = annotations(
            reviewed_row("dataset:one"),
            reviewed_row("documentation:one", command="probe \t inspect  example"),
        )
        reviewed.attrs = dict(merged.attrs)
        deduplicated = deduplicate_annotations(reviewed)
        self.assertEqual(deduplicated["candidate_id"].tolist(), ["dataset:one"])
        self.assertEqual(
            deduplicated.attrs["duplicates"],
            [
                {
                    "duplicate_id": "documentation:one",
                    "survivor_id": "dataset:one",
                    "normalized_command": "probe inspect example",
                    "reason": "exact_normalized_command_and_annotation",
                }
            ],
        )
        self.assertEqual(
            deduplicated.attrs["origins"]["documentation:one"],
            {"branch": "documentation", "candidate_id": "one", "source_id": "maintainer-reference"},
        )

    def test_quotes_shell_syntax_context_and_label_disagreements_are_retained(self) -> None:
        differences: tuple[dict[str, JsonValue], ...] = (
            {"command": "probe inspect 'example'"},
            {"command": 'probe inspect "example"'},
            {"command": "probe inspect $TARGET"},
            {"command": "probe inspect $(target)"},
            {"command": "probe inspect example\n"},
            {"command": "probe inspect example # context"},
            {"command": "probe inspect example | other"},
            {"command": "probe inspect --force example"},
            {"command": "probe inspect other"},
            {"context": "A different operating context."},
            {"class": "ask"},
            {"severity": "high"},
            {"decision": "needs_review"},
            {"risk_confidence": "medium"},
            {"risk_id": "CAT-8.2"},
        )
        for difference in differences:
            with self.subTest(difference=difference):
                reviewed = annotations(
                    reviewed_row("dataset:one"),
                    reviewed_row("documentation:two", **difference),
                )
                deduplicated = deduplicate_annotations(reviewed)
                self.assertEqual(deduplicated["candidate_id"].tolist(), ["dataset:one", "documentation:two"])
                self.assertEqual(deduplicated.attrs["duplicates"], [])
        ambiguous = annotations(
            reviewed_row("one", command="probe inspect $(target)"),
            reviewed_row("two", command="probe inspect $(target)"),
        )
        self.assertEqual(len(deduplicate_annotations(ambiguous)), 1)

    def test_exact_quoted_duplicates_preserve_other_quote_and_payload_differences(self) -> None:
        reviewed = annotations(
            reviewed_row("one", command="probe inspect 'a  b'"),
            reviewed_row("two", command="probe inspect 'a  b'"),
            reviewed_row("three", command="probe inspect 'a b'"),
            reviewed_row("four", command='probe inspect "a  b"'),
        )
        self.assertEqual(
            deduplicate_annotations(reviewed)["candidate_id"].tolist(),
            ["one", "three", "four"],
        )
