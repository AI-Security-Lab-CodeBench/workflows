import unittest

from pandera.typing import DataFrame
from pydantic import JsonValue
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.category_eligibility import category_eligible_annotations
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    ProposedAnnotationsTable,
    validated_frame,
)
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.dataset_finalization import deduplicate_annotations, merge_candidates
from codebench_pipeline.dataset_tables import review_queue


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
    def test_category_filter_retains_supported_confidence_and_preserves_exclusions(self) -> None:
        original = annotations(
            reviewed_row("high"),
            reviewed_row("medium", category_confidence="medium"),
            reviewed_row("low", category_confidence="low"),
            reviewed_row("irrelevant", decision="reject"),
            reviewed_row("foreign", category_id="CAT-9"),
            reviewed_row("uncertain", decision="needs_review", risk_confidence="low"),
        )
        original.attrs["duplicates"] = [{"duplicate_id": "duplicate", "survivor_id": "high"}]
        retained = category_eligible_annotations(original, make_rules())
        self.assertEqual(retained["candidate_id"].tolist(), ["high", "medium", "uncertain"])
        self.assertEqual(
            retained.attrs["category_exclusions"],
            [
                {"candidate_id": "low", "reasons": ["low_category_confidence"]},
                {"candidate_id": "irrelevant", "reasons": ["label_rejected"]},
                {"candidate_id": "foreign", "reasons": ["different_category"]},
            ],
        )
        self.assertEqual(retained.attrs["duplicates"], original.attrs["duplicates"])
        self.assertEqual(len(original), 6)
        self.assertNotIn("category_exclusions", original.attrs)

    def test_retained_medium_confidence_in_each_dimension_requires_human_review(self) -> None:
        fields = ("category_confidence", "risk_confidence", "severity_confidence", "class_confidence")
        original = annotations(
            reviewed_row("automatic"),
            *(reviewed_row(field, **{field: "medium"}) for field in fields),
            reviewed_row("excluded", category_confidence="low"),
        )
        retained = category_eligible_annotations(original, make_rules())
        queue = review_queue(retained, make_rules(), PublicationConfig().review_columns)
        self.assertEqual(queue["candidate_id"].tolist(), list(fields))

    def test_review_queue_preserves_optional_source_weight_even_when_empty(self) -> None:
        for weight in (None, 0.8123456789012345):
            for confidence in ("high", "medium"):
                with self.subTest(weight=weight, confidence=confidence):
                    source = {} if weight is None else {"source_weight": weight}
                    original = annotations(reviewed_row("candidate", risk_confidence=confidence, **source))

                    queue = review_queue(original, make_rules(), PublicationConfig().review_columns)

                    self.assertEqual(
                        queue["candidate_id"].tolist(), ["candidate"] if confidence == "medium" else []
                    )
                    if weight is None:
                        self.assertNotIn("source_weight", queue)
                    else:
                        self.assertEqual(
                            queue["source_weight"].tolist(), [weight] if confidence == "medium" else []
                        )

    def test_category_filter_accepts_empty_and_all_excluded_tables(self) -> None:
        for original in (annotations(), annotations(reviewed_row("low", category_confidence="low"))):
            with self.subTest(rows=len(original)):
                retained = category_eligible_annotations(original, make_rules())
                self.assertTrue(retained.empty)
                self.assertEqual(len(retained.attrs["category_exclusions"]), len(original))

    def test_branch_collisions_retain_original_identity_and_duplicate_mapping(self) -> None:
        merged = merge_candidates(
            validated_frame([make_candidate("one").model_dump(mode="json")], CommandCandidatesTable),
            validated_frame([make_candidate("one").model_dump(mode="json")], CommandCandidatesTable),
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

    def test_exact_duplicate_retains_strongest_source_in_either_order(self) -> None:
        low = reviewed_row("dataset:low", source_weight=0.55)
        high = reviewed_row("documentation:high", source_weight=0.95)
        disagreement = reviewed_row("keenable:disagreement", source_weight=0.3, severity="high")
        for order in ((low, high), (high, low)):
            with self.subTest(order=[row["candidate_id"] for row in order]):
                result = deduplicate_annotations(annotations(*order, disagreement))
                self.assertEqual(
                    result["candidate_id"].tolist(), ["documentation:high", "keenable:disagreement"]
                )
                self.assertEqual(result["source_weight"].tolist(), [0.95, 0.3])
                self.assertEqual(
                    [(row["duplicate_id"], row["survivor_id"]) for row in result.attrs["duplicates"]],
                    [("dataset:low", "documentation:high")],
                )
