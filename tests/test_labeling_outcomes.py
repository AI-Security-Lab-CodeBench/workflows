import unittest

from pydantic import ValidationError
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.labeling_outcomes import (
    LabelingAttempt,
    failed_attempt,
    label_attempt,
    validate_attempts,
)


class LabelingOutcomeTests(unittest.TestCase):
    def test_mixed_outcomes_cover_candidates_without_manufacturing_failed_labels(self) -> None:
        rules = make_rules()
        first, second = make_candidate("one"), make_candidate("two")
        success = label_attempt(first, make_label(first, rules), documentation(), "a" * 64, "batch/1")
        failure = failed_attempt(
            second, "grounding_missing", "No operation evidence.", [], "b" * 64, "batch/1"
        )
        validate_attempts([first, second], [failure, success], rules)
        self.assertIsNone(failure.label)
        self.assertEqual(success.failure_code, "")
        self.assertEqual(success.candidate_id, "one")

    def test_missing_foreign_duplicate_or_changed_evidence_cannot_pass_coverage(self) -> None:
        rules, candidate = make_rules(), make_candidate("one")
        attempt = label_attempt(candidate, make_label(candidate, rules), documentation(), "a" * 64, "batch/1")
        foreign = make_candidate("foreign")
        other = failed_attempt(foreign, "missing", "No docs.", [], "b" * 64, "batch/2")
        for attempts in (
            [],
            [other],
            [attempt, attempt],
            [attempt.model_copy(update={"context": "changed"})],
        ):
            with self.subTest(attempts=attempts), self.assertRaises(ValueError):
                validate_attempts([candidate], attempts, rules)

    def test_conditional_payload_and_document_digest_are_validated_at_boundary(self) -> None:
        candidate = make_candidate("one")
        failure = failed_attempt(candidate, "missing", "No docs.", [], "a" * 64, "batch/1")
        for change in (
            {"label": make_label(candidate, make_rules()).model_dump(mode="json")},
            {"documentation": "[]"},
            {"failure_code": ""},
            {"status": "success"},
            {"documentation_manifest_digest": "b" * 64},
        ):
            with self.subTest(change=change), self.assertRaises(ValidationError):
                LabelingAttempt.model_validate({**failure.model_dump(), **change})
