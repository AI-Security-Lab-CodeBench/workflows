import unittest

from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.review_identity import review_identity


class ReviewIdentityTests(unittest.TestCase):
    def test_identity_uses_only_selected_original_typed_values(self) -> None:
        row = {"command": "printf 'a  b'", "severity": "low", "candidate_id": "old"}
        original = review_identity(row, ("command", "severity"))
        self.assertEqual(
            original,
            review_identity({**row, "candidate_id": "new", "evidence": "changed"}, ("severity", "command")),
        )
        for edits in ({"command": "printf 'a b'"}, {"severity": "high"}):
            with self.subTest(edits=edits):
                self.assertNotEqual(original, review_identity({**row, **edits}, ("command", "severity")))
        self.assertEqual(
            len({review_identity({"value": value}, ("value",)) for value in (None, False, 0, "0", [], {})}),
            6,
        )

    def test_missing_duplicate_empty_and_nonfinite_identity_inputs_fail(self) -> None:
        for row, columns in (
            ({}, ("missing",)),
            ({"value": None}, ()),
            ({"value": None}, ("value", "value")),
            ({"value": float("nan")}, ("value",)),
            ({"value": [float("inf")]}, ("value",)),
        ):
            with self.subTest(row=row, columns=columns), self.assertRaises(HumanReviewError):
                review_identity(row, columns)
