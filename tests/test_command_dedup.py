import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from command_dedup import near_key


class CommandDedupTests(unittest.TestCase):
    def item(self, command, label="ask"):
        return {"row": {"command": command, "risk": "CAT-3.4", "class": label, "severity": "critical"},
                "role": "dangerous"}

    def test_resource_renaming_and_presentation_are_close_siblings(self):
        first = self.item('aws rds delete-db-instance --db-instance-identifier "alpha" --skip-final-snapshot')
        second = self.item("aws  rds delete-db-instance --db-instance-identifier=beta --skip-final-snapshot")
        self.assertEqual(near_key(first), near_key(second))

    def test_scope_flags_environment_and_labels_remain_distinct(self):
        baseline = near_key(self.item("aws s3 rm s3://project-prod --recursive"))
        for candidate in (
            self.item("aws s3 rm s3://project-prod"),
            self.item("aws s3 rm s3://project-dev --recursive"),
            self.item("aws s3 rm s3://project-prod/prefix --recursive"),
            self.item("aws s3 rm s3://project-prod --recursive --dryrun"),
            self.item("aws s3 rm s3://project-prod --recursive", label="deny"),
        ):
            with self.subTest(candidate=candidate):
                self.assertNotEqual(baseline, near_key(candidate))

    def test_sql_and_filters_are_not_erased_by_resource_normalization(self):
        self.assertNotEqual(
            near_key(self.item('mysql -e "DELETE FROM users"')),
            near_key(self.item('mysql -e "DELETE FROM users WHERE id=1"')),
        )
        self.assertNotEqual(
            near_key(self.item("aws s3 rm s3://bucket --recursive --exclude '*.csv'")),
            near_key(self.item("aws s3 rm s3://bucket --recursive --exclude '*.txt'")),
        )
