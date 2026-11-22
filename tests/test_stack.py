from unittest import TestCase
from unittest.mock import patch

from scripts import stack


class StackSafetyTests(TestCase):
    def test_stopped_legacy_container_blocks_start_before_compose(self) -> None:
        with (
            patch("sys.argv", ["stack.py", "up"]),
            patch.object(stack, "output", return_value="webserver isolated-v1\nold-daemon "),
            patch.object(stack, "run") as run,
            self.assertRaisesRegex(SystemExit, "old-daemon"),
        ):
            stack.main()
        run.assert_not_called()

    def test_orphaned_database_volume_is_not_adopted_by_a_fresh_install(self) -> None:
        with (
            patch("sys.argv", ["stack.py", "up"]),
            patch.object(stack, "output", side_effect=["", "codebench_review-postgres "]),
            patch.object(stack, "run") as run,
            self.assertRaisesRegex(SystemExit, "codebench_review-postgres"),
        ):
            stack.main()
        run.assert_not_called()

    def test_directory_is_not_an_e2e_selector(self) -> None:
        with patch.object(stack, "run") as run, self.assertRaisesRegex(SystemExit, "one pytest node"):
            stack.e2e([], "tests/e2e")
        run.assert_not_called()
