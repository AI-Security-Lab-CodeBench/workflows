from subprocess import CalledProcessError
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

    def test_mlflow_volume_without_project_labels_blocks_start(self) -> None:
        def docker_output(*args: str) -> str:
            if args[:3] == ("docker", "volume", "ls") and "name=^codebench_mlflow-data$" in args:
                return "codebench_mlflow-data"
            return ""

        with (
            patch("sys.argv", ["stack.py", "up"]),
            patch.object(stack, "output", side_effect=docker_output),
            patch.object(stack, "run") as run,
            self.assertRaisesRegex(SystemExit, "Refusing unmarked codebench resource codebench_mlflow-data"),
        ):
            stack.main()
        run.assert_not_called()

    def test_up_starts_production_even_when_mlflow_is_unhealthy(self) -> None:
        production_started = False

        def docker_output(*args: str) -> str:
            return "webserver\ningress\nmlflow" if "--services" in args else ""

        def docker_run(*args: str) -> None:
            nonlocal production_started
            if "mlflow" in args:
                raise CalledProcessError(1, args)
            production_started = True

        with (
            patch("sys.argv", ["stack.py", "up"]),
            patch.object(stack, "output", side_effect=docker_output),
            patch.object(stack, "run", side_effect=docker_run),
            self.assertRaises(CalledProcessError),
        ):
            stack.main()
        self.assertTrue(production_started)

    def test_directory_is_not_an_e2e_selector(self) -> None:
        with patch.object(stack, "run") as run, self.assertRaisesRegex(SystemExit, "one pytest node"):
            stack.e2e([], "tests/e2e")
        run.assert_not_called()
