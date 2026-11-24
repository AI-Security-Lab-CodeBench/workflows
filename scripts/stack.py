"""Operate the shared CodeBench deployment using the active Docker context."""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from time import monotonic
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = "/opt/codebench/config/workspace.yaml"
WAIT = ["--wait", "--wait-timeout", "120"]


class Options(argparse.Namespace):
    action: str
    env_file: Path
    dataset: str
    stage: str
    node: str
    manifest: Path
    category: str
    stats_only: bool


def output(*args: str) -> str:
    return subprocess.check_output(args, stdin=subprocess.DEVNULL, text=True).strip()


def run(*args: str, timeout: float | None = None) -> None:
    subprocess.run(args, stdin=subprocess.DEVNULL, check=True, timeout=timeout)


def require_managed_project() -> None:
    selector = "label=com.docker.compose.project=codebench"
    label = '{{.Label "io.codebench.runtime"}}'
    for command, field in ((["ps", "--all"], "{{.Names}}"), (["volume", "ls"], "{{.Name}}")):
        template = f"{field} {label}"
        for row in output("docker", *command, "--filter", selector, "--format", template).splitlines():
            if not row.endswith(" isolated-v1"):
                raise SystemExit(
                    f"Refusing unmarked codebench resource {row.split()[0]}. No automatic cutover."
                )


def require_clean_revision() -> None:
    git = ["git", "-C", str(ROOT)]
    if output(*git, "status", "--porcelain") or output(*git, "rev-parse", "HEAD") != output(
        *git, "rev-parse", "origin/master"
    ):
        raise SystemExit("Build from a clean checkout at the published origin/master revision.")


def e2e(compose: list[str], node: str) -> None:
    if not re.fullmatch(r"tests/e2e/[\w/]+\.py::(?:\w+::)*\w+(?:\[[^\r\n]+\])?", node):
        raise SystemExit("Select one pytest node: tests/e2e/test_file.py::test_name")
    db = f"codebench_e2e_{uuid4().hex}"
    deadline = monotonic() + 300
    admin = [*compose, "exec", "-T", "review-db", "timeout", "15s"]
    try:
        run(*admin, "createdb", "-U", "codebench_admin", "-O", "codebench_review", db, timeout=25)
        budget = int(deadline - monotonic()) - 25
        run(
            *compose,
            "exec",
            "-T",
            "-e",
            f"CODEBENCH_E2E_DATABASE={db}",
            "-e",
            "PYTHONPATH=.:tests",
            "webserver",
            "timeout",
            "--kill-after=5s",
            f"{budget - 7}s",
            "python",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-o",
            "tmp_path_retention_policy=none",
            node,
            timeout=budget,
        )
    finally:
        run(
            *admin,
            "dropdb",
            "-U",
            "codebench_admin",
            "--if-exists",
            "--force",
            db,
            timeout=max(0, deadline - monotonic()),
        )


def main() -> None:
    sys.path.insert(0, str(ROOT))
    from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env", help="Compose deployment settings")
    commands = parser.add_subparsers(dest="action", required=True)
    for name, help_text in {
        "status": "List project containers without changing anything",
        "up": "Start missing services without replacing existing containers",
        "prepare": "Roll out published code and prepare CAT-3 through filtering, without labeling",
        "stages": "List stages from the running code",
    }.items():
        commands.add_parser(name, help=help_text)
    through = commands.add_parser("through", help="Materialize one stage and its ancestors")
    through.add_argument("stage")
    unsynced = commands.add_parser(
        "unsynced", help="Refresh settings and enable build automation (can label)"
    )
    for action in (through, unsynced):
        action.add_argument("--dataset", default=CODEBENCH_DATASET_ID)
    commands.add_parser("e2e", help="Run one affected pytest node using a disposable database").add_argument(
        "node"
    )
    configure = commands.add_parser(
        "configure-splits", help="Register pinned train and test inputs from private JSON"
    )
    configure.add_argument("manifest", type=Path)
    configure.add_argument("--dataset", choices=(CODEBENCH_DATASET_ID,), default=CODEBENCH_DATASET_ID)
    for action in ("train-load", "test-load", "train-merge", "test-merge", "full-dataset"):
        split = commands.add_parser(action)
        split.add_argument("--dataset", choices=(CODEBENCH_DATASET_ID,), default=CODEBENCH_DATASET_ID)
        if action.endswith("-load"):
            split.add_argument("category")
            split.add_argument("--stats-only", action="store_true", help="Compute stats from persisted rows.")
    args = parser.parse_args(namespace=Options())
    if args.action == "status":
        run("docker", "ps", "--all", "--filter", "label=com.docker.compose.project=codebench")
        return
    require_managed_project()
    os.environ["COMPOSE_REMOVE_ORPHANS"] = "false"
    compose = [
        "docker",
        "compose",
        "-p",
        "codebench",
        "--env-file",
        str(args.env_file.resolve()),
        "-f",
        str(ROOT / "compose.yaml"),
    ]
    cli = [*compose, "exec", "-T", "webserver", "python", "-m", "codebench_pipeline.dataset_cli"]
    sensor = [*compose, "exec", "-T", "webserver", "dagster", "sensor"]
    if args.action == "up":
        images = set(output(*compose, "config", "--images").splitlines())
        if images.difference(
            output("docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}}").splitlines()
        ):
            require_clean_revision()
            run(*compose, "build")
        run(*compose, "up", "--no-build", "--no-recreate", *WAIT)
    elif args.action == "prepare":
        require_clean_revision()
        run(*compose, "build", "webserver", "daemon")
        run(*sensor, "stop", "-w", WORKSPACE, "dataset_build_requests")
        run(*cli, "check-idle")
        run(*compose, "up", "--no-deps", "--force-recreate", *WAIT, "webserver", "daemon")
        run(*cli, "register", "--config", "business-configs/datasets/cat3.json")
        run(*cli, "build", "--dataset", CODEBENCH_DATASET_ID, "--force", "--prepare-only")
    elif args.action == "stages":
        run(*cli, "stages")
    elif args.action == "through":
        run(*sensor, "stop", "-w", WORKSPACE, "dataset_build_requests")
        run(*cli, "through", "--dataset", args.dataset, "--stage", args.stage)
    elif args.action == "unsynced":
        run(*cli, "build", "--dataset", args.dataset)
        run(*sensor, "start", "-w", WORKSPACE, "dataset_build_requests")
    elif args.action == "e2e":
        e2e(compose, args.node)
    elif args.action == "configure-splits":
        with args.manifest.open("rb") as manifest:
            subprocess.run([*cli, "configure-splits", "--dataset", args.dataset], stdin=manifest, check=True)
    elif args.action in {"train-load", "test-load", "train-merge", "test-merge", "full-dataset"}:
        category = ["--category", args.category] if args.action.endswith("-load") else []
        if category and args.stats_only:
            category.append("--stats-only")
        run(*cli, args.action, "--dataset", args.dataset, *category)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode)
