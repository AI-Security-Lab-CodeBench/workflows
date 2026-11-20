import os
import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def compose_environment() -> dict[str, str]:
    subprocess.run(["make", "remote-up"], check=True)
    settings = dict(
        line.split("=", 1)
        for line in Path(".env").read_text().splitlines()
        if line.startswith(("REMOTE_SSH=", "REMOTE_SSH_PORT="))
    )
    environment = os.environ.copy()
    environment.pop("DOCKER_CONTEXT", None)
    remote = settings.get("REMOTE_SSH")
    environment["DOCKER_HOST"] = (
        f"ssh://{remote}:{settings.get('REMOTE_SSH_PORT') or '22'}"
        if remote
        else "unix:///var/run/docker.sock"
    )
    return environment


def test_collection_group_contracts(compose_environment: dict[str, str]) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            "codebench",
            "exec",
            "-T",
            "-w",
            os.environ.get("CODEBENCH_QA_WORKDIR", "/opt/codebench-qa-src"),
            "-e",
            "PYTHONPATH=.:tests",
            "-e",
            "PYTHONDONTWRITEBYTECODE=1",
            "-e",
            "DAGSTER_DISABLE_TELEMETRY=1",
            "-e",
            "UV_CACHE_DIR=/tmp/codebench-qa-uv",
            "-e",
            "UV_PROJECT_ENVIRONMENT=/opt/codebench/.venv",
            "webserver",
            "timeout",
            "--signal=INT",
            "--kill-after=20s",
            "270s",
            "uv",
            "run",
            "--no-sync",
            "--with-requirements",
            "tests/e2e/requirements.txt",
            "python",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-o",
            "tmp_path_retention_policy=none",
            "tests/e2e",
        ],
        env=compose_environment,
        check=True,
    )
