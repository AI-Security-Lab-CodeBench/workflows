import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from psycopg.conninfo import make_conninfo


def pytest_configure(config: pytest.Config) -> None:
    workspace = TemporaryDirectory(prefix="codebench-e2e-", dir="/tmp")
    config.add_cleanup(workspace.cleanup)
    config.option.basetemp = str(Path(workspace.name) / "cases")


def pytest_collection_finish(session: pytest.Session) -> None:
    if len(session.items) != 1:
        raise pytest.UsageError("Select exactly one Compose E2E test case by node ID.")


@pytest.fixture(autouse=True)
def isolated_review_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = os.environ.get("CODEBENCH_E2E_DATABASE", "")
    if not re.fullmatch(r"codebench_e2e_[0-9a-f]{32}", database):
        pytest.fail("E2E requires a disposable Compose QA database; use make e2e with E2E_NODE.")
    server = os.environ["CODEBENCH_REVIEW_DATABASE_URL"]
    monkeypatch.setenv("CODEBENCH_REVIEW_DATABASE_URL", make_conninfo(server, dbname=database))
    monkeypatch.setenv("CODEBENCH_HOME", str(tmp_path))
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
