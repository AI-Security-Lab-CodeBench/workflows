import os
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo


def pytest_configure(config: pytest.Config) -> None:
    workspace = TemporaryDirectory(prefix="codebench-e2e-", dir="/tmp")
    config.add_cleanup(workspace.cleanup)
    config.option.basetemp = str(Path(workspace.name) / "cases")


@pytest.fixture(autouse=True)
def isolated_review_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    if not os.statvfs(".").f_flag & os.ST_RDONLY:
        pytest.fail("E2E requires a read-only QA checkout in the Compose service")
    server = os.environ["CODEBENCH_REVIEW_DATABASE_URL"]
    database = f"codebench_e2e_{uuid4().hex}"
    identifier = sql.Identifier(database)
    with psycopg.connect(make_conninfo(server, dbname="postgres"), autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(identifier))
        try:
            monkeypatch.setenv("CODEBENCH_REVIEW_DATABASE_URL", make_conninfo(server, dbname=database))
            monkeypatch.setenv("CODEBENCH_HOME", str(tmp_path))
            monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
            yield
        finally:
            admin.execute(sql.SQL("DROP DATABASE {}").format(identifier))
