from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path

from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_schema import initialize
from codebench_pipeline.human_review_transactions import HumanReviewTransaction


class HumanReviewStore:
    def __init__(self, home: Path) -> None:
        self.path = home.resolve() / "human-review.sqlite3"

    @contextmanager
    def reader(self) -> Generator[HumanReviewTransaction]:
        """Read committed review state, including while the caller holds the writer lock."""
        if not self.path.is_file():
            raise HumanReviewError("not_found", "No review ledger exists")
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as connection:
            connection.execute("BEGIN")
            columns = {row[1] for row in connection.execute("PRAGMA table_info(human_review_batches)")}
            if "review_stage" not in columns:
                raise HumanReviewError(
                    "not_found", "The review ledger has not been initialized for stage review"
                )
            yield HumanReviewTransaction(connection)

    @contextmanager
    def transaction(self, *, timeout: float = 0) -> Generator[HumanReviewTransaction]:
        """Hold the write lock through server snapshot resolution and release registration."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=timeout)) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                with connection:
                    connection.execute("BEGIN IMMEDIATE")
                    initialize(connection)
                    yield HumanReviewTransaction(connection)
            except sqlite3.OperationalError as error:
                if error.sqlite_errorcode in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise HumanReviewError("conflict", "Another review transaction holds the lock") from error
                raise
