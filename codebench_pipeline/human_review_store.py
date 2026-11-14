from __future__ import annotations

import hashlib
import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg import IsolationLevel, sql
from psycopg.errors import DeadlockDetected, LockNotAvailable, SerializationFailure, UndefinedTable
from psycopg.rows import dict_row
from pydantic import JsonValue

from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_schema import initialize
from codebench_pipeline.human_review_transactions import HumanReviewTransaction


class HumanReviewStore:
    def __init__(self, home: Path) -> None:
        self.path = home.resolve() / "human-review.sqlite3"
        self.schema = "review_" + hashlib.sha256(str(home.resolve()).encode()).hexdigest()[:40]
        self.dsn = os.environ.get("CODEBENCH_REVIEW_DATABASE_URL", "")
        if not self.dsn:
            raise HumanReviewError("conflict", "CODEBENCH_REVIEW_DATABASE_URL is required")

    @contextmanager
    def reader(self) -> Generator[HumanReviewTransaction]:
        with psycopg.Connection[dict[str, JsonValue]].connect(self.dsn, row_factory=dict_row) as connection:
            connection.isolation_level = IsolationLevel.REPEATABLE_READ
            connection.read_only = True
            try:
                with connection.transaction():
                    exists = connection.execute(
                        "SELECT 1 FROM pg_namespace WHERE nspname = %s", (self.schema,)
                    ).fetchone()
                    if not exists:
                        raise HumanReviewError("not_found", "No review ledger exists")
                    connection.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(self.schema)))
                    yield HumanReviewTransaction(connection, namespace=self.schema)
            except UndefinedTable as error:
                raise HumanReviewError("not_found", "Review schema has not been initialized") from error

    @contextmanager
    def transaction(
        self, *, timeout: float = 0, dataset_id: str | None = None
    ) -> Generator[HumanReviewTransaction]:
        if timeout < 0:
            raise HumanReviewError("conflict", "Transaction timeout must be nonnegative")
        try:
            with psycopg.Connection[dict[str, JsonValue]].connect(self.dsn, row_factory=dict_row) as connection:
                with connection.transaction():
                    initialize(connection, self.schema)
                with connection.transaction():
                    connection.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(self.schema)))
                    transaction = HumanReviewTransaction(connection, namespace=self.schema, timeout=timeout)
                    if dataset_id is not None:
                        transaction.lock_dataset(dataset_id)
                    yield transaction
        except (LockNotAvailable, DeadlockDetected, SerializationFailure) as error:
            raise HumanReviewError("conflict", "Another review transaction changed or locked this dataset") from error
