"""Acquire pinned training CSVs and derive tables without interpreting commands."""

from __future__ import annotations

import csv
import io
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from hashlib import sha256
from typing import Final, Protocol

import pandas as pd
from pandera.typing import DataFrame
from pydantic import TypeAdapter

from codebench_pipeline.repository_config import Commit, RepositoryName, check_path
from codebench_pipeline.repository_tree import RepositoryLocation, fetch_repository_bytes, raw_url
from codebench_pipeline.split_schema import SplitRowsTable
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_config import SplitMarginalCount, SplitStats, TrainGroupColumn

TRAIN_COLUMNS: Final = (
    "command",
    "description",
    "util",
    "severity",
    "category",
    "risk",
    "source",
    "seed",
    "class",
)
_MAX_TRAIN_BYTES: Final = 32 * 1024 * 1024
_CATEGORY_ID: Final = re.compile(r"CAT-[1-9][0-9]*")
_REPOSITORY_NAME: Final = TypeAdapter[RepositoryName](RepositoryName)
_COMMIT: Final = TypeAdapter[Commit](Commit)


class TrainSource(RepositoryLocation, Protocol):
    """Read-only shape shared with the training configuration model."""

    @property
    def branch(self) -> str: ...

    @property
    def path(self) -> str: ...

    @property
    def category(self) -> str: ...

    @property
    def sha256(self) -> str: ...


def load_train_category(
    sources: Sequence[TrainSource],
    *,
    fetch_bytes: Callable[[str, int], bytes] = fetch_repository_bytes,
) -> DataFrame[SplitRowsTable]:
    """Read one category in source/record order; fetch_bytes receives the pinned URL and byte cap."""
    categories = {source.category for source in sources}
    if len(categories) > 1 or any(not _CATEGORY_ID.fullmatch(category) for category in categories):
        raise IntegrityError("Training sources must belong to one canonical category")

    rows: list[list[str]] = []
    category_column = TRAIN_COLUMNS.index("category")
    for source in sources:
        _REPOSITORY_NAME.validate_python(source.repository, strict=True)
        _COMMIT.validate_python(source.commit, strict=True)
        check_path(source.path)
        url = raw_url(source, source.path)
        body = fetch_bytes(url, _MAX_TRAIN_BYTES)
        if len(body) > _MAX_TRAIN_BYTES:
            raise IntegrityError(f"Training CSV exceeded its capture limit: {url}")
        if sha256(body).hexdigest() != source.sha256:
            raise IntegrityError(f"Training CSV SHA-256 mismatch: {url}")
        try:
            records = csv.reader(io.StringIO(body.decode("utf-8"), newline=""), strict=True)
            if next(records, None) != list(TRAIN_COLUMNS):
                raise IntegrityError(f"Training CSV header does not match the nine-column schema: {url}")
            for number, row in enumerate(records, start=2):
                if len(row) != len(TRAIN_COLUMNS):
                    raise IntegrityError(f"Training CSV record {number} has incorrect width: {url}")
                if row[category_column].partition(" ")[0] != source.category:
                    raise IntegrityError(f"Training CSV record {number} has the wrong category: {url}")
                rows.append(row)
        except (UnicodeError, csv.Error) as error:
            raise IntegrityError(f"Training CSV is not readable UTF-8 CSV: {url}") from error

    return SplitRowsTable.validate(pd.DataFrame(rows, columns=list(TRAIN_COLUMNS)))


def train_stats(frame: DataFrame[SplitRowsTable], group_by: Sequence[TrainGroupColumn]) -> SplitStats:
    """Count each label independently, in label order and then category/value order."""
    category_values: pd.Series[str] = frame["category"]
    categories = [category.partition(" ")[0] for category in category_values]
    counts: list[SplitMarginalCount] = []
    for label in dict.fromkeys(group_by):
        values: pd.Series[str] = frame[label]
        histogram = Counter(zip(categories, values, strict=True))
        counts.extend(
            SplitMarginalCount(category=category, label=label, value=value, count=count)
            for (category, value), count in sorted(histogram.items())
        )
    return SplitStats(total_rows=len(frame), marginal_counts=tuple(counts))


def merge_train_categories(frames: Mapping[str, pd.DataFrame]) -> DataFrame[SplitRowsTable]:
    """Concatenate in category-key order without sorting or deduplicating within a frame."""
    if not frames:
        return SplitRowsTable.validate(pd.DataFrame(columns=list(TRAIN_COLUMNS)))
    return SplitRowsTable.validate(
        pd.concat([frames[category] for category in sorted(frames)], ignore_index=True)
    )
