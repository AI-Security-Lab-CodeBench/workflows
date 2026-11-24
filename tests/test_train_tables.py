from __future__ import annotations

import csv
import io
import unittest
from dataclasses import dataclass, replace
from hashlib import sha256

import pandas as pd
from pandas.testing import assert_frame_equal

from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_tables import count_train_labels, load_train_category, merge_train_categories

HEADER = ["command", "description", "util", "severity", "category", "risk", "source", "seed", "class"]
SHA = "a" * 40
RAW = f"https://raw.githubusercontent.com/example/training/{SHA}"
ROW = [" echo 雪 ", 'quoted "text",\r\nsecond line', "echo", "low", "CAT-3 Display", "", "NA", "0001", "ask"]


@dataclass(frozen=True, slots=True)
class Source:
    repository: str
    branch: str
    commit: str
    path: str
    category: str
    sha256: str


def csv_bytes(*rows: list[str], header: list[str] | None = None) -> bytes:
    output = io.StringIO(newline="")
    csv.writer(output).writerows([HEADER if header is None else header, *rows])
    return output.getvalue().encode()


def source_for(body: bytes, path: str = "train.csv") -> Source:
    return Source("example/training", "not-the-sha", SHA, path, "CAT-3", sha256(body).hexdigest())


class TrainTablesTests(unittest.TestCase):
    def test_load_preserves_source_order_rows_and_exact_text(self) -> None:
        exact = [*ROW[:4], "CAT-3", *ROW[5:]]
        last = ["last", *ROW[1:]]
        older = csv_bytes(ROW, exact, ROW)
        newer = csv_bytes(last)
        responses = {f"{RAW}/z%20folder/first%23.csv": older, f"{RAW}/a.csv": newer}

        def fetch(url: str, limit: int) -> bytes:
            self.assertGreaterEqual(limit, len(responses[url]))
            self.assertLessEqual(limit, 32 * 1024 * 1024)
            return responses[url]

        result = load_train_category(
            [source_for(older, "z folder/first#.csv"), source_for(newer, "a.csv")],
            fetch_bytes=fetch,
        )

        assert_frame_equal(result, pd.DataFrame([ROW, exact, ROW, last], columns=HEADER))

    def test_load_rejects_invalid_content_even_when_csv_can_be_read(self) -> None:
        valid = csv_bytes(ROW)
        cases = (
            (source_for(valid), csv_bytes(["changed", *ROW[1:]]), "SHA-256 mismatch"),
            (None, csv_bytes(ROW, header=HEADER[::-1]), "header"),
            (None, csv_bytes(ROW[:-1]), "incorrect width"),
            (None, csv_bytes([*ROW, "extra"]), "incorrect width"),
            (None, csv_bytes([*ROW[:4], "CAT-30 Display", *ROW[5:]]), "wrong category"),
            (None, csv_bytes([*ROW[:4], "CAT-3.1 Detail", *ROW[5:]]), "wrong category"),
            (None, csv_bytes() + b'"unterminated', "not readable"),
            (None, csv_bytes() + b"\xff", "not readable"),
        )
        for source, body, error in cases:
            with self.subTest(error=error, body=body):
                with self.assertRaisesRegex(IntegrityError, error):
                    load_train_category([source or source_for(body)], fetch_bytes=lambda _url, _limit: body)

    def test_load_rejects_a_nonpinned_commit_before_fetch(self) -> None:
        body = csv_bytes(ROW)

        def fetch(_url: str, _limit: int) -> bytes:
            self.fail("an unpinned source must not be fetched")

        with self.assertRaisesRegex(ValueError, "commit|pattern"):
            load_train_category([replace(source_for(body), commit="main")], fetch_bytes=fetch)

    def test_load_rejects_a_response_over_the_fetch_limit(self) -> None:
        body = csv_bytes(ROW)

        def fetch(_url: str, limit: int) -> bytes:
            return b"x" * (limit + 1)

        with self.assertRaisesRegex(IntegrityError, "capture limit"):
            load_train_category([source_for(body)], fetch_bytes=fetch)

    def test_label_counts_are_marginals_and_combine_category_display_variants(self) -> None:
        frame = pd.DataFrame(
            [
                ("CAT-4 Other", "high", "ask"),
                ("CAT-3 Name A", "low", "allow"),
                ("CAT-3 Name B", "low", "ask"),
                ("CAT-3", "", "ask"),
                ("CAT-3 Name B", "low", "ask"),
            ],
            columns=["category", "severity", "class"],
            index=[1, 1, 4, 5, 6],
        )

        result = count_train_labels(frame, ["severity", "class"])

        assert_frame_equal(
            result,
            pd.DataFrame(
                [
                    ("CAT-3", "severity", "", 1),
                    ("CAT-3", "severity", "low", 3),
                    ("CAT-4", "severity", "high", 1),
                    ("CAT-3", "class", "allow", 1),
                    ("CAT-3", "class", "ask", 3),
                    ("CAT-4", "class", "ask", 1),
                ],
                columns=["category", "label", "value", "count"],
            ),
        )

    def test_merge_orders_categories_but_keeps_each_table_order_and_duplicates(self) -> None:
        first = ["z", *ROW[1:]]
        second = ["a", *ROW[1:]]
        other = [*ROW[:4], "CAT-4 Other", *ROW[5:]]
        frames = {
            "CAT-4": pd.DataFrame([other], columns=HEADER, index=[10]),
            "CAT-3": pd.DataFrame([first, second, first], columns=HEADER, index=[9, 5, 4]),
        }

        result = merge_train_categories(frames)

        assert_frame_equal(result, pd.DataFrame([first, second, first, other], columns=HEADER))

    def test_empty_inputs_return_consumer_schemas(self) -> None:
        empty = pd.DataFrame(columns=HEADER)

        assert_frame_equal(load_train_category([]), empty)
        assert_frame_equal(merge_train_categories({}), empty)
        assert_frame_equal(
            count_train_labels(empty, "class"),
            pd.DataFrame(columns=["category", "label", "value", "count"]),
        )
