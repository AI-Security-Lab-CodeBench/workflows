from __future__ import annotations

import csv
import io
import json
import unittest
from dataclasses import dataclass, replace
from hashlib import sha256

import pandas as pd
from dagster import AssetKey, check_dagster_type
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from codebench_pipeline.dataframe_io import frame_bytes, read_frame
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.train_assets import TRAIN_TABLES
from codebench_pipeline.train_config import MixedTrainSource, SplitStats, TrainConfig
from codebench_pipeline.train_tables import load_train_category, merge_train_categories, train_stats

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

    def test_mixed_source_load_rejects_undeclared_category_in_any_partition(self) -> None:
        for value in ("CAT-5 Unexpected", "CAT-4.1 Detail"):
            with self.subTest(category=value):
                body = csv_bytes(ROW, [*ROW[:4], value, *ROW[5:]])
                mixed = MixedTrainSource(
                    repository="example/training",
                    branch="feature",
                    commit=SHA,
                    path="mixed.csv",
                    sha256=sha256(body).hexdigest(),
                    categories=["CAT-3", "CAT-4"],
                )
                sources = TrainConfig(sources=[mixed]).sources_by_category()
                with self.assertRaisesRegex(IntegrityError, "wrong category"):
                    load_train_category(sources["CAT-3"].sources, fetch_bytes=lambda _url, _limit: body)

    def test_split_manifest_requires_one_valid_category_declaration(self) -> None:
        pin = {
            "repository": "example/training",
            "branch": "feature",
            "commit": SHA,
            "path": "mixed.csv",
            "sha256": "b" * 64,
        }
        declarations: tuple[dict[str, str | list[str]], ...] = (
            {},
            {"category": "CAT-3", "categories": ["CAT-3", "CAT-4"]},
            {"categories": []},
            {"categories": ["CAT-3", "CAT-3"]},
            {"categories": ["CAT-03"]},
        )
        for declaration in declarations:
            with self.subTest(declaration=declaration), self.assertRaises(ValidationError):
                TrainConfig.model_validate({"sources": [{**pin, **declaration}]})
        with self.assertRaisesRegex(ValidationError, "distinct pinned files"):
            TrainConfig.model_validate(
                {"sources": [{**pin, "category": "CAT-3"}, {**pin, "categories": ["CAT-3", "CAT-4"]}]}
            )
        legacy = {**pin, "category": "CAT-3"}
        config = TrainConfig.model_validate({"sources": [legacy]})
        self.assertEqual(config.model_dump()["sources"], [legacy])
        self.assertEqual(config.sources_by_category()["CAT-3"].model_dump(), {"sources": [legacy]})

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
        body = csv_bytes()
        loaded = load_train_category([source_for(body)], fetch_bytes=lambda _url, _limit: body)

        assert_frame_equal(load_train_category([]), empty)
        assert_frame_equal(read_frame(frame_bytes(loaded)), empty)
        assert_frame_equal(merge_train_categories({}), empty)
        self.assertEqual(train_stats(loaded, ["class"]), SplitStats(total_rows=0, marginal_counts=()))

    def test_merge_fan_in_rejects_invalid_nested_tables(self) -> None:
        merge = next(table for table in TRAIN_TABLES if table.key == AssetKey(["train", "merged_rows"]))
        contract = merge.op.input_def_named("categories").dagster_type
        frame = pd.DataFrame([ROW], columns=HEADER)

        self.assertTrue(check_dagster_type(contract, {"CAT-3": frame}).success)
        for invalid in (
            frame.assign(seed=pd.Series([1], dtype=object)),
            frame.assign(seed=None),
            frame.assign(extra=""),
            frame.drop(columns="seed"),
            frame.loc[:, HEADER[::-1]],
            pd.concat([frame, frame[["seed"]]], axis="columns"),
        ):
            with self.subTest(columns=invalid.columns.tolist(), seeds=invalid.dtypes.tolist()):
                self.assertFalse(check_dagster_type(contract, {"CAT-3": invalid}).success)
        self.assertFalse(check_dagster_type(contract, {3: frame}).success)

    def test_stats_json_preserves_ordered_marginals_and_total_rows(self) -> None:
        low = [*ROW[:-1], "allow"]
        high = [*ROW[:3], "high", "CAT-3", *ROW[5:]]
        other = [*ROW[:3], "high", "CAT-4 Other", *ROW[5:]]
        cat3 = pd.DataFrame([ROW, low, high, ROW], columns=HEADER, index=[1, 1, 4, 5])
        cat4 = pd.DataFrame([other], columns=HEADER)
        frame = merge_train_categories({"CAT-4": cat4, "CAT-3": cat3})

        payload = train_stats(frame, ["severity", "class", "risk"]).model_dump_json()

        self.assertEqual(
            json.loads(payload),
            {
                "total_rows": 5,
                "marginal_counts": [
                    {"category": "CAT-3", "label": "severity", "value": "high", "count": 1},
                    {"category": "CAT-3", "label": "severity", "value": "low", "count": 3},
                    {"category": "CAT-4", "label": "severity", "value": "high", "count": 1},
                    {"category": "CAT-3", "label": "class", "value": "allow", "count": 1},
                    {"category": "CAT-3", "label": "class", "value": "ask", "count": 3},
                    {"category": "CAT-4", "label": "class", "value": "ask", "count": 1},
                    {"category": "CAT-3", "label": "risk", "value": "", "count": 4},
                    {"category": "CAT-4", "label": "risk", "value": "", "count": 1},
                ],
            },
        )
        self.assertEqual(SplitStats.model_validate_json(payload).total_rows, 5)

    def test_stats_json_rejects_coerced_counts_and_unknown_fields(self) -> None:
        for payload in (
            '{"total_rows":"1","marginal_counts":[]}',
            '{"total_rows":0,"marginal_counts":[],"unexpected":true}',
            '{"total_rows":1,"marginal_counts":[{"category":"CAT-3","label":"risk","value":"","count":"1"}]}',
        ):
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                SplitStats.model_validate_json(payload)
