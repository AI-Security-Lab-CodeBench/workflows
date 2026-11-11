import unittest

import pandas as pd
from pydantic import ValidationError

from codebench_pipeline.table_query import (
    ColumnSort,
    NullFilter,
    TableQuery,
    TableQueryError,
    TextFilter,
    query_table,
)


class TableQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = '<img src="inert" onerror="not_executed">' + "full evidence " * 100
        self.frame = pd.DataFrame(
            {
                "command": ["inspect z", "destroy -auto-approve", "inspect a", "inspect a", "inspect x"],
                "context": [self.context] * 5,
                "score": [1, 2, 2, 2, None],
            }
        )

    def test_pages_preserve_full_values_and_original_row_positions(self) -> None:
        page = query_table(self.frame, TableQuery(offset=2, limit=2))
        self.assertEqual((page.total_rows, page.matched_rows), (5, 5))
        self.assertEqual(tuple(row.row_index for row in page.rows), (2, 3))
        self.assertEqual(page.rows[0].values["context"], self.context)
        self.assertEqual(page.columns, ("command", "context", "score"))
        end = query_table(self.frame, TableQuery(offset=5, limit=2))
        self.assertEqual(end.rows, ())
        self.assertEqual(end.matched_rows, 5)

    def test_search_and_column_filters_use_literal_text_and_intersection(self) -> None:
        page = query_table(
            self.frame,
            TableQuery(
                search="FULL EVIDENCE",
                filters=(TextFilter(field="command", operator="contains", value="-AUTO-APPROVE"),),
            ),
        )
        self.assertEqual(page.matched_rows, 1)
        self.assertEqual(page.rows[0].row_index, 1)
        exact = query_table(
            self.frame,
            TableQuery(filters=(TextFilter(field="command", operator="equals", value="inspect a"),)),
        )
        self.assertEqual(tuple(row.row_index for row in exact.rows), (2, 3))
        for text in ("2", "2.0", "2e0"):
            with self.subTest(text=text):
                numeric = query_table(
                    self.frame,
                    TableQuery(filters=(TextFilter(field="score", operator="equals", value=text),)),
                )
                self.assertEqual(tuple(row.row_index for row in numeric.rows), (1, 2, 3))
        empty = query_table(self.frame, TableQuery(search="__import__('os').system('inert')"))
        self.assertEqual((empty.matched_rows, empty.rows), (0, ()))

    def test_multicolumn_sort_is_stable_and_keeps_nulls_last(self) -> None:
        page = query_table(
            self.frame,
            TableQuery(
                sort=(
                    ColumnSort(field="score", direction="desc"),
                    ColumnSort(field="command", direction="asc"),
                )
            ),
        )
        self.assertEqual(tuple(row.row_index for row in page.rows), (1, 2, 3, 0, 4))
        nulls = query_table(self.frame, TableQuery(filters=(NullFilter(field="score", operator="is_null"),)))
        self.assertEqual(tuple(row.row_index for row in nulls.rows), (4,))

    def test_unknown_columns_and_duplicate_sort_fail_even_for_empty_tables(self) -> None:
        queries = (
            TableQuery(filters=(TextFilter(field="unknown", operator="contains", value=""),)),
            TableQuery(sort=(ColumnSort(field="unknown"),)),
            TableQuery(sort=(ColumnSort(field="command"), ColumnSort(field="command"))),
        )
        for query in queries:
            with self.subTest(query=query):
                with self.assertRaises(TableQueryError):
                    query_table(self.frame.iloc[:0], query)

    def test_query_boundary_rejects_malformed_or_unbounded_requests(self) -> None:
        values = (
            '{"offset":-1}',
            '{"limit":0}',
            '{"limit":201}',
            '{"limit":"2"}',
            '{"filters":[{"field":"command","operator":"eval","value":"inert"}]}',
            '{"filters":[{"field":"command","operator":"is_null","value":"unexpected"}]}',
            '{"sort":[{"field":"command","direction":"sideways"}]}',
            '{"sql":"select inert"}',
        )
        for value in values:
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    TableQuery.model_validate_json(value)


if __name__ == "__main__":
    unittest.main()
