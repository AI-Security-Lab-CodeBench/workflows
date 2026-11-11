from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal, assert_never

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from codebench_pipeline.dataframe_schemas import frame_records

ColumnName = Annotated[str, Field(min_length=1, max_length=128)]
FilterText = Annotated[str, Field(max_length=8192)]


class QueryModel(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class TextFilter(QueryModel):
    field: ColumnName
    operator: Literal["contains", "equals"]
    value: FilterText


class NullFilter(QueryModel):
    field: ColumnName
    operator: Literal["is_null"]


ColumnFilter = Annotated[TextFilter | NullFilter, Field(discriminator="operator")]


class ColumnSort(QueryModel):
    field: ColumnName
    direction: Literal["asc", "desc"] = "asc"


class TableQuery(QueryModel):
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=25, ge=1, le=200)
    search: str = Field(default="", max_length=2000)
    filters: tuple[ColumnFilter, ...] = Field(default=(), max_length=20)
    sort: tuple[ColumnSort, ...] = Field(default=(), max_length=4)


class TableRow(QueryModel):
    row_index: int
    values: dict[str, JsonValue]


class TablePage(QueryModel):
    columns: tuple[str, ...]
    total_rows: int
    matched_rows: int
    offset: int
    limit: int
    rows: tuple[TableRow, ...]


class TableQueryError(ValueError):
    field: str
    detail: str

    def __init__(self, field: str, detail: str) -> None:
        self.field = field
        self.detail = detail
        super().__init__(detail)


def cell_text(value: JsonValue) -> str:
    match value:
        case str():
            return value
        case None | bool() | int() | float() | list() | dict():
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        case _:
            assert_never(value)


def _matches(value: JsonValue, condition: ColumnFilter) -> bool:
    match condition:
        case NullFilter():
            return value is None
        case TextFilter():
            match condition.operator:
                case "contains":
                    return condition.value.casefold() in cell_text(value).casefold()
                case "equals":
                    match value:
                        case None | bool() | str() | list() | dict():
                            return condition.value == cell_text(value)
                        case int() | float():
                            try:
                                return Decimal(condition.value) == Decimal(str(value))
                            except InvalidOperation:
                                return False
                        case _:
                            assert_never(value)
                case _:
                    assert_never(condition.operator)
        case _:
            assert_never(condition)


def _sort_key(value: JsonValue) -> tuple[int, str | int | float]:
    match value:
        case None:
            return (4, "")
        case bool():
            return (0, int(value))
        case int() | float():
            return (1, value)
        case str():
            return (2, value.casefold())
        case list() | dict():
            return (3, cell_text(value))
        case _:
            assert_never(value)


def query_table(frame: pd.DataFrame, query: TableQuery) -> TablePage:
    columns = tuple(str(column) for column in frame.columns)
    for clause in (*query.filters, *query.sort):
        if clause.field not in columns:
            raise TableQueryError(clause.field, f"Unknown column: {clause.field}")
    sort_fields = [clause.field for clause in query.sort]
    if len(set(sort_fields)) != len(sort_fields):
        raise TableQueryError("sort", "Sort columns must be distinct")
    search = query.search.casefold()
    rows = [
        TableRow(row_index=index, values=row)
        for index, row in enumerate(frame_records(frame))
        if (not search or any(search in cell_text(value).casefold() for value in row.values()))
        and all(_matches(row[condition.field], condition) for condition in query.filters)
    ]
    for ordering in reversed(query.sort):
        rows.sort(
            key=lambda row: _sort_key(row.values[ordering.field]),
            reverse=ordering.direction == "desc",
        )
        rows.sort(key=lambda row: row.values[ordering.field] is None)
    return TablePage(
        columns=columns,
        total_rows=len(frame),
        matched_rows=len(rows),
        offset=query.offset,
        limit=query.limit,
        rows=tuple(rows[query.offset : query.offset + query.limit]),
    )
