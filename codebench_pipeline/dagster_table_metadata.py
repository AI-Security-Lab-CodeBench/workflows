import json
from collections.abc import Mapping
from typing import Protocol, assert_never

import pandas as pd
from dagster import (
    MetadataValue,
    TableColumn,
    TableMetadataValue,
    TableRecord,
    TableSchema,
    TableSchemaMetadataValue,
)
from pydantic import JsonValue, TypeAdapter


class MetadataSink(Protocol):
    def add_output_metadata(
        self, metadata: Mapping[str, str | int | TableMetadataValue | TableSchemaMetadataValue]
    ) -> None: ...


def preview_value(value: JsonValue) -> str | int | float | bool | None:
    match value:
        case None | bool() | int() | float():
            return value
        case str():
            return value[:500]
        case list() | dict():
            return json.dumps(value, ensure_ascii=False)[:500]
        case _:
            assert_never(value)


def record_preview(context: MetadataSink, frame: pd.DataFrame, sample_rows: int) -> None:
    schema = TableSchema(
        columns=[TableColumn(name=str(name), type=str(dtype)) for name, dtype in frame.dtypes.items()]
    )
    rows = TypeAdapter(list[dict[str, JsonValue]]).validate_json(
        frame.head(sample_rows).to_json(
            orient="records",
            date_format="iso",
            default_handler=str,
        )
    )
    sample = [TableRecord({name: preview_value(value) for name, value in row.items()}) for row in rows]
    context.add_output_metadata(
        {
            "dagster/row_count": len(frame),
            "dagster/column_schema": MetadataValue.table_schema(schema),
            "sample": MetadataValue.table(sample, schema=schema),
        }
    )
