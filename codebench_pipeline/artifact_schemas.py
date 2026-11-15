from io import BytesIO
from typing import Annotated, Literal, get_args, get_origin, get_type_hints

import pandera.pandas as pa
import pyarrow as arrow
import pyarrow.parquet as parquet
from dagster import AssetsDefinition, Definitions, OpDefinition, Output
from dagster._core.definitions.decorators.op_decorator import DecoratedOpFunction
from jsonschema import Draft202012Validator
from pandera.typing import DataFrame, Series
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from codebench_pipeline.table_query import QueryModel


class ArtifactSchema(QueryModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", serialize_by_alias=True)

    source: Literal["declared", "storage"]
    schema_: dict[str, JsonValue] = Field(alias="schema")


def declared_artifact_schemas(definitions: Definitions) -> dict[tuple[str, ...], dict[str, JsonValue]]:
    schemas: dict[tuple[str, ...], dict[str, JsonValue]] = {}
    for asset in definitions.assets or ():
        if not isinstance(asset, AssetsDefinition) or len(asset.keys) != 1:
            continue
        node = asset.node_def
        if not isinstance(node, OpDefinition) or not isinstance(node.compute_fn, DecoratedOpFunction):
            continue
        annotation = get_type_hints(node.compute_fn.decorated_fn).get("return")
        while get_origin(annotation) in (Annotated, Output):
            annotation = get_args(annotation)[0]
        if get_origin(annotation) is DataFrame:
            annotation = get_args(annotation)[0]
        key = tuple(next(iter(asset.keys)).path)
        if isinstance(annotation, type) and issubclass(annotation, pa.DataFrameModel):
            schemas[key] = declared_table_schema(annotation)
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
            schemas[key] = TypeAdapter(dict[str, JsonValue]).validate_python(annotation.model_json_schema())
    return schemas


def declared_table_schema(model: type[pa.DataFrameModel]) -> dict[str, JsonValue]:
    table = model.to_schema()
    annotations = {
        str(getattr(model, name)): get_args(annotation)[0]
        for name, annotation in get_type_hints(model).items()
        if get_origin(annotation) is Series
    }
    properties: dict[str, JsonValue] = {}
    for name, column in table.columns.items():
        field = TypeAdapter(dict[str, JsonValue]).validate_python(
            TypeAdapter(annotations[name]).json_schema()
        )
        for check in column.checks:
            statistics = check.statistics
            match check.name:
                case "isin":
                    field["enum"] = list(statistics["allowed_values"])
                case "equal_to":
                    field["const"] = statistics["value"]
                case "greater_than_or_equal_to":
                    field["minimum"] = statistics["min_value"]
                case "str_matches":
                    field["pattern"] = statistics["pattern"]
        if column.description:
            field["description"] = column.description
        if column.unique:
            field["x-column-unique"] = True
        if column.nullable:
            field = {"anyOf": [field, {"type": "null"}]}
        properties[name] = field
    return {
        "title": model.__name__,
        "type": "object",
        "properties": properties,
        "required": [name for name, column in table.columns.items() if column.required],
        "additionalProperties": not table.strict,
    }


def storage_field_schema(field: arrow.Field) -> dict[str, JsonValue]:
    dtype = field.type
    schema: dict[str, JsonValue] = {}
    if arrow.types.is_dictionary(dtype):
        return storage_field_schema(arrow.field(field.name, dtype.value_type, field.nullable))
    if arrow.types.is_boolean(dtype):
        schema = {"type": "boolean"}
    elif arrow.types.is_integer(dtype):
        schema = {"type": "integer"}
    elif arrow.types.is_floating(dtype) or arrow.types.is_decimal(dtype):
        schema = {"type": "number"}
    elif arrow.types.is_string(dtype) or arrow.types.is_large_string(dtype):
        schema = {"type": "string"}
    elif arrow.types.is_null(dtype):
        return {"type": "null"}
    elif arrow.types.is_list(dtype) or arrow.types.is_large_list(dtype) or arrow.types.is_fixed_size_list(dtype):
        schema = {"type": "array", "items": storage_field_schema(dtype.value_field)}
        if arrow.types.is_fixed_size_list(dtype):
            schema.update(minItems=dtype.list_size, maxItems=dtype.list_size)
    elif arrow.types.is_struct(dtype):
        schema = {
            "type": "object",
            "properties": {child.name: storage_field_schema(child) for child in dtype},
            "required": [child.name for child in dtype],
            "additionalProperties": False,
        }
    elif arrow.types.is_temporal(dtype):
        schema = {"type": "number"}
    if field.nullable and schema:
        return {"anyOf": [schema, {"type": "null"}]}
    return schema


def table_artifact_schema(
    payload: bytes, columns: tuple[str, ...], declared: dict[str, JsonValue] | None
) -> ArtifactSchema:
    properties = declared.get("properties") if declared is not None else None
    if declared is not None and isinstance(properties, dict) and set(columns) == set(properties):
        return ArtifactSchema(source="declared", schema=declared)
    stored = parquet.read_schema(BytesIO(payload))
    return ArtifactSchema(
        source="storage",
        schema={
            "title": "Stored artifact row",
            "type": "object",
            "properties": {name: storage_field_schema(stored.field(name)) for name in columns},
            "required": list(columns),
            "additionalProperties": False,
        },
    )


def rules_artifact_schema(value: JsonValue, declared: dict[str, JsonValue] | None) -> ArtifactSchema:
    if declared is not None and Draft202012Validator(declared).is_valid(value):
        return ArtifactSchema(source="declared", schema=declared)
    if isinstance(value, dict):
        return ArtifactSchema(
            source="storage",
            schema={
                "title": "Stored JSON artifact",
                "type": "object",
                "properties": {name: {} for name in value},
                "required": list(value),
                "additionalProperties": False,
            },
        )
    return ArtifactSchema(source="storage", schema={"title": "Stored JSON artifact"})
