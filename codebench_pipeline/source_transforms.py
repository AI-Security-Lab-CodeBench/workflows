import csv
import hashlib
import io
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import assert_never

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.source_transform_contract import (
    BatchTransform,
    CanonicalSourceRow,
    SourceRow,
    SourceTransformConfig,
    TransformContext,
    TransformFunction,
)
from codebench_pipeline.storage import IntegrityError


def transformation_bytes(config: SourceTransformConfig) -> bytes:
    path = Path(config.module_path)
    if path.suffix != ".py":
        raise IntegrityError("Trusted transformations must reference a Python source file")
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != config.sha256:
        raise IntegrityError("Trusted transformation code differs from its manifest hash")
    return body


def source_rows(content: str, format_name: str) -> tuple[SourceRow, ...]:
    if format_name == "jsonl":
        return tuple(
            SourceRow(f"line {number}", TypeAdapter(JsonValue).validate_json(line))
            for number, line in enumerate(content.splitlines(), 1)
            if line.strip()
        )
    if format_name == "json":
        value = TypeAdapter(JsonValue).validate_json(content)
        values = value if isinstance(value, list) else [value]
        return tuple(SourceRow(f"record {number}", value) for number, value in enumerate(values, 1))
    if format_name == "csv":
        return tuple(
            SourceRow(f"record {number}", TypeAdapter(JsonValue).validate_python(row))
            for number, row in enumerate(csv.DictReader(io.StringIO(content)), 1)
        )
    return (SourceRow("document", content),)


def transformed_rows(
    config: SourceTransformConfig,
    rows: tuple[SourceRow, ...],
    context: TransformContext,
) -> Iterator[CanonicalSourceRow]:
    body = transformation_bytes(config)
    name = f"_codebench_trusted_{config.sha256}"
    module = ModuleType(name)
    module.__file__ = config.module_path
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        exec(compile(body, config.module_path, "exec"), module.__dict__)
        entry = getattr(module, config.entrypoint, None)
        transform: TransformFunction
        match config.kind:
            case "function":
                if not isinstance(entry, TransformFunction):
                    raise IntegrityError("Transformation entrypoint is not callable")
                transform = entry
            case "class":
                if not isinstance(entry, type):
                    raise IntegrityError("Transformation entrypoint is not a class")
                instance = entry()
                if not isinstance(instance, BatchTransform):
                    raise IntegrityError("Transformation class lacks the batch transform method")
                transform = instance.transform
            case _:
                assert_never(config.kind)
        for start in range(0, len(rows), config.batch_size):
            for row in transform(rows[start : start + config.batch_size], context):
                yield CanonicalSourceRow.model_validate(row)
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
