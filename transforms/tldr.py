import re
from collections.abc import Iterable, Sequence

from pydantic import TypeAdapter

from codebench_pipeline.source_transform_contract import (
    CanonicalSourceRow,
    SourceRow,
    TransformContext,
)


def transform(
    rows: Sequence[SourceRow], context: TransformContext
) -> Iterable[CanonicalSourceRow]:
    radius = TypeAdapter(int).validate_python(context.options.get("context_characters", 100), strict=True)
    for row in rows:
        text = TypeAdapter(str).validate_python(row.value, strict=True)
        for match in re.finditer(r"(?m)^`([^`\n]+)`[ \t\r]*$", text):
            start, end = match.span(1)
            yield CanonicalSourceRow(
                command=match[1],
                context=text[max(0, start - radius):end + radius],
                source_position=f"{row.position}; characters {start}:{end}",
                transformations=["removed enclosing tldr example backticks"],
            )
