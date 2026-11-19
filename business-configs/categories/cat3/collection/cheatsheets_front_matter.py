from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Final

from pydantic import TypeAdapter
from yaml import BaseLoader, MappingNode, YAMLError

from codebench_pipeline.source_transform_contract import (
    CanonicalSourceRow,
    SourceRow,
    SourceTransformConfig,
    TransformContext,
)
from codebench_pipeline.source_transforms import transformed_rows
from codebench_pipeline.storage import IntegrityError

_LEGACY_PATH: Final = Path(__file__).resolve().parents[4] / "transforms/cheatsheets.py"
_LEGACY_DIGEST: Final = "7580c09e876750b9f3d2affd042559f2c0e9744c8c8278f109f54dc3d90ad371"
_TEXT: Final = TypeAdapter(str)


def _front_matter_lines(lines: Sequence[str]) -> int:
    if not lines or lines[0].lstrip("\ufeff").rstrip("\r\n \t") != "---":
        return 0
    for index, line in enumerate(lines[1:], 1):
        if line.rstrip("\r\n \t") not in {"---", "..."}:
            continue
        try:
            node = BaseLoader("".join(lines[1:index])).get_single_node()
        except (YAMLError, RecursionError):
            return 0
        return index + 1 if isinstance(node, MappingNode) and node.value else 0
    return 0


def transform(rows: Sequence[SourceRow], context: TransformContext) -> Iterator[CanonicalSourceRow]:
    """Strip confirmed leading YAML metadata before the pinned literal cheatsheet parser."""
    if _LEGACY_PATH.resolve(strict=True) != _LEGACY_PATH:
        raise IntegrityError("Legacy cheatsheet ETL escaped its pinned deployment path")
    legacy = SourceTransformConfig(
        module_path=str(_LEGACY_PATH),
        sha256=_LEGACY_DIGEST,
        entrypoint="Cheatsheet",
        kind="class",
        trusted=True,
    )
    for row in rows:
        lines = _TEXT.validate_python(row.value, strict=True).splitlines(keepends=True)
        skipped = _front_matter_lines(lines)
        body = SourceRow(row.position, "".join(lines[skipped:]))
        for candidate in transformed_rows(legacy, (body,), context):
            if not skipped:
                yield candidate
                continue
            position, _, span = candidate.source_position.rpartition("; lines ")
            first, last = map(int, span.split(":"))
            yield candidate.model_copy(
                update={
                    "source_position": f"{position}; lines {first + skipped}:{last + skipped}",
                    "transformations": ["excluded leading YAML front matter", *candidate.transformations],
                }
            )
