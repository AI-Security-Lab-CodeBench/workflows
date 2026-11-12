from collections.abc import Iterable, Sequence

from pydantic import TypeAdapter

from codebench_pipeline.source_transform_contract import (
    CanonicalSourceRow,
    SourceRow,
    TransformContext,
)


class Cheatsheet:
    def transform(
        self, rows: Sequence[SourceRow], context: TransformContext
    ) -> Iterable[CanonicalSourceRow]:
        radius = TypeAdapter(int).validate_python(
            context.options.get("context_characters", 100), strict=True
        )
        for row in rows:
            text = TypeAdapter(str).validate_python(row.value, strict=True)
            lines = text.splitlines(keepends=True)
            offsets = [0]
            for line in lines:
                offsets.append(offsets[-1] + len(line))
            index = 0
            while index < len(lines):
                first = index
                command = lines[index].lstrip().rstrip("\r\n")
                index += 1
                if not command or command.startswith("#"):
                    continue
                changes = ["removed surrounding source indentation"]
                if command.startswith("$ "):
                    command = command[2:]
                    changes.append("removed shell prompt")
                while (
                    (len(command) - len(command.rstrip("\\"))) % 2 == 1
                    and index < len(lines)
                ):
                    command += "\n" + lines[index].rstrip("\r\n")
                    index += 1
                start, end = offsets[first], offsets[index]
                if "\n" in command and "\r\n" in text[start:end]:
                    changes.append("normalized example line endings to LF")
                yield CanonicalSourceRow(
                    command=command,
                    context=text[max(0, start - radius):end + radius],
                    source_position=f"{row.position}; lines {first + 1}:{index}",
                    transformations=changes,
                )
