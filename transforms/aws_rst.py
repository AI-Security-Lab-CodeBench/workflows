import shlex
from collections.abc import Iterable, Sequence

from pydantic import TypeAdapter

from codebench_pipeline.source_transform_contract import (
    CanonicalSourceRow,
    SourceRow,
    TransformContext,
)


class AwsExamples:
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
                original = lines[index]
                indentation = len(original) - len(original.lstrip())
                command = original[indentation:].rstrip("\r\n")
                index += 1
                if indentation < 4:
                    continue
                changes = ["removed RST code-block indentation"]
                if command.startswith("$ "):
                    command = command[2:]
                    changes.append("removed shell prompt")
                if not command.startswith("aws "):
                    continue
                while index < len(lines):
                    try:
                        shlex.split(command)
                    except ValueError:
                        if not lines[index].strip() or not lines[index].startswith(" " * indentation):
                            break
                        command += "\n" + lines[index][indentation:].rstrip("\r\n")
                        index += 1
                    else:
                        break
                start, end = offsets[first], offsets[index]
                if "\n" in command and "\r\n" in text[start:end]:
                    changes.append("normalized RST example line endings to LF")
                yield CanonicalSourceRow(
                    command=command,
                    context=text[max(0, start - radius):end + radius],
                    source_position=f"{row.position}; lines {first + 1}:{index}",
                    transformations=changes,
                )
