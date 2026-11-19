import re
from collections.abc import Iterator, Sequence
from typing import Final, Literal, assert_never

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from codebench_pipeline.source_transform_contract import (
    CanonicalSourceRow,
    SourceRow,
    TransformContext,
)

_FENCE: Final = re.compile(r"^[ \t]*(`{3,}|~{3,})[ \t]*([^\r\n]*?)[ \t]*$")
_SHELLS: Final = frozenset({"bash", "sh", "shell", "zsh"})
_SESSIONS: Final = frozenset({"", "console", "shell-session"})
_UTILITY_SEPARATOR: Final = re.compile(r"[ \t]")
_SPAN: Final = re.compile(r"(?<![\\`])(`+)(?!`)(.+?)(?<!`)\1(?!`)")
_USAGE: Final = re.compile(r"\b(?:run|use|using)[ \t]+$", re.IGNORECASE)
_WORD: Final = r"""(?:[A-Za-z0-9_./:=,@%+-]+|'[A-Za-z0-9_./:=,@%+ -]+'|"[A-Za-z0-9_./:=,@%+ -]+")"""
_ARGUMENTS: Final = re.compile(rf"(?:[ \t]+{_WORD})*")
_CONTINUED: Final = re.compile(r"[ \t]+\\$")
_JOIN: Final = re.compile(r"[ \t]+\\\r?\n[ \t]+")
_EXPANSION: Final = ("$(", "${")
_UNSUPPORTED: Final = (*_EXPANSION, "<(", ">(", "$'", "`")
_TEXT: Final = TypeAdapter(str)

ShellSpan = tuple[int, int, tuple[str, ...]]


class _ShellOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    context_characters: int = Field(ge=0, le=1000)


class _SingleLineOptions(_ShellOptions):
    utilities: tuple[
        Literal["aws", "docker", "docker-compose", "gcloud", "helm", "kubectl", "terraform"], ...
    ] = Field(min_length=1, strict=False)


class _LiteralOptions(_ShellOptions):
    utility: Literal["podman", "pulumi"]
    inline_usage: bool = False


_OPTIONS: Final = TypeAdapter(_SingleLineOptions | _LiteralOptions)


def _shell_line(line: str) -> tuple[int, bool, int]:
    indent = len(line) - len(line.lstrip(" \t"))
    prompt = line[indent:].startswith("$ ")
    return indent, prompt, indent + (2 if prompt else 0)


def _selection(description: str, indent: int, prompt: bool) -> tuple[str, ...]:
    changes = (description,)
    if indent:
        changes += ("removed source indentation",)
    if prompt:
        changes += ("removed shell prompt",)
    return changes


def _single_line_spans(block: str, language: str, options: _SingleLineOptions) -> Iterator[ShellSpan]:
    offset = 0
    continued = False
    for raw in block.splitlines(keepends=True):
        line = raw.rstrip("\r\n")
        indent, prompt, skip = _shell_line(line)
        command = line[skip:]
        continuation = (len(command) - len(command.rstrip("\\"))) % 2 == 1
        if (
            not continued
            and not continuation
            and (prompt or language in _SHELLS)
            and _UTILITY_SEPARATOR.split(command, maxsplit=1)[0] in options.utilities
            and not any(token in command for token in ("<<", "$(", "`"))
        ):
            yield (
                offset + skip,
                offset + len(line),
                _selection("selected shell example from a Markdown fence", indent, prompt),
            )
        continued = continuation
        offset += len(raw)


def _literal_spans(block: str, language: str, options: _LiteralOptions) -> Iterator[ShellSpan]:
    if "<<" in block:
        return
    offset = 0
    start = 0
    changes: tuple[str, ...] = ()
    continued = False
    quote = ""
    word = False
    for raw in block.splitlines(keepends=True):
        line = raw.rstrip("\r\n")
        indent, prompt, skip = _shell_line(line)
        part = line[skip:]
        was_quoted = bool(quote)
        escaped = False
        if continued or was_quoted or prompt or language in _SHELLS:
            shell = line if continued or was_quoted else part
            if not continued and not was_quoted:
                word = False
            for index, char in enumerate(shell):
                if escaped:
                    escaped = False
                    word = True
                elif quote == "'":
                    if char == quote:
                        quote = ""
                elif char == "\\":
                    escaped = True
                elif quote:
                    if char == quote:
                        quote = ""
                    elif char == "`" or shell.startswith(_EXPANSION, index):
                        return
                elif char == "#" and not word:
                    break
                elif shell.startswith(_UNSUPPORTED, index):
                    return
                elif char in "'\"":
                    quote = char
                    word = True
                else:
                    word = char not in " \t;&|()<>"
        ending = _CONTINUED.search(part)
        content = part[: ending.start()] if ending else part
        if was_quoted or quote:
            changes = ()
        elif continued:
            if not indent or prompt or not _ARGUMENTS.fullmatch(line[:skip] + content):
                changes = ()
        else:
            start = offset + skip
            changes = ()
            if (
                (prompt or language in _SHELLS)
                and content.startswith(options.utility)
                and _ARGUMENTS.fullmatch(content[len(options.utility) :])
            ):
                changes = _selection("selected literal shell example from a Markdown fence", indent, prompt)
        next_line = escaped
        if next_line and not ending:
            changes = ()
        if not next_line and changes:
            if continued:
                changes += ("joined literal shell continuation lines",)
            yield start, offset + len(line), changes
        continued = next_line
        offset += len(raw)


def _markdown_spans(text: str, options: _SingleLineOptions | _LiteralOptions) -> Iterator[ShellSpan]:
    delimiter = ""
    language = ""
    block_start = 0
    offset = 0
    frontmatter = False
    in_comment = False
    literal = isinstance(options, _LiteralOptions)
    for raw in text.splitlines(keepends=True):
        line = raw.rstrip("\r\n")
        marker = _FENCE.fullmatch(line)
        if literal and offset == 0 and line == "---":
            frontmatter = True
        elif frontmatter:
            frontmatter = line not in {"---", "..."}
        elif delimiter:
            if marker and marker[1][0] == delimiter[0] and len(marker[1]) >= len(delimiter) and not marker[2]:
                if language in _SHELLS or language in _SESSIONS:
                    block = text[block_start:offset]
                    match options:
                        case _SingleLineOptions():
                            spans = _single_line_spans(block, language, options)
                        case _LiteralOptions():
                            spans = _literal_spans(block, language, options)
                        case _:
                            assert_never(options)
                    for start, end, changes in spans:
                        yield block_start + start, block_start + end, changes
                delimiter = ""
        elif literal and (in_comment or "<!--" in line):
            in_comment = line.rfind("<!--") > line.rfind("-->") if "<!--" in line else "-->" not in line
        elif marker and not (marker[1].startswith("`") and "`" in marker[2]):
            delimiter, language = marker[1], marker[2].lower()
            block_start = offset + len(raw)
        elif (
            isinstance(options, _LiteralOptions)
            and options.inline_usage
            and not line.startswith(("    ", "\t", ">", "<", "{"))
        ):
            cursor = 0
            for span in _SPAN.finditer(line):
                code = span[2]
                if (
                    len(span[1]) == 1
                    and _USAGE.search(line[cursor : span.start()])
                    and code.startswith(options.utility)
                    and _ARGUMENTS.fullmatch(code[len(options.utility) :])
                ):
                    yield (
                        offset + span.start(2),
                        offset + span.end(2),
                        ("selected literal inline command explicitly introduced as usage",),
                    )
                cursor = span.end()
        offset += len(raw)


def transform(rows: Sequence[SourceRow], context: TransformContext) -> Iterator[CanonicalSourceRow]:
    """Extract command spans with the syntax selected by the source declaration."""
    options = _OPTIONS.validate_python(dict(context.options))
    for row in rows:
        text = _TEXT.validate_python(row.value, strict=True)
        for start, end, changes in _markdown_spans(text, options):
            yield CanonicalSourceRow(
                command=_JOIN.sub(" ", text[start:end]),
                context=text[max(start - options.context_characters, 0) : end + options.context_characters],
                source_position=f"{row.position}; characters {start}:{end}",
                transformations=list(changes),
            )
