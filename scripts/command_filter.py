"""Inert, conservative Bash candidate extraction; eligibility is not a safety label."""

from __future__ import annotations

import argparse
import json
import re
import shlex
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Final, Literal

if __package__:
    from . import command_families
    from .category_config import Taxonomy, load_taxonomy
else:
    import command_families
    from category_config import Taxonomy, load_taxonomy

DEFAULT_TAXONOMY: Final = load_taxonomy()
WRAPPERS: Final = frozenset("sudo env command time xargs".split())
SHELL_WORDS: Final = frozenset(
    "echo printf cd cat true false bash sh eval exec sg if then for while do done fi "
    "read grep awk cut sort head test export".split()
)
PROMPT: Final = re.compile(r"^(?:[\w.-]+@[\w.-]+:[^\n$#]*[$#]|[$])\s+")
MARKDOWN: Final = re.compile(r"^(?:>\s*|[-*+]\s+|\d+[.)]\s+)")


@dataclass(frozen=True, slots=True)
class Extracted:
    command: str
    context: str
    start: int
    end: int
    risk_candidates: tuple[str, ...]
    transformations: tuple[str, ...]
    status: Literal["eligible", "needs_review"]
    reason: str


def _shell(text: str) -> tuple[list[str], str, str, bool]:
    parts: list[str] = []
    clean: list[str] = []
    part: list[str] = []
    quote, issue, escaped = "", "", False
    index = 0
    while index < len(text):
        char = text[index]
        was_escaped = escaped
        if escaped:
            escaped = False
        elif char == "\\" and quote != "'":
            escaped = True
        elif char == quote:
            quote = ""
        elif char in "'\"" and not quote:
            quote = char
        elif quote != "'" and (char == "`" or text.startswith(("$(", "${"), index)):
            issue = "opaque_expansion"
        if not quote and not escaped and not was_escaped:
            if char == "#" and (index == 0 or text[index - 1].isspace()):
                stop = text.find("\n", index)
                index = len(text) if stop < 0 else stop
                continue
            if char in "(){}<>":
                issue = issue or "complex_shell_or_redirection"
            if text.startswith("<<", index):
                issue = "heredoc"
            if char in ";|&\n":
                parts.append("".join(part))
                part = []
                clean.append(char)
                index += 1
                continue
        part.append(char)
        clean.append(char)
        index += 1
    parts.append("".join(part))
    cleaned = "".join(clean).strip()
    incomplete = bool(quote or escaped or cleaned.endswith(("&&", "||", "|", "\\")))
    return parts, cleaned, issue, incomplete


def _tokens(text: str) -> list[str]:
    return shlex.split(text.replace("\\\n", ""), comments=False, posix=True)


def command_words(tokens: list[str]) -> list[str]:
    words: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.partition("=")[0] in command_families.VALUE_FLAGS and "=" not in token:
            index += 2
            continue
        if token.startswith("-") or ("=" in token and not words):
            index += 1
            continue
        words.append(token)
        index += 1
    while words and (words[0] in WRAPPERS | {"if", "then", "do", "while"} or "=" in words[0]):
        words.pop(0)
    return words


def _risks(tokens: list[str], taxonomy: Taxonomy) -> tuple[str, ...]:
    words = command_words(tokens)
    if not words:
        return ()
    utility, *args = words
    utility = utility.rsplit("/", 1)[-1]
    if "--help" in tokens:
        return ()
    return taxonomy.matching_shell(utility, args)


def classify_command(command: str, taxonomy: Taxonomy = DEFAULT_TAXONOMY) -> tuple[str, ...]:
    """Return sorted family candidates, never allow/deny/ask or severity labels."""
    parts, _, _, _ = _shell(command)
    risks: set[str] = set()
    for part in parts:
        try:
            risks.update(_risks(_tokens(part), taxonomy))
        except ValueError:
            continue
    return tuple(sorted(risks))


def assess_command(command: str, taxonomy: Taxonomy = DEFAULT_TAXONOMY) -> Extracted:
    """Assess an entire structured command verbatim, with command-local offsets."""
    parts, _, issue, incomplete = _shell(command)
    try:
        unknown = any(part.strip() and not _risks(_tokens(part), taxonomy) for part in parts)
    except ValueError:
        unknown = True
    reason = ("malformed_or_incomplete_shell" if incomplete else issue) or (
        "unsupported_command_in_composition"
        if unknown or not classify_command(command, taxonomy)
        else "recognized_family"
    )
    return Extracted(
        command,
        command,
        0,
        len(command),
        classify_command(command, taxonomy),
        (),
        "eligible" if reason == "recognized_family" else "needs_review",
        reason,
    )


def _regions(text: str) -> Iterator[tuple[int, str, bool]]:
    fence, allowed, offset, start = "", True, 0, 0
    lines: list[str] = []
    for line in text.splitlines(keepends=True):
        match = re.match(r"^\s*(`{3,}|~{3,})([^\n]*)", line)
        if match and (not fence or match[1][0] == fence[0] and len(match[1]) >= len(fence)):
            if lines:
                yield start, "".join(lines), bool(fence)
                lines = []
            if fence:
                fence, allowed = "", True
            else:
                fence = match[1]
                allowed = match[2].strip().lower() in {"", "bash", "sh", "shell", "console", "shell-session"}
            start = offset + len(line)
        elif allowed:
            if not lines:
                start = offset
            lines.append(line)
        offset += len(line)
    if lines:
        yield start, "".join(lines), bool(fence)


def extract_commands(
    text: str, *, shortlist: bool = True, taxonomy: Taxonomy = DEFAULT_TAXONOMY
) -> Iterator[Extracted]:
    """Yield source-located command candidates without executing or expanding text."""
    for base, body, fenced in _regions(text):
        lines = body.splitlines(keepends=True)
        offset, index = base, 0
        while index < len(lines):
            raw, start = lines[index], offset
            offset += len(lines[index])
            index += 1
            command = raw.strip()
            changes = ["markdown_fence"] if fenced else []
            stripped = MARKDOWN.sub("", command)
            if stripped != command:
                changes.append("markdown_prefix")
            command = stripped
            stripped = PROMPT.sub("", command)
            if stripped != command:
                changes.append("shell_prompt")
            command = stripped
            if command.startswith("`") and command.endswith("`") and len(command) > 1:
                command = command[1:-1]
                changes.append("inline_markdown")
            _, _, issue, incomplete = _shell(command)
            complex_block = command.startswith(("if ", "for ", "while ", "{ ", "( "))
            while index < len(lines) and (incomplete or issue == "heredoc" or complex_block):
                continuation = lines[index]
                raw += continuation
                offset += len(continuation)
                index += 1
                command += "\n" + PROMPT.sub("", continuation.strip())
                _, _, issue, incomplete = _shell(command)
            parts, cleaned, issue, incomplete = _shell(command)
            try:
                tokens = _tokens(parts[0])
            except ValueError:
                tokens = command.split(maxsplit=1)[:1]
            words = command_words(tokens)
            first = words[0].rsplit("/", 1)[-1] if words else ""
            risks = classify_command(command, taxonomy)
            if not first or first.startswith("#"):
                continue
            utilities = {tool for risk in taxonomy.risks for tool, _ in risk.shell_routes}
            if (shortlist or not fenced) and not risks and first not in utilities | SHELL_WORDS | WRAPPERS:
                continue
            if (
                shortlist
                and not risks
                and not issue
                and not incomplete
                and first not in {"bash", "sh", "eval", "exec", "sg"}
            ):
                continue
            if cleaned != command:
                changes.append("comments_or_outer_whitespace")
            if "\\\n" in cleaned:
                cleaned = cleaned.replace("\\\n", "")
                changes.append("line_continuation")
            assessed = assess_command(cleaned, taxonomy)
            end = start + len(raw.rstrip("\r\n"))
            yield Extracted(
                cleaned,
                text[max(0, start - 100) : min(len(text), end + 100)],
                start,
                end,
                risks,
                tuple(changes),
                assessed.status,
                assessed.reason,
            )


def main() -> None:
    """Read UTF-8 text and write one JSON object per extracted candidate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--category-config", type=Path)
    args = parser.parse_args()
    text = args.input.read_text(encoding="utf-8")
    taxonomy = load_taxonomy(args.category_config) if args.category_config else DEFAULT_TAXONOMY
    with args.output.open("w", encoding="utf-8", newline="\n") as output:
        for record in extract_commands(text, taxonomy=taxonomy):
            output.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
