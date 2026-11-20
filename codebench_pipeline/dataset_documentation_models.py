from __future__ import annotations

import shlex
from dataclasses import dataclass

from scripts.command_filter import command_words


@dataclass(frozen=True, slots=True)
class Operation:
    candidate_id: str
    utility: str
    words: tuple[str, ...]
    arguments: tuple[str, ...] = ()
    recognized: bool = True

    def matches(self, utility: str, subcommand: str) -> bool:
        if subcommand == "<default>":
            return self.utility == utility and self.recognized and not self.words and not self.arguments
        parts = tuple(subcommand.split())
        if len(parts) == 1 and parts[0].startswith("-"):
            return self.utility == utility and any(
                token.split("=", 1)[0] == parts[0] for token in self.arguments
            )
        return bool(parts) and self.utility == utility and self.words[: len(parts)] == parts


def command_operations(identifier: str, utility: str, command: str) -> list[Operation]:
    operations: list[Operation] = []
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        lexer.commenters = ""
        segments: list[list[str]] = [[]]
        for token in lexer:
            if token and all(char in ";&|" for char in token):
                segments.append([])
            else:
                segments[-1].append(token)
        for segment in segments:
            words = command_words(segment)
            if words and words[0].rsplit("/", 1)[-1] == utility:
                operations.append(Operation(identifier, utility, tuple(words[1:]), tuple(segment[1:])))
    except ValueError:
        return [Operation(identifier, utility, (), recognized=False)]
    return operations or [Operation(identifier, utility, (), recognized=False)]
