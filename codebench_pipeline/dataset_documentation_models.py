from __future__ import annotations

import shlex
from dataclasses import dataclass

from pandera.typing import DataFrame
from pydantic import Field, JsonValue

from codebench_pipeline.category_rules import StrictModel, Text
from codebench_pipeline.dataframe_schemas import FilteredCommandsTable, frame_records
from codebench_pipeline.models import SemanticSnippet
from scripts.command_filter import command_words


class SnippetProposal(StrictModel):
    utility: Text
    subcommands: list[Text] = Field(min_length=1)
    url: str = Field(pattern=r"^https?://")
    snippet: Text


class SnippetProposals(StrictModel):
    snippets: list[SnippetProposal]


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


def eligible_operations(commands: DataFrame[FilteredCommandsTable]) -> list[Operation]:
    operations: list[Operation] = []
    for row in frame_records(commands):
        if row["filter_decision"] != "eligible":
            continue
        utility = str(row["util"])
        identifier = str(row["candidate_id"])
        operations.extend(command_operations(identifier, utility, str(row["command"])))
    return operations


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


def coverage(operations: list[Operation], snippets: list[SemanticSnippet]) -> list[dict[str, JsonValue]]:
    rows: list[dict[str, JsonValue]] = []
    for operation in operations:
        matches: list[JsonValue] = [
            doc.url
            for doc in snippets
            if any(operation.matches(doc.utility, subcommand) for subcommand in doc.subcommands)
        ]
        reason = ""
        if not matches:
            reason = (
                "utility_not_documented"
                if not any(doc.utility == operation.utility for doc in snippets)
                else "subcommand_not_documented"
            )
        words: list[JsonValue] = [word for word in operation.words]
        rows.append(
            {
                "candidate_id": operation.candidate_id,
                "utility": operation.utility,
                "command_words": words,
                "documentation_urls": matches,
                "status": "covered" if matches else "gap",
                "reason": reason,
            }
        )
    return rows
