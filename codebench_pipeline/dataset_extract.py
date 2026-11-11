import csv
import hashlib
import io
import re
import shlex
from collections.abc import Iterator

import pandera.pandas as pa
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    DocumentAdaptersTable,
    FilteredCommandsTable,
    SourceDocumentsTable,
    frame_records,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_config import ExtractionConfig, FilteringConfig
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.models import AdapterSpec
from codebench_pipeline.storage import IntegrityError


def text_records(text: str, adapter: AdapterSpec) -> list[tuple[str, str, str]]:
    if adapter.format in {"text", "markdown"}:
        return [("document", text, "")]
    rows: list[JsonValue]
    if adapter.format == "jsonl":
        rows = [TypeAdapter(JsonValue).validate_json(line) for line in text.splitlines() if line.strip()]
    elif adapter.format == "json":
        value = TypeAdapter(JsonValue).validate_json(text)
        rows = value if isinstance(value, list) else [value]
    else:
        rows = [TypeAdapter(JsonValue).validate_python(row) for row in csv.DictReader(io.StringIO(text))]
    records: list[tuple[str, str, str]] = []
    for number, row in enumerate(rows, 1):
        value = row
        if adapter.text_field != "$":
            for field in adapter.text_field.split("."):
                if not isinstance(value, dict) or field not in value:
                    raise IntegrityError(f"Missing adapter field: {adapter.text_field}")
                value = value[field]
        context = row.get(adapter.context_field, "") if isinstance(row, dict) else ""
        if not isinstance(value, str) or not isinstance(context, str):
            raise IntegrityError("Adapter must select textual command/context fields")
        records.append((f"record {number}; field {adapter.text_field}", value, context))
    return records


def text_commands(text: str) -> Iterator[tuple[str, int, int, list[str]]]:
    offset = 0
    fenced = False
    shell_fence = False
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("```"):
            if not fenced:
                shell_fence = stripped[3:].strip().lower() in {"", "bash", "sh", "shell", "console", "zsh"}
            fenced = not fenced
        elif fenced and shell_fence:
            if stripped and not stripped.startswith("#"):
                start = offset + line.index(stripped)
                command = re.sub(r"^\$\s+", "", stripped)
                yield command, start, start + len(stripped), ["markdown_fence"]
        elif not fenced:
            inline = list(re.finditer(r"`([^`\n]+)`", line))
            if inline:
                for match in inline:
                    yield match[1], offset + match.start(), offset + match.end(), ["inline_markdown"]
            elif stripped and not stripped.startswith(("#", "-", ">")):
                start = offset + line.index(stripped)
                command = re.sub(r"^\$\s+", "", stripped)
                yield command, start, start + len(stripped), []
        offset += len(line)


@pa.check_types
def extract_candidates(
    documents: DataFrame[SourceDocumentsTable],
    adapters: DataFrame[DocumentAdaptersTable],
    config: ExtractionConfig,
) -> DataFrame[CommandCandidatesTable]:
    mappings = {
        str(row["source_id"]): AdapterSpec.model_validate(
            {key: value for key, value in row.items() if key != "source_id"}
        )
        for row in frame_records(adapters)
    }
    records: list[dict[str, JsonValue]] = []
    for document in frame_records(documents):
        identifier = str(document["source_id"])
        for location, text, context in text_records(str(document["content"]), mappings[identifier]):
            for command, start, end, transformations in text_commands(text):
                try:
                    words = shlex.split(command)
                except ValueError:
                    words = command.split(maxsplit=1)
                if not words:
                    continue
                position = f"{location}; characters {start}:{end}"
                key = hashlib.sha256((identifier + "\n" + position + "\n" + command).encode()).hexdigest()
                candidate = CandidateRecord(
                    candidate_id=key,
                    command=command,
                    util=words[0].rsplit("/", 1)[-1],
                    context=context
                    + "\n"
                    + text[max(0, start - config.context_characters) : end + config.context_characters],
                    source_id=identifier,
                    source_url=str(document["source_url"]),
                    source_position=position,
                    transformations=transformations,
                    risk_candidates=[],
                )
                records.append(candidate.model_dump(mode="json"))
    return validated_frame(records, CommandCandidatesTable)


@pa.check_types
def filter_candidates(
    raw: DataFrame[CommandCandidatesTable], rules: CategoryRules, config: FilteringConfig
) -> DataFrame[FilteredCommandsTable]:
    rows: list[dict[str, JsonValue]] = []
    for candidate in model_records(raw, CandidateRecord):
        reason = ""
        risks = rules.matching_risks(candidate.command)
        if not risks:
            reason = "outside_configured_command_families"
        elif len(candidate.command) > config.maximum_command_characters or "\x00" in candidate.command:
            reason = "oversized_or_control_character"
        elif re.search(r"\{\{|\}\}|(?:^|\s)\.\.\.(?:\s|$)|…", candidate.command):
            reason = "unresolved_template"
        elif config.quarantine_dynamic_shell and any(
            token in candidate.command for token in ("$(", "`", "<<")
        ):
            reason = "opaque_dynamic_shell_requires_review"
        else:
            try:
                shlex.split(candidate.command)
            except ValueError:
                reason = "unbalanced_shell_quotes"
        rows.append(
            {
                **candidate.model_dump(mode="json"),
                "risk_candidates": [risk for risk in risks],
                "filter_decision": "needs_review" if reason else "eligible",
                "filter_reason": reason,
            }
        )
    return validated_frame(rows, FilteredCommandsTable)
