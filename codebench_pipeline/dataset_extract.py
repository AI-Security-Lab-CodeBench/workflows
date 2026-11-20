import json
import re
import shlex

import pandera.pandas as pa
from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    FilteredCommandsTable,
    frame_records,
    validated_frame,
)
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.labeling_stage_settings import RiskRoute, matching_risks


def unresolved_template(command: str) -> bool:
    if re.search(r"\{\{|<[A-Za-z_][A-Za-z0-9_.|:/-]*>|(?:^|\s)\.\.\.(?:\s|$)|…", command):
        return True
    if "}}" not in command:
        return False
    try:
        words = shlex.split(command)
    except ValueError:
        return True
    structural_closers = 0
    for word in words:
        if "}}" not in word:
            continue
        word = re.sub(r"^--?[A-Za-z0-9][A-Za-z0-9_-]*=", "", word, count=1)
        if not word.startswith(("{", "[")):
            return True
        try:
            json.loads(word)
        except (ValueError, RecursionError):
            return True
        if any("}}" in string for string in re.findall(r'"(?:[^"\\]|\\.)*"', word)):
            return True
        structural_closers += word.count("}}")
    return structural_closers != command.count("}}")


@pa.check_types
def filter_candidates(
    raw: DataFrame[CommandCandidatesTable], routes: tuple[RiskRoute, ...], config: FilteringConfig
) -> DataFrame[FilteredCommandsTable]:
    rows: list[dict[str, JsonValue]] = []
    for row in frame_records(raw):
        candidate = CandidateRecord.model_validate({name: row[name] for name in CandidateRecord.model_fields})
        reason = ""
        risks = matching_risks(candidate.command, routes)
        if not risks:
            reason = "outside_configured_command_families"
        elif len(candidate.command) > config.maximum_command_characters or "\x00" in candidate.command:
            reason = "oversized_or_control_character"
        elif unresolved_template(candidate.command):
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
                **row,
                "risk_candidates": [risk for risk in risks],
                "filter_decision": "needs_review" if reason else "eligible",
                "filter_reason": reason,
            }
        )
    return validated_frame(rows, FilteredCommandsTable)
