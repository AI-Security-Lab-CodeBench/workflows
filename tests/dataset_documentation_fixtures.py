from pandera.typing import DataFrame
from pydantic import JsonValue
from test_dataset_rules import make_candidate

from codebench_pipeline.dataframe_schemas import FilteredCommandsTable, validated_frame
from codebench_pipeline.dataset_seed_config import DocumentationSeed


def commands(*texts: str, decision: str = "eligible") -> DataFrame[FilteredCommandsTable]:
    rows: list[dict[str, JsonValue]] = []
    for index, text in enumerate(texts):
        row = make_candidate(str(index)).model_dump(mode="json")
        row.update(command=text, filter_decision=decision, filter_reason="")
        rows.append(row)
    return validated_frame(rows, FilteredCommandsTable)


def seed() -> DocumentationSeed:
    return DocumentationSeed(
        utility="probe",
        subcommands=["inspect"],
        url="https://example.org/docs",
        retrieved_at="2026-11-10",
        snippet="probe inspect reads metadata.",
        version_limits="Unverified releases.",
    )
