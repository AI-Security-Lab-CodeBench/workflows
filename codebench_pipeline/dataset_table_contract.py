from typing import Final

import pandera.polars as pa

from codebench_pipeline.category_rules import CategoryRules

FIELDS: Final = (
    "command",
    "description",
    "util",
    "severity",
    "category",
    "risk",
    "source",
    "seed",
    "class",
    "human_reviewed",
)
CONFIDENCES: Final = tuple(f"{key}_confidence" for key in ("category", "risk", "severity", "class"))
REVIEW_FIELDS: Final = (
    *FIELDS,
    *"candidate_id csv_record context source_id source_url source_position transformations documentation_urls".split(),
    *CONFIDENCES,
    "review_confidence",
    *"label_decision review_verdict label_rationale review_rationale exclusion_reason".split(),
    *"evidence_jsonl evidence_record".split(),
)


def nonblank(value: str) -> bool:
    return bool(value.strip())


def one_of(values: list[str]) -> pa.Check:
    def included(value: str) -> bool:
        return value in values

    return pa.Check(included, element_wise=True)


def table_schema(rules: CategoryRules) -> pa.DataFrameSchema:
    values: dict[str, list[str]] = {
        "category": [rules.canonical],
        "risk": [risk.canonical for risk in rules.risks],
        "class": [rule.name for rule in rules.classes],
        "severity": [rule.name for rule in rules.severities],
        "seed": ["-"],
        "human_reviewed": ["true", "false"],
    }
    return pa.DataFrameSchema(
        {
            field: pa.Column(
                str,
                [pa.Check(nonblank, element_wise=True)]
                + ([one_of(values[field])] if field in values else []),
                coerce=field == "human_reviewed",
            )
            for field in FIELDS
        },
        strict=True,
        ordered=True,
    )
