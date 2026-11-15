from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import assert_never

from pydantic import JsonValue

from codebench_pipeline.human_review import ReviewDecision
from codebench_pipeline.table_query import cell_text


def json_type(value: JsonValue) -> str:
    match value:
        case None:
            return "null"
        case bool():
            return "boolean"
        case int() | float():
            return "number"
        case str():
            return "string"
        case list():
            return "array"
        case dict():
            return "object"
        case _:
            assert_never(value)


def decision_values(decision: ReviewDecision) -> dict[str, JsonValue]:
    return {"human_decision": decision.decision, "human_reviewer": decision.reviewer,
            "human_notes": decision.notes, "human_edits": decision.edits}


def number_key(text: str) -> str | None:
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    if not value.is_finite():
        return None
    if value.is_zero():
        return "0"
    parts = value.as_tuple()
    digits = list(parts.digits)
    exponent = int(parts.exponent)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    return f"{parts.sign}:{''.join(str(digit) for digit in digits)}:{exponent}"


def row_cells(row: dict[str, JsonValue]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for column, value in row.items():
        text = cell_text(value)
        number: int | float | None = None
        numeric_key: str | None = None
        match value:
            case None:
                kind = 4
                sort = ""
            case bool():
                kind = 0
                number = int(value)
                sort = ""
            case int() | float():
                kind = 1
                number = value
                numeric_key = number_key(str(value))
                sort = ""
            case str():
                kind = 2
                sort = value.casefold()
            case list() | dict():
                kind = 3
                sort = text
            case _:
                assert_never(value)
        result[column] = {"text": text, "fold": text.casefold(), "kind": kind, "number": number,
                          "number_key": numeric_key, "sort": sort}
    return result


def same_json_type(left: JsonValue, right: JsonValue) -> bool:
    match left, right:
        case (None, None) | (bool(), bool()) | (str(), str()) | (list(), list()) | (dict(), dict()):
            return True
        case (bool(), _) | (_, bool()):
            return False
        case (int() | float(), int() | float()):
            return True
        case _:
            return False
