import csv
import io
import json
from dataclasses import dataclass

from pydantic import JsonValue


def csv_text(rows: list[dict[str, str]], fields: tuple[str, ...]) -> str:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def json_text(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


@dataclass(frozen=True, slots=True)
class Publication:
    files: dict[str, str]
    statistics: dict[str, JsonValue]
    commands: list[dict[str, str]]
    pending: list[dict[str, str]]
    eligible_ids: tuple[str, ...]
    human_review_ids: tuple[str, ...]
