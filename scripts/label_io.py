import hashlib
import json
from datetime import datetime
from pathlib import Path

from validate_dataset import DatasetError, contains_secret, present, unique_object


def require(condition, message):
    if not condition:
        raise DatasetError(message)


def digest(value):
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=unique_object,
                          parse_constant=lambda value: require(False, f"Nonfinite JSON number: {value}"))
    except (ValueError, RecursionError) as error:
        raise DatasetError(f"Invalid JSON: {path}") from error


def timestamp(value, label, date_only=False):
    require(present(value), f"{label}: missing timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise DatasetError(f"{label}: invalid ISO timestamp") from error
    require(date_only or ("T" in value and parsed.tzinfo is not None),
            f"{label}: expected timezone-aware ISO datetime")


def indexed(records, key, fields):
    result = {}
    for record in records:
        require(isinstance(record, dict) and set(record) == set(fields), f"{key}: incorrect schema")
        require(present(record[key]) and record[key] not in result, f"{key}: missing or duplicate ID")
        require(not contains_secret(record), f"{key}: possible secret indicator; value withheld")
        result[record[key]] = record
    return result
