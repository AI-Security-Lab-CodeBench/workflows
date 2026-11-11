import json
import re
import shlex

if __package__:
    from .category_config import Taxonomy
    from .dataset_schema import DEFAULT_TAXONOMY, FIELDS, DatasetError, contains_secret, present
else:
    from category_config import Taxonomy
    from dataset_schema import DEFAULT_TAXONOMY, FIELDS, DatasetError, contains_secret, present


def load_jsonl(path):
    try:
        text = path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeError) as error:
        raise DatasetError(f"Cannot read UTF-8 JSONL: {path}") from error
    records = []
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            raise DatasetError(f"Blank JSONL record: {path}:{number}")
        try:
            value = json.loads(line, object_pairs_hook=unique_object)
        except (ValueError, RecursionError) as error:
            raise DatasetError(f"Invalid JSON object: {path}:{number}") from error
        if not isinstance(value, dict):
            raise DatasetError(f"Expected JSON object: {path}:{number}")
        records.append(value)
    return records


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def load_candidates(root, taxonomy=DEFAULT_TAXONOMY):
    result = []
    for risk in taxonomy.risk_ids:
        path = root / "data/candidates" / risk / "candidates.jsonl"
        if path.is_file():
            result.extend(load_jsonl(path))
    return result


def trivial_key(command):
    try:
        tokens = shlex.split(command, posix=False)
    except ValueError:
        return None
    normalized = []
    for token in tokens:
        if re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z][a-z0-9]*)+-[0-9]+", token):
            token = re.sub(r"-[0-9]+$", "-<resource-number>", token)
        normalized.append(token)
    return tuple(normalized)


def validate_rows(rows: list[dict[str, str]], taxonomy: Taxonomy = DEFAULT_TAXONOMY) -> list[str]:
    errors: list[str] = []
    for index, row in enumerate(rows, 1):
        where = f"CSV data record {index}"
        if not isinstance(row, dict) or set(row) != set(FIELDS):
            errors.append(f"{where}: expected exactly nine named fields")
            continue
        if not all(present(row[field]) for field in FIELDS):
            errors.append(f"{where}: fields must be nonempty strings")
            continue
        if row["category"] != taxonomy.canonical or row["risk"] not in taxonomy.risk_names:
            errors.append(f"{where}: noncanonical category or risk")
        if row["severity"] not in {"critical", "high", "medium", "low"}:
            errors.append(f"{where}: invalid severity")
        if row["class"] not in {"allow", "deny", "ask"}:
            errors.append(f"{where}: excluded or invalid class")
        if row["source"] == "synthetic":
            if row["seed"].strip() == "-":
                errors.append(f"{where}: synthetic seed missing")
        elif row["seed"] != "-":
            errors.append(f"{where}: nonsynthetic seed must be -")
        if taxonomy.description_language == "Russian" and not re.search(r"[А-Яа-яЁё]", row["description"]):
            errors.append(f"{where}: Russian description missing")
        if "\x00" in "".join(row.values()):
            errors.append(f"{where}: NUL character forbidden")
        if contains_secret(row):
            errors.append(f"{where}: possible secret indicator; inspect privately, value withheld")
    return errors
