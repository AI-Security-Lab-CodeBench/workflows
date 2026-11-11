import argparse
import csv
import io
import json
import os
import sys
import tempfile
from pathlib import Path

from validate_dataset import (
    CANDIDATE_FIELDS,
    FIELDS,
    PROVENANCE_FIELDS,
    DatasetError,
    load_candidates,
    load_jsonl,
    present,
    string_list,
    summary,
    validate_bundle,
)


def prepare(candidates):
    accepted = []
    seen = set()
    for position, candidate in enumerate(candidates, 1):
        candidate_id = candidate.get("candidate_id")
        if not present(candidate_id) or candidate_id in seen:
            raise DatasetError(f"Candidate entry {position}: missing or duplicate identifier")
        seen.add(candidate_id)
        if candidate.get("status") != "accepted":
            continue
        if set(candidate) != set(CANDIDATE_FIELDS):
            raise DatasetError(f"Candidate entry {position}: incorrect schema")
        row = candidate.get("row")
        if not isinstance(row, dict) or set(row) != set(FIELDS) or not all(present(row[field]) for field in FIELDS):
            raise DatasetError(f"Candidate entry {position}: expected nine nonempty string row fields")
        if not string_list(candidate.get("paired_candidate_ids")):
            raise DatasetError(f"Candidate entry {position}: paired candidate IDs must be an explicit string list")
        accepted.append(candidate)
    accepted.sort(key=lambda candidate: candidate["candidate_id"])
    rows = []
    provenance = []
    for index, candidate in enumerate(accepted, 1):
        item = {}
        for field in PROVENANCE_FIELDS:
            if field == "csv_record":
                item[field] = index
            elif field == "paired_csv_records":
                item[field] = []
            else:
                item[field] = candidate[field]
        rows.append({field: candidate["row"][field] for field in FIELDS})
        provenance.append(item)
    return rows, provenance


def serialize(rows, provenance):
    csv_stream = io.StringIO(newline="")
    writer = csv.DictWriter(csv_stream, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    csv_text = csv_stream.getvalue()
    parsed = list(csv.reader(io.StringIO(csv_text, newline=""), strict=True))
    if parsed != [list(FIELDS)] + [[row[field] for field in FIELDS] for row in rows]:
        raise DatasetError("CSV serialization changed parsed field values")
    provenance_text = "".join(json.dumps(item, ensure_ascii=False, allow_nan=False) + "\n" for item in provenance)
    return csv_text, provenance_text


def persist(root, csv_text, provenance_text):
    destinations = (
        (root / "commands.csv", csv_text),
        (root / "data/processed/provenance.jsonl", provenance_text),
    )
    staged = []
    try:
        for destination, text in destinations:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=destination.parent,
                                             prefix=".dataset-", suffix=".tmp", delete=False) as stream:
                staged.append((Path(stream.name), destination))
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
        for temporary, destination in staged:
            os.replace(temporary, destination)
    finally:
        for temporary, _ in staged:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Integrate only accepted CAT-3 candidates after inert validation; overwrite final CSV/provenance only on success.")
    parser.add_argument("root", nargs="?", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        candidates = load_candidates(args.root)
        rows, provenance = prepare(candidates)
        errors = validate_bundle(
            rows, provenance, candidates, load_jsonl(args.root / "research/sources.jsonl"),
            load_jsonl(args.root / "data/review/pending.jsonl"),
        )
        result = summary(rows, errors)
        if errors:
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 1
        csv_text, provenance_text = serialize(rows, provenance)
        persist(args.root, csv_text, provenance_text)
        result["written"] = ["commands.csv", "data/processed/provenance.jsonl"]
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (DatasetError, OSError, UnicodeError, csv.Error, TypeError, KeyError, AttributeError, RecursionError) as error:
        message = str(error) if isinstance(error, DatasetError) else "Dataset artifact processing failed; inspect input structure and filesystem permissions"
        print(json.dumps({"valid": False, "errors": [message]}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
