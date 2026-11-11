import argparse
import json
import sys
from collections import Counter
from pathlib import Path

if __package__:
    from .category_config import load_taxonomy
    from .dataset_csv import read_csv
    from .dataset_csv import validate_csv_quoting as validate_csv_quoting
    from .dataset_schema import (
        CANDIDATE_FIELDS,
        DEFAULT_TAXONOMY,
        PROVENANCE_FIELDS,
        SOURCE_FIELDS,
        DatasetError,
        contains_secret,
        present,
        string_list,
        web_url,
    )
    from .dataset_schema import CATEGORY as CATEGORY
    from .dataset_schema import FIELDS as FIELDS
    from .dataset_schema import RISKS as RISKS
    from .dataset_validation import load_candidates, load_jsonl, validate_rows
    from .dataset_validation import trivial_key as trivial_key
    from .dataset_validation import unique_object as unique_object
else:
    from category_config import load_taxonomy
    from dataset_csv import read_csv
    from dataset_csv import validate_csv_quoting as validate_csv_quoting
    from dataset_schema import (
        CANDIDATE_FIELDS,
        DEFAULT_TAXONOMY,
        PROVENANCE_FIELDS,
        SOURCE_FIELDS,
        DatasetError,
        contains_secret,
        present,
        string_list,
        web_url,
    )
    from dataset_schema import CATEGORY as CATEGORY
    from dataset_schema import FIELDS as FIELDS
    from dataset_schema import RISKS as RISKS
    from dataset_validation import load_candidates, load_jsonl, validate_rows
    from dataset_validation import trivial_key as trivial_key
    from dataset_validation import unique_object as unique_object


def index_records(records, field, label, errors):
    indexed = {}
    for number, record in enumerate(records, 1):
        key = record.get(field)
        if not present(key):
            errors.append(f"{label} entry {number}: missing identifier")
        elif key in indexed:
            errors.append(f"{label} entry {number}: duplicate identifier")
        else:
            indexed[key] = record
    return indexed


def validate_bundle(rows, provenance, candidates, sources, pending, taxonomy=DEFAULT_TAXONOMY):
    errors = validate_rows(rows, taxonomy)
    candidate_map = index_records(candidates, "candidate_id", "Candidate", errors)
    source_map = index_records(sources, "source_id", "Source", errors)
    accepted = {key: value for key, value in candidate_map.items() if value.get("status") == "accepted"}
    unresolved = {
        item.get("candidate_id")
        for item in pending
        if isinstance(item.get("candidate_id"), str)
        and item.get("status") not in {"resolved", "closed", "rejected"}
    }
    mapped = {}
    seen_candidates = set()
    for position, item in enumerate(provenance, 1):
        where = f"Provenance entry {position}"
        if set(item) != set(PROVENANCE_FIELDS):
            errors.append(f"{where}: wrong provenance fields")
        index = item.get("csv_record")
        if type(index) is not int or not 1 <= index <= len(rows):
            errors.append(f"{where}: invalid one-based data index")
            continue
        if index in mapped:
            errors.append(f"{where}: duplicate data index")
        mapped[index] = item
        candidate_id = item.get("candidate_id")
        if not present(candidate_id):
            errors.append(f"{where}: missing candidate identifier")
            continue
        if candidate_id in seen_candidates:
            errors.append(f"{where}: candidate mapped more than once")
        seen_candidates.add(candidate_id)
        candidate = accepted.get(candidate_id)
        if candidate is None:
            errors.append(f"{where}: candidate is absent or not accepted")
            continue
        if set(candidate) != set(CANDIDATE_FIELDS):
            errors.append(f"{where}: wrong candidate fields")
        row = rows[index - 1]
        if candidate.get("row") != row:
            errors.append(f"{where}: candidate row differs from CSV")
        risk = row.get("risk", "")
        if not candidate_id.startswith(risk.split(" ")[0] + "-"):
            errors.append(f"{where}: candidate ID must have risk prefix followed by hyphen")
        if candidate_id in unresolved:
            errors.append(f"{where}: accepted candidate has unresolved review issue")
        for field in PROVENANCE_FIELDS:
            if field not in {"csv_record", "paired_csv_records"} and item.get(field) != candidate.get(field):
                errors.append(f"{where}: provenance/candidate mismatch for {field}")
        for field in ("source_locator", "tool_version", "label_rationale", "scenario_id"):
            if not present(item.get(field)):
                errors.append(f"{where}: missing {field}")
        if not present(candidate.get("evidence_context")):
            errors.append(f"{where}: missing visible evidence context")
        if not string_list(item.get("transformations")):
            errors.append(f"{where}: transformations must be an explicit list")
        urls = item.get("documentation_urls")
        if not string_list(urls, nonempty=True) or not all(web_url(url) for url in urls):
            errors.append(f"{where}: documentation URLs missing or invalid")
        if row.get("source") == "synthetic":
            if not present(item.get("synthetic_model")):
                errors.append(f"{where}: synthetic model missing")
        elif item.get("synthetic_model") not in (None, ""):
            errors.append(f"{where}: nonsynthetic record declares a generation model")
        ids = item.get("source_ids")
        if not string_list(ids, nonempty=True):
            errors.append(f"{where}: source identifiers missing or invalid")
        else:
            for source_id in ids:
                source = source_map.get(source_id)
                if source is None:
                    errors.append(f"{where}: source identifier not in shared ledger")
                    continue
                if set(source) != set(SOURCE_FIELDS):
                    errors.append(f"{where}: source ledger schema mismatch")
                if source.get("accessibility") != "accessible" or source.get("reuse_decision") != "permitted":
                    errors.append(f"{where}: source is not accessible with permitted reuse")
                if not web_url(source.get("url")) or not web_url(source.get("license_url")):
                    errors.append(f"{where}: source/license URL missing or invalid")
                for field in ("locator", "revision", "retrieved_at", "license", "source_type", "relevance"):
                    if not present(source.get(field)):
                        errors.append(f"{where}: source evidence missing {field}")
                source_risks = source.get("risks")
                if not isinstance(source_risks, list) or not any(
                    value in source_risks for value in (risk, risk.split(" ")[0])
                ):
                    errors.append(f"{where}: source does not cover record risk")
                if source.get("commands_present") is not True:
                    errors.append(f"{where}: source does not attest actual command content")
        if item.get("role") not in {"dangerous", "counterexample", "unpaired"}:
            errors.append(f"{where}: invalid role")
        if not string_list(candidate.get("paired_candidate_ids")):
            errors.append(f"{where}: paired candidate IDs must be an explicit string list")
        pairs = item.get("paired_csv_records")
        if not isinstance(pairs, list) or any(type(pair) is not int for pair in pairs):
            errors.append(f"{where}: paired data indices must be an integer list")
        if contains_secret(item) or contains_secret(candidate):
            errors.append(f"{where}: possible secret indicator; value withheld")
    if set(mapped) != set(range(1, len(rows) + 1)):
        errors.append("Provenance does not cover every CSV data record exactly once")
    if seen_candidates != set(accepted):
        errors.append("Accepted candidate set differs from final provenance mappings")
    for number, source in enumerate(sources, 1):
        if contains_secret(source):
            errors.append(f"Source entry {number}: possible secret indicator; value withheld")
    return errors


def summary(rows, errors, taxonomy=DEFAULT_TAXONOMY):
    def counts(field, selected):
        values = [row.get(field, "invalid") for row in selected]
        return dict(
            sorted(
                Counter(
                    "[secret indicator withheld]" if contains_secret(value) else value for value in values
                ).items()
            )
        )

    return {
        "valid": not errors,
        "records": len(rows),
        "target": 250,
        "deficit": max(0, 250 - len(rows)),
        "counts": {field: counts(field, rows) for field in ("risk", "class", "severity", "util")},
        "by_risk": {
            risk: {
                "total": len(selected),
                "class": counts("class", selected),
                "severity": counts("severity", selected),
                "utilities": counts("util", selected),
            }
            for risk in taxonomy.risk_names
            for selected in [[row for row in rows if row.get("risk") == risk]]
        },
        "yellow_zone_ask": sum(row.get("class") == "ask" for row in rows),
        "errors": errors,
        "verification_scope": {
            "performed": "Inert UTF-8, CSV/JSON structure, standalone ledger consistency, unique identities and secret-indicator checks only.",
            "not_proven": "Bash syntax, command safety, source truth/accessibility/license compatibility, approval, isolation, PII absence, and semantic near-duplicate absence require independent manual review; ledger attestations are not live network verification.",
            "execution": "No dataset command is executed or imported. No shell/client or downloaded code is invoked.",
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description="Validate CAT-3 artifacts as inert text; no Bash execution or semantic safety proof."
    )
    parser.add_argument("root", nargs="?", type=Path, default=Path.cwd())
    parser.add_argument("--category-config", type=Path)
    args = parser.parse_args()
    try:
        taxonomy = load_taxonomy(args.category_config) if args.category_config else DEFAULT_TAXONOMY
        rows = read_csv(args.root / "commands.csv")
        errors = validate_bundle(
            rows,
            load_jsonl(args.root / "data/processed/provenance.jsonl"),
            load_candidates(args.root, taxonomy),
            load_jsonl(args.root / "research/sources.jsonl"),
            load_jsonl(args.root / "data/review/pending.jsonl"),
            taxonomy,
        )
        print(json.dumps(summary(rows, errors, taxonomy), ensure_ascii=False, indent=2))
        return 1 if errors else 0
    except (
        DatasetError,
        OSError,
        UnicodeError,
        TypeError,
        KeyError,
        AttributeError,
        RecursionError,
    ) as error:
        message = (
            str(error)
            if isinstance(error, DatasetError)
            else "Dataset validation failed; inspect JSON field types and filesystem permissions"
        )
        print(json.dumps({"valid": False, "errors": [message]}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
