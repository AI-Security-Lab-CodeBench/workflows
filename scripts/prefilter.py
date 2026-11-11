from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

if __package__:
    from .category_config import load_taxonomy
    from .command_filter import extract_commands
    from .source_pipeline import source_eligible, write_csv, write_jsonl
    from .validate_dataset import contains_secret, load_jsonl
else:
    from category_config import load_taxonomy
    from command_filter import extract_commands
    from source_pipeline import source_eligible, write_csv, write_jsonl
    from validate_dataset import contains_secret, load_jsonl


def text_records(document: dict, manifest: Path) -> list[tuple[str, str, str]]:
    path = Path(document["local_path"])
    if not path.is_absolute():
        path = manifest.parent / path
    text = path.read_text(encoding="utf-8")
    format_name = document.get("format", "text")
    match format_name:
        case "text" | "markdown":
            return [("document", text, "")]
        case "jsonl":
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        case "json":
            value = json.loads(text)
            rows = value if isinstance(value, list) else [value]
        case "csv":
            with path.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
        case _:
            raise ValueError(f"Unsupported declared format: {format_name}")
    field = document["text_field"]
    result = []
    for index, row in enumerate(rows, 1):
        value = row
        if field != "$":
            for key in field.split("."):
                value = value[key]
        if not isinstance(value, str):
            raise ValueError(f"{path}: record {index} field {field} must be text")
        context = row.get(document.get("context_field", ""), "") if isinstance(row, dict) else ""
        if not isinstance(context, str):
            raise ValueError(f"{path}: record {index} context must be text")
        result.append((f"record {index}; field {field}", value, context))
    return result


def prefilter(manifest: Path, out: Path, *, category_config: Path | None = None) -> dict[str, int]:
    if out.exists():
        raise ValueError(f"Preserve the existing prefilter audit; choose a fresh output directory: {out}")
    records, withheld, inputs = [], [], []
    taxonomy = load_taxonomy(category_config) if category_config else load_taxonomy()
    for document in load_jsonl(manifest):
        if not source_eligible(document):
            raise ValueError(f"Source eligibility not established: {document['source_id']}")
        path = Path(document["local_path"])
        if not path.is_absolute():
            path = manifest.parent / path
        inputs.append(
            {
                "source_id": document["source_id"],
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
        for position, text, surrounding in text_records(document, manifest):
            for item in extract_commands(text, shortlist=False, taxonomy=taxonomy):
                locator = f"{position}; characters {item.start}:{item.end}"
                raw_command = text[item.start : item.end]
                raw_id = hashlib.sha256(
                    (document["source_id"] + "\n" + locator + "\n" + raw_command).encode()
                ).hexdigest()
                record = {
                    "raw_id": raw_id,
                    "source_id": document["source_id"],
                    "doc_full_url": document["url"],
                    "doc_pos": locator,
                    "local_path": document["local_path"],
                    "format": document.get("format", "text"),
                    "raw_command": raw_command,
                    "command": item.command,
                    "context": surrounding + ("\n" if surrounding else "") + item.context,
                    "extraction_transformations": list(item.transformations),
                }
                if contains_secret(record):
                    withheld.append(
                        {
                            "raw_id": raw_id,
                            "source_id": document["source_id"],
                            "doc_pos": locator,
                            "reason": "sensitive_indicator_withheld",
                        }
                    )
                else:
                    records.append(record)
    write_jsonl(out / "raw_commands.jsonl", records)
    write_jsonl(out / "withheld.jsonl", withheld)
    write_csv(
        out / "raw_commands.csv",
        records,
        (
            "raw_id",
            "command",
            "raw_command",
            "context",
            "doc_pos",
            "doc_full_url",
            "source_id",
            "local_path",
            "format",
            "extraction_transformations",
        ),
    )
    report = {"raw_candidates": len(records), "withheld": len(withheld)}
    audit = dict(
        report,
        source_manifest=str(manifest),
        source_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        inputs=inputs,
        output_sha256=hashlib.sha256((out / "raw_commands.jsonl").read_bytes()).hexdigest(),
    )
    (out / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Broad, source-located extraction before CAT-3 cleanup.")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--category-config", type=Path)
    args = parser.parse_args()
    print(json.dumps(prefilter(args.manifest, args.out, category_config=args.category_config)))


if __name__ == "__main__":
    main()
