from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

if __package__:
    from .command_filter import assess_command, extract_commands
    from .source_io import capture_sources, inventory_tables, source_eligible, write_csv, write_jsonl
    from .validate_dataset import contains_secret, load_candidates, load_jsonl
else:
    from command_filter import assess_command, extract_commands
    from source_io import capture_sources, inventory_tables, source_eligible, write_csv, write_jsonl
    from validate_dataset import contains_secret, load_candidates, load_jsonl


def import_candidates(root: Path, out: Path) -> dict[str, int]:
    sources = load_jsonl(root / "research/sources.jsonl")
    indexed = {source["source_id"]: source for source in sources}
    selected, pending, extracted, raw_records = [], [], [], []
    seen = set()
    for candidate in load_candidates(root):
        candidate_id = candidate["candidate_id"]
        if candidate_id in seen:
            raise ValueError(f"Duplicate candidate record identifier: {candidate_id}")
        seen.add(candidate_id)
        command = candidate["row"]["command"]
        assessment = assess_command(command)
        reason = assessment.reason
        allowed = assessment.status == "eligible"
        if not candidate["source_ids"] or any(
            source_id not in indexed or not source_eligible(indexed[source_id])
            for source_id in candidate["source_ids"]
        ):
            allowed, reason = False, "Source inaccessible, reuse unresolved, or excluded dataset origin."
        elif contains_secret(candidate):
            allowed, reason = False, "Possible sensitive value; command withheld from extraction table."
        elif allowed and candidate["row"]["risk"].split()[0] not in assessment.risk_candidates:
            allowed, reason = (
                False,
                "Declared risk does not match the command filter; requires boundary review.",
            )
        if allowed:
            selected.append(
                dict(
                    candidate,
                    paired_candidate_ids=[],
                    role="unpaired",
                    scenario_id="unpaired:" + candidate_id,
                )
            )
        else:
            pending.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "risk": candidate["row"]["risk"],
                    "issue": reason,
                    "status": "needs_review",
                }
            )
        if not contains_secret(candidate):
            urls = [indexed[key]["url"] for key in candidate["source_ids"] if key in indexed]
            raw_records.append(
                {
                    "raw_id": "legacy-" + candidate["candidate_id"],
                    "candidate_id": candidate["candidate_id"],
                    "source_id": candidate["source_ids"][0] if candidate["source_ids"] else "unresolved",
                    "doc_full_url": urls[0] if urls else "",
                    "source_urls": urls,
                    "doc_pos": candidate["source_locator"],
                    "raw_command": command,
                    "command": command,
                    "context": candidate["evidence_context"],
                    "format": "legacy_candidate_jsonl",
                    "risk_hint": candidate["row"]["risk"].split()[0],
                    "upstream_eligible": allowed,
                    "upstream_reason": reason,
                    "extraction_transformations": candidate["transformations"],
                }
            )
            extracted.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "command": command,
                    "context": candidate["evidence_context"],
                    "doc_pos": candidate["source_locator"],
                    "doc_full_url": [
                        indexed[key]["url"] for key in candidate["source_ids"] if key in indexed
                    ],
                    "doc_id": candidate["source_ids"],
                    "source_id": candidate["source_ids"],
                    "filter_status": "eligible" if allowed else "needs_review",
                    "reason": reason,
                    "context_origin": "legacy documented source context; not a new raw-text extraction",
                }
            )
    write_jsonl(out / "candidates.jsonl", selected)
    write_jsonl(out / "sources.jsonl", sources)
    write_jsonl(out / "filter-pending.jsonl", pending)
    write_jsonl(out / "raw_commands.jsonl", raw_records)
    write_csv(
        out / "extracted_commands.csv",
        extracted,
        (
            "candidate_id",
            "command",
            "context",
            "doc_pos",
            "doc_full_url",
            "doc_id",
            "source_id",
            "filter_status",
            "reason",
            "context_origin",
        ),
    )
    return {"eligible": len(selected), "pending": len(pending), "examined": len(extracted)}


def select_candidates(inputs: Path, eligible: Path, out: Path) -> dict[str, int]:
    kept = {row["candidate_id"] for row in load_jsonl(eligible) if row.get("candidate_id")}
    candidates, pair_changes = [], []
    seen = set()
    for row in load_jsonl(inputs / "candidates.jsonl"):
        candidate_id = row["candidate_id"]
        if candidate_id in seen:
            raise ValueError(f"Duplicate candidate record identifier: {candidate_id}")
        seen.add(candidate_id)
        if row["candidate_id"] not in kept:
            continue
        if row.get("paired_candidate_ids") or row.get("role") != "unpaired":
            pair_changes.append(
                {
                    "candidate_id": row["candidate_id"],
                    "original": row.get("paired_candidate_ids", []),
                    "retained": [],
                    "reason": "Pairing deferred until after labeling",
                }
            )
        candidates.append(
            dict(row, paired_candidate_ids=[], role="unpaired", scenario_id="unpaired:" + candidate_id)
        )
    wanted = {source_id for row in candidates for source_id in row["source_ids"]}
    write_jsonl(out / "candidates.jsonl", candidates)
    write_jsonl(
        out / "sources.jsonl",
        [row for row in load_jsonl(inputs / "sources.jsonl") if row["source_id"] in wanted],
    )
    write_jsonl(out / "pair-filtering.jsonl", pair_changes)
    return {"selected_for_labeling": len(candidates)}


def extract_documents(manifest: Path, out: Path) -> dict[str, int]:
    extracted = []
    for document in load_jsonl(manifest):
        if not source_eligible(document):
            raise ValueError(f"Ineligible document source: {document.get('source_id')}")
        path = Path(document["local_path"])
        if not path.is_absolute():
            path = manifest.parent / path
        text = path.read_text(encoding="utf-8")
        for result in extract_commands(text):
            record = asdict(result)
            if contains_secret(record):
                continue
            extracted.append(
                {
                    "command": result.command,
                    "context": result.context,
                    "doc_pos": f"characters {result.start}:{result.end}",
                    "doc_full_url": document["url"],
                    "doc_id": str(path),
                    "source_id": document["source_id"],
                    "filter_status": result.status,
                    "reason": result.reason,
                    "risk_candidates": result.risk_candidates,
                    "transformations": result.transformations,
                }
            )
    write_jsonl(out / "extracted.jsonl", extracted)
    write_csv(
        out / "extracted_commands.csv",
        extracted,
        (
            "command",
            "context",
            "doc_pos",
            "doc_full_url",
            "doc_id",
            "source_id",
            "filter_status",
            "reason",
            "risk_candidates",
            "transformations",
        ),
    )
    return {"extracted": len(extracted)}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inert source inventory, extraction, and legacy candidate filtering."
    )
    actions = parser.add_subparsers(dest="action", required=True)
    legacy = actions.add_parser("import")
    legacy.add_argument("--root", type=Path, default=Path.cwd())
    legacy.add_argument("--out", type=Path, required=True)
    extract = actions.add_parser("extract")
    extract.add_argument("--manifest", type=Path, required=True)
    extract.add_argument("--out", type=Path, required=True)
    inventory = actions.add_parser("inventory")
    inventory.add_argument("--research", type=Path, required=True)
    inventory.add_argument("--out", type=Path, required=True)
    capture = actions.add_parser("capture")
    capture.add_argument("--inputs", type=Path, required=True)
    capture.add_argument("--out", type=Path, required=True)
    select = actions.add_parser("select")
    select.add_argument("--inputs", type=Path, required=True)
    select.add_argument("--eligible", type=Path, required=True)
    select.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    match args.action:
        case "import":
            report = import_candidates(args.root, args.out)
        case "extract":
            report = extract_documents(args.manifest, args.out)
        case "inventory":
            report = inventory_tables(args.research, args.out)
        case "capture":
            report = capture_sources(args.inputs, args.out)
        case "select":
            report = select_candidates(args.inputs, args.eligible, args.out)
        case _:
            raise ValueError("Unknown pipeline action")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
