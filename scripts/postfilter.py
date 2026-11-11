from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
from collections import Counter
from pathlib import Path

if __package__:
    from .category_config import load_taxonomy
    from .command_filter import assess_command
    from .source_pipeline import write_csv, write_jsonl
    from .validate_dataset import contains_secret, load_jsonl, web_url
else:
    from category_config import load_taxonomy
    from command_filter import assess_command
    from source_pipeline import write_csv, write_jsonl
    from validate_dataset import contains_secret, load_jsonl, web_url


ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def postfilter(inputs: list[Path], out: Path, *, category_config: Path | None = None) -> dict:
    if out.exists():
        raise ValueError(f"Preserve the existing postfilter audit; choose a fresh output directory: {out}")
    selected, rejected = [], []
    taxonomy = load_taxonomy(category_config) if category_config else load_taxonomy()
    seen_ids = set()
    manifest = []
    for path in inputs:
        manifest.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        for original in load_jsonl(path):
            raw_id = original["raw_id"]
            if raw_id in seen_ids:
                raise ValueError(f"Duplicate raw record identifier: {raw_id}")
            seen_ids.add(raw_id)
            if (not web_url(original.get("doc_full_url")) or not original.get("doc_pos")) and original.get(
                "upstream_eligible"
            ) is not False:
                raise ValueError(f"Missing source backlink or locator: {raw_id}")
            command = original["command"]
            transformations = list(original.get("extraction_transformations", []))
            cleaned = ANSI.sub("", command)
            if cleaned != command:
                transformations.append("remove_ansi_presentation")
            if original.get("format") in {"html", "html_snippet"}:
                decoded = html.unescape(cleaned)
                if decoded != cleaned:
                    transformations.append("decode_html_entities")
                cleaned = decoded
            assessment = assess_command(cleaned, taxonomy)
            reason = assessment.reason
            status = assessment.status
            if contains_secret(original):
                rejected.append(
                    {
                        "raw_id": raw_id,
                        "source_id": original["source_id"],
                        "reason": "sensitive_indicator_withheld",
                        "status": "needs_review",
                    }
                )
                continue
            if original.get("upstream_eligible") is False:
                status, reason = "needs_review", original["upstream_reason"]
            elif not cleaned.strip() or "\x00" in cleaned or len(cleaned) > 32_768:
                status, reason = "needs_review", "empty_control_or_oversized_command"
            elif re.search(r"\{\{|\}\}|(?:^|\s)\.\.\.(?:\s|$)|…", cleaned):
                status, reason = "needs_review", "unresolved_template_or_truncated_example"
            elif (
                len(assessment.risk_candidates) > 1
                and original.get("risk_hint") not in assessment.risk_candidates
            ):
                status, reason = "needs_review", "multiple_possible_risks_require_boundary_review"
            row = dict(
                original,
                command=cleaned,
                transformations=transformations,
                risk_candidates=list(assessment.risk_candidates),
                filter_status=status,
                filter_reason=reason,
                duplicate_of="",
            )
            if status == "eligible":
                selected.append(row)
            else:
                rejected.append(row)
    write_jsonl(out / "eligible.jsonl", selected)
    write_jsonl(out / "rejected.jsonl", rejected)
    fields = (
        "raw_id",
        "command",
        "raw_command",
        "context",
        "doc_pos",
        "doc_full_url",
        "source_id",
        "filter_status",
        "filter_reason",
        "duplicate_of",
        "transformations",
    )
    write_csv(out / "eligible.csv", selected, fields)
    write_csv(out / "rejected.csv", rejected, fields)
    report = {
        "input_files": manifest,
        "examined": len(seen_ids),
        "eligible": len(selected),
        "rejected_or_needs_review": len(rejected),
        "reasons": dict(Counter(row.get("filter_reason", row.get("reason")) for row in rejected)),
        "deduplication_scope": "Deferred to a later stage; distinct raw record IDs are retained regardless of command text.",
    }
    (out / "audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auditable cleanup after source collection; never execute command text."
    )
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--category-config", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(postfilter(args.input, args.out, category_config=args.category_config), ensure_ascii=False)
    )


if __name__ == "__main__":
    main()
