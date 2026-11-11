from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from codebench_pipeline.schemas import RISKS, CandidateRow, SourceEvidence, read_jsonl


def write_dataset_card(bundle: Path, historical: bool) -> None:
    with (bundle / "commands.csv").open(encoding="utf-8", newline="") as stream:
        rows = [CandidateRow.model_validate(row) for row in csv.DictReader(stream)]
    counts = Counter(row.risk for row in rows)
    classes = Counter(row.class_ for row in rows)
    severities = Counter(row.severity for row in rows)
    sources = read_jsonl(bundle / "research/sources.jsonl", SourceEvidence)
    coverage = "\n".join(f"| {risk} | {counts[risk]} | {max(0, 250 - counts[risk])} |" for risk in RISKS)
    source_lines = "\n".join(
        f"- {source.source_id}: [{source.url}]({source.url}); revision `{source.revision}`; "
        f"[{source.license}]({source.license_url}); retrieved {source.retrieved_at}."
        for source in sources
    )
    text = (
        "# CAT-3 reviewed Bash commands\n\n"
        f"Version: reproducible artifact v0.1. Mode: {'historical replay' if historical else 'fresh annotation'}.\n\n"
        f"Accepted records: {len(rows)}. Classes: {dict(classes)}. Severity: {dict(severities)}.\n\n"
        "| Risk | Accepted | Deficit to 250 |\n| --- | ---: | ---: |\n" + coverage + "\n\n"
        "## Evidence sources\n\n" + source_lines + "\n\n"
        "## Review and provenance\n\n"
        "Every accepted row requires an accepting label and a separate approving review. "
        "Confidence is self-reported, not a calibrated safety probability. Agentic review is not human approval.\n\n"
        "- [Canonical CSV](commands.csv)\n"
        "- [Record provenance and transformations](data/processed/provenance.jsonl)\n"
        "- [All proposed labels and decisions](review/labeled_all.csv)\n"
        "- [Pending human review](review/pending.csv)\n"
        "- [Counts and deficits](statistics.json)\n\n"
        "No collected command was executed. Source text and original judgments remain in upstream artifacts "
        "with hashes, model/invocation receipts and retrieval metadata. No synthetic expansion, random sampling "
        "or train/test split is performed. Pairing and command deduplication are deferred. "
        "Class is limited to allow/deny/ask and is independent of severity; unresolved modify/defer cases "
        "cannot enter the accepted CSV. Installed utility versions and missing context are not invented. "
        "Historical replay is not a fresh source-access or model-judgment pass.\n"
    )
    (bundle / "README.md").write_text(text, encoding="utf-8")
