from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from pydantic import JsonValue

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataset_publication import Publication as Publication
from codebench_pipeline.dataset_publication import csv_text as csv_text
from codebench_pipeline.dataset_publication import json_text
from codebench_pipeline.dataset_records import (
    CandidateRecord,
    LabelDecision,
    ReviewDecision,
    index_records,
    validate_label,
)
from codebench_pipeline.dataset_table_contract import FIELDS, REVIEW_FIELDS
from codebench_pipeline.dataset_table_contract import table_schema as table_schema


def prepare_dataset(
    candidates: list[CandidateRecord],
    labels: list[LabelDecision],
    reviews: list[ReviewDecision],
    rules: CategoryRules,
    limit_per_risk: int,
    *,
    approved_ids: frozenset[str] | None = None,
) -> Publication:
    if type(limit_per_risk) is not int or limit_per_risk < 0:
        raise ValueError("Per-risk limit must be a nonnegative integer")
    indexed = index_records(candidates)
    proposed, checked = index_records(labels), index_records(reviews)
    if indexed.keys() != proposed.keys() or indexed.keys() != checked.keys():
        raise ValueError("Missing or extra label/review identities")
    risks = {risk.id: risk.canonical for risk in rules.risks}
    for candidate in candidates:
        label = LabelDecision.model_validate(proposed[candidate.candidate_id].model_dump(by_alias=True))
        validate_label(candidate, label, rules)
        ReviewDecision.model_validate(checked[candidate.candidate_id].model_dump())
    counts: Counter[str] = Counter()
    rows: list[dict[str, str]] = []
    accepted: list[dict[str, str]] = []
    pending: list[dict[str, str]] = []
    reviewed: list[dict[str, str]] = []
    provenance: list[str] = []
    pending_evidence: list[str] = []
    all_evidence: list[str] = []
    eligible_ids: list[str] = []
    human_review_ids: list[str] = []
    for candidate in candidates:
        label, decision = proposed[candidate.candidate_id], checked[candidate.candidate_id]
        human_review_ids.append(candidate.candidate_id)
        reason = ""
        if label.decision != "accept" or decision.verdict != "approve":
            reason = f"label={label.decision}; review={decision.verdict}"
        else:
            eligible_ids.append(candidate.candidate_id)
        if not reason:
            if approved_ids is None:
                reason = "human review required"
            elif candidate.candidate_id not in approved_ids:
                reason = "human review excluded"
        if not reason and counts[label.risk_id] >= limit_per_risk:
            reason = "per-risk cap reached"
        row = {
            "command": candidate.command,
            "description": label.description,
            "util": label.util,
            "severity": label.severity,
            "category": rules.canonical,
            "risk": risks[label.risk_id],
            "source": candidate.source_id,
            "seed": "-",
            "class": label.class_,
            "human_reviewed": "true" if not reason else "false",
        }
        evidence: dict[str, JsonValue] = {
            "candidate_id": candidate.candidate_id,
            "csv_record": None if reason else len(rows) + 1,
            "candidate": candidate.model_dump(mode="json"),
            "label": label.model_dump(mode="json", by_alias=True),
            "review": decision.model_dump(mode="json"),
            "exclusion_reason": reason,
            "source_id": candidate.source_id,
            "source_url": candidate.source_url,
            "source_position": candidate.source_position,
            "transformations": [value for value in candidate.transformations],
            "documentation_urls": [url for url in label.documentation_urls],
        }
        serialized = json_text(evidence)
        all_evidence.append(serialized)
        flat = {
            **row,
            "candidate_id": candidate.candidate_id,
            "csv_record": str(evidence["csv_record"] or "-"),
            "context": candidate.context,
            "source_id": candidate.source_id,
            "source_url": candidate.source_url,
            "source_position": candidate.source_position,
            "transformations": "\n".join(candidate.transformations),
            "documentation_urls": "\n".join(label.documentation_urls),
            "category_confidence": str(label.category_confidence),
            "risk_confidence": str(label.risk_confidence),
            "severity_confidence": str(label.severity_confidence),
            "class_confidence": str(label.class_confidence),
            "review_confidence": decision.confidence,
            "label_decision": label.decision,
            "review_verdict": decision.verdict,
            "label_rationale": label.rationale,
            "review_rationale": decision.rationale,
            "exclusion_reason": reason,
            "evidence_jsonl": "review/reviewed.jsonl",
            "evidence_record": str(len(all_evidence)),
        }
        reviewed.append(flat)
        if reason:
            pending.append(flat)
            pending_evidence.append(serialized)
        else:
            counts[label.risk_id] += 1
            rows.append(row)
            accepted.append(flat)
            provenance.append(serialized)
    frame = pl.DataFrame(rows, schema={field: pl.String for field in FIELDS})
    table_schema(rules).validate(frame)
    if any(row["source"] == "synthetic" for row in rows):
        raise ValueError("CandidateRecord has no synthetic seed/model contract")
    by_risk: dict[str, JsonValue] = {}
    for risk in rules.risks:
        subset = [row for row in rows if row["risk"] == risk.canonical]
        by_risk[risk.id] = {
            "name": risk.name,
            "total": counts[risk.id],
            "deficit": max(0, limit_per_risk - counts[risk.id]),
            "class": {rule.name: sum(row["class"] == rule.name for row in subset) for rule in rules.classes},
            "severity": {
                rule.name: sum(row["severity"] == rule.name for row in subset) for rule in rules.severities
            },
            "utilities": dict(Counter(row["util"] for row in subset)),
        }
    source_coverage: dict[str, JsonValue] = {}
    for candidate in candidates:
        source_summary: dict[str, JsonValue] = {
            "urls": [
                url
                for url in sorted(
                    {item.source_url for item in candidates if item.source_id == candidate.source_id}
                )
            ],
            "candidates": sum(item.source_id == candidate.source_id for item in candidates),
            "published": sum(row["source"] == candidate.source_id for row in rows),
        }
        source_coverage[candidate.source_id] = source_summary
    statistics: dict[str, JsonValue] = {
        "category_id": rules.category_id,
        "category": rules.canonical,
        "version": rules.version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "limit_per_risk": limit_per_risk,
        "candidates": len(candidates),
        "records": len(rows),
        "pending": len(pending),
        "human_reviewed": sum(row["human_reviewed"] == "true" for row in rows),
        "automatically_admitted": sum(row["human_reviewed"] == "false" for row in rows),
        "category_coverage": {
            "records": len(rows),
            "target": limit_per_risk * len(risks),
            "deficit": sum(max(0, limit_per_risk - counts[risk]) for risk in risks),
        },
        "by_risk": by_risk,
        "sources": source_coverage,
        "by_class": {rule.name: sum(row["class"] == rule.name for row in rows) for rule in rules.classes},
        "by_severity": {
            rule.name: sum(row["severity"] == rule.name for row in rows) for rule in rules.severities
        },
        "exclusions": dict(Counter(row["exclusion_reason"] for row in pending)),
    }
    readme = (
        f"# {rules.canonical}\n\nVersion: {rules.version}. Built: {statistics['generated_at']}.\n\n"
        f"Language: Bash. Description language: {rules.description_language}.\n"
        f"Published: {len(rows)}. Pending or excluded: {len(pending)}.\n\n"
        "## Evidence and review\n\n"
        "commands.csv has the nine command columns plus human_reviewed ('true' or 'false'). "
        "Source is the exact supplied source_id; seed is '-'. "
        "provenance.jsonl maps one-based CSV data records to exact source URLs, positions, "
        "transformations, documentation and separate label/review decisions. "
        "review/commands.csv is the flat accepted review table; review/pending.csv contains withheld "
        "and capped records; review/reviewed.csv and reviewed.jsonl retain all decisions.\n\n"
        "Labeling and review use separate isolated invocations; receipts remain in caller output directories. "
        "Agentic review is not human approval. Confidences are low/medium/high self-reported judgments, "
        "not calibrated probabilities. Every retained candidate requires human review, including "
        "high-confidence records; human_reviewed marks actual human-approved inclusions. "
        "Category policy is preserved in rules.json.\n\n"
        "## Coverage and limitations\n\n"
        "statistics.json reports per-risk counts, class, severity, utilities, deficits and source coverage. "
        "The cap is independent per risk and selects in input order; no random sampling or split "
        "is performed. Duplicate command text remains distinct by candidate identity. "
        "No linking, pairing, deduplication, synthesis or command execution is performed.\n\n"
        "Source eligibility, licensing and reuse evidence are owned by the upstream source_catalog; "
        "this export does not approve them. Consult that catalog before reuse. Structural validation "
        "does not prove semantics. Version limits remain in inference evidence. "
        + (
            "Selection also follows supplied human-approved identities; the caller retains the decision ledger.\n\n"
            if approved_ids is not None
            else "Uncertain records remain withheld for human review.\n\n"
        )
        + "```json\n"
        + json.dumps(statistics, ensure_ascii=False, indent=2, allow_nan=False)
        + "\n```\n"
    )
    artifacts = {
        "commands.csv": csv_text(rows, FIELDS),
        "review/commands.csv": csv_text(accepted, REVIEW_FIELDS),
        "review/pending.csv": csv_text(pending, REVIEW_FIELDS),
        "review/reviewed.csv": csv_text(reviewed, REVIEW_FIELDS),
        "provenance.jsonl": "".join(line + "\n" for line in provenance),
        "review/pending.jsonl": "".join(line + "\n" for line in pending_evidence),
        "review/reviewed.jsonl": "".join(line + "\n" for line in all_evidence),
        "statistics.json": json_text(statistics) + "\n",
        "rules.json": rules.model_dump_json(indent=2) + "\n",
        "README.md": readme,
    }
    return Publication(artifacts, statistics, rows, pending, tuple(eligible_ids), tuple(human_review_ids))


def publish_dataset(
    candidates: list[CandidateRecord],
    labels: list[LabelDecision],
    reviews: list[ReviewDecision],
    rules: CategoryRules,
    out: Path,
    limit_per_risk: int,
) -> dict[str, JsonValue]:
    publication = prepare_dataset(candidates, labels, reviews, rules, limit_per_risk)
    for name in publication.files:
        if (out / name).exists():
            raise FileExistsError(out / name)
    for name, text in publication.files.items():
        path = out / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8", newline="") as stream:
            stream.write(text)
    return publication.statistics
