import copy
import csv
import io
import json
from pathlib import Path

from integrate_dataset import prepare as integrate, serialize
from label_contract import (
    CONFIDENCES, facts, load_exchange, load_inputs, require, utility_matches, validate_label_input,
)
from label_pipeline import json_text, write_new_tree
from validate_dataset import (
    FIELDS, RISKS, present, summary, validate_bundle,
)


REVIEW_FIELDS = (*FIELDS, "candidate_id", "csv_record", "context", "source_urls",
                 "source_locator", "documentation_urls", *CONFIDENCES, "rationale",
                 "review_rationale")
PENDING_FIELDS = (
    "candidate_id", "command", "risk", "class", "severity", "llm_rationale", "exclusion_reason",
    "label_decision", "review_verdict", "rationale_source", "description", "util",
    *CONFIDENCES, "context", "source_urls", "documentation_urls", "source_locator",
    "label_rationale", "review_rationale", "category", "source", "seed",
    "required_decision", "evidence_jsonl", "evidence_record",
)


def jsonl(records):
    return "".join(json.dumps(item, ensure_ascii=False, allow_nan=False) + "\n" for item in records)


def csv_text(records, fields):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    for record in records:
        require(not any(isinstance(record[field], dict) for field in fields),
                "Nested evidence belongs in JSONL, not a human-review CSV cell")
        writer.writerow({field: "\n".join(record[field]) if isinstance(record[field], list)
                         else record[field] for field in fields})
    return stream.getvalue()


def export(candidates, sources, packets, responses, review_packets, reviews, out, limit_per_risk=250):
    require(type(limit_per_risk) is int and limit_per_risk > 0, "limit-per-risk must be positive")
    originals, source_map = load_inputs(candidates, sources)
    label_packets, label_envelopes, labels = load_exchange(packets, responses, "label")
    reviewer_packets, _, verdicts = load_exchange(review_packets, reviews, "review")
    require(set(labels) == set(originals) == set(verdicts), "Exchange IDs differ from selected candidates")
    require(set(label_packets) == set(reviewer_packets), "Review packet filenames differ from label packets")
    for name, packet in label_packets.items():
        payload = validate_label_input(packet)
        for item in payload["candidates"]:
            key = item["candidate_id"]
            require(key in originals, "Unknown evidence candidate")
            require(item == facts(originals[key], source_map, item["documentation"]),
                    f"Candidate/source metadata drift: {key}")
        expected = {"original_packet": packet, "label_response": label_envelopes[name]}
        require(reviewer_packets[name]["input"] == expected, "Review does not match original input and labels")
        require(reviewer_packets[name]["candidate_ids"] == packet["candidate_ids"], "Review IDs differ")
    selected, reasons = {}, {}
    for key, original in originals.items():
        label, review = labels[key], verdicts[key]
        issues = []
        if label["decision"] != "accept":
            issues.append(f"label decision: {label['decision']}")
        if review["verdict"] != "approve":
            issues.append(f"independent review: {review['verdict']}")
        if not utility_matches(original, label["util"]):
            issues.append("proposed utility does not match the source invocation")
        if label["risk"] != original["row"]["risk"]:
            issues.append("risk reassignment requires candidate ID/source metadata repair")
        if not present(original["tool_version"]):
            issues.append("missing tool version evidence")
        if not key.startswith(label["risk"].split()[0] + "-"):
            issues.append("candidate identifier has wrong risk prefix")
        for source_id in original["source_ids"]:
            source = source_map[source_id]
            if (source["accessibility"] != "accessible" or source["reuse_decision"] != "permitted"
                    or source["commands_present"] is not True
                    or not any(risk in source["risks"] for risk in (label["risk"], label["risk"].split()[0]))):
                issues.append(f"source not eligible for acceptance: {source_id}")
        if issues:
            reasons[key] = issues
            continue
        item = copy.deepcopy(original)
        item["status"] = "accepted"
        item["role"] = "unpaired"
        item["paired_candidate_ids"] = []
        for field in ("category", "risk", "severity", "class", "description"):
            item["row"][field] = label[field]
        item["documentation_urls"] = label["documentation_urls"]
        item["label_rationale"] = label["rationale"]
        selected[key] = item
    counts = {risk: 0 for risk in RISKS}
    for key in sorted(selected):
        risk = selected[key]["row"]["risk"]
        if counts[risk] >= limit_per_risk:
            reasons[key] = ["per-risk cap: reviewed record retained outside the accepted CSV"]
        else:
            counts[risk] += 1
    for key in reasons:
        selected.pop(key, None)
    pending = []
    all_candidates = []
    for key, original in originals.items():
        if key in selected:
            all_candidates.append(selected[key])
        else:
            all_candidates.append(dict(original, status="needs_review", role="unpaired", paired_candidate_ids=[]))
            pending.append({
                "candidate_id": key, "risk": original["row"]["risk"],
                "issue": "; ".join(reasons[key]),
                "evidence": {"candidate": original, "label": labels[key], "review": verdicts[key]},
                "required_decision": "Resolve the stated evidence or annotation issue; retain the original model decisions.",
                "status": "needs_review",
            })
    rows, provenance = integrate(all_candidates)
    errors = validate_bundle(rows, provenance, all_candidates, list(source_map.values()), pending)
    require(not errors, "Bundle validation failed: " + "; ".join(errors))
    commands, provenance_text = serialize(rows, provenance)
    review_rows, all_review_rows, pending_rows = [], [], []
    pending_positions = {item["candidate_id"]: index for index, item in enumerate(pending, 1)}
    indices = {record["candidate_id"]: record["csv_record"] for record in provenance}
    for key in sorted(originals):
        candidate, label = selected.get(key, originals[key]), labels[key]
        reviewed = {
            **candidate["row"],
            **{field: label[field] for field in ("category", "risk", "severity", "class", "description")},
            "candidate_id": key, "csv_record": indices.get(key, ""),
            "context": candidate["evidence_context"],
            "source_urls": [source_map[source_id]["url"] for source_id in candidate["source_ids"]],
            "source_locator": candidate["source_locator"],
            "documentation_urls": label["documentation_urls"],
            **{field: label[field] for field in CONFIDENCES}, "rationale": label["rationale"],
            "review_rationale": verdicts[key]["rationale"],
        }
        if key in selected:
            review_rows.append(reviewed)
        all_review_rows.append(dict(reviewed, label_decision=label["decision"],
                                    review_verdict=verdicts[key]["verdict"],
                                    export_status="accepted" if key in selected else "needs_review",
                                    exclusion_reason="; ".join(reasons.get(key, [])),
                                    proposed_util=label["util"]))
        if key not in selected:
            rationale = verdicts[key]["rationale"] if verdicts[key]["verdict"] != "approve" else label["rationale"]
            pending_rows.append(dict(
                reviewed, label_decision=label["decision"], review_verdict=verdicts[key]["verdict"],
                llm_rationale=rationale, exclusion_reason="; ".join(reasons[key]),
                rationale_source="review" if verdicts[key]["verdict"] != "approve" else "label",
                label_rationale=label["rationale"],
                required_decision=pending[pending_positions[key] - 1]["required_decision"],
                evidence_jsonl="data/review/pending.jsonl", evidence_record=pending_positions[key],
            ))
    stats = summary(rows, [])
    stats.pop("target")
    stats.pop("deficit")
    stats["target_per_risk"] = 250
    stats["limit_per_risk"] = limit_per_risk
    stats["pending"] = len(pending)
    stats["pairing"] = "deferred"
    stats["deduplication"] = "deferred"
    utility_changes = [{"candidate_id": key, "proposed_util": labels[key]["util"],
                        "canonical_util": item["row"]["util"]}
                       for key, item in selected.items() if labels[key]["util"] != item["row"]["util"]]
    stats["utility_displays_normalized"] = len(utility_changes)
    for value in stats["by_risk"].values():
        value["deficit"] = max(0, 250 - value["total"])
    files = {
        "commands.csv": commands, "data/processed/provenance.jsonl": provenance_text,
        "research/sources.jsonl": jsonl(source_map.values()),
        "data/review/pending.jsonl": jsonl(pending), "review/pending.csv": csv_text(pending_rows, PENDING_FIELDS),
        "review/commands.csv": csv_text(review_rows, REVIEW_FIELDS), "statistics.json": json_text(stats),
        "data/review/utility-normalization.jsonl": jsonl(utility_changes),
    }
    all_fields = (*REVIEW_FIELDS, "label_decision", "review_verdict", "export_status", "exclusion_reason", "proposed_util")
    files["review/labeled_all.csv"] = csv_text(all_review_rows, all_fields)
    for risk in RISKS:
        code = risk.split()[0]
        files[f"data/candidates/{code}/candidates.jsonl"] = jsonl(
            item for item in all_candidates if item["row"]["risk"] == risk)
        files[f"review/{code}.csv"] = csv_text(
            [item for item in review_rows if item["risk"] == risk], REVIEW_FIELDS)
        files[f"review/labeled-{code}.csv"] = csv_text(
            [item for item in all_review_rows if item["risk"] == risk], all_fields)
    require(sum(item["row"]["risk"] in RISKS for item in all_candidates) == len(all_candidates),
            "Noncanonical original risk cannot be placed in bundle")
    write_new_tree(Path(out), files)
    return stats
