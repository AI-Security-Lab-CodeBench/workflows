from pathlib import Path

from label_io import indexed, require, timestamp
from validate_dataset import (
    CANDIDATE_FIELDS, FIELDS, RISKS, SOURCE_FIELDS, contains_secret, load_jsonl, present, string_list, web_url,
)


SEMANTICS_FIELDS = ("utility", "subcommands", "url", "retrieved_at", "snippet", "version_limits")


def load_inputs(candidates_path, sources_path):
    path = Path(candidates_path)
    candidates = load_jsonl(path) if path.is_file() else [
        item for file in sorted(path.rglob("candidates.jsonl")) for item in load_jsonl(file)
    ]
    require(bool(candidates), "No selected candidates")
    candidate_map = indexed(candidates, "candidate_id", CANDIDATE_FIELDS)
    source_map = indexed(load_jsonl(Path(sources_path)), "source_id", SOURCE_FIELDS)
    for source in source_map.values():
        require(all(present(source[field]) for field in SOURCE_FIELDS
                    if field not in {"risks", "commands_present", "notes"}), "Malformed source evidence")
        require(isinstance(source["notes"], str), "Source notes must be text")
        require(web_url(source["url"]) and web_url(source["license_url"]), "Malformed source URLs")
        require(type(source["commands_present"]) is bool, "Source commands_present must be boolean")
        require(string_list(source["risks"], nonempty=True)
                and all(risk in RISKS or risk in {item.split()[0] for item in RISKS}
                        for risk in source["risks"]), "Malformed source risks")
        timestamp(source["retrieved_at"], "Source", date_only=True)
    for candidate in candidate_map.values():
        row = candidate["row"]
        require(isinstance(row, dict) and set(row) == set(FIELDS), "Incorrect candidate row schema")
        require(all(isinstance(row[field], str) for field in FIELDS), "Candidate row fields must be text")
        require(all(present(row[field]) for field in ("command", "util", "source", "seed")),
                "Missing candidate command or origin")
        require(row["seed"] != "-" if row["source"] == "synthetic" else row["seed"] == "-",
                "Invalid candidate source/seed")
        require(present(candidate["synthetic_model"]) if row["source"] == "synthetic"
                else candidate["synthetic_model"] in (None, ""), "Invalid synthetic model evidence")
        require(candidate["status"] in {"candidate", "accepted", "rejected", "needs_review"},
                "Invalid candidate status")
        for field in ("source_locator", "evidence_context", "scenario_id"):
            require(present(candidate[field]), f"Missing candidate evidence: {field}")
        require(candidate["tool_version"] is None or isinstance(candidate["tool_version"], str),
                "Malformed tool version")
        require(candidate["label_rationale"] is None or isinstance(candidate["label_rationale"], str),
                "Malformed previous rationale")
        require(candidate["role"] in {"dangerous", "counterexample", "unpaired"}, "Invalid candidate role")
        for field in ("source_ids", "paired_candidate_ids", "transformations", "documentation_urls"):
            require(string_list(candidate[field], nonempty=field == "source_ids"),
                    f"Malformed candidate list: {field}")
        require(all(web_url(url) for url in candidate["documentation_urls"]), "Malformed documentation URLs")
        require(all(source_id in source_map for source_id in candidate["source_ids"]), "Unknown source ID")
    return candidate_map, source_map


def validate_semantics(records):
    for record in records:
        require(isinstance(record, dict) and set(record) == set(SEMANTICS_FIELDS), "Malformed semantics schema")
        require(all(present(record[field]) for field in SEMANTICS_FIELDS if field != "subcommands"),
                "Malformed semantics evidence")
        require(string_list(record["subcommands"], nonempty=True), "Semantics subcommands must be a nonempty list")
        require(web_url(record["url"]) and not contains_secret(record), "Malformed semantics URL or secret indicator")
        timestamp(record["retrieved_at"], "Semantics", date_only=True)
    return records
