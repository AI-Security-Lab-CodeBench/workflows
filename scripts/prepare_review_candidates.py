from __future__ import annotations

import argparse
import shlex
from pathlib import Path

if __package__:
    from .category_config import Taxonomy, load_taxonomy
    from .command_filter import command_words
    from .source_pipeline import source_eligible, write_jsonl
    from .validate_dataset import DEFAULT_TAXONOMY, load_jsonl
else:
    from category_config import Taxonomy, load_taxonomy
    from command_filter import command_words
    from source_pipeline import source_eligible, write_jsonl
    from validate_dataset import DEFAULT_TAXONOMY, load_jsonl


def prepare(args: argparse.Namespace, taxonomy: Taxonomy = DEFAULT_TAXONOMY) -> dict[str, int]:
    candidates = load_jsonl(args.inputs / "candidates.jsonl")
    candidate_ids = set()
    for candidate in candidates:
        candidate_id = candidate["candidate_id"]
        if candidate_id in candidate_ids:
            raise ValueError(f"Duplicate candidate record identifier: {candidate_id}")
        candidate_ids.add(candidate_id)
        candidate.update(paired_candidate_ids=[], role="unpaired", scenario_id="unpaired:" + candidate_id)
    sources = {row["source_id"]: row for row in load_jsonl(args.inputs / "sources.jsonl")}
    documents = {row["source_id"]: row for row in load_jsonl(args.documents)}
    codes = {risk.id: risk.canonical for risk in taxonomy.risks}
    added = 0
    raw_ids = set()
    for raw in load_jsonl(args.eligible):
        raw_id = raw["raw_id"]
        if raw_id in raw_ids:
            raise ValueError(f"Duplicate raw record identifier: {raw_id}")
        raw_ids.add(raw_id)
        if raw.get("candidate_id"):
            continue
        document = documents[raw["source_id"]]
        if not source_eligible(document):
            raise ValueError(f"Ineligible source: {raw['source_id']}")
        (risk,) = raw["risk_candidates"]
        tokens = shlex.split(raw["command"].replace("\\\n", " "))
        words = command_words(tokens)
        if not words:
            raise ValueError(f"No utility routing evidence: {raw['raw_id']}")
        util = words[0].rsplit("/", 1)[-1]
        source_id = raw["source_id"]
        if source_id not in sources:
            sources[source_id] = {
                "source_id": source_id,
                "url": raw["doc_full_url"],
                "locator": document.get("source_locator", document["local_path"]),
                "revision": document["revision"],
                "retrieved_at": document["retrieved_at"],
                "source_type": document["source_type"],
                "risks": [risk],
                "accessibility": "accessible",
                "relevance": "Original command document; raw records preserved before cleanup",
                "commands_present": True,
                "license": document["license"],
                "license_url": document["license_url"],
                "reuse_decision": "permitted",
                "notes": "Source origin and terms recorded in research/pipeline; no command execution or synthetic expansion.",
            }
        elif risk not in sources[source_id]["risks"]:
            sources[source_id]["risks"].append(risk)
        candidate_id = risk + "-raw-" + raw["raw_id"][:16]
        if candidate_id in candidate_ids:
            raise ValueError(f"Duplicate candidate record identifier: {candidate_id}")
        candidate_ids.add(candidate_id)
        candidates.append(
            {
                "candidate_id": candidate_id,
                "row": {
                    "command": raw["command"],
                    "description": "",
                    "util": util,
                    "severity": "",
                    "category": taxonomy.canonical,
                    "risk": codes[risk],
                    "source": "real",
                    "seed": "-",
                    "class": "",
                },
                "source_ids": [source_id],
                "source_locator": raw["doc_pos"],
                "evidence_context": raw["context"],
                "transformations": raw["transformations"],
                "documentation_urls": [],
                "tool_version": "Source revision "
                + document["revision"]
                + "; installed CLI version is not established.",
                "label_rationale": None,
                "scenario_id": "unpaired:" + candidate_id,
                "role": "unpaired",
                "paired_candidate_ids": [],
                "synthetic_model": None,
                "status": "candidate",
            }
        )
        added += 1
    write_jsonl(args.out / "candidates.jsonl", candidates)
    write_jsonl(args.out / "sources.jsonl", list(sources.values()))
    return {"candidates": len(candidates), "new_unpaired_candidates": added}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Route all post-filtered commands to isolated labeling, without inventing pairs."
    )
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--eligible", type=Path, required=True)
    parser.add_argument("--documents", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--category-config", type=Path)
    args = parser.parse_args()
    import json

    taxonomy = load_taxonomy(args.category_config) if args.category_config else DEFAULT_TAXONOMY
    print(json.dumps(prepare(args, taxonomy)))


if __name__ == "__main__":
    main()
