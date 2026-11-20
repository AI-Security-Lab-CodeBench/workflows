import argparse
import json
import os
import re
import shlex
import sys
import tempfile
from pathlib import Path

from category_config import ScriptPolicy, Taxonomy, add_policy_options, cli_script_policy, cli_taxonomy
from command_filter import command_words
from label_contract import (
    DatasetError,
    facts,
    load_exchange,
    load_inputs,
    make_packet,
    read_json,
    require,
    validate_packet,
    validate_response,
    validate_semantics,
)
from validate_dataset import load_jsonl


def write_new_tree(out, files):
    out = Path(out)
    require(not out.exists(), f"Output already exists; use a fresh directory: {out}")
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".label-pipeline-", dir=out.parent) as temporary:
        root = Path(temporary) / "bundle"
        root.mkdir()
        for relative, text in files.items():
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(text, encoding="utf-8", newline="")
        require(not out.exists(), "Output appeared concurrently; refusing to overwrite")
        os.rename(root, out)


def json_text(value):
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"


def documentation_for(candidate, docs):
    command = re.sub(r"\s+", " ", candidate["row"]["command"].replace("\\\n", " "))
    tokens = shlex.split(command)
    words = command_words(tokens)
    routed = [
        doc
        for doc in docs
        if doc["utility"] in candidate["row"]["util"].split("; ")
        or re.search(r"(?<![\w-])" + re.escape(doc["utility"]) + r"(?=\s|$)", command)
    ]
    relevant = []
    for doc in routed:
        for operation in doc["subcommands"]:
            if operation == doc["utility"] and doc["utility"] in {"vercel", "redis-cli"}:
                invocation = (
                    command_words([token for token in tokens if token != "-c"])
                    if doc["utility"] == "redis-cli"
                    else words
                )
                matches = invocation == doc["utility"].split()
            else:
                pattern = r"(?<![\w-])" + re.escape(operation) + r"(?![\w-])"
                matches = any(re.search(pattern, surface, re.I) for surface in (command, " ".join(words)))
            if matches:
                relevant.append(doc)
                break
    return relevant


def prepare(candidates, sources, semantics, out, batch_size=8, *, policy: ScriptPolicy):
    require(type(batch_size) is int and batch_size > 0, "batch-size must be positive")
    candidate_map, source_map = load_inputs(candidates, sources, policy.taxonomy)
    docs = validate_semantics(load_jsonl(Path(semantics)))
    evidence, missing = {}, []
    for key, candidate in candidate_map.items():
        relevant = documentation_for(candidate, docs)
        if not relevant:
            missing.append(key)
            continue
        evidence[key] = facts(candidate, source_map, relevant)
    require(not missing, "No semantics snippets for " + ", ".join(missing))
    ids = sorted(candidate_map)
    files = {}
    for offset in range(0, len(ids), batch_size):
        selected = ids[offset : offset + batch_size]
        payload = {"candidates": [evidence[key] for key in selected]}
        system_prompt = policy.render_prompt([evidence[key]["row"]["command"] for key in selected])
        packet = make_packet("label", selected, payload, system_prompt, taxonomy=policy.taxonomy)
        files[f"packets/label-{offset // batch_size + 1:05d}.json"] = json_text(packet)
    write_new_tree(out, files)
    return {"valid": True, "candidates": len(ids), "packets": len(files), "out": str(out)}


def review_prepare(packets, responses, out, *, taxonomy: Taxonomy):
    packet_map, envelopes, records = load_exchange(packets, responses, "label", taxonomy)
    files = {}
    for name, original in packet_map.items():
        payload = {"original_packet": original, "label_response": envelopes[name]}
        packet = make_packet(
            "review", original["candidate_ids"], payload, original["system_prompt"], taxonomy=taxonomy
        )
        files[f"packets/{name}"] = json_text(packet)
    write_new_tree(out, files)
    return {"valid": True, "candidates": len(records), "packets": len(files), "out": str(out)}


def repair(packets, responses, out, batch_size=6, *, taxonomy: Taxonomy):
    require(type(batch_size) is int and batch_size > 0, "batch-size must be positive")
    files, receipt = {}, []
    paths = sorted(Path(packets).glob("*.json"))
    require(bool(paths), "No label packets to repair")
    for path in paths:
        original = validate_packet(read_json(path), "label", taxonomy)
        response = Path(responses) / path.name
        if response.exists():
            envelope = read_json(response)
            require(envelope["packet_sha256"] == original["packet_sha256"], "Response hash mismatch")
            validate_response(original, envelope["result"], taxonomy)
            files[f"packets/{path.name}"] = path.read_text(encoding="utf-8")
            files[f"responses/{path.name}"] = response.read_text(encoding="utf-8")
            receipt.append({"packet": path.name, "action": "preserved", "original_response": str(response)})
            continue
        facts_by_id = {item["candidate_id"]: item for item in original["input"]["candidates"]}
        ids = original["candidate_ids"]
        for offset in range(0, len(ids), batch_size):
            selected = ids[offset : offset + batch_size]
            payload = {"candidates": [facts_by_id[key] for key in selected]}
            packet = make_packet("label", selected, payload, original["system_prompt"], taxonomy=taxonomy)
            name = f"{path.stem}-retry-{offset // batch_size + 1:03d}.json"
            files[f"packets/{name}"] = json_text(packet)
            receipt.append(
                {
                    "packet": name,
                    "action": "reduced_batch",
                    "parent_packet": str(path),
                    "parent_sha256": original["packet_sha256"],
                    "candidate_ids": selected,
                }
            )
    files["recovery.json"] = json_text(receipt)
    write_new_tree(out, files)
    return {
        "valid": True,
        "preserved": sum(row["action"] == "preserved" for row in receipt),
        "retry_packets": sum(row["action"] == "reduced_batch" for row in receipt),
        "out": str(out),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Inert isolated labeling/review exchange; no provider or execution tools."
    )
    add_policy_options(parser)
    commands = parser.add_subparsers(dest="action", required=True)
    prepare_parser = commands.add_parser("prepare")
    for name in ("candidates", "sources", "semantics", "out"):
        prepare_parser.add_argument(f"--{name}", type=Path, required=True)
    prepare_parser.add_argument("--batch-size", type=int, default=8)
    review_parser = commands.add_parser("review-prepare")
    for name in ("packets", "responses", "out"):
        review_parser.add_argument(f"--{name}", type=Path, required=True)
    repair_parser = commands.add_parser("repair")
    for name in ("packets", "responses", "out"):
        repair_parser.add_argument(f"--{name}", type=Path, required=True)
    repair_parser.add_argument("--batch-size", type=int, default=6)
    export_parser = commands.add_parser("export")
    for name in ("candidates", "sources", "packets", "responses", "review-packets", "reviews", "out"):
        export_parser.add_argument(f"--{name}", type=Path, required=True)
    export_parser.add_argument("--limit-per-risk", type=int, default=250)
    invocation = parser.parse_args(argv)
    args = {
        key: value
        for key, value in vars(invocation).items()
        if key not in {"compiled_policy", "category_config"}
    }
    action = args.pop("action")
    try:
        if action == "prepare":
            result = prepare(**args, policy=cli_script_policy(invocation))
        elif action == "review-prepare":
            result = review_prepare(**args, taxonomy=cli_taxonomy(invocation))
        elif action == "repair":
            result = repair(**args, taxonomy=cli_taxonomy(invocation))
        else:
            from pipeline_export import export

            result = export(**args, taxonomy=cli_taxonomy(invocation))
        print(json_text(result), end="")
        return 0
    except (DatasetError, OSError, UnicodeError, ValueError, TypeError, KeyError, RecursionError) as error:
        print(json_text({"valid": False, "errors": [str(error)]}), end="")
        return 1


if __name__ == "__main__":
    sys.exit(main())
