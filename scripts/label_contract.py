import json
import math
import re
import shlex
from pathlib import Path

from category_config import load_labeling_prompt
from command_filter import command_words
from label_inputs import SEMANTICS_FIELDS as SEMANTICS_FIELDS
from label_inputs import load_inputs as load_inputs
from label_inputs import validate_semantics
from label_io import digest, indexed, read_json, require, timestamp
from validate_dataset import (
    CANDIDATE_FIELDS,
    CATEGORY,
    RISKS,
    SOURCE_FIELDS,
    present,
    string_list,
    web_url,
)
from validate_dataset import FIELDS as FIELDS
from validate_dataset import DatasetError as DatasetError
from validate_dataset import contains_secret as contains_secret
from validate_dataset import load_jsonl as load_jsonl

CONFIDENCES = tuple(f"{field}_confidence" for field in ("category", "risk", "severity", "class"))
LABEL_FIELDS = (
    "candidate_id", "decision", "category", "risk", "severity", "class",
    "description", "util", *CONFIDENCES, "rationale", "documentation_urls",
)
FACT_FIELDS = tuple(field for field in CANDIDATE_FIELDS if field not in {
    "row", "status", "label_rationale", "role", "scenario_id", "paired_candidate_ids",
})


def utility_matches(candidate, proposed):
    declared = [part.strip() for part in candidate["row"]["util"].split(";")]
    tokens = shlex.split(candidate["row"]["command"].replace("\\\n", " "))
    words = command_words(tokens)
    if not words or words[0].rsplit("/", 1)[-1] not in declared:
        return False
    words[0] = words[0].rsplit("/", 1)[-1]
    display = re.split(r"[;\s]+", proposed.strip())
    return display == declared or display == words[:len(display)] or display == tokens[:len(display)]




def facts(candidate, sources, semantics):
    item = {field: candidate[field] for field in FACT_FIELDS}
    item["row"] = {field: candidate["row"][field] for field in ("command", "util", "source", "seed")}
    item["candidate_sha256"] = digest(candidate)
    item["source_evidence"] = [sources[key] for key in candidate["source_ids"]]
    item["documentation"] = semantics
    item["documentation_urls"] = list(dict.fromkeys(doc["url"] for doc in semantics))
    return item


def response_schema(stage, ids=(), documentation_urls=()):
    properties = {"candidate_id": {"type": "string", "minLength": 1},
                  "rationale": {"type": "string", "minLength": 1, "maxLength": 400}}
    if stage == "label":
        properties.update({
            "decision": {"enum": ["accept", "needs_review", "reject"]},
            "category": {"enum": [CATEGORY]}, "risk": {"enum": list(RISKS)},
            "severity": {"enum": ["critical", "high", "medium", "low"]},
            "class": {"enum": ["allow", "deny", "ask"]},
            "description": {"type": "string", "minLength": 1},
            "util": {"type": "string", "minLength": 1},
            "documentation_urls": {"type": "array", "minItems": 1, "uniqueItems": True,
                                   "items": {"type": "string", "format": "uri"}},
        })
        properties.update({field: {"type": "number", "minimum": 0, "maximum": 1} for field in CONFIDENCES})
        if documentation_urls:
            properties["documentation_urls"]["items"]["enum"] = sorted(set(documentation_urls))
    else:
        properties["verdict"] = {"enum": ["approve", "needs_review", "reject"]}
    key = "labels" if stage == "label" else "reviews"
    if ids:
        properties["candidate_id"]["enum"] = list(ids)
    array = {"type": "array", "items": {
        "type": "object", "additionalProperties": False,
        "required": list(properties), "properties": properties,
    }}
    if ids:
        array.update(minItems=len(ids), maxItems=len(ids))
    return {"type": "object", "additionalProperties": False, "required": [key],
            "properties": {key: array}}


def make_packet(stage, ids, payload, system_prompt=None, schema=None):
    instruction = ("Label independently from evidence; no previous verdict is supplied." if stage == "label"
                   else "Independently audit every proposed label and all four confidences against original "
                   "source truth and documentation. Do not rubber-stamp the labeler.")
    urls = [doc["url"] for item in payload["candidates"] for doc in item["documentation"]] if stage == "label" else []
    schema = response_schema(stage, ids, urls) if schema is None else schema
    packet = {"stage": stage, "candidate_ids": ids, "input": payload,
              "response_schema": schema,
              "system_prompt": system_prompt if system_prompt is not None else load_labeling_prompt(
                  [item["row"]["command"] for item in payload["candidates"]] if stage == "label" else []
              ),
              "prompt": instruction + "\nReturn exactly one result for each TARGET_CANDIDATE_ID below. "
                        "Assess each record independently; linking and deduplication are deferred. "
                        "Give a rationale of at most 400 characters. For a withheld label, name the "
                        "specific unresolved problem and what evidence would resolve it."
                        "\nTARGET_CANDIDATE_IDS:\n" + json.dumps(ids)
                        + "\nRESPONSE_SCHEMA:\n"
                        + json.dumps(schema, ensure_ascii=False, allow_nan=False) + "\nINPUT:\n"
                        + json.dumps(payload, ensure_ascii=False, allow_nan=False)}
    packet["packet_sha256"] = digest(packet)
    return packet


def validate_packet(packet, stage):
    require(isinstance(packet, dict) and set(packet) == {
        "stage", "candidate_ids", "input", "response_schema", "system_prompt", "prompt", "packet_sha256",
    }, "Malformed packet schema")
    require(packet["stage"] == stage, "Wrong packet stage")
    require(string_list(packet["candidate_ids"], nonempty=True), "Malformed packet IDs")
    require(packet["packet_sha256"] == digest({key: value for key, value in packet.items()
                                              if key != "packet_sha256"}), "Altered packet hash")
    urls = [doc["url"] for item in packet["input"]["candidates"] for doc in item["documentation"]] if stage == "label" else []
    require(packet["response_schema"] in (
        response_schema(stage, packet["candidate_ids"]),
        response_schema(stage, packet["candidate_ids"], urls),
    ), "Altered response schema")
    require(present(packet["system_prompt"]), "Missing system instructions")
    require(packet == make_packet(stage, packet["candidate_ids"], packet["input"], packet["system_prompt"],
                                  packet["response_schema"]),
            "Altered packet prompt")
    return packet


def validate_result(result, stage, ids):
    key = "labels" if stage == "label" else "reviews"
    require(isinstance(result, dict) and set(result) == {key} and isinstance(result[key], list),
            "Malformed result object")
    fields = LABEL_FIELDS if stage == "label" else ("candidate_id", "verdict", "rationale")
    records = indexed(result[key], "candidate_id", fields)
    require(set(records) == set(ids), "Missing or unknown result IDs")
    for item in records.values():
        require(present(item["rationale"]) and len(item["rationale"]) <= 400,
                "Rationale must contain 1 to 400 characters")
        if stage == "review":
            require(item["verdict"] in {"approve", "needs_review", "reject"}, "Invalid review verdict")
            continue
        require(item["decision"] in {"accept", "needs_review", "reject"}, "Invalid label decision")
        require(item["category"] == CATEGORY and item["risk"] in RISKS, "Invalid category/risk")
        require(item["severity"] in {"critical", "high", "medium", "low"}, "Invalid severity")
        require(item["class"] in {"allow", "deny", "ask"}, "Invalid class")
        require(present(item["util"]) and present(item["description"])
                and re.search(r"[А-Яа-яЁё]", item["description"])
                and "\n" not in item["description"]
                and len(re.split(r"[.!?]+(?:\s|$)", item["description"].strip().rstrip(".!?"))) == 1,
                "Expected one Russian description sentence and utility")
        require(string_list(item["documentation_urls"], nonempty=True)
                and all(web_url(url) for url in item["documentation_urls"]), "Invalid label documentation URLs")
        for field in CONFIDENCES:
            value = item[field]
            require(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1,
                    f"Invalid confidence: {field}")
    return records


def validate_response(packet, result):
    records = validate_result(result, packet["stage"], packet["candidate_ids"])
    if packet["stage"] == "label":
        payload = validate_label_input(packet)
        for item in payload["candidates"]:
            supplied = {doc["url"] for doc in item["documentation"]}
            require(set(records[item["candidate_id"]]["documentation_urls"]) <= supplied,
                    "Label cites documentation not present in packet")
    return records


def validate_label_input(packet):
    payload = packet["input"]
    require(isinstance(payload, dict) and set(payload) == {"candidates"}, "Malformed packet evidence")
    require(isinstance(payload["candidates"], list), "Malformed packet evidence list")
    require([item.get("candidate_id") for item in payload["candidates"] if isinstance(item, dict)]
            == packet["candidate_ids"], "Packet evidence IDs differ")
    fields = (*FACT_FIELDS, "row", "candidate_sha256", "source_evidence", "documentation")
    for group in payload.values():
        records = indexed(group, "candidate_id", fields)
        for item in records.values():
            require(isinstance(item["row"], dict) and set(item["row"]) == {"command", "util", "source", "seed"}
                    and all(present(value) for value in item["row"].values()), "Malformed command evidence")
            require(isinstance(item["candidate_sha256"], str)
                    and re.fullmatch(r"[0-9a-f]{64}", item["candidate_sha256"]), "Malformed candidate hash")
            for field in ("source_locator", "evidence_context"):
                require(present(item[field]), f"Missing packet evidence: {field}")
            for field in ("source_ids", "documentation_urls", "transformations"):
                require(string_list(item[field], nonempty=field == "source_ids"), "Malformed packet evidence list")
            require(isinstance(item["source_evidence"], list), "Malformed packet source evidence")
            sources = indexed(item["source_evidence"], "source_id", SOURCE_FIELDS)
            require(list(sources) == item["source_ids"], "Packet source evidence IDs differ")
            for source in sources.values():
                require(web_url(source["url"]) and web_url(source["license_url"])
                        and all(present(source[field]) for field in (
                            "locator", "revision", "retrieved_at", "source_type", "relevance", "license",
                            "accessibility", "reuse_decision",
                        )) and type(source["commands_present"]) is bool
                        and string_list(source["risks"], nonempty=True), "Malformed packet source evidence")
                timestamp(source["retrieved_at"], "Source", date_only=True)
            require(isinstance(item["documentation"], list) and bool(item["documentation"]),
                    "Missing packet documentation")
            validate_semantics(item["documentation"])
    return payload


def load_exchange(packets_dir, responses_dir, stage):
    packets_dir, responses_dir = Path(packets_dir), Path(responses_dir)
    packet_files = sorted(packets_dir.glob("*.json"))
    require(bool(packet_files), "No exchange packets")
    require({path.name for path in packet_files} == {path.name for path in responses_dir.glob("*.json")},
            "Missing or unknown response files")
    packets, envelopes, records = {}, {}, {}
    for path in packet_files:
        packet = validate_packet(read_json(path), stage)
        if stage == "label":
            validate_label_input(packet)
        envelope = read_json(responses_dir / path.name)
        required_fields = {"packet_sha256", "model", "responded_at", "result"}
        require(isinstance(envelope, dict) and required_fields <= set(envelope)
                and set(envelope) <= required_fields | {"invocation_id"}, "Malformed response envelope")
        if "invocation_id" in envelope:
            require(present(envelope["invocation_id"]), "Invalid optional invocation_id")
        require(envelope["packet_sha256"] == packet["packet_sha256"], "Response packet hash mismatch")
        require(present(envelope["model"]), "Missing model identifier or honest alias")
        timestamp(envelope["responded_at"], "Response")
        results = validate_response(packet, envelope["result"])
        require(not set(results).intersection(records), "Duplicate candidate across packets")
        records.update(results)
        packets[path.name], envelopes[path.name] = packet, envelope
    return packets, envelopes, records
