import re
from hashlib import sha256
from typing import assert_never

import pandas as pd
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.human_review import HumanReviewError, ReviewSnapshot
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.review_api_models import EvidenceResponse
from codebench_pipeline.review_snapshots import ReviewManifest, ReviewSnapshots
from codebench_pipeline.storage import IntegrityError


def _inference(
    manifest: ReviewManifest,
    candidate_id: str,
    branch_prefix: str = "",
    unified_id: str | None = None,
) -> dict[str, JsonValue]:
    """Read this candidate's labels and any retained historical review artifacts."""
    result: dict[str, JsonValue] = {}
    store = ObjectStore(manifest.storage)
    for stage, invocation in (("proposed_annotations", "label"), ("reviewed_annotations", "review")):
        shared_review = invocation == "review" and "merged_proposed_annotations" in manifest.references
        shared_label = invocation == "label" and "merged_command_candidates" in manifest.references
        evidence_stage = stage if shared_review or shared_label else f"{branch_prefix}{stage}"
        if invocation == "review" and evidence_stage not in manifest.references:
            continue
        identity = unified_id if (shared_review or shared_label) and unified_id is not None else candidate_id
        candidate_hash = sha256(identity.encode("utf-8")).hexdigest()
        metadata = TypeAdapter(dict[str, JsonValue]).validate_python(manifest.evidence[evidence_stage])
        receipts = TypeAdapter(dict[str, str]).validate_python(metadata["receipts"], strict=True)
        artifacts: dict[str, JsonValue] = {}
        prefix = f"{invocation}/{candidate_hash}/"
        for name, reference in receipts.items():
            if not name.startswith(prefix):
                continue
            matched = re.search(r"/(evidence/[0-9a-f]{64})$", reference)
            if matched is None or store.uri(matched.group(1)) != reference:
                raise IntegrityError("Inference evidence reference has a different storage binding")
            text = store.read(matched.group(1)).decode("utf-8")
            filename = name.removeprefix(prefix)
            if filename.endswith(".json"):
                value = TypeAdapter(JsonValue).validate_json(text)
                if filename == "receipt.json":
                    receipt = TypeAdapter(dict[str, JsonValue]).validate_python(value)
                    value = {key: item for key, item in receipt.items() if key != "provider_config"}
                artifacts[filename] = value
            else:
                artifacts[filename] = text
        result[evidence_stage] = artifacts
    return result


def review_evidence(
    snapshots: ReviewSnapshots, snapshot: ReviewSnapshot, candidate_id: str
) -> EvidenceResponse:
    manifest = snapshots.manifest(snapshot.snapshot_id)
    if manifest.dataset_id != snapshot.dataset_id:
        raise HumanReviewError("conflict", "Review manifest has a different dataset binding")
    review_stage = (
        "deduplicated_annotations"
        if "deduplicated_annotations" in manifest.references
        else "reviewed_annotations"
    )
    frame = snapshots.frame(manifest, review_stage)
    reviewed = [row for row in frame_records(frame) if row["candidate_id"] == candidate_id]
    if len(reviewed) != 1:
        raise HumanReviewError("conflict", "Immutable review evidence lacks a unique candidate")
    row = reviewed[0]
    origin = TypeAdapter(dict[str, JsonValue]).validate_python(
        frame.attrs.get("origins", {}).get(candidate_id, {})
    )
    shared_processing = "merged_command_candidates" in manifest.references
    branch_prefix = "documentation/" if origin.get("branch") == "documentation" else ""
    sources = [
        source
        for source in frame_records(snapshots.frame(manifest, f"{branch_prefix}source_documents"))
        if source["source_id"] == origin.get("source_id", row["source_id"])
    ]
    if len(sources) != 1:
        raise HumanReviewError("conflict", "Immutable review evidence lacks a unique source")
    documentation: list[JsonValue] = [
        doc
        for doc in frame_records(
            snapshots.frame(
                manifest,
                "labeling_documentation" if shared_processing else f"{branch_prefix}labeling_documentation",
            )
        )
        if doc["utility"] == row["util"]
        or doc["url"] in TypeAdapter(list[str]).validate_python(row["documentation_urls"], strict=True)
    ]
    duplicates = TypeAdapter(list[dict[str, JsonValue]]).validate_python(frame.attrs.get("duplicates", []))
    duplicate_evidence: list[JsonValue] = []
    merged_stage = next(
        (
            stage
            for stage in (
                "reviewed_annotations",
                "proposed_annotations" if shared_processing else "merged_proposed_annotations",
                "merged_reviewed_annotations",
            )
            if stage in manifest.references
        ),
        None,
    )
    if merged_stage is not None and duplicates:
        merged = snapshots.frame(manifest, merged_stage)
        duplicate_ids = {
            str(item["duplicate_id"]) for item in duplicates if item["survivor_id"] == candidate_id
        }
        origins = TypeAdapter(dict[str, dict[str, str]]).validate_python(merged.attrs["origins"])
        for duplicate in frame_records(merged):
            identifier = str(duplicate["candidate_id"])
            if identifier not in duplicate_ids:
                continue
            duplicate_origin = origins[identifier]
            prefix = "documentation/" if duplicate_origin["branch"] == "documentation" else ""
            duplicate_evidence.append(
                {
                    "reviewed_annotation": duplicate,
                    "origin": {key: value for key, value in duplicate_origin.items()},
                    "source_document": next(
                        source
                        for source in frame_records(snapshots.frame(manifest, f"{prefix}source_documents"))
                        if source["source_id"] == duplicate_origin["source_id"]
                    ),
                    "inference": _inference(manifest, duplicate_origin["candidate_id"], prefix, identifier),
                }
            )
    taxonomy = DataFrameIOManager(storage=manifest.storage).read_version(
        manifest.references["category_rules"]
    )
    match taxonomy:
        case CategoryRules():
            rules = taxonomy.model_dump(mode="json")
        case pd.DataFrame():
            raise IntegrityError("Immutable taxonomy is not category rules")
        case _:
            assert_never(taxonomy)
    return EvidenceResponse(
        fields={
            "reviewed_annotation": row,
            "source_document": sources[0],
            "documentation": documentation,
            "taxonomy": rules,
            "origin": origin,
            "duplicates": TypeAdapter(JsonValue).validate_python(frame.attrs.get("duplicates", [])),
            "duplicate_evidence": duplicate_evidence,
            "inference": _inference(
                manifest,
                str(origin.get("candidate_id", candidate_id)),
                branch_prefix,
                candidate_id,
            ),
        }
    )
