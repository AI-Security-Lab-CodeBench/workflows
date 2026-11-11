import json
from pathlib import Path
from typing import Literal, assert_never

import pandas as pd
from dagster import DagsterInstance, OutputContext
from pydantic import ValidationError

from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import (
    AssetReference,
    AssetSnapshot,
    DataFrameIOManager,
    MetadataSink,
    read_frame,
)
from codebench_pipeline.dataframe_schemas import CommandsTable, frame_records, validated_frame
from codebench_pipeline.dataset_branches import Branch, asset_branch
from codebench_pipeline.dataset_export import csv_text
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.human_review import CompletionToken, HumanReviewError, PublicationPayload
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import ObjectStore, sha256
from codebench_pipeline.publication_authorization import authorize_publication
from codebench_pipeline.review_frames import REVIEW_BINDING, decision_frame, require_decision_frame
from codebench_pipeline.review_snapshots import ReviewSnapshots, load_manifest
from codebench_pipeline.storage import IntegrityError


def record_review(context: MetadataSink, token: CompletionToken, publication_id: str = "") -> None:
    context.add_output_metadata(
        {
            "human_review/batch_id": token.batch_id,
            "human_review/snapshot_id": token.snapshot_id,
            "human_review/generation": token.generation,
            "human_review/decision_digest": token.decision_digest,
            "human_review/publication_id": publication_id,
        }
    )


def write_review_output(
    manager: DataFrameIOManager, context: OutputContext, frame: pd.DataFrame
) -> AssetReference:
    if tuple(context.asset_key.path) not in {
        ("datasets", "human_review_decisions"),
        ("datasets", "dataset_release"),
    }:
        raise HumanReviewError("conflict", "Only shared human-reviewed outputs can be published")
    if manager.review_home is None:
        raise HumanReviewError("pending", "A registered publication authority is required")
    try:
        if manager.review_token is not None:
            token = CompletionToken.model_validate_json(manager.review_token)
        else:
            raise HumanReviewError("pending", "A completed human review is required")
    except ValidationError as error:
        raise HumanReviewError("stale", "Invalid review authorization") from error
    home = Path(manager.review_home)
    data = DatasetDataService(home, context.step_context.instance)
    branch = asset_branch(context.asset_key.path)
    snapshots = ReviewSnapshots(home, data, branch)
    with HumanReviewStore(home).transaction(timeout=10) as tx:
        resolved = snapshots.current(context.asset_partition_key)
        if manager.storage.model_dump() != resolved.manifest.storage.model_dump():
            raise HumanReviewError("conflict", "Release storage differs from the registered dataset")
        authorization_snapshot = authorize_publication(resolved, token, tx)
        decisions = tx.list_decisions(token.batch_id)
        expected_decisions = decision_frame(token, decisions)
        if context.asset_key.path[-1] == "human_review_decisions":
            require_decision_frame(frame, expected_decisions)
            reference = manager.persist_value(expected_decisions)
            manager.publish_reference(context, reference)
            record_review(context, token)
            return reference
        approved = frozenset(row.candidate_id for row in decisions if row.decision == "approve")
        prepared = publication(
            resolved.reviewed, resolved.rules, resolved.manifest.publication, approved_ids=approved
        )
        expected = validated_frame(
            [{key: value for key, value in row.items()} for row in prepared.commands], CommandsTable
        )
        if not frame.equals(expected):
            raise HumanReviewError("stale", "Release rows differ from approved eligible records")
        expected.attrs[REVIEW_BINDING] = token.model_dump(mode="json")
        reference = manager.persist_value(expected)
        prefix = f"releases/{resolved.snapshot.dataset_id}/{branch}/{context.run_id}"
        store = ObjectStore(manager.storage)
        for name, text in prepared.files.items():
            store.write(f"{prefix}/{name}", text.encode())
        store.write(
            f"{prefix}/deduplication.json",
            json.dumps(
                {
                    "origins": resolved.reviewed.attrs.get("origins", {}),
                    "duplicates": resolved.reviewed.attrs.get("duplicates", []),
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode(),
        )
        human_rows = [
            {key: str(value) for key, value in row.items()} for row in frame_records(expected_decisions)
        ]
        fields = tuple(str(name) for name in expected_decisions.columns)
        store.write(f"{prefix}/human-review.csv", csv_text(human_rows, fields).encode())
        store.write(
            f"{prefix}/human-review.jsonl",
            "".join(row.model_dump_json() + "\n" for row in decisions).encode(),
        )
        store.write(f"{prefix}/review-manifest.json", resolved.manifest.payload())
        store.write(
            f"{prefix}/review-authorization.json",
            json.dumps(
                {"batch": tx.get_batch(token.batch_id).model_dump(mode="json")},
                ensure_ascii=False,
                sort_keys=True,
            ).encode(),
        )
        registered = tx.record_publication(
            authorization_snapshot,
            token,
            PublicationPayload(
                payload_key=f"objects/{reference.digest}.parquet",
                payload_hash=reference.digest,
                csv_reference=f"{prefix}/commands.csv",
                bundle_reference=prefix,
                dagster_run_id=context.run_id,
            ),
        )
        record_review(context, token, registered.publication_id)
        return reference


def read_publication(
    home: Path, dataset_id: str, instance: DagsterInstance, branch: Branch = "dataset"
) -> AssetSnapshot | None:
    state: Literal["published", "stale"] = "published"
    try:
        with HumanReviewStore(home).reader() as tx:
            published = tx.last_publication(dataset_id, branch=branch)
            if published is not None:
                try:
                    resolved = ReviewSnapshots(home, DatasetDataService(home, instance), branch).current(
                        dataset_id
                    )
                    authorize_publication(resolved, published.authorization, tx)
                except (HumanReviewError, DatasetViewError):
                    state = "stale"
    except HumanReviewError as error:
        if error.code == "not_found":
            return None
        raise
    if published is None:
        return None
    manifest = load_manifest(home, published.authorization.snapshot_id)
    payload = ObjectStore(manifest.storage).read(published.payload_key)
    if sha256(payload) != published.payload_hash:
        raise IntegrityError("Published Parquet digest mismatch")
    value = read_frame(payload)
    match value:
        case pd.DataFrame():
            return AssetSnapshot(
                AssetReference(format="parquet", digest=published.payload_hash), value, state
            )
        case _:
            assert_never(value)
