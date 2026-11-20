import json
from collections.abc import Callable
from pathlib import Path
from typing import Literal, assert_never

import pandas as pd
from dagster import DagsterInstance, InputContext, OutputContext
from pydantic import ValidationError

from codebench_pipeline.dagster_table_metadata import MetadataSink
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import (
    AssetReference,
    AssetSnapshot,
    DataFrameIOManager,
    read_frame,
)
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_branches import Branch, asset_branch
from codebench_pipeline.dataset_export import csv_text
from codebench_pipeline.guarded_publication import ReleaseInputs, prepare_guarded_release
from codebench_pipeline.guarded_publication_controls import require_proposal_authorized
from codebench_pipeline.guarded_publication_runtime import bound_proposal, verify_current_release
from codebench_pipeline.human_review import (
    CompletionToken,
    HumanReviewError,
    PublicationPayload,
    PublicationReference,
    ReviewSnapshot,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.input_provenance import producer_run_id
from codebench_pipeline.object_store import StorageConfig, sha256
from codebench_pipeline.publication_authorization import authorize_publication
from codebench_pipeline.review_frames import REVIEW_BINDING, decision_frame, require_decision_frame
from codebench_pipeline.review_snapshots import (
    ReviewManifest,
    ReviewSnapshots,
    load_bound_manifest,
    load_manifest,
)
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.stage_implementation import capture_implementation
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
    snapshots = ReviewSnapshots.for_manager(home, data, manager, branch)
    with manager.review_store.transaction(timeout=10, dataset_id=context.asset_partition_key) as tx:
        registered = manager.run_configuration(context.run_id, context.asset_partition_key)
        resolved = snapshots.authoritative_registered(registered, manager)
        authorization_snapshot = authorize_publication(resolved, token, tx)
        decisions = tx.list_decisions(token.batch_id)
        expected_decisions = decision_frame(token, decisions)
        if context.asset_key.path[-1] == "human_review_decisions":
            require_decision_frame(frame, expected_decisions)
            reference = manager.persist_value(expected_decisions)
            manager.publish_reference(context, reference)
            record_review(context, token)
            return reference
        proposal = prepare_guarded_release(
            ReleaseInputs(snapshots, resolved, token, decisions),
            manager.repository_run(context.asset_partition_key, context.run_id),
            registered,
            publication_versions=manager.review_store.publication_versions(),
        )
        require_proposal_authorized(tx, registered.dataset_id, proposal)
        expected = proposal.output
        if not frame.equals(expected):
            raise HumanReviewError("stale", "Release rows differ from approved eligible records")
        expected.attrs[REVIEW_BINDING] = token.model_dump(mode="json")
        reference = manager.persist_value(expected)
        if reference.digest != proposal.manifest["parquet_sha256"]:
            raise IntegrityError("Persisted release table differs from its attested bytes")
        prefix = f"releases/{resolved.snapshot.dataset_id}/{branch}/{context.run_id}"
        store = manager.object_store()
        for name, text in proposal.publication.files.items():
            store.write(f"{prefix}/{name}", text.encode())
        store.write(f"{prefix}/proposal-manifest.json", proposal.body)
        store.write(
            f"{prefix}/registration.json",
            json.dumps(
                {"registered_spec": registered.registration_digest, "revision": registered.revision}
            ).encode(),
        )
        store.write(
            f"{prefix}/deduplication.json",
            json.dumps(
                {
                    "origins": resolved.reviewed.attrs.get("origins", {}),
                    "duplicates": resolved.reviewed.attrs.get("duplicates", []),
                    "category_exclusions": resolved.reviewed.attrs.get("category_exclusions", []),
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
                proposal_digest=proposal.digest,
            ),
        )
        record_review(context, token, registered.publication_id)
        return reference


def read_publication(
    home: Path, dataset_id: str, instance: DagsterInstance, branch: Branch = "dataset"
) -> AssetSnapshot | None:
    review = HumanReviewStore(home, implementation=capture_implementation())

    def storage_manager(storage: StorageConfig) -> DataFrameIOManager:
        manager = DataFrameIOManager.from_environment(storage, review_home=str(home))
        manager.bind_review_store(review)
        return manager

    def verify(published: PublicationReference, transaction: HumanReviewTransaction) -> bool:
        data = DatasetDataService(home, instance)
        spec = data.specification(dataset_id)
        if not spec.has_declared_collection:
            return False
        manager = storage_manager(spec.storage)
        snapshots = ReviewSnapshots.for_manager(home, data, manager, branch)
        proposal = bound_proposal(
            snapshots, published.authorization, transaction, DeclaredGlobalConfig.from_spec(spec), manager
        )
        verify_current_release(published, transaction, proposal, manager.object_store)
        return True

    def manifest_reader(snapshot: ReviewSnapshot) -> ReviewManifest:
        storage = snapshot.artifact_storage
        store = None if storage is None else storage_manager(storage).object_store()
        return load_manifest(home, snapshot.snapshot_id, store)

    return publication_snapshot(dataset_id, branch, review, storage_manager, verify, manifest_reader)


def read_bound_publication(
    manager: DataFrameIOManager, run_id: str, dataset_id: str, instance: DagsterInstance, branch: Branch
) -> AssetSnapshot | None:
    if manager.review_home is None:
        return None
    home = Path(manager.review_home)

    def verify(published: PublicationReference, transaction: HumanReviewTransaction) -> bool:
        registered = manager.run_configuration(run_id, dataset_id)
        snapshots = ReviewSnapshots.for_manager(home, DatasetDataService(home, instance), manager, branch)
        proposal = bound_proposal(snapshots, published.authorization, transaction, registered, manager)
        verify_current_release(published, transaction, proposal, manager.object_store)
        return True

    def storage_manager(storage: StorageConfig) -> DataFrameIOManager:
        if storage != manager.storage:
            raise HumanReviewError("stale", "Publication storage differs from the bound dataset storage")
        return manager

    def manifest_reader(snapshot: ReviewSnapshot) -> ReviewManifest:
        if snapshot.artifact_storage != manager.storage:
            raise HumanReviewError("stale", "Publication snapshot differs from the bound dataset storage")
        return load_bound_manifest(snapshot.snapshot_id, manager.object_store())

    return publication_snapshot(
        dataset_id,
        branch,
        manager.review_store,
        storage_manager,
        verify,
        manifest_reader,
    )


def load_authorized_release(context: InputContext, manager: DataFrameIOManager) -> pd.DataFrame:
    if manager.review_home is None or manager.registered_revision is None:
        raise HumanReviewError("stale", "No current authorized dataset release is available")
    manager.bind_configuration_producer(context)
    published = read_bound_publication(
        manager,
        context.step_context.run_id,
        context.asset_partition_key,
        context.instance,
        asset_branch(context.asset_key.path),
    )
    if published is None or published.publication_state != "published":
        raise HumanReviewError("stale", "No current authorized dataset release is available")
    if (
        producer_run_id(context.upstream_output) is not None
        and manager.input_reference(context) != published.reference
    ):
        raise HumanReviewError("stale", "The selected release producer is no longer authorized")
    if not isinstance(published.value, pd.DataFrame):
        raise IntegrityError("An authorized release must be a table")
    return published.value


def publication_snapshot(
    dataset_id: str,
    branch: Branch,
    review: HumanReviewStore,
    storage_manager: Callable[[StorageConfig], DataFrameIOManager],
    verify: Callable[[PublicationReference, HumanReviewTransaction], bool],
    manifest_reader: Callable[[ReviewSnapshot], ReviewManifest],
) -> AssetSnapshot | None:
    state: Literal["published", "stale"] = "published"
    try:
        with review.reader() as tx:
            published = tx.last_publication(dataset_id, branch=branch)
            snapshot = tx.get_batch(published.authorization.batch_id).snapshot if published else None
            if published is not None:
                if published.proposal_digest is None:
                    state = "stale"
                else:
                    try:
                        if not verify(published, tx):
                            state = "stale"
                    except (HumanReviewError, DatasetViewError):
                        state = "stale"
    except HumanReviewError as error:
        if error.code == "not_found":
            return None
        raise
    if published is None or snapshot is None:
        return None
    if snapshot.snapshot_id != published.authorization.snapshot_id:
        raise IntegrityError("Publication review snapshot binding mismatch")
    manifest = manifest_reader(snapshot)
    if manifest.dataset_id != dataset_id or manifest.branch != branch:
        raise IntegrityError("Publication manifest belongs to another dataset or branch")
    payload = storage_manager(manifest.storage).object_store().read(published.payload_key)
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
