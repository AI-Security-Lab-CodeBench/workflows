from pathlib import Path

from dagster import DagsterInstance

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import frame_bytes
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.guarded_publication import ReleaseInputs, ReleaseProposal, prepare_guarded_release
from codebench_pipeline.guarded_publication_controls import (
    Action,
    actor_uid,
    record_control,
    require_proposal_authorized,
)
from codebench_pipeline.human_review import CompletionToken, HumanReviewError, PublicationReference
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.repository_receipts import publish_bytes
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.storage import IntegrityError


def current_proposal(
    snapshots: ReviewSnapshots,
    token: CompletionToken,
    transaction: HumanReviewTransaction,
) -> ReleaseProposal:
    spec = DatasetRegistry(snapshots.home).get_snapshot(token.dataset_id)
    if not spec.has_declared_collection:
        raise HumanReviewError(
            "ineligible", "Exact release proposals require a dataset with a declared collection"
        )
    resolved = snapshots.authoritative(token.dataset_id)
    transaction.authorize_completion(resolved.snapshot, token)
    inputs = ReleaseInputs(snapshots, resolved, token, transaction.list_decisions(token.batch_id))
    return prepare_guarded_release(inputs, snapshots.home, spec)


def preview_proposal(home: Path, instance: DagsterInstance, dataset_id: str) -> tuple[str, Path]:
    """Persist a content-addressed preview without changing review or publication authorization."""
    snapshots = ReviewSnapshots(home, DatasetDataService(home, instance))
    with HumanReviewStore(home).transaction(dataset_id=dataset_id) as tx:
        token = tx.get_current_batch(dataset_id).completion
        if token is None:
            raise HumanReviewError("pending", "Prepare the exact proposal after review completion")
        proposal = current_proposal(snapshots, token, tx)
        path = home / "release-proposals" / dataset_id / proposal.digest
        publish_bytes(path / "commands.csv", proposal.publication.files["commands.csv"].encode())
        publish_bytes(path / "commands.parquet", frame_bytes(proposal.output))
        publish_bytes(path / "manifest.json", proposal.body)
        return proposal.digest, path


def update_publication_control(
    home: Path,
    instance: DagsterInstance,
    dataset_id: str,
    action: Action,
    proposal_digest: str,
) -> None:
    actor = actor_uid(home, "operators" if action in {"hold", "clear"} else "stewards")
    with HumanReviewStore(home).transaction(dataset_id=dataset_id) as tx:
        if action == "attest":
            token = tx.get_current_batch(dataset_id).completion
            if token is None:
                raise HumanReviewError(
                    "pending", "Complete review before attesting to a guarded release proposal"
                )
            snapshots = ReviewSnapshots(home, DatasetDataService(home, instance))
            proposal = current_proposal(snapshots, token, tx)
            if proposal.digest != proposal_digest:
                raise HumanReviewError("stale", "The proposed release changed before attestation")
            proposal.require_resolved_sampling()
            preview = home / "release-proposals" / dataset_id / proposal.digest
            if (
                (preview / "manifest.json").read_bytes() != proposal.body
                or (preview / "commands.csv").read_bytes()
                != proposal.publication.files["commands.csv"].encode()
                or (preview / "commands.parquet").read_bytes() != frame_bytes(proposal.output)
            ):
                raise IntegrityError("The immutable proposal preview differs from its current exact output")
        record_control(tx, dataset_id, action, actor=actor, proposal=proposal_digest)


def require_current_release(
    snapshots: ReviewSnapshots,
    publication: PublicationReference,
    transaction: HumanReviewTransaction,
    *,
    proposal: ReleaseProposal | None = None,
) -> None:
    """Cached return has the same attestation and evidence gate as a fresh publication."""
    token = publication.authorization
    proposal = proposal or current_proposal(snapshots, token, transaction)
    if publication.proposal_digest != proposal.digest:
        raise HumanReviewError("stale", "Published output no longer matches the exact current proposal")
    require_proposal_authorized(transaction, token.dataset_id, proposal)
    store = ObjectStore(snapshots.data.specification(token.dataset_id).storage)
    if (
        publication.payload_hash != proposal.manifest["parquet_sha256"]
        or store.read(publication.payload_key) != frame_bytes(proposal.output)
        or store.read(publication.csv_reference) != proposal.publication.files["commands.csv"].encode()
        or store.read(f"{publication.bundle_reference}/proposal-manifest.json") != proposal.body
    ):
        raise IntegrityError("Published bytes no longer match the attested output")
