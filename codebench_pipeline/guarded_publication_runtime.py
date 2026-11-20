from collections.abc import Callable
from pathlib import Path

from dagster import DagsterInstance

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataset_config import DatasetBuildSpec
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
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.stage_implementation import capture_implementation
from codebench_pipeline.storage import IntegrityError


def current_proposal(
    snapshots: ReviewSnapshots,
    token: CompletionToken,
    transaction: HumanReviewTransaction,
    spec: DatasetBuildSpec,
) -> ReleaseProposal:
    if not spec.has_declared_collection:
        raise HumanReviewError(
            "ineligible", "Exact release proposals require a dataset with a declared collection"
        )
    manager = DataFrameIOManager.from_environment(spec.storage, review_home=str(snapshots.home))
    manager.bind_review_store(
        HumanReviewStore(snapshots.home, implementation=capture_implementation(manager.implementation_source))
    )
    bound = ReviewSnapshots.for_manager(snapshots.home, snapshots.data, manager, snapshots.branch)
    return bound_proposal(bound, token, transaction, DeclaredGlobalConfig.from_spec(spec), manager)


def bound_proposal(
    snapshots: ReviewSnapshots,
    token: CompletionToken,
    transaction: HumanReviewTransaction,
    registered: DeclaredGlobalConfig,
    manager: DataFrameIOManager,
) -> ReleaseProposal:
    resolved = snapshots.authoritative_registered(registered, manager)
    transaction.authorize_completion(resolved.snapshot, token)
    inputs = ReleaseInputs(snapshots, resolved, token, transaction.list_decisions(token.batch_id))
    return prepare_guarded_release(
        inputs,
        manager.repository_run(registered.dataset_id, "release-verification"),
        registered,
        publication_versions=manager.review_store.publication_versions(),
    )


def preview_proposal(home: Path, instance: DagsterInstance, dataset_id: str) -> tuple[str, Path]:
    """Persist a content-addressed preview without changing review or publication authorization."""
    snapshots = ReviewSnapshots.from_environment(home, DatasetDataService(home, instance))
    with HumanReviewStore(home).transaction(dataset_id=dataset_id) as tx:
        token = tx.get_current_batch(dataset_id).completion
        if token is None:
            raise HumanReviewError("pending", "Prepare the exact proposal after review completion")
        proposal = current_proposal(snapshots, token, tx, snapshots.data.specification(dataset_id))
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
            snapshots = ReviewSnapshots.from_environment(home, DatasetDataService(home, instance))
            proposal = current_proposal(snapshots, token, tx, snapshots.data.specification(dataset_id))
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
    spec: DatasetBuildSpec,
    *,
    proposal: ReleaseProposal | None = None,
) -> None:
    """Cached return has the same attestation and evidence gate as a fresh publication."""
    proposal = proposal or current_proposal(snapshots, publication.authorization, transaction, spec)
    verify_current_release(
        publication, transaction, proposal, lambda: ObjectStore.from_environment(spec.storage)
    )


def verify_current_release(
    publication: PublicationReference,
    transaction: HumanReviewTransaction,
    proposal: ReleaseProposal,
    load_store: Callable[[], ObjectStore],
) -> None:
    if publication.proposal_digest != proposal.digest:
        raise HumanReviewError("stale", "Published output no longer matches the exact current proposal")
    require_proposal_authorized(transaction, publication.authorization.dataset_id, proposal)
    store = load_store()
    if (
        publication.payload_hash != proposal.manifest["parquet_sha256"]
        or store.read(publication.payload_key) != frame_bytes(proposal.output)
        or store.read(publication.csv_reference) != proposal.publication.files["commands.csv"].encode()
        or store.read(f"{publication.bundle_reference}/proposal-manifest.json") != proposal.body
    ):
        raise IntegrityError("Published bytes no longer match the attested output")
