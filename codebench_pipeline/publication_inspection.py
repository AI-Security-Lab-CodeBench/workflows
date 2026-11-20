from pathlib import Path

from dagster import DagsterInstance
from pydantic import BaseModel, ValidationError

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import AssetSnapshot, DataFrameIOManager
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.human_review import (
    CompletionToken,
    HumanReviewError,
    PublicationReference,
    ReviewSnapshot,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import StorageConfig, sha256
from codebench_pipeline.review_snapshots import (
    ReviewManifest,
    ReviewSnapshots,
    load_manifest,
    release_configuration_digest,
)
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.stage_implementation import capture_implementation
from codebench_pipeline.storage import IntegrityError


class InspectedProposal(BaseModel):
    partition: str
    registered_spec: Digest | None = None
    release_configuration: Digest | None = None
    review_snapshot: Digest
    review_completion: CompletionToken
    implementation: dict[str, Digest]
    parquet_sha256: Digest
    csv_sha256: Digest


def recorded_proposal_is_attested(tx: HumanReviewTransaction, dataset_id: str, proposal: str) -> bool:
    """Read the publication control ledger; publication admission runs its own full gate."""
    state = tx.connection.execute(
        """SELECT
        (SELECT action FROM v2_publication_controls WHERE dataset_id=%s AND action IN ('hold', 'clear')
            ORDER BY sequence DESC LIMIT 1) AS hold,
        (SELECT action FROM v2_publication_controls
            WHERE dataset_id=%s AND proposal=%s AND action IN ('attest', 'revoke')
            ORDER BY sequence DESC LIMIT 1) AS attestation""",
        (dataset_id, dataset_id, proposal),
    ).fetchone()
    return state is not None and state["hold"] != "hold" and state["attestation"] == "attest"


def read_inspected_publication(
    home: Path, dataset_id: str, instance: DagsterInstance, branch: Branch
) -> AssetSnapshot | None:
    from codebench_pipeline.release_admission import publication_snapshot

    data = DatasetDataService(home, instance)

    def manager_for(storage: StorageConfig) -> DataFrameIOManager:
        return DataFrameIOManager.from_environment(storage)

    def manifest_reader(snapshot: ReviewSnapshot) -> ReviewManifest:
        storage = snapshot.artifact_storage
        return load_manifest(
            home, snapshot.snapshot_id, None if storage is None else manager_for(storage).object_store()
        )

    def verify(published: PublicationReference, tx: HumanReviewTransaction) -> bool:
        try:
            proposal_digest = published.proposal_digest
            spec = data.specification(dataset_id)
            if proposal_digest is None or not spec.has_declared_collection or branch != "dataset":
                return False
            registered = DeclaredGlobalConfig.from_spec(spec)
            manager = manager_for(spec.storage)
            implementation = capture_implementation(manager.implementation_source)
            manager.bind_review_store(HumanReviewStore(home, implementation=implementation))
            snapshots = ReviewSnapshots.for_manager(home, data, manager, branch)
            resolved = snapshots.authoritative_registered(registered, manager)
            snapshot = resolved.snapshot
            tx.authorize_completion(snapshot, published.authorization)
            store = manager.object_store()
            payload = store.read(f"{published.bundle_reference}/proposal-manifest.json")
            proposal = InspectedProposal.model_validate_json(payload)
            configuration_matches = (
                proposal.registered_spec == registered.registration_digest
                if proposal.release_configuration is None
                else proposal.release_configuration == release_configuration_digest(registered)
            )
            if (
                sha256(payload) != proposal_digest
                or proposal.partition != dataset_id
                or not configuration_matches
                or proposal.review_snapshot != snapshot.snapshot_id
                or proposal.review_completion != published.authorization
                or proposal.parquet_sha256 != published.payload_hash
                or proposal.csv_sha256 != sha256(store.read(published.csv_reference))
                or proposal.implementation != {**implementation.review_policy, **implementation.publication}
                or not recorded_proposal_is_attested(tx, dataset_id, proposal_digest)
            ):
                return False
            return True
        except (FileNotFoundError, HumanReviewError, IntegrityError, ValidationError):
            return False

    return publication_snapshot(
        dataset_id, branch, HumanReviewStore(home), manager_for, verify, manifest_reader
    )
