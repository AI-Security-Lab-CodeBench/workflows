import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, assert_never

import pandas as pd
from pandera.typing import DataFrame
from pydantic import Field, JsonValue

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import ProposedAnnotationsTable, frame_records, model_records
from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    Branch,
    stage_name,
)
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.dataset_tables import review_queue
from codebench_pipeline.human_review import ReviewModel, ReviewSnapshot
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.review_artifact_admission import checked_review_artifacts
from codebench_pipeline.review_artifacts import (
    read_review_artifact,
    review_artifact_key,
    write_review_artifact,
)
from codebench_pipeline.review_identity import review_identity
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.stage_implementation import capture_implementation
from codebench_pipeline.storage import IntegrityError, digest, fingerprint

type ReviewArtifactReader = Callable[[StorageConfig, AssetReference], pd.DataFrame | CategoryRules]

REVIEW_POLICY_FILES: Final = (
    "human_review.py",
    "human_review_store.py",
    "human_review_transactions.py",
    "human_review_schema.py",
    "review_snapshots.py",
    "release_admission.py",
    "release_runner.py",
    "review_frames.py",
    "review_assets.py",
    "review_config.py",
    "review_workflow.py",
    "review_stages.py",
    "review_service.py",
    "review_identity.py",
    "review_artifact_admission.py",
    "review_preparation.py",
    "review_postgres_lifecycle.py",
    "review_postgres_queries.py",
    "review_postgres_values.py",
    "source_review_guard.py",
    "review_policy.py",
    "publication_authorization.py",
    "documentation_authorization.py",
    "dataset_branches.py",
    "dataset_finalization.py",
    "dataset_export.py",
    "dataset_tables.py",
    "dataset_annotation.py",
    "annotation_cache.py",
    "category_eligibility.py",
    "label_corrections.py",
    "stage_implementation.py",
)


class ReviewManifest(ReviewModel):
    version: Literal[1, 2] = 1
    dataset_id: Identifier
    branch: Branch = "dataset"
    storage: StorageConfig
    publication: PublicationConfig
    references: dict[str, AssetReference]
    evidence: dict[str, JsonValue]
    implementation: dict[str, Digest]
    record_fingerprints: dict[str, Digest] = Field(default_factory=dict)
    review_columns: tuple[str, ...] = ()

    def payload(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()


@dataclass(frozen=True, slots=True)
class ResolvedReview:
    manifest: ReviewManifest
    snapshot: ReviewSnapshot
    reviewed: DataFrame[ProposedAnnotationsTable]
    rules: CategoryRules


def release_configuration_digest(registered: DeclaredGlobalConfig) -> Digest:
    """Bind approval to effective registered settings while ignoring the revision label."""
    return fingerprint(registered.model_dump_json(exclude={"revision", "registration_digest"}))


def load_manifest(home: Path, snapshot_id: str, store: ObjectStore | None = None) -> ReviewManifest:
    if store is not None:
        return load_bound_manifest(snapshot_id, store)
    manifest = ReviewManifest.model_validate_json(read_review_artifact(home, "snapshots", snapshot_id, None))
    if manifest.version != 1:
        raise IntegrityError("Configured review manifest requires an explicit storage binding")
    return manifest


def load_bound_manifest(snapshot_id: str, store: ObjectStore) -> ReviewManifest:
    payload = store.read(review_artifact_key("snapshots", snapshot_id))
    if sha256(payload) != snapshot_id:
        raise IntegrityError("Review artifact digest mismatch")
    manifest = ReviewManifest.model_validate_json(payload)
    if manifest.version != 2 or store.config != manifest.storage:
        raise IntegrityError("Review manifest differs from its storage binding")
    return manifest


class ReviewSnapshots:
    def __init__(
        self,
        home: Path,
        data: DatasetDataService,
        reader: ReviewArtifactReader,
        branch: Branch = "dataset",
        *,
        manifest_reader: Callable[[ReviewSnapshot], ReviewManifest],
    ) -> None:
        self.data = data
        self.home = home.resolve()
        self.branch: Branch = branch
        self._read_version = reader
        self._manifest_reader = manifest_reader

    @staticmethod
    def from_environment(
        home: Path, data: DatasetDataService, branch: Branch = "dataset"
    ) -> "ReviewSnapshots":
        managers: dict[str, DataFrameIOManager] = {}

        def manager_for(storage: StorageConfig) -> DataFrameIOManager:
            key = storage.model_dump_json()
            if key not in managers:
                managers[key] = DataFrameIOManager.from_environment(storage)
            return managers[key]

        return ReviewSnapshots(
            home,
            data,
            lambda storage, reference: manager_for(storage).read_version(reference),
            branch,
            manifest_reader=lambda snapshot: load_manifest(
                home,
                snapshot.snapshot_id,
                None
                if snapshot.artifact_storage is None
                else manager_for(snapshot.artifact_storage).object_store(),
            ),
        )

    @staticmethod
    def for_manager(
        home: Path, data: DatasetDataService, manager: DataFrameIOManager, branch: Branch = "dataset"
    ) -> "ReviewSnapshots":
        def manifest_reader(snapshot: ReviewSnapshot) -> ReviewManifest:
            if snapshot.artifact_storage != manager.storage:
                raise IntegrityError("Review manifest storage differs from the bound dataset storage")
            return load_bound_manifest(snapshot.snapshot_id, manager.object_store())

        return ReviewSnapshots(
            home, data, manager.read_registered_version, branch, manifest_reader=manifest_reader
        )

    def capture(self, dataset_id: str, review_columns: tuple[str, ...] | None = None) -> ResolvedReview:
        spec = self.data.specification(dataset_id)
        manager = DataFrameIOManager.from_environment(spec.storage)
        manager.bind_review_store(
            HumanReviewStore(self.home, implementation=capture_implementation(manager.implementation_source))
        )
        return self.capture_registered(
            DeclaredGlobalConfig.from_spec(spec),
            manager,
            tuple(spec.publication.review_columns) if review_columns is None else review_columns,
        )

    def capture_registered(
        self,
        registered: DeclaredGlobalConfig,
        manager: DataFrameIOManager,
        review_columns: tuple[str, ...],
    ) -> ResolvedReview:
        """Capture verified immutable inputs during preparation, never during a page read."""
        dataset_id = registered.dataset_id
        publication = registered.scopes.human_review_queue.human_review_queue
        if review_columns != tuple(publication.review_columns):
            raise DatasetViewError("not_ready", "Review columns differ from the registered policy")
        if self.branch != "dataset":
            raise DatasetViewError("not_ready", "Annotation review is shared across collection branches")
        artifacts = checked_review_artifacts(manager, self.data.instance, registered, include_queue=False)
        references = {stage: artifact.snapshot.reference for stage, artifact in artifacts.items()}
        binding: dict[str, JsonValue] = {}
        for descriptor in COLLECTION_BRANCHES:
            branch = descriptor.namespace
            stage = stage_name("command_candidates", branch)
            binding[branch] = {"state": "configured", "digest": references[stage].digest}
        annotations = artifacts["category_eligible_annotations"].snapshot.value
        match annotations:
            case pd.DataFrame():
                if annotations.attrs.get(FINALIZATION_BINDING) != binding:
                    raise DatasetViewError("not_ready", "Annotations belong to different collection inputs")
                annotation_metadata = annotations.attrs
                reviewed = ProposedAnnotationsTable.validate(pd.DataFrame(annotations))
            case CategoryRules():
                raise IntegrityError("Annotations are not a table")
            case _:
                assert_never(annotations)
        rules = registered.rules
        evidence = {
            stage: artifact.evidence() for stage, artifact in artifacts.items() if stage != "category_rules"
        }
        package = Path(__file__).parent
        implementation = {name: digest(package / name) for name in REVIEW_POLICY_FILES}
        record_fingerprints = {
            str(row["candidate_id"]): review_identity(row, review_columns) for row in frame_records(reviewed)
        }
        manifest = ReviewManifest(
            version=2,
            dataset_id=dataset_id,
            branch=self.branch,
            storage=manager.storage,
            publication=publication,
            references=references,
            evidence=evidence,
            implementation=implementation,
            record_fingerprints=record_fingerprints,
            review_columns=review_columns,
        )
        payload = manifest.payload()
        snapshot_id = sha256(payload)
        queue = review_queue(reviewed, rules, review_columns)
        candidate_ids = tuple(candidate.candidate_id for candidate in model_records(queue, CandidateRecord))
        eligible_candidate_ids = tuple(
            candidate.candidate_id
            for candidate in model_records(queue.loc[queue["exclusion_reason"] == ""], CandidateRecord)
        )
        reviewed.attrs = annotation_metadata
        snapshot = ReviewSnapshot(
            dataset_id=dataset_id,
            review_stage="annotations",
            snapshot_id=snapshot_id,
            artifact_storage=manager.storage,
            candidate_ids=candidate_ids,
            record_fingerprints={identifier: record_fingerprints[identifier] for identifier in candidate_ids},
            review_columns=review_columns,
            eligible_candidate_ids=eligible_candidate_ids,
        )
        store = manager.object_store()
        for name, expected in implementation.items():
            source = (package / name).read_bytes()
            if sha256(source) != expected:
                raise IntegrityError("Review implementation changed during snapshot capture")
            store.write(f"review/snapshots/implementation/{expected}/{name}", source)
        write_review_artifact("snapshots", payload, store)
        return ResolvedReview(manifest, snapshot, reviewed, rules)

    def current(self, dataset_id: str) -> ResolvedReview:
        return self.current_with_store(dataset_id, HumanReviewStore(self.home))

    def current_with_store(self, dataset_id: str, review_store: HumanReviewStore) -> ResolvedReview:
        with review_store.reader() as tx:
            snapshot = tx.get_current_batch(dataset_id, "annotations").snapshot
        manifest = self.manifest(snapshot)
        reviewed = ProposedAnnotationsTable.validate(self.frame(manifest, "category_eligible_annotations"))
        rules = self._read_version(manifest.storage, manifest.references["category_rules"])
        if not isinstance(rules, CategoryRules):
            raise IntegrityError("Prepared review rules are not a taxonomy")
        return ResolvedReview(manifest, snapshot, reviewed, rules)

    def authoritative(self, dataset_id: str) -> ResolvedReview:
        spec = self.data.specification(dataset_id)
        manager = DataFrameIOManager.from_environment(spec.storage)
        manager.bind_review_store(
            HumanReviewStore(self.home, implementation=capture_implementation(manager.implementation_source))
        )
        return self.authoritative_registered(DeclaredGlobalConfig.from_spec(spec), manager)

    def authoritative_registered(
        self, registered: DeclaredGlobalConfig, manager: DataFrameIOManager
    ) -> ResolvedReview:
        resolved = self.current_with_store(registered.dataset_id, manager.review_store)
        artifacts = checked_review_artifacts(manager, self.data.instance, registered, include_queue=True)
        if (
            resolved.rules != registered.rules
            or resolved.manifest.storage != manager.storage
            or resolved.manifest.publication != registered.scopes.human_review_queue.human_review_queue
            or resolved.manifest.implementation != manager.review_store.bound_implementation().review_policy
            or resolved.manifest.references
            != {
                stage: artifact.snapshot.reference
                for stage, artifact in artifacts.items()
                if stage != "human_review_queue"
            }
        ):
            raise DatasetViewError("not_ready", "Publication inputs differ from prepared review")
        return resolved

    def manifest(self, snapshot: ReviewSnapshot) -> ReviewManifest:
        manifest = self._manifest_reader(snapshot)
        if manifest.dataset_id != snapshot.dataset_id or manifest.branch != self.branch:
            raise IntegrityError("Review manifest belongs to another dataset or branch")
        return manifest

    def frame(self, manifest: ReviewManifest, stage: str) -> pd.DataFrame:
        value = self._read_version(manifest.storage, manifest.references[stage])
        match value:
            case pd.DataFrame():
                return value
            case CategoryRules():
                raise IntegrityError("Review evidence is not a table")
            case _:
                assert_never(value)
