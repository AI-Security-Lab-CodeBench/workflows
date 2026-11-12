import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, assert_never

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, build_input_context
from pandera.typing import DataFrame
from pydantic import Field, JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager
from codebench_pipeline.dataframe_schemas import ProposedAnnotationsTable, frame_records
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import FINALIZATION_STAGES, Branch, stage_name
from codebench_pipeline.dataset_config import PublicationConfig
from codebench_pipeline.dataset_export import Publication
from codebench_pipeline.dataset_finalization import FINALIZATION_BINDING
from codebench_pipeline.dataset_plan import PREPARATION_STAGES, plan_dataset
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.human_review import ReviewModel, ReviewSnapshot
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import IntegrityError, digest, fingerprint

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
    "source_review_inputs.py",
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
)


class ReviewManifest(ReviewModel):
    version: Literal[1] = 1
    dataset_id: Identifier
    branch: Branch = "dataset"
    storage: StorageConfig
    publication: PublicationConfig
    references: dict[str, AssetReference]
    evidence: dict[str, JsonValue]
    implementation: dict[str, Digest]
    record_fingerprints: dict[str, Digest] = Field(default_factory=dict)

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
    prepared: Publication


def manifest_store(home: Path) -> ObjectStore:
    return ObjectStore(StorageConfig(local_root=str(home.resolve() / "human-review"), prefix="snapshots"))


def load_manifest(home: Path, snapshot_id: str) -> ReviewManifest:
    key = TypeAdapter(Digest).validate_python(snapshot_id, strict=True)
    payload = manifest_store(home).read(f"{key}.json")
    if sha256(payload) != key:
        raise IntegrityError("Review manifest digest mismatch")
    return ReviewManifest.model_validate_json(payload)


class ReviewSnapshots:
    def __init__(self, home: Path, data: DatasetDataService, branch: Branch = "dataset") -> None:
        self.data = data
        self.home = home.resolve()
        self.branch: Branch = branch
        self.store = manifest_store(home)

    def current(self, dataset_id: str) -> ResolvedReview:
        """Resolve while the caller holds the shared review transaction."""
        spec = self.data.specification(dataset_id)
        plan = plan_dataset(spec, stage_configs(spec), self.data.instance, home=self.home)
        if self.branch != "dataset":
            raise DatasetViewError("not_ready", "Annotation review is shared across collection branches")
        if plan.waiting_for:
            raise DatasetViewError(
                "not_ready", "Complete Source selection on source_catalog, then continue the pipeline."
            )
        input_stages = tuple(
            stage
            for stage in PREPARATION_STAGES
            if stage in ("category_rules", *FINALIZATION_STAGES)
            or ("documentation" if stage.startswith("documentation/") else "dataset") not in plan.unconfigured
        )
        if plan.selected or plan.blocked:
            raise DatasetViewError("not_ready", "Preparation is incomplete or changed")
        manager = DataFrameIOManager(storage=spec.storage)
        binding: dict[str, JsonValue] = {}
        branches: tuple[Branch, ...] = ("dataset", "documentation")
        for branch in branches:
            if branch in plan.unconfigured:
                binding[branch] = {"state": "unconfigured"}
                continue
            stage = stage_name("proposed_annotations", branch)
            stored = self.data.read_current(dataset_id, stage)
            if stored.publication_state == "stale" or stored.reference != plan.references[stage]:
                raise DatasetViewError("not_ready", "Collection inputs changed during snapshot capture")
            binding[branch] = {"state": "configured", "digest": stored.reference.digest}
        match manager.read_version(plan.references["deduplicated_annotations"]):
            case pd.DataFrame() as value:
                if value.attrs.get(FINALIZATION_BINDING) != binding:
                    raise DatasetViewError("not_ready", "Annotations belong to different collection inputs")
                annotation_metadata = value.attrs
                reviewed = ProposedAnnotationsTable.validate(pd.DataFrame(value))
            case CategoryRules():
                raise IntegrityError("Annotations are not a table")
            case other:
                assert_never(other)
        match manager.read_version(plan.references["category_rules"]):
            case CategoryRules() as rules:
                if rules != spec.rules:
                    raise DatasetViewError("not_ready", "Stored rules differ from registered rules")
            case pd.DataFrame():
                raise IntegrityError("Category rules are not a taxonomy")
            case other:
                assert_never(other)
        references: dict[str, AssetReference] = {}
        for stage in input_stages:
            with build_input_context(
                asset_key=AssetKey(["datasets", *stage.split("/")]),
                partition_key=dataset_id,
                asset_partitions_def=DATASET_PARTITIONS,
                instance=self.data.instance,
            ) as context:
                reference = manager.current_reference(context)
            if reference != plan.references[stage]:
                raise DatasetViewError("not_ready", "Preparation changed during snapshot capture")
            references[stage] = reference
        evidence: dict[str, JsonValue] = {}
        for stage in input_stages:
            if stage == "category_rules":
                continue
            records = self.data.instance.fetch_materializations(
                AssetRecordsFilter(
                    asset_key=AssetKey(["datasets", *stage.split("/")]), asset_partitions=[dataset_id]
                ),
                limit=1,
            ).records
            materialization = records[0].asset_materialization
            if materialization is None:
                raise IntegrityError("Preparation evidence has no materialization")
            if materialization.metadata["codebench/content_digest"].value != references[stage].digest:
                raise DatasetViewError("not_ready", "Preparation evidence changed during snapshot capture")
            evidence[stage] = TypeAdapter(JsonValue).validate_python(
                materialization.metadata["evidence"].value
            )
        package = Path(__file__).parent
        implementation = {name: digest(package / name) for name in REVIEW_POLICY_FILES}
        record_fingerprints = {
            str(row["candidate_id"]): fingerprint(
                {
                    "record": row,
                    "documentation": TypeAdapter(JsonValue).validate_python(
                        annotation_metadata["row_documentation"][str(row["candidate_id"])]
                    ),
                    "rules": rules.model_dump_json(),
                    "label_model": spec.annotation.model.model_dump_json(),
                    "policy": {
                        name: implementation[name]
                        for name in (
                            "human_review.py",
                            "human_review_transactions.py",
                            "review_policy.py",
                            "dataset_export.py",
                            "dataset_annotation.py",
                        )
                    },
                }
            )
            for row in frame_records(reviewed)
        }
        manifest = ReviewManifest(
            dataset_id=dataset_id,
            branch=self.branch,
            storage=spec.storage,
            publication=spec.publication,
            references=references,
            evidence=evidence,
            implementation=implementation,
            record_fingerprints=record_fingerprints,
        )
        payload = manifest.payload()
        snapshot_id = sha256(payload)
        prepared = publication(reviewed, rules, spec.publication)
        reviewed.attrs = annotation_metadata
        snapshot = ReviewSnapshot(
            dataset_id=dataset_id,
            review_stage="annotations",
            snapshot_id=snapshot_id,
            candidate_ids=prepared.human_review_ids,
            record_fingerprints={
                identifier: record_fingerprints[identifier] for identifier in prepared.human_review_ids
            },
            eligible_candidate_ids=tuple(
                identifier for identifier in prepared.eligible_ids if identifier in prepared.human_review_ids
            ),
        )
        self.store.write(f"{snapshot_id}.json", payload)
        for name, expected in implementation.items():
            source = (package / name).read_bytes()
            if sha256(source) != expected:
                raise IntegrityError("Review implementation changed during snapshot capture")
            self.store.write(f"implementation/{expected}/{name}", source)
        return ResolvedReview(manifest, snapshot, reviewed, rules, prepared)

    def manifest(self, snapshot_id: str) -> ReviewManifest:
        return load_manifest(self.home, snapshot_id)

    def frame(self, manifest: ReviewManifest, stage: str) -> pd.DataFrame:
        value = DataFrameIOManager(storage=manifest.storage).read_version(manifest.references[stage])
        match value:
            case pd.DataFrame():
                return value
            case CategoryRules():
                raise IntegrityError("Review evidence is not a table")
            case _:
                assert_never(value)
