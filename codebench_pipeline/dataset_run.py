from contextlib import nullcontext
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES, FINALIZATION_STAGES, Branch, stage_branch
from codebench_pipeline.dataset_dependencies import DECLARED_INPUT_STAGES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.storage import exclusive

BuildStatus = Literal[
    "prepared",
    "waiting_for_sources",
    "awaiting_human_review",
    "reviewed",
    "review_completed",
    "unchanged",
    "published",
]
ReviewStage = Literal["annotations"]


class BranchOutcome(BaseModel):
    model_config = ConfigDict(frozen=True)
    materialized_stages: tuple[str, ...]
    skipped_stages: tuple[str, ...]
    blocked_stages: tuple[str, ...]
    status: BuildStatus
    review_stage: ReviewStage | None = None
    review_batch_id: str | None = None
    review_snapshot_id: str | None = None


class BuildOutcome(BaseModel):
    model_config = ConfigDict(frozen=True)
    run_id: str | None
    release_run_id: str | None = None
    publication_waiting_reason: str | None = None
    job: str = "build_dataset"
    dataset: str
    category: str
    storage: str
    materialized_stages: tuple[str, ...]
    skipped_stages: tuple[str, ...]
    blocked_stages: tuple[str, ...]
    selection_reasons: dict[str, str]
    cache_hits: dict[str, bool]
    status: BuildStatus
    review_stage: ReviewStage | None
    review_batch_id: str | None
    review_snapshot_id: str | None
    branches: dict[Branch, BranchOutcome]


def refresh_registered_configuration(dataset_id: str, home: Path) -> str:
    DatasetRegistry(home).get(dataset_id)
    with dataset_instance(home) as instance:
        instance.add_dynamic_partitions("dataset_builds", [dataset_id])
        result = (
            build_definitions(home)
            .resolve_job_def("refresh_dataset_configuration")
            .execute_in_process(
                instance=instance,
                partition_key=dataset_id,
            )
        )
        return result.run_id


def run_registered(dataset_id: str, home: Path, *, prepare_only: bool = False) -> BuildOutcome:
    """Force the named native job for one registered partition."""
    spec = DatasetRegistry(home).get(dataset_id)
    job_name = "prepare_dataset" if prepare_only else "build_dataset"
    with exclusive(home / "build-locks" / f"{spec.dataset_id}.lock"), dataset_instance(home) as instance:
        instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        result = (
            build_definitions(home)
            .resolve_job_def(job_name)
            .execute_in_process(
                instance=instance,
                partition_key=spec.dataset_id,
                run_config=dataset_run_config(spec),
            )
        )
        caches: dict[str, bool] = {}
        for event in result.get_asset_materialization_events():
            materialization = event.step_materialization_data.materialization
            stage = "/".join(materialization.asset_key.path[1:])
            if stage in DECLARED_INPUT_STAGES:
                caches[stage] = materialization.metadata["cache_hit"].value is True
        branches: dict[Branch, BranchOutcome] = {}
        review = nullcontext() if prepare_only else HumanReviewStore(home).transaction(dataset_id=dataset_id)
        with review as transaction:
            shared_batch = (
                transaction.get_current_batch(dataset_id, "annotations") if transaction is not None else None
            )
            for descriptor in COLLECTION_BRANCHES:
                branch = descriptor.namespace
                materialized = tuple(
                    stage
                    for stage in caches
                    if stage not in FINALIZATION_STAGES and stage_branch(stage) == branch
                )
                batch = shared_batch
                status: BuildStatus = (
                    "unchanged"
                    if not materialized
                    else "prepared"
                    if prepare_only
                    else "reviewed"
                    if batch is None
                    else "awaiting_human_review"
                    if batch.state == "open"
                    else "review_completed"
                )
                branches[branch] = BranchOutcome(
                    materialized_stages=materialized,
                    skipped_stages=(),
                    blocked_stages=(),
                    status=status,
                    review_stage="annotations" if batch is not None else None,
                    review_batch_id=batch.batch_id if batch is not None else None,
                    review_snapshot_id=batch.snapshot.snapshot_id if batch is not None else None,
                )
        primary = branches["dataset"]
        status = primary.status
        if prepare_only and caches:
            status = "prepared"
        if shared_batch is not None:
            if shared_batch.state == "open":
                status = "awaiting_human_review"
            elif caches:
                status = "review_completed"
        release_run_id: str | None = None
        publication_waiting_reason: str | None = None
        if shared_batch is not None and shared_batch.completion is not None:
            with HumanReviewStore(home).reader() as transaction:
                previous = transaction.last_publication(dataset_id)
            try:
                published = ReleaseRunner(home, instance)(shared_batch.completion)
            except HumanReviewError as error:
                if error.code != "pending":
                    raise
                status = "prepared"
                publication_waiting_reason = error.detail
            else:
                if previous != published:
                    release_run_id = published.dagster_run_id
                    caches.update({"human_review_decisions": False, "dataset_release": False})
                    status = "published"
        outcome = BuildOutcome(
            run_id=result.run_id,
            release_run_id=release_run_id,
            publication_waiting_reason=publication_waiting_reason,
            job=job_name,
            dataset=dataset_id,
            category=spec.rules.category_id,
            storage=spec.storage.backend,
            materialized_stages=tuple(caches),
            skipped_stages=(),
            blocked_stages=(),
            selection_reasons={},
            cache_hits=caches,
            status=status,
            review_stage="annotations" if shared_batch is not None else primary.review_stage,
            review_batch_id=shared_batch.batch_id if shared_batch is not None else primary.review_batch_id,
            review_snapshot_id=shared_batch.snapshot.snapshot_id
            if shared_batch is not None
            else primary.review_snapshot_id,
            branches=branches,
        )
        (home / f"build-{result.run_id}.json").write_text(outcome.model_dump_json(indent=2), encoding="utf-8")
        return outcome
