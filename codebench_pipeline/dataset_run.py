from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_branches import FINALIZATION_STAGES, Branch, stage_branch
from codebench_pipeline.dataset_plan import plan_dataset
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import exclusive

BuildStatus = Literal[
    "unconfigured",
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


def run_registered(dataset_id: str, home: Path) -> BuildOutcome:
    spec = DatasetRegistry(home).get(dataset_id)
    with exclusive(home / "build-locks" / f"{spec.dataset_id}.lock"), dataset_instance(home) as instance:
        instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        plan = plan_dataset(spec, stage_configs(spec), instance, home=home)
        caches: dict[str, bool] = {}
        run_id: str | None = None
        if plan.selected:
            job = build_definitions(home).resolve_job_def("build_dataset")
            result = job.execute_in_process(
                instance=instance,
                partition_key=spec.dataset_id,
                asset_selection=plan.asset_selection,
                run_config=dataset_run_config(spec, home),
            )
            if not result.success:
                raise RuntimeError(f"Dataset job failed: {result.run_id}")
            run_id = result.run_id
            for stage in plan.selected:
                materializations = result.asset_materializations_for_node(
                    f"datasets__{stage.replace('/', '__')}"
                )
                if len(materializations) != 1:
                    raise RuntimeError(f"Missing planned materialization: {stage}")
                caches[stage] = materializations[0].metadata["cache_hit"].value is True
        current = plan_dataset(spec, stage_configs(spec), instance, home=home)
        branches: dict[Branch, BranchOutcome] = {}
        branch_names: tuple[Branch, ...] = ("dataset", "documentation")
        with HumanReviewStore(home).transaction() as transaction:
            shared_batch = (
                transaction.ensure_current_batch(
                    ReviewSnapshots(home, DatasetDataService(home, instance)).current(dataset_id).snapshot
                )
                if not current.selected and not current.blocked
                else None
            )
            if (
                shared_batch is not None
                and not shared_batch.snapshot.candidate_ids
                and shared_batch.completion is None
            ):
                transaction.complete(
                    shared_batch.snapshot, shared_batch, reviewer="automatic low-uncertainty policy"
                )
                shared_batch = transaction.get_batch(shared_batch.batch_id)
            for branch in branch_names:
                materialized = tuple(
                    stage
                    for stage in caches
                    if stage not in FINALIZATION_STAGES and stage_branch(stage) == branch
                )
                skipped = tuple(
                    stage
                    for stage in plan.unchanged
                    if stage not in FINALIZATION_STAGES and stage_branch(stage) == branch
                )
                blocked = tuple(
                    stage
                    for stage in current.blocked
                    if stage not in FINALIZATION_STAGES and stage_branch(stage) == branch
                )
                if branch in current.unconfigured:
                    branches[branch] = BranchOutcome(
                        materialized_stages=materialized,
                        skipped_stages=skipped,
                        blocked_stages=blocked,
                        status="unconfigured",
                    )
                    continue
                batch = shared_batch
                status: BuildStatus = (
                    "unchanged"
                    if not materialized
                    else "reviewed"
                    if batch is None
                    else "awaiting_human_review"
                    if batch.state == "open"
                    else "review_completed"
                )
                branches[branch] = BranchOutcome(
                    materialized_stages=materialized,
                    skipped_stages=skipped,
                    blocked_stages=blocked,
                    status=status,
                    review_stage="annotations"
                    if batch is not None
                    else None,
                    review_batch_id=batch.batch_id if batch is not None else None,
                    review_snapshot_id=batch.snapshot.snapshot_id if batch is not None else None,
                )
        primary = branches["documentation"] if "dataset" in current.unconfigured else branches["dataset"]
        status = primary.status
        if shared_batch is not None and caches:
            status = "awaiting_human_review" if shared_batch.state == "open" else "review_completed"
        release_run_id: str | None = None
        if shared_batch is not None and shared_batch.completion is not None:
            with HumanReviewStore(home).reader() as transaction:
                previous = transaction.last_publication(dataset_id)
            published = ReleaseRunner(home, instance)(shared_batch.completion)
            if previous != published:
                release_run_id = published.dagster_run_id
                caches.update({"human_review_decisions": False, "dataset_release": False})
                status = "published"
        outcome = BuildOutcome(
            run_id=run_id,
            release_run_id=release_run_id,
            dataset=dataset_id,
            category=spec.rules.category_id,
            storage=spec.storage.backend,
            materialized_stages=tuple(caches),
            skipped_stages=plan.unchanged,
            blocked_stages=current.blocked,
            selection_reasons=plan.reasons,
            cache_hits=caches,
            status=status,
            review_stage="annotations" if shared_batch is not None else primary.review_stage,
            review_batch_id=shared_batch.batch_id if shared_batch is not None else primary.review_batch_id,
            review_snapshot_id=shared_batch.snapshot.snapshot_id
            if shared_batch is not None
            else primary.review_snapshot_id,
            branches=branches,
        )
        if run_id is not None:
            (home / f"build-{run_id}.json").write_text(outcome.model_dump_json(indent=2), encoding="utf-8")
        return outcome
