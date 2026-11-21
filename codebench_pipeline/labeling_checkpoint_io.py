"""Scope labeling checkpoint reads to Dagster's failed step lineage."""

from dataclasses import dataclass
from pathlib import Path
from typing import Final

from dagster import AssetKey, InitResourceContext, ResourceDefinition
from dagster._core.execution.stats import StepEventStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG

from codebench_pipeline.batch_labeling_rows import BatchRow
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.repository_config import repository_component
from codebench_pipeline.storage import IntegrityError, fingerprint

LABELING_ATTEMPTS: Final = AssetKey(["datasets", "labeling_attempts"])


def labeling_generation_identity(
    partition: str, stage_signature: str, expected_model_path: str | None
) -> str:
    return fingerprint(
        {
            "asset": LABELING_ATTEMPTS.to_user_string(),
            "partition": partition,
            "input_output_signature": stage_signature,
            "model_binding": fingerprint(expected_model_path) if expected_model_path is not None else None,
        }
    )


@dataclass(frozen=True, slots=True)
class _RunLabelingCheckpoints(LabelingCheckpointCache):
    failed_parents: tuple[LabelingCheckpointCache, ...]

    def resume(self, generation: str, batch: str) -> list[BatchRow] | None:
        rows = super(_RunLabelingCheckpoints, self).resume(generation, batch)
        if rows is not None:
            return rows
        for parent in self.failed_parents:
            if (rows := parent.resume(generation, batch)) is not None:
                return rows
        return None


def labeling_checkpoint_resource(home: Path) -> ResourceDefinition:
    def bind(context: InitResourceContext) -> LabelingCheckpointCache:
        run = context.run
        instance = context.instance
        partition = run.tags.get(PARTITION_NAME_TAG) if run is not None else None
        if run is None or partition is None or instance is None:
            raise IntegrityError("Labeling checkpoints require a dataset partition")
        dataset = repository_component(partition)
        step_key = LABELING_ATTEMPTS.to_python_identifier()
        root = home / "row-inference-v2" / dataset / "runs"
        operations = home / "label-grounding-v2" / dataset
        current = root / fingerprint([run.run_id, step_key])
        plan = (
            instance.get_execution_plan_snapshot(run.execution_plan_snapshot_id)
            if run.is_resume_retry and run.execution_plan_snapshot_id is not None
            else None
        )
        known_state = plan.initial_known_state if plan is not None else None
        state = known_state.parent_state if known_state is not None else None
        parents: list[LabelingCheckpointCache] = []
        while run.is_resume_retry and state is not None and state.run_id == run.parent_run_id:
            parent = instance.get_run_by_id(state.run_id)
            if (
                parent is None
                or not parent.is_failure_or_canceled
                or parent.tags.get(PARTITION_NAME_TAG) != partition
            ):
                break
            stats = instance.get_run_step_stats(parent.run_id, step_keys=[step_key])
            if len(stats) != 1 or stats[0].status != StepEventStatus.FAILURE:
                break
            parents.append(LabelingCheckpointCache(root / fingerprint([parent.run_id, step_key]), operations))
            run = parent
            state = state.get_parent_state()
        return _RunLabelingCheckpoints(current, operations, tuple(parents))

    return ResourceDefinition(resource_fn=bind, config_schema={})
