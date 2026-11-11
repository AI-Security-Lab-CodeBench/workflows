import json
from pathlib import Path
from typing import Final
from urllib.error import URLError
from urllib.request import Request, urlopen

from dagster import AssetKey, DagsterInstance, RunsFilter
from pydantic import BaseModel, Field, JsonValue, TypeAdapter

from codebench_pipeline.dataset_branches import FINALIZATION_STAGES
from codebench_pipeline.dataset_plan import plan_dataset
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import dataset_run_config
from codebench_pipeline.human_review import CompletionEffect, CompletionToken, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.source_review_inputs import SourceReviewSource
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import exclusive

LAUNCH: Final = """
mutation ContinueReviewedSources($params: ExecutionParams!) {
  launchRun(executionParams: $params) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
  }
}
"""


class LaunchedRun(BaseModel):
    runId: str


class LaunchResult(BaseModel):
    kind: str = Field(alias="__typename")
    run: LaunchedRun | None = None


class LaunchData(BaseModel):
    launchRun: LaunchResult


class LaunchResponse(BaseModel):
    data: LaunchData | None = None


def launch_reviewed_run(port: int, params: dict[str, JsonValue]) -> str:
    request = Request(
        f"http://127.0.0.1:{port}/graphql",
        data=json.dumps({"query": LAUNCH, "variables": {"params": params}}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as response:
            body = response.read(65_537)
    except (URLError, TimeoutError) as error:
        raise HumanReviewError("continuation_failed", "Dagster could not launch downstream assets") from error
    if len(body) > 65_536:
        raise HumanReviewError("continuation_failed", "Dagster returned an oversized launch response")
    result = LaunchResponse.model_validate_json(body)
    if (
        result.data is None
        or result.data.launchRun.kind != "LaunchRunSuccess"
        or result.data.launchRun.run is None
    ):
        raise HumanReviewError("continuation_failed", "Dagster rejected the downstream launch")
    return result.data.launchRun.run.runId


class SourceReviewRunner:
    """Submit through Dagster's run launcher; source acquisition must not block the UI request."""

    def __init__(self, home: Path, instance: DagsterInstance, port: int) -> None:
        self.home = home.resolve()
        self.instance = instance
        self.port = port

    def __call__(self, token: CompletionToken) -> CompletionEffect:
        with exclusive(self.home / "continuation-locks" / f"{token.dataset_id}.lock"):
            with HumanReviewStore(self.home).transaction() as tx:
                source = SourceReviewSource(self.home, self.instance).current(token.dataset_id)
                tx.authorize_completion(source.snapshot, token)
            spec = DatasetRegistry(self.home).get(token.dataset_id)
            plan = plan_dataset(spec, stage_configs(spec), self.instance, home=self.home)
            if plan.waiting_for or "dataset" in plan.unconfigured:
                raise HumanReviewError("stale", "Source selection changed before continuation")
            selected = [
                AssetKey(["datasets", stage])
                for stage in plan.selected
                if not stage.startswith("documentation/") and stage not in FINALIZATION_STAGES
            ]
            if selected and not any(stage.startswith("documentation/") for stage in plan.selected):
                selected.extend(
                    AssetKey(["datasets", stage]) for stage in plan.selected if stage in FINALIZATION_STAGES
                )
            if not selected:
                return CompletionEffect()
            active = self.instance.get_runs(
                filters=RunsFilter(job_name="build_dataset", tags={"dagster/partition": token.dataset_id}),
                limit=1,
            )
            if active and not active[0].is_finished:
                raise HumanReviewError("conflict", "Preparation is already running for this dataset")
            params = {
                "selector": {
                    "repositoryLocationName": "Dataset building",
                    "repositoryName": "__repository__",
                    "jobName": "build_dataset",
                    "assetSelection": [{"path": key.path} for key in selected],
                },
                "runConfigData": dataset_run_config(spec, self.home).to_config_dict(),
                "executionMetadata": {
                    "tags": [
                        {"key": "dagster/partition", "value": token.dataset_id},
                        {"key": "dataset/category", "value": spec.rules.category_id},
                        {"key": "human_review/source_batch", "value": token.batch_id},
                    ]
                },
            }
            return CompletionEffect(
                run_id=launch_reviewed_run(
                    self.port, TypeAdapter(dict[str, JsonValue]).validate_python(params)
                )
            )
