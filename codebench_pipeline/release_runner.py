import json
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from dagster import DagsterInstance, RunRequest, RunsFilter, SensorEvaluationContext, SensorResult
from dagster._core.errors import DagsterUserCodeExecutionError
from pydantic import BaseModel, Field, JsonValue, TypeAdapter

from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, release_run_config
from codebench_pipeline.guarded_publication_controls import require_proposal_authorized
from codebench_pipeline.guarded_publication_runtime import current_proposal, require_current_release
from codebench_pipeline.human_review import (
    CompletionEffect,
    CompletionToken,
    HumanReviewError,
    PublicationReference,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.publication_authorization import authorize_publication
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.storage import IntegrityError, exclusive, fingerprint


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
    query = """
    mutation ReleaseReviewedDataset($params: ExecutionParams!) {
      launchRun(executionParams: $params) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
      }
    }
    """
    request = Request(
        f"http://127.0.0.1:{port}/graphql",
        data=json.dumps({"query": query, "variables": {"params": params}}).encode(),
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


def _release_request(
    home: Path, instance: DagsterInstance, token: CompletionToken
) -> RunRequest | CompletionEffect:
    snapshots = ReviewSnapshots.from_environment(home, DatasetDataService(home, instance))
    with HumanReviewStore(home).transaction(dataset_id=token.dataset_id) as tx:
        spec = snapshots.data.specification(token.dataset_id)
        if not spec.has_declared_collection:
            raise HumanReviewError("ineligible", "Dataset release requires a declared collection")
        batch = tx.get_current_batch(token.dataset_id, token.review_stage)
        tx.authorize_completion(batch.snapshot, token)
        previous = tx.last_publication(token.dataset_id)
        try:
            proposal = current_proposal(snapshots, token, tx, spec)
            require_proposal_authorized(tx, token.dataset_id, proposal)
            if previous is not None and previous.proposal_digest == proposal.digest:
                require_current_release(snapshots, previous, tx, spec, proposal=proposal)
                return CompletionEffect(publication=previous, run_id=previous.dagster_run_id)
        except HumanReviewError as error:
            if error.code == "pending":
                return CompletionEffect()
            raise
    identity = fingerprint(token.model_dump(mode="json"))
    tags = {"dagster/partition": token.dataset_id, "human_review/completion": identity}
    runs = instance.get_runs(filters=RunsFilter(job_name="release_dataset", tags=tags), limit=1)
    if runs and not runs[0].is_finished:
        return CompletionEffect(run_id=runs[0].run_id)
    return RunRequest(
        partition_key=token.dataset_id,
        run_key=identity,
        run_config=release_run_config(spec, token).to_config_dict(),
        tags=tags,
    )


def dataset_release_requests(context: SensorEvaluationContext, home: Path) -> SensorResult:
    store = HumanReviewStore(home)
    requests: list[RunRequest] = []
    for key in DatasetRegistry(home).keys():
        try:
            with store.reader() as tx:
                batch = tx.get_current_batch(key, "annotations")
            if batch.completion is None or batch.snapshot.candidate_ids:
                continue
            request = _release_request(home, context.instance, batch.completion)
        except HumanReviewError as error:
            if error.code in {"not_found", "stale", "pending"}:
                continue
            raise
        except DatasetViewError as error:
            if error.code == "not_ready":
                continue
            raise
        if isinstance(request, RunRequest):
            requests.append(request)
    return SensorResult(run_requests=requests)


class ReleaseContinuation:
    """Launch native publication without keeping the browser's completion request open."""

    def __init__(self, home: Path, instance: DagsterInstance, port: int) -> None:
        self.home = home.resolve()
        self.instance = instance
        self.port = port

    def __call__(self, token: CompletionToken) -> CompletionEffect:
        with exclusive(self.home / "release-locks" / f"{token.dataset_id}.lock"):
            request = _release_request(self.home, self.instance, token)
            if isinstance(request, CompletionEffect):
                return request
            params = {
                "selector": {
                    "repositoryLocationName": "Dataset building",
                    "repositoryName": "__repository__",
                    "jobName": "release_dataset",
                },
                "runConfigData": request.run_config,
                "executionMetadata": {
                    "tags": [{"key": key, "value": value} for key, value in request.tags.items()]
                },
            }
            return CompletionEffect(
                run_id=launch_reviewed_run(
                    self.port, TypeAdapter(dict[str, JsonValue]).validate_python(params)
                )
            )


class ReleaseRunner:
    def __init__(self, home: Path, instance: DagsterInstance) -> None:
        self.home = home.resolve()
        self.instance = instance

    def __call__(self, token: CompletionToken) -> PublicationReference:
        with exclusive(self.home / "release-locks" / f"{token.dataset_id}.lock"):
            return self._release(token)

    def _release(self, token: CompletionToken) -> PublicationReference:
        snapshots = ReviewSnapshots.from_environment(self.home, DatasetDataService(self.home, self.instance))
        with HumanReviewStore(self.home).transaction(dataset_id=token.dataset_id) as tx:
            spec = snapshots.data.specification(token.dataset_id)
            if not spec.has_declared_collection:
                raise HumanReviewError("ineligible", "Dataset release requires a declared collection")
            resolved = snapshots.current(token.dataset_id)
            authorize_publication(resolved, token, tx)
            previous = tx.last_publication(token.dataset_id)
            proposal = current_proposal(snapshots, token, tx, spec)
            require_proposal_authorized(tx, token.dataset_id, proposal)
            if previous is not None and previous.proposal_digest == proposal.digest:
                require_current_release(snapshots, previous, tx, spec, proposal=proposal)
                return previous
        job = build_definitions(self.home).resolve_job_def("release_dataset")
        try:
            result = job.execute_in_process(
                instance=self.instance,
                partition_key=token.dataset_id,
                run_config=release_run_config(spec, token),
            )
        except DagsterUserCodeExecutionError as error:
            match error.__cause__:
                case HumanReviewError() as domain_error:
                    raise domain_error from error
                case _:
                    raise HumanReviewError("publication_failed", "Dagster publication failed") from error
        if not result.success:
            raise HumanReviewError("publication_failed", "Dagster publication failed")
        with HumanReviewStore(self.home).transaction(dataset_id=token.dataset_id) as tx:
            matches = [
                reference
                for reference in tx.list_publications(token.dataset_id)
                if reference.dagster_run_id == result.run_id
            ]
        if len(matches) != 1:
            raise IntegrityError("Successful release has no unique committed publication")
        return matches[0]
