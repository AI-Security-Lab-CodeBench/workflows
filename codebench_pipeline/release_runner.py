from pathlib import Path

from dagster import DagsterInstance, RunsFilter
from dagster._core.errors import DagsterUserCodeExecutionError
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, release_run_config
from codebench_pipeline.human_review import (
    CompletionEffect,
    CompletionToken,
    HumanReviewError,
    PublicationReference,
)
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.publication_authorization import authorize_publication
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.source_review_runner import launch_reviewed_run
from codebench_pipeline.storage import IntegrityError, exclusive, fingerprint


class ReleaseContinuation:
    """Launch native publication without keeping the browser's completion request open."""

    def __init__(self, home: Path, instance: DagsterInstance, port: int) -> None:
        self.home = home.resolve()
        self.instance = instance
        self.port = port

    def __call__(self, token: CompletionToken) -> CompletionEffect:
        with exclusive(self.home / "release-locks" / f"{token.dataset_id}.lock"):
            with HumanReviewStore(self.home).reader() as tx:
                resolved = ReviewSnapshots(self.home, DatasetDataService(self.home, self.instance)).current(
                    token.dataset_id
                )
                authorize_publication(resolved, token, tx)
                previous = tx.last_publication(token.dataset_id)
                if previous is not None and previous.authorization == token:
                    return CompletionEffect(publication=previous, run_id=previous.dagster_run_id)
            tags = {
                "dagster/partition": token.dataset_id,
                "human_review/completion": fingerprint(token.model_dump(mode="json")),
            }
            runs = self.instance.get_runs(filters=RunsFilter(job_name="release_dataset", tags=tags), limit=1)
            if runs and not runs[0].is_finished:
                return CompletionEffect(run_id=runs[0].run_id)
            spec = DatasetRegistry(self.home).get(token.dataset_id)
            params = {
                "selector": {
                    "repositoryLocationName": "Dataset building",
                    "repositoryName": "__repository__",
                    "jobName": "release_dataset",
                },
                "runConfigData": release_run_config(spec, self.home, token).to_config_dict(),
                "executionMetadata": {"tags": [{"key": key, "value": value} for key, value in tags.items()]},
            }
            return CompletionEffect(
                run_id=launch_reviewed_run(
                    self.port, TypeAdapter(dict[str, JsonValue]).validate_python(params)
                )
            )


class ReleaseRunner:
    def __init__(self, home: Path, instance: DagsterInstance, branch: Branch = "dataset") -> None:
        self.home = home.resolve()
        self.instance = instance
        self.branch: Branch = branch

    def __call__(self, token: CompletionToken) -> PublicationReference:
        with exclusive(self.home / "release-locks" / f"{token.dataset_id}.lock"):
            return self._release(token)

    def _release(self, token: CompletionToken) -> PublicationReference:
        with HumanReviewStore(self.home).reader() as tx:
            resolved = ReviewSnapshots(self.home, DatasetDataService(self.home, self.instance)).current(
                token.dataset_id
            )
            authorize_publication(resolved, token, tx)
            previous = tx.last_publication(token.dataset_id)
            if previous is not None and previous.authorization == token:
                return previous
        spec = DatasetRegistry(self.home).get(token.dataset_id)
        job = build_definitions(self.home).resolve_job_def("release_dataset")
        try:
            result = job.execute_in_process(
                instance=self.instance,
                partition_key=token.dataset_id,
                run_config=release_run_config(spec, self.home, token, branch=self.branch),
            )
        except DagsterUserCodeExecutionError as error:
            match error.__cause__:
                case HumanReviewError() as domain_error:
                    raise domain_error from error
                case _:
                    raise HumanReviewError("publication_failed", "Dagster publication failed") from error
        if not result.success:
            raise HumanReviewError("publication_failed", "Dagster publication failed")
        with HumanReviewStore(self.home).transaction() as tx:
            matches = [
                reference
                for reference in tx.list_publications(token.dataset_id)
                if reference.dagster_run_id == result.run_id
            ]
        if len(matches) != 1:
            raise IntegrityError("Successful release has no unique committed publication")
        return matches[0]
