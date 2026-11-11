import argparse
import os
from pathlib import Path
from typing import Literal, Protocol

import uvicorn
from dagster import __version__
from dagster._core.workspace.context import WorkspaceProcessContext, WorkspaceRequestContext
from dagster._core.workspace.load_target import ModuleTarget
from dagster_webserver.webserver import DagsterWebserver
from pydantic import BaseModel, ConfigDict, Field
from starlette.applications import Starlette
from starlette.routing import Mount

from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.release_runner import ReleaseContinuation
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.review_stages import ReviewRegistration
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_stage
from codebench_pipeline.source_review_runner import SourceReviewRunner
from codebench_pipeline.viewer import viewer_environment


class BuildManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    dagster_version: Literal["1.13.29"]
    upstream_commit: Literal["7dca97fabebf771d4ac0515d674e50e2a742446f"]
    source_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_files: dict[str, str] = Field(default_factory=dict)


class ApplicationFactory(Protocol):
    def create_asgi_app(self) -> Starlette: ...


class NativeWebserver(DagsterWebserver[WorkspaceProcessContext, WorkspaceRequestContext]):
    def __init__(self, workspace: WorkspaceProcessContext, bundle: Path) -> None:
        self._bundle = bundle
        super().__init__(workspace)

    def relative_path(self, rel: str) -> str:
        prefix = "webapp/build/"
        if rel.startswith(prefix):
            return str(self._bundle / rel.removeprefix(prefix))
        return super().relative_path(rel)


def integrated_app(factory: ApplicationFactory, api: Starlette) -> Starlette:
    return Starlette(routes=[Mount("/codebench/api", app=api), Mount("/", app=factory.create_asgi_app())])


def serve(home: Path, bundle: Path, port: int) -> None:
    if not 1 <= port <= 65535:
        raise ValueError("UI port must be between 1 and 65535")
    manifest_path = bundle / "codebench-build.json"
    if not manifest_path.is_file():
        raise FileNotFoundError("Build the native extension with python -m codebench_pipeline.ui_build")
    manifest = BuildManifest.model_validate_json(manifest_path.read_bytes())
    if __version__ != manifest.dagster_version:
        raise RuntimeError("Installed Dagster differs from the pinned frontend build")
    os.environ.update(viewer_environment(home))
    target = ModuleTarget(
        module_name="codebench_pipeline.definitions",
        attribute=None,
        working_directory=str(Path(__file__).resolve().parents[1]),
        location_name="Dataset building",
    )
    with dataset_instance(home) as instance:
        data = DatasetDataService(home, instance)
        review = ReviewService(home, data, ReleaseContinuation(home, instance, port))
        sources = ReviewWorkflow(
            home,
            source_stage(),
            SourceReviewSource(home, instance),
            on_complete=SourceReviewRunner(home, instance, port),
        )
        api = make_dataset_api(
            data,
            review,
            port,
            stages=(ReviewRegistration(source_stage(), sources),),
        )
        with WorkspaceProcessContext(instance, target, version=__version__) as workspace:
            app = integrated_app(NativeWebserver(workspace, bundle), api)
            uvicorn.run(app, host="127.0.0.1", port=port)


def main() -> None:
    parser = argparse.ArgumentParser(description="Native Dagster data and human-review panels.")
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--port", type=int, default=3000)
    parser.add_argument(
        "--bundle", type=Path, default=Path(__file__).resolve().parents[1] / "dagster_ui" / "build"
    )
    args = parser.parse_args()
    serve(Path(args.home).resolve(), Path(args.bundle).resolve(), int(args.port))


if __name__ == "__main__":
    main()
