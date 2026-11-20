from dagster import RunConfig

from codebench_pipeline.config_assets import GlobalConfig
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_storage_io import StorageBinding
from codebench_pipeline.human_review import CompletionToken
from codebench_pipeline.storage import IntegrityError


def dataset_run_config(spec: DatasetBuildSpec) -> RunConfig:
    if not spec.has_declared_collection:
        raise IntegrityError("Dataset runs require a declared collection")
    root = GlobalConfig.for_spec(spec)
    return RunConfig(
        ops={"datasets__global_config": root},
        resources={
            "dataset_io_manager": {
                "config": StorageBinding(registered_revision=root.expected_revision).model_dump(
                    exclude_none=True
                )
            },
        },
    )


def release_run_config(
    spec: DatasetBuildSpec,
    token: CompletionToken | None = None,
) -> RunConfig:
    if not spec.has_declared_collection:
        raise IntegrityError("Dataset release requires a declared collection")
    authorization = token.model_dump_json() if token is not None else ""
    root = GlobalConfig.for_spec(spec)
    return RunConfig(
        ops={"datasets__global_config": root},
        resources={
            "dataset_io_manager": {
                "config": StorageBinding(
                    registered_revision=root.expected_revision, review_token=authorization or None
                ).model_dump(exclude_none=True)
            },
        },
    )
