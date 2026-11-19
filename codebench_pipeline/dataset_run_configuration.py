from pathlib import Path

from dagster import RunConfig

from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.config_assets import GlobalConfig
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_dependencies import collection_selection, preparation_stages
from codebench_pipeline.human_review import CompletionToken
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.stage_configuration import stage_configs


def dataset_run_config(
    spec: DatasetBuildSpec, home: Path, branch: Branch | None = None, *, prepare_only: bool = False
) -> RunConfig:
    selection = (
        preparation_stages(declared=spec.has_declared_collection)
        if branch is None
        else collection_selection(branch)
    )
    if prepare_only:
        selection = selection[: selection.index("filtered_commands") + 1]
    return RunConfig(
        ops={
            "datasets__global_config": GlobalConfig(snapshot=spec.model_dump_json()),
            **{
                f"datasets__{name.replace('/', '__')}": config
                for name, config in stage_configs(spec).items()
                if name in selection
                and name != "category_rules"
                and not isinstance(config, DeclaredSourceStageConfig)
            },
        },
        resources={"dataset_io_manager": DataFrameIOManager(storage=spec.storage, review_home=str(home))},
    )


def release_run_config(
    spec: DatasetBuildSpec,
    home: Path,
    token: CompletionToken | None = None,
    branch: Branch = "dataset",
) -> RunConfig:
    authorization = token.model_dump_json() if token is not None else ""
    return RunConfig(
        ops={
            "datasets__global_config": GlobalConfig(snapshot=spec.model_dump_json()),
            **{
                f"datasets__{name}": HumanReviewConfig(authorization=authorization)
                for name in ("human_review_decisions", "dataset_release")
            },
        },
        resources={
            "dataset_io_manager": DataFrameIOManager(
                storage=spec.storage, review_home=str(home), review_token=authorization or None
            )
        },
    )
