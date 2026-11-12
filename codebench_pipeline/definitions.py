import os
from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    PartitionedConfig,
    RunConfig,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SensorResult,
)
from dagster._config.pythonic_config.io_manager import PartialIOManager
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_assets_annotations import annotation_assets
from codebench_pipeline.dataset_assets_sources import source_assets
from codebench_pipeline.dataset_branches import Branch, stage_branch
from codebench_pipeline.dataset_config import DatasetBuildSpec, catalog_for_branch
from codebench_pipeline.dataset_engine import code_versions, stage_signature
from codebench_pipeline.dataset_finalization import finalization_assets
from codebench_pipeline.dataset_plan import PREPARATION_STAGES, RELEASE_STAGES, plan_dataset
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import CompletionToken
from codebench_pipeline.review_assets import human_assets
from codebench_pipeline.review_config import HumanReviewConfig
from codebench_pipeline.stage_configuration import stage_configs as stage_configs
from codebench_pipeline.stage_descriptions import describe_stage
from codebench_pipeline.storage import fingerprint

DEFAULT_HOME = Path(os.environ.get("CODEBENCH_HOME", ".codebench-datasets")).resolve()


def dataset_run_config(spec: DatasetBuildSpec, home: Path, branch: Branch | None = None) -> RunConfig:
    return RunConfig(
        ops={
            f"datasets__{name.replace('/', '__')}": config
            for name, config in stage_configs(spec).items()
            if name in PREPARATION_STAGES
            and (
                branch is None
                or name == "category_rules"
                or stage_branch(name) == branch
                or name == "merged_proposed_annotations"
            )
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
            f"datasets__{name}": HumanReviewConfig(authorization=authorization)
            for name in ("human_review_decisions", "dataset_release")
        },
        resources={
            "dataset_io_manager": DataFrameIOManager(
                storage=spec.storage, review_home=str(home), review_token=authorization or None
            )
        },
    )


def build_definitions(home: Path) -> Definitions:
    assets = [
        definition.map_asset_specs(describe_stage)
        for definition in [
            *source_assets(),
            *annotation_assets(),
            *human_assets(),
            *source_assets("documentation"),
            *annotation_assets("documentation"),
            *finalization_assets(),
        ]
    ]

    def partition_config(key: str) -> dict[str, JsonValue]:
        return TypeAdapter(dict[str, JsonValue]).validate_python(
            dataset_run_config(DatasetRegistry(home).get(key), home).to_config_dict()
        )

    def release_config(key: str) -> dict[str, JsonValue]:
        return TypeAdapter(dict[str, JsonValue]).validate_python(
            release_run_config(DatasetRegistry(home).get(key), home).to_config_dict()
        )

    def documentation_config(key: str) -> dict[str, JsonValue]:
        return TypeAdapter(dict[str, JsonValue]).validate_python(
            dataset_run_config(DatasetRegistry(home).get(key), home, "documentation").to_config_dict()
        )

    def partition_tags(key: str) -> dict[str, str]:
        spec = DatasetRegistry(home).get(key)
        return {"dataset/category": spec.rules.category_id, "dataset/revision": spec.revision}

    job = UnresolvedAssetJobDefinition(
        name="build_dataset",
        description="Process eligible corpus and documentation inventories without source approval.",
        selection=AssetSelection.assets(
            *(AssetKey(["datasets", *name.split("/")]) for name in PREPARATION_STAGES)
        ),
        partitions_def=DATASET_PARTITIONS,
        config=PartitionedConfig(
            DATASET_PARTITIONS,
            run_config_for_partition_key_fn=partition_config,
            tags_for_partition_key_fn=partition_tags,
        ),
    )
    documentation_job = UnresolvedAssetJobDefinition(
        name="build_documentation",
        description="Automatically prepare eligible documentation through per-command labeling.",
        selection=AssetSelection.assets(
            *(
                AssetKey(["datasets", *name.split("/")])
                for name in PREPARATION_STAGES
                if name == "category_rules"
                or stage_branch(name) == "documentation"
                or name == "merged_proposed_annotations"
            )
        ),
        partitions_def=DATASET_PARTITIONS,
        config=PartitionedConfig(
            DATASET_PARTITIONS,
            run_config_for_partition_key_fn=documentation_config,
            tags_for_partition_key_fn=partition_tags,
        ),
    )
    release_job = UnresolvedAssetJobDefinition(
        name="release_dataset",
        description="Release eligible low-uncertainty rows automatically; uncertain rows require completed human review.",
        selection=AssetSelection.assets(
            *(
                AssetKey(["datasets", name])
                for name in RELEASE_STAGES
                if not name.startswith("documentation/")
            )
        ),
        partitions_def=DATASET_PARTITIONS,
        config=PartitionedConfig(
            DATASET_PARTITIONS,
            run_config_for_partition_key_fn=release_config,
            tags_for_partition_key_fn=partition_tags,
        ),
    )

    def dataset_build_requests(context: SensorEvaluationContext) -> SensorResult:
        registry = DatasetRegistry(home)
        keys = registry.keys()
        requests: list[RunRequest] = []
        branches: tuple[Branch, ...] = ("dataset", "documentation")
        for key in keys:
            spec = registry.get(key)
            plan = plan_dataset(spec, stage_configs(spec), context.instance, home=home)
            if not plan.selected:
                continue
            version = fingerprint(
                {
                    "specification": spec.model_dump_json(),
                    "source_inputs": {
                        branch: stage_signature(
                            "source_catalog",
                            catalog_for_branch(spec, branch),
                            {"category_rules": spec.rules},
                        )
                        for branch in branches
                    },
                    "documentation": spec.documentation.model_dump_json(),
                    "code": {name: value for name, value in code_versions().items()},
                }
            )
            requests.append(
                RunRequest(
                    run_key=f"{key}:{version}",
                    partition_key=key,
                    asset_selection=plan.asset_selection,
                )
            )
        return SensorResult(
            run_requests=requests,
            dynamic_partitions_requests=[DATASET_PARTITIONS.build_add_request(keys)] if keys else [],
        )

    return Definitions(
        assets=assets,
        jobs=[job, documentation_job, release_job],
        sensors=[
            SensorDefinition(name="dataset_build_requests", job=job, evaluation_fn=dataset_build_requests)
        ],
        resources={
            "dataset_io_manager": PartialIOManager(
                resource_cls=DataFrameIOManager, data={"review_home": str(home)}
            )
        },
    )


defs = build_definitions(DEFAULT_HOME)
