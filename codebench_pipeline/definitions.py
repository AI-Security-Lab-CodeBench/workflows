import os
from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    PartitionedConfig,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SensorResult,
)
from dagster._config.pythonic_config.io_manager import PartialIOManager
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.bashguard_audit_asset import (
    AUDIT_IO_MANAGER,
    bashguard_audit_job,
    bashguard_syntax_audit,
)
from codebench_pipeline.collection_config import CAT3_DATASET_ID
from codebench_pipeline.config_assets import (
    ConfigRunIOManager,
    configuration_assets,
    configuration_key,
    configured_selection,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_assets_annotations import annotation_assets
from codebench_pipeline.dataset_assets_sources import processing_assets, source_assets
from codebench_pipeline.dataset_branches import Branch, collection_branches
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_engine import code_versions, stage_signature
from codebench_pipeline.dataset_finalization import finalization_assets
from codebench_pipeline.dataset_plan import (
    RELEASE_STAGES,
    collection_selection,
    plan_dataset,
    preparation_stages,
)
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run_configuration import dataset_run_config, release_run_config
from codebench_pipeline.review_assets import human_assets
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.stage_descriptions import describe_stage
from codebench_pipeline.storage import IntegrityError, fingerprint

DEFAULT_HOME = Path(os.environ.get("CODEBENCH_HOME", ".codebench-datasets")).resolve()

__all__ = ["DEFAULT_HOME", "build_definitions", "dataset_run_config", "defs", "release_run_config"]


def build_definitions(home: Path, *, specification: DatasetBuildSpec | None = None) -> Definitions:
    registry = DatasetRegistry(home)
    if specification is None:
        try:
            specification = registry.get_snapshot(CAT3_DATASET_ID)
        except FileNotFoundError:
            pass
    declared = specification is not None and specification.has_declared_collection
    branches = collection_branches(declared=declared)
    stages = preparation_stages(declared=declared)

    def with_configuration(spec: AssetSpec) -> AssetSpec:
        described = describe_stage(spec, declared=declared)
        upstream = configuration_key("/".join(spec.key.path[1:]))
        return (
            described
            if upstream in {dep.asset_key for dep in spec.deps}
            else described.merge_attributes(deps=[upstream])
        )

    existing_sources = source_assets()
    if declared:
        from codebench_pipeline.declared_source_assets import declared_source_assets

        sources = [
            existing_sources[0],
            *(asset for branch in branches for asset in declared_source_assets(branch)),
        ]
    else:
        sources = [*existing_sources, *source_assets("documentation")]
    processing = processing_assets()
    assets = [
        definition.map_asset_specs(with_configuration)
        for definition in [
            *sources,
            *(processing[:1] if declared else processing),
            *annotation_assets(declared=declared),
            *human_assets(),
            *finalization_assets(declared=declared),
        ]
    ]

    def partition_config(key: str) -> dict[str, JsonValue]:
        spec = matching_spec(key)
        return TypeAdapter(dict[str, JsonValue]).validate_python(
            dataset_run_config(spec, home).to_config_dict()
        )

    def release_config(key: str) -> dict[str, JsonValue]:
        return TypeAdapter(dict[str, JsonValue]).validate_python(
            release_run_config(matching_spec(key), home).to_config_dict()
        )

    def matching_spec(key: str) -> DatasetBuildSpec:
        spec = registry.get(key)
        if spec.has_declared_collection != declared:
            raise IntegrityError("Dataset registration does not match this configured source inputs")
        return spec

    def partition_tags(key: str) -> dict[str, str]:
        spec = matching_spec(key)
        return {"dataset/category": spec.rules.category_id, "dataset/revision": spec.revision}

    job = UnresolvedAssetJobDefinition(
        name="build_dataset",
        description="Process eligible corpus and documentation inventories without source approval.",
        selection=AssetSelection.assets(
            *configured_selection(AssetKey(["datasets", *name.split("/")]) for name in stages)
        ),
        partitions_def=DATASET_PARTITIONS,
        config=PartitionedConfig(
            DATASET_PARTITIONS,
            run_config_for_partition_key_fn=partition_config,
            tags_for_partition_key_fn=partition_tags,
        ),
    )

    def collection_job(branch: Branch, name: str) -> UnresolvedAssetJobDefinition:
        def configuration(key: str) -> dict[str, JsonValue]:
            return TypeAdapter(dict[str, JsonValue]).validate_python(
                dataset_run_config(matching_spec(key), home, branch).to_config_dict()
            )

        return UnresolvedAssetJobDefinition(
            name=name,
            description=f"Acquire and extract {branch} candidates independently of labeling and review.",
            selection=AssetSelection.assets(
                *configured_selection(
                    AssetKey(["datasets", *stage.split("/")]) for stage in collection_selection(branch)
                )
            ),
            partitions_def=DATASET_PARTITIONS,
            config=PartitionedConfig(
                DATASET_PARTITIONS,
                run_config_for_partition_key_fn=configuration,
                tags_for_partition_key_fn=partition_tags,
            ),
        )

    collection_jobs = [
        collection_job(branch.namespace, branch.job)
        for branch in branches
        if declared or branch.namespace == "documentation"
    ]
    if declared:
        prepare = stages[: stages.index("filtered_commands") + 1]

        def preparation_config(key: str) -> dict[str, JsonValue]:
            return TypeAdapter(dict[str, JsonValue]).validate_python(
                dataset_run_config(matching_spec(key), home, prepare_only=True).to_config_dict()
            )

        collection_jobs.append(
            UnresolvedAssetJobDefinition(
                name="prepare_dataset",
                description="Acquire and filter all three branches before labeling.",
                selection=AssetSelection.assets(
                    *configured_selection(AssetKey(["datasets", *name.split("/")]) for name in prepare)
                ),
                partitions_def=DATASET_PARTITIONS,
                config=PartitionedConfig(
                    DATASET_PARTITIONS,
                    run_config_for_partition_key_fn=preparation_config,
                    tags_for_partition_key_fn=partition_tags,
                ),
            )
        )
    release_job = UnresolvedAssetJobDefinition(
        name="release_dataset",
        description="Release eligible low-uncertainty rows automatically; uncertain rows require completed human review.",
        selection=AssetSelection.assets(
            *configured_selection(
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
        for key in keys:
            spec = registry.get(key)
            if spec.has_declared_collection != declared:
                continue
            configs = stage_configs(spec)
            plan = plan_dataset(spec, configs, context.instance, home=home)
            if not plan.selected:
                continue
            version = fingerprint(
                {
                    "specification": spec.model_dump_json(),
                    "source_inputs": {
                        branch.namespace: stage_signature(
                            "source_catalog",
                            configs[collection_selection(branch.namespace)[1]],
                            {"category_rules": spec.rules},
                        )
                        for branch in branches
                    },
                    "code": {name: value for name, value in code_versions().items()},
                }
            )
            requests.append(
                RunRequest(
                    run_key=f"{key}:{version}",
                    partition_key=key,
                    asset_selection=plan.asset_selection,
                    run_config=dataset_run_config(spec, home).to_config_dict(),
                    tags={
                        "dataset/category": spec.rules.category_id,
                        "dataset/revision": spec.revision,
                    },
                )
            )
        return SensorResult(
            run_requests=requests,
            dynamic_partitions_requests=[DATASET_PARTITIONS.build_add_request(keys)] if keys else [],
        )

    return Definitions(
        assets=[*configuration_assets(home, declared=declared), *assets, bashguard_syntax_audit],
        jobs=[job, *collection_jobs, release_job, bashguard_audit_job(home)],
        sensors=[
            SensorDefinition(name="dataset_build_requests", job=job, evaluation_fn=dataset_build_requests)
        ],
        resources={
            "config_run_io_manager": ConfigRunIOManager(home=str(home)),
            AUDIT_IO_MANAGER: PartialIOManager(resource_cls=DataFrameIOManager, data={}),
            "dataset_io_manager": PartialIOManager(
                resource_cls=DataFrameIOManager, data={"review_home": str(home)}
            ),
        },
    )


defs = build_definitions(DEFAULT_HOME)
