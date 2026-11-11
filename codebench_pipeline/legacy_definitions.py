"""Historical CAT-3 replay graph retained separately from dataset building."""

import os
from pathlib import Path

from dagster import (
    AllPartitionMapping,
    AssetDep,
    AssetExecutionContext,
    AssetIn,
    AssetsDefinition,
    AssetSelection,
    DataVersion,
    Definitions,
    DynamicPartitionsDefinition,
    MetadataValue,
    Output,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SensorResult,
    TableColumn,
    TableSchema,
    asset,
    fs_io_manager,
)

from codebench_pipeline.discovery_assets import discovery_assets
from codebench_pipeline.engine import DEPENDENCIES, execute_stage, stage_inputs
from codebench_pipeline.models import Stage
from codebench_pipeline.publication import publish
from codebench_pipeline.schemas import FIELDS
from codebench_pipeline.storage import ArtifactStore, Registry, fingerprint

PARTITIONS = DynamicPartitionsDefinition(name="datasets")
DEFAULT_ROOT = Path(os.environ.get("CODEBENCH_HOME", ".codebench")).resolve()


def make_asset(stage: Stage, root: Path) -> AssetsDefinition:
    @asset(
        name=stage,
        partitions_def=PARTITIONS,
        ins={name: AssetIn(key=name, dagster_type=str) for name in DEPENDENCIES[stage]},
        deps=[AssetDep("source_acquisition")] if stage == "source_snapshot" else [],
        group_name="collection"
        if stage in {"source_snapshot", "adapter", "extraction", "filtering"}
        else "annotation",
        description=f"Immutable {stage.replace('_', ' ')} per dataset. Cache verified by content hashes.",
        compute_kind="Pandera" if stage in {"filtering", "export"} else "Python",
    )
    def stage_asset(context: AssetExecutionContext, **paths: str) -> Output[str]:
        spec = Registry(root).get(context.partition_key)
        artifact, cached = execute_stage(
            ArtifactStore(root), spec, stage, {key: Path(value) for key, value in paths.items()}
        )
        return Output(
            str(artifact.path),
            data_version=DataVersion(artifact.fingerprint),
            metadata={
                "artifact": str(artifact.path),
                "fingerprint": artifact.fingerprint,
                "cache_hit": cached,
                "files": len(artifact.files),
                "details": MetadataValue.json(artifact.metadata),
                "mode": "historical_replay" if spec.replay else "fresh",
                **(
                    {
                        "dagster/column_schema": TableSchema(
                            columns=[TableColumn(name=field, type="string") for field in FIELDS]
                        )
                    }
                    if stage == "export"
                    else {}
                ),
            },
        )

    return stage_asset


def build_definitions(root: Path) -> Definitions:
    assets = [make_asset(stage, root) for stage in DEPENDENCIES]

    @asset(
        group_name="publication",
        ins={"export": AssetIn(partition_mapping=AllPartitionMapping())},
        description="Combine completed datasets with the global 250-record-per-risk cap, without relabeling.",
    )
    def published_release(export: dict[str, str]) -> Output[str]:
        destination = publish(root, export)
        return Output(str(destination), metadata={"bundle": str(destination / "bundle")})

    def registered_datasets(context: SensorEvaluationContext) -> SensorResult:
        registry = Registry(root)
        requests: list[RunRequest] = []
        keys = registry.keys()
        for key in keys:
            spec = registry.get(key)
            version = fingerprint(
                {
                    "spec": spec.model_dump_json(),
                    "stages": {
                        stage: fingerprint(
                            {name: value for name, value in stage_inputs(spec, stage, {}).items()}
                        )
                        for stage in DEPENDENCIES
                    },
                }
            )
            requests.append(RunRequest(run_key=f"{key}:{version}", partition_key=key))
        return SensorResult(
            run_requests=requests,
            dynamic_partitions_requests=[PARTITIONS.build_add_request(keys)] if keys else [],
        )

    return Definitions(
        assets=[*discovery_assets(root), *assets, published_release],
        sensors=[
            SensorDefinition(
                name="registered_datasets",
                evaluation_fn=registered_datasets,
                asset_selection=AssetSelection.assets(*assets),
            )
        ],
        resources={"io_manager": fs_io_manager.configured({"base_dir": str(root / "dagster-io")})},
    )


defs = build_definitions(DEFAULT_ROOT)
