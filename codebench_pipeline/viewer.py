import os
import subprocess
import sys
from pathlib import Path
from typing import assert_never

import pandas as pd
from dagster import AssetKey, build_input_context
from pydantic import JsonValue

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import HISTORICAL_PROCESSING_STAGES
from codebench_pipeline.dataset_plan import INPUT_STAGES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.runtime_instance import dataset_instance


def viewer_environment(home: Path) -> dict[str, str]:
    resolved = home.resolve()
    directory = resolved / "dagster"
    if not directory.is_dir():
        raise FileNotFoundError(
            f"No dataset instance at {directory}. Choose the home used by build or register."
        )
    return {**os.environ, "CODEBENCH_HOME": str(resolved), "DAGSTER_HOME": str(directory)}


def launch_ui(home: Path, port: int) -> None:
    if not 1 <= port <= 65535:
        raise ValueError("UI port must be between 1 and 65535")
    environment = viewer_environment(home)
    print(f"Dataset home: {environment['CODEBENCH_HOME']}", flush=True)
    print(f"Dagster history: {environment['DAGSTER_HOME']}", flush=True)
    print(f"View data: http://127.0.0.1:{port}/assets/datasets/deduplicated_annotations?view=data", flush=True)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "codebench_pipeline.ui_server",
            "--home",
            str(home.resolve()),
            "--port",
            str(port),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        check=True,
    )


def preview_asset(home: Path, dataset: str, stage: str, limit: int) -> dict[str, JsonValue]:
    if stage not in INPUT_STAGES and stage not in HISTORICAL_PROCESSING_STAGES:
        raise ValueError(f"Unknown dataset stage: {stage}")
    if not 1 <= limit <= 1000:
        raise ValueError("Preview limit must be between 1 and 1000")
    spec = DatasetRegistry(home).get(dataset)
    manager = DataFrameIOManager(storage=spec.storage, review_home=str(home))
    with dataset_instance(home) as instance:
        with build_input_context(
            asset_key=AssetKey(["datasets", *stage.split("/")]),
            partition_key=dataset,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=instance,
        ) as context:
            snapshot = manager.read_current(context)
            value = snapshot.value
    result: dict[str, JsonValue] = {
        "home": str(home.resolve()),
        "dataset": dataset,
        "stage": stage,
        "review_state": snapshot.publication_state,
    }
    match value:
        case pd.DataFrame():
            rows = frame_records(value.head(limit))
            result.update(
                kind="table",
                row_count=len(value),
                shown_rows=len(rows),
                data=[{key: cell for key, cell in row.items()} for row in rows],
            )
        case CategoryRules():
            result.update(kind="rules", data=value.model_dump(mode="json"))
        case _:
            assert_never(value)
    return result
