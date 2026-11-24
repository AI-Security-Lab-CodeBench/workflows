from hashlib import sha256
from io import BytesIO, TextIOWrapper
from pathlib import Path
from unittest.mock import patch

import httpx2
import pandas as pd
import pytest
from dagster import AssetKey, MultiPartitionKey
from pydantic import ValidationError
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.config_assets import GLOBAL_CONFIG
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_cli import main
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.train_config import SplitSections, TrainConfig, TrainSource


def test_train_loading(tmp_path: Path) -> None:
    header = b"command,description,util,severity,category,risk,source,seed,class\n"
    blobs: dict[str, bytes] = {}

    def source(number: int, category: str, rows: bytes) -> TrainSource:
        commit = f"{number:040x}"
        data = header + rows
        blobs[f"/fixture/data/{commit}/commands.csv"] = data
        return TrainSource(
            repository="fixture/data",
            branch=f"feat/data_{number}",
            commit=commit,
            path="commands.csv",
            category=category,
            sha256=sha256(data).hexdigest(),
        )

    a = source(1, "CAT-3", b"pwd,,,low,CAT-3,CAT-3.1,fixture,,allow\n")
    b = source(2, "CAT-3", b"ls,,,low,CAT-3 demo,CAT-3.2,fixture,,allow\n")
    c = source(3, "CAT-4", b"date,,,low,CAT-4,CAT-4.1,fixture,,allow\n")
    held = source(4, "CAT-3", b"whoami,,,low,CAT-3,CAT-3.2,fixture,,ask\n")
    train = TrainConfig(sources=[c, a, b])
    spec = fixture_spec(tmp_path).model_copy(update={"train": train, "test": TrainConfig(sources=[held])})
    registry = DatasetRegistry(tmp_path)
    with pytest.raises(ValidationError, match="both train and test"):
        registry.register(spec.model_copy(update={"test": TrainConfig(sources=[a])}))
    registry.register(fixture_spec(tmp_path))
    defs = build_definitions(tmp_path)
    manager = DataFrameIOManager(storage=spec.storage)

    def table(split: str, name: str, partition: str) -> pd.DataFrame:
        ref = manager.reference_at([split, name], partition)
        value = manager.read_version(ref)
        assert isinstance(value, pd.DataFrame)
        return value

    def reply(request: httpx2.Request) -> httpx2.Response:
        assert request.url.host == "raw.githubusercontent.com"
        return httpx2.Response(200, stream=httpx2.ByteStream(blobs[request.url.path]))

    def cli(operation: str, *extra: str) -> None:
        with patch(
            "sys.argv",
            ["dataset_cli", operation, "--dataset", spec.dataset_id, "--home", str(tmp_path), *extra],
        ):
            main()

    with (
        dataset_instance(tmp_path) as instance,
        httpx2.Client(transport=httpx2.MockTransport(reply)) as client,
        patch("codebench_pipeline.repository_tree._repository_client", return_value=client),
    ):
        sections = SplitSections(train=train, test=TrainConfig(sources=[held]))
        with patch("sys.stdin", TextIOWrapper(BytesIO(sections.model_dump_json().encode()))):
            cli("configure-splits")
        config_job = defs.resolve_job_def("refresh_dataset_configuration")
        merge_train = defs.resolve_job_def("merge_train_data")
        load_train = defs.resolve_job_def("load_train_category")
        p3 = MultiPartitionKey({"dataset": spec.dataset_id, "category": "CAT-3"})
        cli("train-load", "--category", "CAT-3")
        incomplete = merge_train.execute_in_process(
            instance=instance, partition_key=spec.dataset_id, raise_on_error=False
        )
        assert not incomplete.success
        assert not instance.get_latest_materialization_event(AssetKey(["train", "merged_rows"]))
        cli("train-load", "--category", "CAT-4")
        cli("train-merge")
        cli("test-load", "--category", "CAT-3")
        cli("test-merge")
        counts = table("train", "label_counts", p3).set_index(["category", "label", "value"])
        assert counts["count"].to_dict() == {
            ("CAT-3", "risk", "CAT-3.1"): 1,
            ("CAT-3", "risk", "CAT-3.2"): 1,
            ("CAT-3", "severity", "low"): 2,
            ("CAT-3", "class", "allow"): 2,
        }
        assert table("train", "merged_rows", spec.dataset_id)["command"].tolist() == ["pwd", "ls", "date"]
        assert table("test", "merged_rows", spec.dataset_id)["command"].tolist() == ["whoami"]
        key = ["train", "merged_rows"]
        digest = manager.reference_at(key, spec.dataset_id)
        load_train.execute_in_process(instance=instance, partition_key=p3)
        merge_train.execute_in_process(instance=instance, partition_key=spec.dataset_id)
        assert manager.reference_at(key, spec.dataset_id) == digest
        revised = source(5, "CAT-3", b"echo,,,low,CAT-3,CAT-3.1,fixture,,allow\n")
        registry.register(spec.model_copy(update={"train": TrainConfig(sources=[revised, c])}))
        config_job.execute_in_process(
            instance=instance, partition_key=spec.dataset_id, asset_selection=[GLOBAL_CONFIG]
        )
        stale = merge_train.execute_in_process(
            instance=instance, partition_key=spec.dataset_id, raise_on_error=False
        )
        assert not stale.success
        assert manager.reference_at(key, spec.dataset_id) == digest
