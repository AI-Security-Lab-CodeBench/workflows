from hashlib import sha256
from io import BytesIO, TextIOWrapper
from pathlib import Path
from unittest.mock import patch

import httpx2
import pandas as pd
import pytest
from dagster import AssetKey
from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from pydantic import ValidationError
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.artifact_views import (
    ArtifactDataService,
    ArtifactJSON,
    ArtifactKey,
    ArtifactQuery,
    ArtifactTable,
)
from codebench_pipeline.config_assets import GLOBAL_CONFIG, SINGLETON_RUN_CONFIGS, SPLIT_CATEGORY_CONFIGS
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_artifacts import DatasetArtifactCatalog
from codebench_pipeline.dataset_cli import main
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.full_dataset import FullDatasetBundle
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.train_assets import register_dataset_partitions
from codebench_pipeline.train_config import SplitSections, TrainConfig, TrainSource

_VALIDATOR: type[Validator] = Draft202012Validator


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
    empty = source(6, "CAT-5", b"")
    train = TrainConfig(sources=[c, a, b])
    test = TrainConfig(sources=[held, empty])
    spec = fixture_spec(tmp_path).model_copy(update={"train": train, "test": test})
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
        register_dataset_partitions(instance, spec.model_copy(update={"train": test}))
        sections = SplitSections(train=train, test=test)
        with patch("sys.stdin", TextIOWrapper(BytesIO(sections.model_dump_json().encode()))):
            cli("configure-splits")
        config_job = defs.resolve_job_def("refresh_dataset_configuration")
        merge_train = defs.resolve_job_def("merge_train_data")
        load_train = defs.resolve_job_def("load_train_category")
        p3 = "CAT-3"
        graph = defs.resolve_asset_graph()
        register_dataset_partitions(
            instance, spec.model_copy(update={"dataset_id": "unrelated", "train": test, "test": None})
        )
        for split, expected_keys in (("train", {"CAT-3", "CAT-4"}), ("test", {"CAT-3", "CAT-5"})):
            for name in ("category_rows", "stats", "merged_rows"):
                partitions_def = graph.get(AssetKey([split, name])).partitions_def
                assert partitions_def is not None
                assert set(partitions_def.get_partition_keys(dynamic_partitions_store=instance)) == (
                    {"codebench"} if name == "merged_rows" else expected_keys
                )
        cli("train-load", "--category", "CAT-3")
        incomplete = merge_train.execute_in_process(
            instance=instance, partition_key=spec.dataset_id, raise_on_error=False
        )
        assert not incomplete.success
        assert not instance.get_latest_materialization_event(AssetKey(["train", "merged_rows"]))
        viewer = ArtifactDataService(DatasetArtifactCatalog(tmp_path, instance), defs)
        assert not viewer.partitions(ArtifactKey(asset_key=("train", "merged_rows"))).partitions
        assert not viewer.partitions(ArtifactKey(asset_key=("test", "stats"))).partitions
        cli("train-load", "--category", "CAT-4")
        cli("train-merge")
        cli("test-load", "--category", "CAT-3")
        cli("test-load", "--category", "CAT-5")
        cli("test-merge")
        cli("full-dataset")

        for keys, partition in ((SPLIT_CATEGORY_CONFIGS, "CAT-3"), (SINGLETON_RUN_CONFIGS, "codebench")):
            for key in sorted(keys):
                assert partition in viewer.partitions(ArtifactKey(asset_key=key)).partitions, key
                result = viewer.query(ArtifactQuery(asset_key=key, partition_key=partition))
                assert isinstance(result, ArtifactJSON), key
                assert result.data_schema.source == "declared", key
                schema = result.data_schema.schema_
                Draft202012Validator.check_schema(schema)
                properties = schema["properties"]
                assert isinstance(properties, dict)
                assert set(result.value) <= set(properties), key
                validator = _VALIDATOR(schema)
                assert validator.is_valid(result.value), key
                assert not validator.is_valid("{}"), key

        def full_dataset() -> FullDatasetBundle:
            request = ArtifactKey(asset_key=("codebench", "full_dataset"))
            assert viewer.partitions(request).partitions == ("codebench",)
            result = viewer.query(ArtifactQuery(asset_key=request.asset_key, partition_key="codebench"))
            assert isinstance(result, ArtifactJSON)
            assert result.data_schema.source == "declared"
            return FullDatasetBundle.model_validate(result.value)

        original = full_dataset()
        assert original.train.row_count == 3
        assert original.test.row_count == 1
        assert original.train.load()["command"].tolist() == ["pwd", "ls", "date"]
        assert original.test.load()["command"].tolist() == ["whoami"]
        with patch("codebench_pipeline.repository_tree._repository_client", side_effect=AssertionError):
            cli("train-load", "--category", "CAT-3", "--stats-only")
        p4 = "CAT-4"
        for split, partitions, total, expected in (
            (
                "train",
                (p3, p4),
                2,
                [
                    ("risk", "CAT-3.1", 1),
                    ("risk", "CAT-3.2", 1),
                    ("severity", "low", 2),
                    ("class", "allow", 2),
                ],
            ),
            ("test", (p3, "CAT-5"), 1, [("risk", "CAT-3.2", 1), ("severity", "low", 1), ("class", "ask", 1)]),
        ):
            stats_key = (split, "stats")
            assert viewer.partitions(ArtifactKey(asset_key=stats_key)).partitions == partitions
            stats = viewer.query(ArtifactQuery(asset_key=stats_key, partition_key=p3))
            assert isinstance(stats, ArtifactJSON)
            assert stats.data_schema.source == "declared"
            assert stats.value == {
                "total_rows": total,
                "marginal_counts": [
                    {"category": "CAT-3", "label": label, "value": value, "count": count}
                    for label, value, count in expected
                ],
            }
            category_key = (split, "category_rows")
            assert viewer.partitions(ArtifactKey(asset_key=category_key)).partitions == partitions
            category = viewer.query(ArtifactQuery(asset_key=category_key, partition_key=p3))
            assert isinstance(category, ArtifactTable)
            assert category.data_schema.source == "declared"
            assert len(category.page.rows) == total
        empty_stats = viewer.query(ArtifactQuery(asset_key=("test", "stats"), partition_key="CAT-5"))
        assert isinstance(empty_stats, ArtifactJSON)
        assert empty_stats.value == {"total_rows": 0, "marginal_counts": []}
        assert table("train", "merged_rows", spec.dataset_id)["command"].tolist() == ["pwd", "ls", "date"]
        assert table("test", "merged_rows", spec.dataset_id)["command"].tolist() == ["whoami"]
        for split, commands in (("train", ["pwd", "ls", "date"]), ("test", ["whoami"])):
            asset_key = (split, "merged_rows")
            assert viewer.partitions(ArtifactKey(asset_key=asset_key)).partitions == (spec.dataset_id,)
            artifact = viewer.query(ArtifactQuery(asset_key=asset_key, partition_key=spec.dataset_id))
            assert isinstance(artifact, ArtifactTable)
            assert artifact.publication_state is None
            assert [row.values["command"] for row in artifact.page.rows] == commands
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
        cli("train-load", "--category", "CAT-3")
        cli("train-merge")
        assert table("train", "merged_rows", spec.dataset_id)["command"].tolist() == ["echo", "date"]
        assert table("test", "merged_rows", spec.dataset_id)["command"].tolist() == ["whoami"]
        assert original.train.load()["command"].tolist() == ["pwd", "ls", "date"]
        build_bundle = defs.resolve_job_def("build_full_dataset")
        assert not build_bundle.execute_in_process(
            instance=instance, partition_key="codebench", raise_on_error=False
        ).success
        cli("test-merge")
        cli("full-dataset")
        current = full_dataset()
        assert current.train.load()["command"].tolist() == ["echo", "date"]
        assert current.test.load()["command"].tolist() == ["whoami"]
        assert current.train.reference != original.train.reference
        assert current.test.reference == original.test.reference
