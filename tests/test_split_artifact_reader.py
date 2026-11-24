from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    Definitions,
    DynamicPartitionsDefinition,
    IOManagerDefinition,
    StaticPartitionsDefinition,
    asset,
    build_output_context,
    materialize,
)
from pydantic import BaseModel, ConfigDict

from codebench_pipeline.artifact_views import ArtifactDataService, ArtifactJSON, ArtifactKey, ArtifactQuery
from codebench_pipeline.config_assets import ConfigRunIOManager
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataset_artifacts import DatasetArtifactCatalog
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
from codebench_pipeline.dataset_registry import StoredObjectLocation
from codebench_pipeline.dataset_storage_io import dataset_storage_io
from codebench_pipeline.object_store import StorageConfig, sha256


class ManifestFixture(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    train_rows: int
    test_rows: int


class SplitArtifactReaderTests(TestCase):
    def test_full_dataset_manifest_is_discovered_and_read_as_declared_json(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        instance = self.enterContext(DagsterInstance.ephemeral())
        manager = ConfigRunIOManager(home=str(home))

        @asset(
            key=["codebench", "full_dataset"],
            partitions_def=StaticPartitionsDefinition([CODEBENCH_DATASET_ID]),
            io_manager_key="config_run_io_manager",
        )
        def manifest() -> ManifestFixture:
            return ManifestFixture(train_rows=3, test_rows=1)

        resources = {"config_run_io_manager": IOManagerDefinition.hardcoded_io_manager(manager)}
        self.assertTrue(
            materialize([manifest], resources=resources, instance=instance, partition_key="codebench").success
        )
        service = ArtifactDataService(
            DatasetArtifactCatalog(home, instance), Definitions(assets=[manifest], resources=resources)
        )

        key = ("codebench", "full_dataset")
        self.assertEqual(service.partitions(ArtifactKey(asset_key=key)).partitions, ("codebench",))
        artifact = service.query(ArtifactQuery(asset_key=key, partition_key="codebench"))
        assert isinstance(artifact, ArtifactJSON)
        self.assertEqual(artifact.data_schema.source, "declared")
        self.assertEqual(artifact.value, {"train_rows": 3, "test_rows": 1})

    def test_config_writes_for_two_partitions_in_one_run_stay_separate(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        manager = ConfigRunIOManager(home=str(home))
        key = AssetKey(["train", "stats"])
        partitions = StaticPartitionsDefinition(["CAT-3", "CAT-4"])

        for partition, rows in (("CAT-3", 3), ("CAT-4", 4)):
            with build_output_context(
                asset_key=key, run_id="fixture", partition_key=partition, asset_partitions_def=partitions
            ) as context:
                manager.handle_output(context, ManifestFixture(train_rows=rows, test_rows=0))

        for partition, rows in (("CAT-3", 3), ("CAT-4", 4)):
            path = manager.run_output_path("fixture", key, partition)
            self.assertEqual(ManifestFixture.model_validate_json(path.read_bytes()).train_rows, rows)

    def test_category_reader_uses_producing_storage_and_excludes_inactive_partitions(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        instance = self.enterContext(DagsterInstance.ephemeral())
        registration = home / "dataset-specifications" / CODEBENCH_DATASET_ID
        registration.mkdir(parents=True)
        revisions: list[str] = []
        for name in ("producing", "replacement"):
            snapshot = StoredObjectLocation(
                dataset_id=CODEBENCH_DATASET_ID, storage=StorageConfig(local_root=str(home / name))
            ).model_dump_json()
            revision = sha256(snapshot.encode())
            (registration / f"{revision}.json").write_text(snapshot, encoding="utf-8")
            revisions.append(revision)

        key = AssetKey(["train", "category_rows"])
        dimension = "codebench_train_categories"
        dimensions = DynamicPartitionsDefinition(name=dimension)
        instance.add_dynamic_partitions(dimension, ["CAT-3", "CAT-4"])

        @asset(key=key, partitions_def=dimensions, io_manager_key="train_table_io")
        def category() -> pd.DataFrame:
            return pd.DataFrame({"command": ["pwd"]})

        (registration / "current").write_text(revisions[0], encoding="ascii")
        resources = {
            "train_table_io": dataset_storage_io(home, review=False, dataset_id=CODEBENCH_DATASET_ID)
        }
        for partition in ("CAT-3", "CAT-4"):
            self.assertTrue(
                materialize(
                    [category], resources=resources, instance=instance, partition_key=partition
                ).success
            )
        instance.delete_dynamic_partition(dimension, "CAT-4")
        instance.report_runless_asset_event(AssetMaterialization(key, partition="CAT-3|old_dataset"))
        (registration / "current").write_text(revisions[1], encoding="ascii")

        service = ArtifactDataService(DatasetArtifactCatalog(home, instance))
        asset_key = ("train", "category_rows")
        self.assertEqual(service.partitions(ArtifactKey(asset_key=asset_key)).partitions, ("CAT-3",))
        output = service.query(ArtifactQuery(asset_key=asset_key, partition_key="CAT-3"))
        assert output.kind == "table"
        self.assertEqual([row.values["command"] for row in output.page.rows], ["pwd"])
        for inactive in ("CAT-4", "CAT-3|old_dataset"):
            with self.assertRaises(DatasetViewError) as rejected:
                service.query(ArtifactQuery(asset_key=asset_key, partition_key=inactive))
            self.assertEqual(rejected.exception.code, "not_found")
