import json
import os
import tempfile
from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, build_input_context
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from test_dataframe_io import s3_fixture

from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION, DataFrameIOManager
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.human_review import HumanReviewError, ReviewSnapshot
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.review_artifacts import review_artifact_key
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.review_stages import ReviewStage
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.storage import IntegrityError


def storage_fixture(prefix: str) -> AbstractContextManager[StorageConfig]:
    endpoint = os.environ.get("CODEBENCH_REVIEW_QA_ENDPOINT")
    if endpoint is not None:
        return nullcontext(
            StorageConfig(
                backend="s3",
                endpoint_url=endpoint,
                bucket=os.environ["CODEBENCH_REVIEW_QA_BUCKET"],
                prefix=prefix,
                access_key_env="CODEBENCH_REVIEW_QA_ACCESS",
                secret_key_env="CODEBENCH_REVIEW_QA_SECRET",
            )
        )
    return _http_fixture_storage(prefix)


@contextmanager
def _http_fixture_storage(prefix: str) -> Generator[StorageConfig]:
    with s3_fixture() as (storage, _):
        yield storage.model_copy(update={"prefix": prefix})


class ReviewObjectStorageTests(TestCase):
    def test_table_snapshot_replays_from_configured_store_and_refuses_replacement(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        config = self.enterContext(storage_fixture("table-review"))
        store = ObjectStore.from_environment(config)
        stage = ReviewStage(
            stage_id="inspection",
            title="Inspection",
            asset_keys=(("inspection",),),
            review_columns=("value",),
        )
        table = ReviewTable(pd.DataFrame([{"id": "row-1", "value": "kept"}]), "id", "r1", {}, ("row-1",))
        source = TableReviewSource(home, stage, lambda _: table, store)
        workflow = ReviewWorkflow.for_home(home, stage, source)
        prepared = workflow.prepare("fixture")
        replayed = workflow.prepare("fixture")

        self.assertEqual(replayed.batch_id, prepared.batch_id)
        self.assertEqual(prepared.snapshot.artifact_storage, config)
        key = review_artifact_key("table-snapshots", prepared.snapshot.snapshot_id)
        raw = store.read(key)
        restarted = TableReviewSource(home, stage, lambda _: table, ObjectStore.from_environment(config))
        self.assertEqual(
            restarted.evidence(prepared.snapshot, "row-1").fields["record"], {"id": "row-1", "value": "kept"}
        )
        with self.assertRaises(IntegrityError):
            store.write(key, b"unrelated contents")
        self.assertEqual(store.read(key), raw)
        self.assertFalse((home / "human-review").exists())

        legacy = ObjectStore(StorageConfig(local_root=str(home / "human-review"), prefix="table-snapshots"))
        historical = json.loads(raw)
        historical.pop("version")
        historical_raw = json.dumps(
            historical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        historical_id = sha256(historical_raw)
        legacy.write(f"{historical_id}.json", historical_raw)
        historical_data = prepared.snapshot.model_dump(exclude={"artifact_storage"})
        historical_data["snapshot_id"] = historical_id
        old_snapshot = ReviewSnapshot.model_validate(historical_data)
        self.assertNotIn("artifact_storage", old_snapshot.model_dump(mode="json"))
        self.assertEqual(
            source.evidence(old_snapshot, "row-1").fields["record"], {"id": "row-1", "value": "kept"}
        )

        unbound_copy = old_snapshot.model_copy(update={"artifact_storage": config})
        with self.assertRaises(FileNotFoundError):
            source.evidence(unbound_copy, "row-1")
        self.assertEqual(legacy.read(f"{historical_id}.json"), historical_raw)

    def test_native_annotation_snapshot_replays_from_configured_store(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        config = self.enterContext(storage_fixture("annotation-review"))
        fixture = LabelingFixture(self, home, native=True)
        dataset_id = DatasetRegistry(home).register(fixture.spec.model_copy(update={"storage": config}))
        queue_key = AssetKey(["datasets", "human_review_queue"])
        manager = DataFrameIOManager.from_environment(config)

        with patch.object(
            ReviewSnapshots, "capture_registered", side_effect=IntegrityError("Fixture capture failure")
        ):
            with self.assertRaisesRegex(IntegrityError, "Fixture capture failure"):
                run_registered(dataset_id, home)
        with self.assertRaises(HumanReviewError) as missing, HumanReviewStore(home).reader() as transaction:
            transaction.get_current_batch(dataset_id)
        self.assertEqual(missing.exception.code, "not_found")
        with dataset_instance(home) as instance:
            queue_input = build_input_context(
                instance=instance,
                asset_key=queue_key,
                partition_key=dataset_id,
                asset_partitions_def=DATASET_PARTITIONS,
            )
            with self.assertRaises(FileNotFoundError):
                manager.current_reference(queue_input)
            records = instance.fetch_materializations(
                AssetRecordsFilter(asset_key=queue_key), limit=1
            ).records
            self.assertFalse(records)

        self.assertIn("human_review_queue", run_registered(dataset_id, home).materialized_stages)
        with HumanReviewStore(home).reader() as transaction:
            snapshot_batch = transaction.get_current_batch(dataset_id)
            snapshot = snapshot_batch.snapshot

        self.assertEqual(snapshot.artifact_storage, config)
        store = ObjectStore.from_environment(config)
        key = review_artifact_key("snapshots", snapshot.snapshot_id)
        raw = store.read(key)
        with dataset_instance(home) as instance:
            data = DatasetDataService(home, instance)
            replayed = ReviewSnapshots.from_environment(home, data).current(dataset_id)
            queue_snapshot = data.read_current(dataset_id, "human_review_queue")
            queue = queue_snapshot.value
            assert isinstance(queue, pd.DataFrame)
            self.assertEqual(
                queue.attrs[REVIEW_PREPARATION],
                {"snapshot_id": snapshot.snapshot_id, "batch_id": snapshot_batch.batch_id},
            )
            self.assertNotIn(
                "human_review_queue", NativeDatasetCycle(home).drain(instance, partition=dataset_id)
            )
            self.assertEqual(
                data.read_current(dataset_id, "human_review_queue").reference, queue_snapshot.reference
            )
        self.assertEqual(replayed.snapshot, snapshot)
        self.assertEqual(replayed.manifest.payload(), raw)
        self.assertEqual(replayed.manifest.version, 2)
        for name, digest in replayed.manifest.implementation.items():
            self.assertTrue(store.read(f"review/snapshots/implementation/{digest}/{name}"))
        with self.assertRaises(IntegrityError):
            store.write(key, b"incorrect snapshot")
        self.assertEqual(store.read(key), raw)
        self.assertFalse((home / "human-review").exists())
