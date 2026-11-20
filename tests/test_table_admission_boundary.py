import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd
from dagster import PartitionKeyRange, StaticPartitionsDefinition, build_output_context
from pandas.testing import assert_frame_equal

from codebench_pipeline.dataframe_io import REVIEW_PREPARATION, DataFrameIOManager
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.review_stages import ReviewStage
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.review_workflow import ReviewWorkflow


class TableAdmissionBoundaryTests(unittest.TestCase):
    def test_byte_storage_makes_no_release_decision_from_an_asset_name(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        manager = DataFrameIOManager(storage=StorageConfig(local_root=str(home)))
        context = build_output_context(asset_key=["datasets", "dataset_release"])
        table = pd.DataFrame({"value": [1]})

        manager.handle_output(context, table)

        stored = manager.read_version(manager.current_reference(context))
        assert isinstance(stored, pd.DataFrame)
        assert_frame_equal(stored, table)

    def test_replaced_queue_preparation_cannot_advance_the_table_reference(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        dataset = "fixture"
        store = HumanReviewStore(home)
        manager = RegisteredDatasetTableIO(
            storage=StorageConfig(local_root=str(home / "tables")), review_home=str(home)
        )
        manager.bind_review_store(store)
        stage = ReviewStage(
            stage_id="annotations",
            title="Fixture review",
            asset_keys=(("datasets", "human_review_queue"),),
            review_columns=("value",),
        )
        queue = pd.DataFrame({"id": ["row-1"], "value": ["current"]})
        table = TableReviewSource(
            home,
            stage,
            lambda _: ReviewTable(queue, "id", "r1", {}, ("row-1",)),
            manager.object_store(),
        )
        batch = ReviewWorkflow(store, stage, table).prepare(dataset)
        context = build_output_context(
            asset_key=["datasets", "human_review_queue"],
            partition_key=dataset,
            asset_partitions_def=StaticPartitionsDefinition([dataset]),
            asset_partition_key_range=PartitionKeyRange(dataset, dataset),
        )
        original = manager.persist_value(queue)
        manager.publish_reference(context, original)
        stale = queue.assign(value="superseded")
        stale.attrs[REVIEW_PREPARATION] = {"batch_id": batch.batch_id, "snapshot_id": "0" * 64}

        with (
            patch("codebench_pipeline.review_preparation.prepare_review_queue", return_value=stale),
            self.assertRaises(HumanReviewError) as rejected,
        ):
            manager.handle_output(context, stale)

        self.assertEqual(rejected.exception.code, "stale")
        self.assertEqual(manager.current_reference(context), original)
