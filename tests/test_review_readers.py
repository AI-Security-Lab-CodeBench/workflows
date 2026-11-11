import tempfile
import unittest
from pathlib import Path

from test_human_review_lifecycle import LifecycleSource

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.human_review import DecisionChange, ReviewSnapshot
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.review_stages import ReviewStage
from codebench_pipeline.review_workflow import ReviewWorkflow


class ReviewReaderTests(unittest.TestCase):
    def test_existing_review_view_does_not_compete_with_an_active_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            workflow = ReviewWorkflow(
                home,
                ReviewStage(stage_id="selection", title="Selection", asset_keys=(("input",),)),
                LifecycleSource("selection"),
            )
            query = DatasetQueryRequest(dataset_id="CAT-3.1")
            original = workflow.view(query)
            with HumanReviewStore(home).transaction() as writer:
                writer.save_decision(
                    original.batch.snapshot,
                    DecisionChange(
                        batch_id=original.batch.batch_id,
                        generation=original.batch.generation,
                        candidate_id="first",
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="writer",
                    ),
                )
                viewed = workflow.view(query)
                self.assertEqual(viewed.counts.pending, 2)
                self.assertEqual(viewed.batch, original.batch)
            self.assertEqual(workflow.view(query).counts.approve, 1)

    def test_read_snapshot_does_not_block_a_writer_and_remains_consistent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = HumanReviewStore(Path(temporary))
            snapshot = ReviewSnapshot(
                dataset_id="records",
                snapshot_id="a" * 64,
                candidate_ids=("one",),
                eligible_candidate_ids=("one",),
            )
            with store.transaction() as tx:
                batch = tx.ensure_current_batch(snapshot)
            with store.reader() as reader:
                before = reader.get_current_batch("records")
                with store.transaction() as writer:
                    writer.save_decision(
                        snapshot,
                        DecisionChange(
                            batch_id=batch.batch_id,
                            generation=batch.generation,
                            candidate_id="one",
                            expected_row_revision=0,
                            decision="approve",
                            reviewer="reviewer",
                        ),
                    )
                self.assertEqual(reader.get_current_batch("records"), before)
                self.assertEqual(reader.counts(batch.batch_id).pending, 1)
            with store.reader() as reader:
                self.assertEqual(reader.counts(batch.batch_id).approve, 1)
                self.assertEqual(reader.get_current_batch("records").batch_revision, 1)
