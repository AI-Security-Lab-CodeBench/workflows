import tempfile
import unittest
from pathlib import Path

import pandas as pd
from starlette.testclient import TestClient
from test_artifact_views import FixtureCatalog

from codebench_pipeline.artifact_views import ArtifactDataService
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.human_review import (
    CompletionToken,
    HumanReviewError,
    review_artifact_id,
    review_partition_id,
)
from codebench_pipeline.review_stages import ReviewRegistration, ReviewStage
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.review_workflow import ReviewWorkflow

ORIGIN = "http://127.0.0.1:3000"
ASSET = ("experiments", "scores")


class ArtifactReviewAPITests(unittest.TestCase):
    def test_generic_host_discovers_scopes_and_retracts_completed_corrected_output(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        home = Path(temporary.name)
        stage = ReviewStage(
            stage_id="score-review",
            title="Score review",
            asset_keys=(ASSET,),
            review_columns=("score",),
        )
        partitions = {review_partition_id(value): value for value in (None, "alpha")}

        def source(dataset_id: str) -> ReviewTable:
            return ReviewTable(
                frame=pd.DataFrame([{"id": "row", "score": 1.0, "origin": "readonly original"}]),
                id_column="id",
                revision="input-1",
                evidence={},
                eligible_ids=("row",),
                artifact_id=review_artifact_id(ASSET),
                scope_id="selected",
                partition_key=partitions[dataset_id],
                column_schema={"score": {"type": "number", "minimum": 0, "maximum": 10}},
            )

        workflow = ReviewWorkflow.for_home(
            home,
            stage,
            TableReviewSource(
                home, stage, source, ObjectStore(StorageConfig(local_root=str(home / "objects")))
            ),
        )
        for dataset_id in partitions:
            workflow.prepare(dataset_id)
        client = self.enterContext(
            TestClient(
                make_dataset_api(
                    None,
                    None,
                    3000,
                    stages=(ReviewRegistration(stage, workflow),),
                    artifacts=ArtifactDataService(FixtureCatalog({})),
                ),
                base_url=ORIGIN,
            )
        )
        path = "/reviews/score-review"
        discovery = client.post(
            path + "/partitions", headers={"Origin": ORIGIN}, json={"asset_key": list(ASSET)}
        )
        self.assertEqual(discovery.status_code, 200)
        self.assertEqual(
            {entry["partition_key"] for entry in discovery.json()["partitions"]}, {None, "alpha"}
        )
        target = {
            "dataset_id": review_partition_id(None),
            "artifact_id": review_artifact_id(ASSET),
            "scope_id": "selected",
        }
        response = client.post(path, headers={"Origin": ORIGIN}, json=target)
        self.assertEqual(response.status_code, 200)
        page = response.json()
        self.assertEqual(page["column_schema"]["origin"]["readOnly"], True)
        batch = page["batch"]
        change = {
            "batch_id": batch["batch_id"],
            "generation": batch["generation"],
            "candidate_id": "row",
            "expected_row_revision": 0,
            "decision": "approve",
            "notes": "",
            "reviewer": "synthetic QA",
        }
        bound = {**target, "snapshot_id": batch["snapshot"]["snapshot_id"]}
        invalid = client.put(
            path + "/decision",
            headers={"Origin": ORIGIN},
            json={**bound, "change": {**change, "edits": {"origin": "changed"}}},
        )
        self.assertEqual(invalid.status_code, 409)
        saved = client.put(
            path + "/decision",
            headers={"Origin": ORIGIN},
            json={**bound, "change": {**change, "edits": {"score": 7.0}}},
        )
        self.assertEqual(saved.status_code, 200)
        batch = saved.json()["batch"]
        expected = {name: batch[name] for name in ("batch_id", "generation", "batch_revision")}
        completed = client.post(
            path + "/complete",
            headers={"Origin": ORIGIN},
            json={**bound, "expected": expected, "reviewer": "synthetic QA"},
        )
        self.assertEqual(completed.status_code, 200)
        token = CompletionToken.model_validate(completed.json()["completion"])
        self.assertEqual(
            workflow.approved_records(target["dataset_id"], token).to_dict(orient="records"),
            [{"id": "row", "score": 7.0, "origin": "readonly original"}],
        )
        other = client.post(
            path,
            headers={"Origin": ORIGIN},
            json={
                **target,
                "dataset_id": review_partition_id("alpha"),
            },
        )
        self.assertEqual(other.json()["counts"]["pending"], 1)
        batch = completed.json()["batch"]
        retracted = client.post(
            path + "/retract",
            headers={"Origin": ORIGIN},
            json={
                **bound,
                "expected": {name: batch[name] for name in ("batch_id", "generation", "batch_revision")},
                "candidate_id": "row",
                "expected_row_revision": saved.json()["decision"]["row_revision"],
                "reviewer": "synthetic QA",
            },
        )
        self.assertEqual(retracted.status_code, 200)
        self.assertEqual(retracted.json()["counts"]["pending"], 1)
        with self.assertRaises(HumanReviewError):
            workflow.approved_records(target["dataset_id"], token)
