from csv import DictReader
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from guarded_labeling_fixture import LabelingFixture
from pydantic import TypeAdapter
from starlette.testclient import TestClient

from codebench_pipeline.artifact_views import ArtifactTable
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import BuildOutcome, run_registered
from codebench_pipeline.guarded_publication_controls import record_control
from codebench_pipeline.guarded_publication_runtime import preview_proposal
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.runtime_instance import dataset_instance

ORIGIN = "http://127.0.0.1:3000"


class GuardedReleaseViewsTests(TestCase):
    def test_prepared_progress_survives_missing_attestation_and_release_views_recheck_revocation(
        self,
    ) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        fixture = LabelingFixture(self, home, native=True)
        dataset_id = DatasetRegistry(home).register(fixture.spec)
        store = HumanReviewStore(home)

        outcome = run_registered(dataset_id, home)
        self.assertEqual(outcome.status, "prepared")
        self.assertIn("exact proposal", outcome.publication_waiting_reason or "")
        self.assertIsNone(outcome.release_run_id)
        self.assertIn("human_review_queue", outcome.materialized_stages)
        self.assertTrue({"human_review_decisions", "dataset_release"}.isdisjoint(outcome.materialized_stages))
        self.assertEqual(
            BuildOutcome.model_validate_json((home / f"build-{outcome.run_id}.json").read_bytes()), outcome
        )
        with store.reader() as tx:
            token = tx.get_current_batch(dataset_id).completion
        assert token is not None

        with dataset_instance(home) as instance:
            data = DatasetDataService(home, instance)
            client = TestClient(make_dataset_api(data, None, 3000), base_url=ORIGIN)
            self.enterContext(client)
            headers = {"Origin": ORIGIN}
            target = {"asset_key": ["datasets", "dataset_release"], "partition_key": dataset_id}
            with self.assertRaises(DatasetViewError) as missing:
                data.read_current(dataset_id, "dataset_release")
            self.assertEqual(missing.exception.code, "not_materialized")
            unbuilt = client.post("/artifacts/data", headers=headers, json=target)
            self.assertEqual(unbuilt.status_code, 404)
            error = TypeAdapter(dict[str, dict[str, str]]).validate_json(unbuilt.content)
            self.assertEqual(error["error"]["code"], "not_materialized")

            proposal, preview = preview_proposal(home, instance, dataset_id)
            with (preview / "commands.csv").open(encoding="utf-8", newline="") as commands:
                self.assertEqual(
                    [row["command"] for row in DictReader(commands)],
                    ["docker ps --all", "kubectl get pods", "docker compose ps"],
                )
            with store.transaction(dataset_id=dataset_id) as tx:
                record_control(tx, dataset_id, "attest", actor=200, proposal=proposal)
            release = ReleaseRunner(home, instance)(token)
            authorized = client.post("/artifacts/data", headers=headers, json=target)
            self.assertEqual(authorized.status_code, 200)
            current = ArtifactTable.model_validate_json(authorized.content)
            self.assertEqual(current.publication_state, "published")
            self.assertEqual(current.snapshot_id, release.payload_hash)

            with store.transaction(dataset_id=dataset_id) as tx:
                record_control(tx, dataset_id, "revoke", actor=200, proposal=proposal)
            stale = client.post("/artifacts/data", headers=headers, json=target)
            self.assertEqual(stale.status_code, 200)
            historical = ArtifactTable.model_validate_json(stale.content)
            self.assertEqual(historical.snapshot_id, release.payload_hash)
            self.assertEqual(historical.publication_state, "stale")
            self.assertEqual(data.read_current(dataset_id, "dataset_release").publication_state, "stale")
