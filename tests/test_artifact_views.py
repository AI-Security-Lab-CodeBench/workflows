import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from starlette.testclient import TestClient

from codebench_pipeline.artifact_views import (
    ArtifactBinding,
    ArtifactDataService,
    ArtifactKey,
    ArtifactPartitions,
    ArtifactQuery,
    ArtifactTable,
    ArtifactTarget,
)
from codebench_pipeline.data_api import make_dataset_api
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.table_query import TableQuery, TextFilter


@dataclass(frozen=True, slots=True)
class FixtureCatalog:
    bindings: dict[tuple[tuple[str, ...], str | None], ArtifactBinding]

    def partitions(self, key: ArtifactKey) -> ArtifactPartitions:
        return ArtifactPartitions(
            asset_key=key.asset_key,
            partitions=tuple(partition for asset, partition in self.bindings if asset == key.asset_key),
        )

    def resolve(self, target: ArtifactTarget) -> ArtifactBinding:
        try:
            return self.bindings[(target.asset_key, target.partition_key)]
        except KeyError as error:
            raise DatasetViewError("not_found", "Unknown artifact partition") from error


class ArtifactViewTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        storage = StorageConfig(local_root=str(Path(temporary.name)))
        manager = DataFrameIOManager(storage=storage)
        self.catalog = FixtureCatalog({
            (("experiments", "measurements"), "run-a"): ArtifactBinding(
                storage, manager.persist_value(pd.DataFrame({
                    "sample": ["alpha", "beta"], "measurement": [1.5, 4.0],
                    "evidence": ["unchanged original", "complete original"],
                })),
            ),
            (("experiments", "measurements"), "run-b"): ArtifactBinding(
                storage, manager.persist_value(pd.DataFrame({"sample": ["gamma"], "measurement": [9.0]})),
            ),
            (("reference",), None): ArtifactBinding(
                storage, manager.persist_value(pd.DataFrame({"name": ["unpartitioned"]})),
            ),
        })
        self.service = ArtifactDataService(self.catalog)

    def test_arbitrary_artifacts_filter_within_the_selected_partition(self) -> None:
        key = ArtifactKey(asset_key=("experiments", "measurements"))
        self.assertEqual(self.service.partitions(key).partitions, ("run-a", "run-b"))
        response = self.service.query(ArtifactQuery(
            asset_key=key.asset_key,
            partition_key="run-a",
            query=TableQuery(filters=(TextFilter(field="sample", operator="equals", value="beta"),)),
        ))
        self.assertIsInstance(response, ArtifactTable)
        self.assertEqual((response.page.total_rows, response.page.matched_rows), (2, 1))
        self.assertEqual(response.page.rows[0].row_index, 1)
        self.assertEqual(response.page.rows[0].values, {
            "sample": "beta", "measurement": 4.0, "evidence": "complete original",
        })
        other = self.service.query(ArtifactQuery(asset_key=key.asset_key, partition_key="run-b"))
        self.assertEqual(other.page.rows[0].values["sample"], "gamma")

    def test_unpartitioned_artifact_and_stale_snapshot_are_distinct(self) -> None:
        self.assertEqual(
            self.service.partitions(ArtifactKey(asset_key=("reference",))).partitions, (None,)
        )
        response = self.service.query(ArtifactQuery(asset_key=("reference",)))
        self.assertEqual(response.page.rows[0].values["name"], "unpartitioned")
        with self.assertRaises(HumanReviewError) as stale:
            self.service.query(ArtifactQuery(asset_key=("reference",), snapshot_id="0" * 64))
        self.assertEqual(stale.exception.code, "stale")
        with self.assertRaises(DatasetViewError):
            self.service.query(ArtifactQuery(asset_key=("reference",), partition_key="run-a"))

    def test_generic_http_host_requires_no_dataset_or_annotation_registration(self) -> None:
        origin = "http://127.0.0.1:3000"
        with TestClient(
            make_dataset_api(None, None, 3000, artifacts=self.service), base_url=origin
        ) as client:
            self.assertEqual(client.get("/review-stages").json(), {"stages": []})
            response = client.post(
                "/artifacts/data",
                headers={"Origin": origin},
                json={"asset_key": ["reference"], "partition_key": None},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["page"]["rows"][0]["values"], {"name": "unpartitioned"})
            schema = response.json()["data_schema"]
            self.assertEqual(schema["source"], "storage")
            self.assertEqual(schema["schema"]["type"], "object")
            self.assertEqual(
                schema["schema"]["properties"]["name"],
                {"anyOf": [{"type": "string"}, {"type": "null"}]},
            )
