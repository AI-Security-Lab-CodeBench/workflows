import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from configuration_fixture import record_artifact, seed_prepared_snapshot
from dagster import AssetKey, build_input_context, build_output_context
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.artifact_views import ArtifactQuery, ArtifactTable
from codebench_pipeline.data_view_models import (
    DatasetQueryRequest,
    DatasetViewError,
    RulesResponse,
    TableResponse,
)
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import AssetReference, DataFrameIOManager, frame_bytes, read_frame
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import StorageConfig, sha256
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.table_query import TableQuery, TableQueryError, TextFilter


class DatasetViewTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.spec = fixture_spec(self.home)
        DatasetRegistry(self.home).register(self.spec)
        self.instance = self.enterContext(dataset_instance(self.home))
        self.instance.add_dynamic_partitions("dataset_builds", [self.spec.dataset_id])
        self.manager = DataFrameIOManager(storage=self.spec.storage)
        self.service = DatasetDataService(self.home, self.instance)
        self.context = "<script>inert evidence</script>" + "complete context " * 100
        self.frame = validated_frame(
            [
                {
                    "candidate_id": str(index),
                    "command": command,
                    "util": "probe",
                    "context": self.context,
                    "source_id": "source",
                    "source_url": "https://example.org/source",
                    "source_position": str(index),
                    "transformations": [],
                    "risk_candidates": ["CAT-99.1"],
                }
                for index, command in enumerate(("probe inspect", "probe inspect --all"))
            ],
            CommandCandidatesTable,
        )
        for stage, value in (("command_candidates", self.frame), ("category_rules", self.spec.rules)):
            with build_output_context(
                asset_key=AssetKey(["datasets", stage]),
                partition_key=self.spec.dataset_id,
                asset_partitions_def=DATASET_PARTITIONS,
                instance=self.instance,
            ) as output:
                self.manager.handle_output(output, value)

    def test_query_reads_full_persisted_values_without_materializing(self) -> None:
        response = self.service.query(
            "command_candidates",
            DatasetQueryRequest(
                dataset_id=self.spec.dataset_id,
                query=TableQuery(
                    limit=1,
                    filters=(TextFilter(field="command", operator="contains", value="--all"),),
                ),
            ),
        )
        assert isinstance(response, TableResponse)
        self.assertEqual(response.page.matched_rows, 1)
        self.assertEqual(response.page.rows[0].row_index, 1)
        self.assertEqual(response.page.rows[0].values["context"], self.context)
        self.assertEqual(self.instance.get_runs(), [])

    def test_browsing_archived_outputs_does_not_resolve_business_inputs_or_upstream_authority(self) -> None:
        seed_prepared_snapshot(self.home, self.spec, DocumentationConfig())
        reference = self.manager.persist_value(self.frame)
        record_artifact(
            self.instance,
            self.spec.dataset_id,
            AssetKey(["datasets", "command_candidates"]),
            reference,
            {"storage": self.spec.storage.model_dump(mode="json")},
        )
        with (
            patch.object(DatasetRegistry, "get", side_effect=AssertionError("Live configuration read")),
            patch.object(
                DatasetRegistry, "get_snapshot", side_effect=AssertionError("Full configuration read")
            ),
            patch(
                "dagster.JobDefinition.execute_in_process",
                side_effect=AssertionError("Archived browsing launched a run"),
            ),
            patch(
                "codebench_pipeline.artifact_currentness.provenance_resolver",
                side_effect=AssertionError("Archived browsing resolved an upstream graph"),
            ),
        ):
            response = self.service.artifacts.query(
                ArtifactQuery(
                    asset_key=("datasets", "command_candidates"),
                    partition_key=self.spec.dataset_id,
                )
            )
        assert isinstance(response, ArtifactTable)
        self.assertEqual(response.page.total_rows, 2)
        self.assertEqual(response.page.rows[0].values["context"], self.context)
        self.assertEqual(response.publication_state, "stale")

    def test_viewer_reads_the_selected_local_bytes_after_an_s3_registration(self) -> None:
        key = AssetKey(["datasets", "command_candidates"])
        dataset_id = self.spec.dataset_id
        prior_bytes = frame_bytes(self.frame)
        prior = self.manager.persist_value(self.frame)
        self.assertEqual(prior.digest, sha256(prior_bytes))
        record_artifact(
            self.instance,
            dataset_id,
            key,
            prior,
            {"storage": self.spec.storage.model_dump(mode="json")},
        )
        registry = DatasetRegistry(self.home)
        storage = StorageConfig(
            backend="s3",
            bucket="fixture-artifacts",
            prefix="new-fixture",
            endpoint_url="https://objects.fixture.invalid",
        )
        registry.register(self.spec.model_copy(update={"storage": storage}))
        current_bytes = frame_bytes(self.frame.iloc[:1])
        current = AssetReference(format="parquet", digest=sha256(current_bytes))
        record_artifact(
            self.instance,
            dataset_id,
            key,
            current,
            {"registered_revision": registry.current_revision(dataset_id)},
        )
        target = ArtifactQuery(asset_key=tuple(key.path), partition_key=dataset_id)
        with (
            patch("codebench_pipeline.artifact_views.read_frame", wraps=read_frame) as parse,
            patch("codebench_pipeline.object_store.configured_client") as connect,
        ):
            selected = self.service.artifacts.query(target.model_copy(update={"snapshot_id": prior.digest}))
            assert isinstance(selected, ArtifactTable)
            self.assertEqual(selected.snapshot_id, prior.digest)
            self.assertEqual(selected.publication_state, "stale")
            self.assertEqual(selected.page.total_rows, 2)
            parse.assert_called_once_with(prior_bytes)
            connect.assert_not_called()

            connect.return_value.get_object.return_value = {
                "Body": BytesIO(current_bytes),
                "Metadata": {"sha256": current.digest},
                "ETag": '"fixture"',
            }
            latest = self.service.artifacts.query(target)
            assert isinstance(latest, ArtifactTable)
            self.assertEqual(latest.snapshot_id, current.digest)
            self.assertEqual(latest.publication_state, "stale")
            self.assertEqual(latest.page.total_rows, 1)
            self.assertTrue(connect.call_args_list)
            self.assertTrue(all(call.args[1] == storage for call in connect.call_args_list))
            connect.return_value.get_object.assert_called_once_with(
                Bucket=storage.bucket, Key=f"{storage.prefix}/objects/{current.digest}.parquet"
            )

    def test_viewer_withholds_an_artifact_without_recorded_binding(self) -> None:
        key = AssetKey(["datasets", "command_candidates"])
        prior = self.manager.persist_value(self.frame)
        record_artifact(self.instance, self.spec.dataset_id, key, prior, {})
        target = ArtifactQuery(
            asset_key=tuple(key.path), partition_key=self.spec.dataset_id, snapshot_id=prior.digest
        )

        with (
            patch("codebench_pipeline.artifact_views.ObjectStore.from_environment") as store,
            self.assertRaisesRegex(DatasetViewError, "no usable storage binding") as error,
        ):
            self.service.artifacts.query(target)
        self.assertEqual(error.exception.code, "not_materialized")
        store.assert_not_called()

    def test_selected_bytes_with_two_incompatible_bindings_are_ambiguous(self) -> None:
        key = AssetKey(["datasets", "command_candidates"])
        reference = self.manager.persist_value(self.frame)
        for storage in (
            self.spec.storage,
            StorageConfig(backend="s3", bucket="fixture-artifacts"),
        ):
            record_artifact(
                self.instance,
                self.spec.dataset_id,
                key,
                reference,
                {"storage": storage.model_dump(mode="json")},
            )
        target = ArtifactQuery(
            asset_key=tuple(key.path), partition_key=self.spec.dataset_id, snapshot_id=reference.digest
        )
        with (
            patch("codebench_pipeline.artifact_views.ObjectStore.from_environment") as store,
            self.assertRaisesRegex(DatasetViewError, "conflicting storage bindings"),
        ):
            self.service.artifacts.query(target)
        store.assert_not_called()

    def test_legacy_release_is_inspectable_but_not_authorized_as_pipeline_input(self) -> None:
        seed_prepared_snapshot(self.home, self.spec, DocumentationConfig())
        legacy = pd.DataFrame(
            [
                {
                    "command": "git status",
                    "description": "Inspects status.",
                    "util": "git",
                    "severity": "low",
                    "category": self.spec.rules.canonical,
                    "risk": self.spec.rules.risks[0].canonical,
                    "source": "fixture",
                    "seed": "-",
                    "class": "allow",
                }
            ],
        )
        with build_output_context(
            asset_key=AssetKey(["datasets", "dataset_release"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as output:
            self.manager.publish_reference(output, self.manager.persist_value(legacy))
        with HumanReviewStore(self.home).transaction():
            response = self.service.query(
                "dataset_release", DatasetQueryRequest(dataset_id=self.spec.dataset_id)
            )
        assert isinstance(response, TableResponse)
        self.assertEqual(response.publication_state, "legacy_unreviewed")
        self.assertEqual(response.page.total_rows, 1)
        self.assertNotIn("human_reviewed", response.page.columns)
        guarded = RegisteredDatasetTableIO(storage=self.spec.storage, review_home=str(self.home))
        with build_input_context(
            asset_key=AssetKey(["datasets", "dataset_release"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as incoming:
            with self.assertRaises(HumanReviewError) as unauthorized:
                guarded.load_input(incoming)
        self.assertEqual(unauthorized.exception.code, "stale")

    def test_changed_snapshot_rejects_old_page_binding_and_preserves_old_bytes(self) -> None:
        original = self.service.read_current(self.spec.dataset_id, "command_candidates")
        with build_output_context(
            asset_key=AssetKey(["datasets", "command_candidates"]),
            partition_key=self.spec.dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as output:
            self.manager.handle_output(output, self.frame.iloc[:1])
        with self.assertRaises(HumanReviewError) as stale:
            self.service.query(
                "command_candidates",
                DatasetQueryRequest(dataset_id=self.spec.dataset_id, snapshot_id=original.reference.digest),
            )
        self.assertEqual(stale.exception.code, "stale")
        old = self.manager.read_version(original.reference)
        assert isinstance(old, pd.DataFrame)
        self.assertEqual(len(old), 2)
        fresh = self.service.query("command_candidates", DatasetQueryRequest(dataset_id=self.spec.dataset_id))
        assert isinstance(fresh, TableResponse)
        self.assertEqual(fresh.page.total_rows, 1)
        self.assertNotEqual(fresh.snapshot_id, original.reference.digest)

    def test_rules_and_missing_states_are_distinct_from_empty_query_results(self) -> None:
        request = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        rules = self.service.query("category_rules", request)
        assert isinstance(rules, RulesResponse)
        self.assertEqual(rules.rules, self.spec.rules)
        for stage, code in (("unknown", "not_found"), ("reviewed_annotations", "not_materialized")):
            with self.subTest(stage=stage), self.assertRaises(DatasetViewError) as error:
                self.service.query(stage, request)
            self.assertEqual(error.exception.code, code)
        with self.assertRaises(TableQueryError):
            self.service.query(
                "category_rules",
                DatasetQueryRequest(dataset_id=self.spec.dataset_id, query=TableQuery(search="policy")),
            )
        self.assertEqual(self.service.datasets().datasets[0].dataset_id, self.spec.dataset_id)


if __name__ == "__main__":
    unittest.main()
