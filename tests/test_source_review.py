import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, OutputContext
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pandera.typing import DataFrame
from pydantic import BaseModel

from codebench_pipeline.artifact_currentness import ArtifactCurrentness
from codebench_pipeline.collection_config import SourceDeclaration
from codebench_pipeline.config_assets import GLOBAL_CONFIG, ConfigRunIOManager, configured_selection
from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import SourceCatalogTable, SourceDocumentsTable
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_dependencies import DECLARED_INPUT_STAGES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration, run_registered
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.documentation_authorization import (
    frame_inventory_authorization,
    inventory_authorization,
    inventory_binding,
)
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.repository import RepositoryRun, repository_documents
from codebench_pipeline.repository_branch_config import (
    DeclaredRepositoryOrigin,
    RepositoryAcquisitionConfig,
    RepositoryBranchDeclaration,
)
from codebench_pipeline.repository_legacy import PreparedCaptureCache
from codebench_pipeline.repository_receipts import RepositoryReceipt
from codebench_pipeline.review_api_models import CompletionRequest, DecisionRequest, RetractionRequest
from codebench_pipeline.review_stages import ReviewStage
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, declared_source_config


def dataset_declaration(spec: DatasetBuildSpec) -> RepositoryBranchDeclaration:
    collection = spec.declared_collection
    assert collection is not None
    declaration = collection.existing_datasets.declaration
    assert isinstance(declaration, RepositoryBranchDeclaration)
    return declaration


def update_dataset_declaration(
    spec: DatasetBuildSpec,
    *,
    revision: str,
    origins: tuple[DeclaredRepositoryOrigin, ...] | None = None,
) -> DatasetBuildSpec:
    collection = spec.declared_collection
    assert collection is not None
    declaration = dataset_declaration(spec)
    updated = declaration.model_copy(
        update={"refresh_revision": revision, "origins": declaration.origins if origins is None else origins}
    )
    removed = {origin.origin_id for origin in declaration.origins} - {
        origin.origin_id for origin in updated.origins
    }
    merge = collection.merge_config
    return spec.model_copy(
        update={
            "declared_collection": collection.model_copy(
                update={
                    "existing_datasets": SourceDeclaration.from_declaration(updated),
                    "merge": merge.model_copy(
                        update={
                            "repository_origin_weights": {
                                key: value
                                for key, value in merge.repository_origin_weights.items()
                                if key not in removed
                            }
                        }
                    ),
                }
            )
        }
    )


class SourceReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.fixture = LabelingFixture(self, self.home)
        self.spec: DatasetBuildSpec = self.fixture.spec
        self.registry = DatasetRegistry(self.home)
        self.registry.register(self.spec)
        refresh_registered_configuration(self.spec.dataset_id, self.home)
        self.instance = self.enterContext(dataset_instance(self.home))
        self.cycle = NativeDatasetCycle(self.home)
        self.prepared = self.cycle.drain(self.instance, partition=self.spec.dataset_id)
        self.data = DatasetDataService(self.home, self.instance)
        self.query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)

    def documents(self) -> TableResponse:
        result = self.data.query("capture_corpus_files", self.query)
        assert isinstance(result, TableResponse)
        return result

    def test_sources_load_without_review_and_unchanged_build_reuses_outputs(self) -> None:
        self.assertIn("capture_corpus_files", self.prepared)
        self.assertIn("human_review_queue", self.prepared)
        self.assertNotIn("human_source_decisions", DECLARED_INPUT_STAGES)
        stored = self.data.read_current(self.spec.dataset_id, "capture_corpus_files")
        assert isinstance(stored.value, pd.DataFrame)
        receipt = RepositoryReceipt.model_validate_json(stored.value.attrs["repository_receipt_json"])
        config = declared_source_config(self.spec, "dataset")
        self.assertEqual(receipt.config_digest, config.declaration_digest)
        origins = receipt.observations["origins"]
        assert isinstance(origins, dict)
        self.assertEqual(
            set(origins), {origin.origin_id for origin in config.repository_config.origins("dataset")}
        )
        self.assertEqual(
            self.instance.fetch_materializations(
                AssetRecordsFilter(asset_key=AssetKey(["datasets", "human_source_decisions"])),
                limit=1,
            ).records,
            [],
        )
        self.assertEqual(self.cycle.tick(self.instance), ())
        self.assertEqual(self.cycle.drain(self.instance, partition=self.spec.dataset_id), {})

    def test_inventory_revision_stales_and_rebinds_identical_source_rows(self) -> None:
        _, before = inventory_authorization(
            DataFrameIOManager(storage=self.spec.storage),
            self.instance,
            DeclaredGlobalConfig.from_spec(self.spec),
            "dataset",
        )
        before_rows = self.documents().page.rows
        revised = update_dataset_declaration(self.spec, revision="refresh")
        self.registry.register(revised)
        self.assertEqual(self.documents().publication_state, "stale")
        rebuilt = run_registered(self.spec.dataset_id, self.home, prepare_only=True)
        self.assertIn("capture_corpus_files", rebuilt.materialized_stages)
        _, after = inventory_authorization(
            DataFrameIOManager(storage=revised.storage),
            self.instance,
            DeclaredGlobalConfig.from_spec(revised),
            "dataset",
        )
        self.assertNotEqual(before, after)
        stored = self.data.read_current(self.spec.dataset_id, "capture_corpus_files")
        assert isinstance(stored.value, pd.DataFrame)
        self.assertEqual(frame_inventory_authorization(stored.value, "dataset"), after)
        self.assertIsNone(stored.publication_state)
        self.assertEqual(
            [
                {key: value for key, value in row.values.items() if key != "retrieved_at"}
                for row in before_rows
            ],
            [
                {key: value for key, value in row.values.items() if key != "retrieved_at"}
                for row in self.documents().page.rows
            ],
        )

    def test_declared_source_list_controls_acquisition_without_source_inference(self) -> None:
        selected, *_ = dataset_declaration(self.spec).origins
        self.registry.register(
            update_dataset_declaration(self.spec, revision="source-selection", origins=(selected,))
        )
        with patch(
            "codebench_pipeline.batch_labeling.guarded_complete", side_effect=AssertionError("Inference")
        ):
            run_registered(self.spec.dataset_id, self.home, prepare_only=True)
        urls = {str(row.values["source_url"]) for row in self.documents().page.rows}
        self.assertEqual(
            urls,
            {f"https://raw.githubusercontent.com/{selected.repository}/{selected.commit}/sample/README.md"},
        )

    def test_archived_source_review_retraction_does_not_invalidate_declared_inventory_outputs(self) -> None:
        stored = self.data.read_current(self.spec.dataset_id, "capture_corpus_files")
        self.assertIsNone(stored.publication_state)
        assert isinstance(stored.value, pd.DataFrame)
        rows = stored.value
        ids = tuple(str(value) for value in rows["source_id"])
        stage = ReviewStage(
            stage_id="sources",
            title="Historical source reviews",
            asset_keys=(("datasets", "source_documents"),),
            review_columns=("source_id",),
        )
        source = TableReviewSource(
            self.home,
            stage,
            lambda _: ReviewTable(rows, "source_id", "historical-fixture", {}, ids),
            ObjectStore.from_environment(self.spec.storage),
        )
        workflow = ReviewWorkflow.for_home(self.home, stage, source)
        batch = workflow.prepare(self.spec.dataset_id)
        for identity in ids:
            workflow.decide(
                DecisionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=batch.batch_id,
                        generation=batch.generation,
                        candidate_id=identity,
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="synthetic reviewer",
                    ),
                )
            )
        completed = workflow.complete(
            CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=batch.snapshot.snapshot_id,
                expected=workflow.view(self.query).batch,
                reviewer="synthetic reviewer",
            )
        )
        workflow.retract(
            RetractionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=batch.snapshot.snapshot_id,
                expected=completed.batch,
                candidate_id=ids[0],
                expected_row_revision=1,
                reviewer="synthetic reviewer",
            )
        )
        after = self.data.read_current(self.spec.dataset_id, "capture_corpus_files")
        self.assertIsNone(after.publication_state)
        self.assertEqual(after.reference, stored.reference)
        self.assertEqual(self.cycle.tick(self.instance), ())

    def test_changed_license_pin_stales_native_capture_without_rebinding_its_producer(self) -> None:
        partition = self.spec.dataset_id
        inspector = ArtifactCurrentness(self.home, self.instance)
        record = inspector.latest(AssetKey(["datasets", "capture_corpus_files"]), partition)
        assert record is not None
        run = self.instance.get_run_by_id(record.run_id)
        assert run is not None
        self.assertEqual(run.run_config, {})
        original_revision = self.registry.current_revision(partition)
        self.assertEqual(inspector.revision(record), original_revision)
        before = self.documents()
        self.assertIsNone(before.publication_state)

        declaration = dataset_declaration(self.spec)
        first, *remaining = declaration.origins
        license_file = first.rights.license_file
        new_pin = "0" * 64 if license_file.sha256 != "0" * 64 else "1" * 64
        rights = first.rights.model_copy(
            update={"license_file": license_file.model_copy(update={"sha256": new_pin})}
        )
        changed = update_dataset_declaration(
            self.spec,
            revision=declaration.refresh_revision,
            origins=(first.model_copy(update={"rights": rights}), *remaining),
        )
        self.registry.register(changed)
        refresh_registered_configuration(partition, self.home)
        self.assertNotEqual(self.registry.current_revision(partition), original_revision)
        self.assertEqual(inspector.revision(record), original_revision)
        after = self.documents()
        self.assertEqual(after.snapshot_id, before.snapshot_id)
        self.assertEqual(after.publication_state, "stale")


class CollectionAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.spec: DatasetBuildSpec = LabelingFixture(self, self.home).spec
        self.registry = DatasetRegistry(self.home)
        self.registry.register(self.spec)
        self.instance = self.enterContext(dataset_instance(self.home))
        self.instance.add_dynamic_partitions("dataset_builds", [self.spec.dataset_id])
        initial = (
            build_definitions(self.home)
            .resolve_job_def("build_existing_datasets")
            .execute_in_process(
                instance=self.instance,
                partition_key=self.spec.dataset_id,
                asset_selection=configured_selection(
                    AssetKey(["datasets", stage])
                    for stage in ("category_rules", "enumerate_corpus_files", "capture_corpus_files")
                ),
            )
        )
        self.assertTrue(initial.success)
        self.data = DatasetDataService(self.home, self.instance)

    def test_inventory_change_during_compute_prevents_output_commit(self) -> None:
        previous = self.data.read_current(self.spec.dataset_id, "capture_corpus_files").reference
        updated = update_dataset_declaration(self.spec, revision="before-capture")
        self.registry.register(updated)

        def changed_inventory(
            catalog: DataFrame[SourceCatalogTable],
            config: RepositoryAcquisitionConfig,
            run: RepositoryRun,
            *,
            prepared_cache: PreparedCaptureCache | None = None,
        ) -> DataFrame[SourceDocumentsTable]:
            frame = repository_documents(catalog, config, run, prepared_cache=prepared_cache)
            self.registry.register(update_dataset_declaration(updated, revision="during-capture"))
            return frame

        stages = ["enumerate_corpus_files", "capture_corpus_files"]
        with patch(
            "codebench_pipeline.declared_source_assets.repository_documents", side_effect=changed_inventory
        ) as capture:
            result = (
                build_definitions(self.home)
                .resolve_job_def("build_existing_datasets")
                .execute_in_process(
                    instance=self.instance,
                    partition_key=self.spec.dataset_id,
                    asset_selection=configured_selection(AssetKey(["datasets", stage]) for stage in stages),
                    raise_on_error=False,
                )
            )
        capture.assert_called_once()
        self.assertFalse(result.success)
        (failure,) = result.get_step_failure_events()
        failure_data = failure.step_failure_data
        assert failure_data is not None and failure_data.error is not None
        self.assertIn(
            "Registered dataset configuration changed during the run", failure_data.error.to_string()
        )
        self.assertEqual(
            self.data.read_current(self.spec.dataset_id, "capture_corpus_files").reference, previous
        )

    def test_source_processing_does_not_reload_registered_settings(self) -> None:
        read_registered = DatasetRegistry.get
        write_configuration = ConfigRunIOManager.handle_output
        configuration_loaded = False

        def reject_reload(registry: DatasetRegistry, dataset_id: str) -> DatasetBuildSpec:
            if configuration_loaded:
                raise AssertionError("Source processing reloaded the registry specification")
            return read_registered(registry, dataset_id)

        def record_configuration(
            manager: ConfigRunIOManager, context: OutputContext, value: BaseModel
        ) -> None:
            nonlocal configuration_loaded
            write_configuration(manager, context, value)
            if context.asset_key == GLOBAL_CONFIG:
                configuration_loaded = True

        stages = ("enumerate_corpus_files", "capture_corpus_files")
        with (
            patch.object(ConfigRunIOManager, "handle_output", record_configuration),
            patch.object(DatasetRegistry, "get", reject_reload),
            patch.object(
                DatasetRegistry, "get_snapshot", side_effect=AssertionError("Read the registry specification")
            ),
        ):
            result = (
                build_definitions(self.home)
                .resolve_job_def("build_existing_datasets")
                .execute_in_process(
                    instance=self.instance,
                    partition_key=self.spec.dataset_id,
                    asset_selection=configured_selection(AssetKey(["datasets", stage]) for stage in stages),
                    run_config=dataset_run_config(self.spec),
                )
            )
        self.assertEqual(len(result.asset_materializations_for_node("datasets__capture_corpus_files")), 1)

    def test_inventory_binding_cannot_cross_collection_branches(self) -> None:
        _, binding = inventory_authorization(
            DataFrameIOManager(storage=self.spec.storage),
            self.instance,
            DeclaredGlobalConfig.from_spec(self.spec),
            "dataset",
        )
        frame = pd.DataFrame()
        frame.attrs[inventory_binding("documentation")] = binding.model_dump(mode="json")
        with self.assertRaises(HumanReviewError) as raised:
            frame_inventory_authorization(frame, "documentation")
        self.assertEqual(raised.exception.code, "stale")
