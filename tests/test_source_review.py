import io
import json
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from configuration_fixture import write_fixture
from dagster import AssetKey, AssetRecordsFilter
from pandera.typing import DataFrame
from test_dataset_incremental import provider_response

from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_schemas import SourceCatalogTable, SourceDocumentsTable
from codebench_pipeline.dataset_cli import execute_build, execute_registered_build
from codebench_pipeline.dataset_config import SnapshotConfig, load_build_config
from codebench_pipeline.dataset_documents import snapshot_documents
from codebench_pipeline.dataset_plan import INPUT_STAGES, plan_dataset
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.documentation_authorization import (
    frame_inventory_authorization,
    inventory_authorization,
    inventory_binding,
)
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.review_api_models import CompletionRequest, DecisionRequest, RetractionRequest
from codebench_pipeline.review_workflow import ReviewWorkflow
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_review_inputs import SourceReviewSource, source_stage
from codebench_pipeline.stage_configuration import stage_configs


class SourceReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.home = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        original = write_fixture(self.home)
        config = json.loads(original.read_text())
        config["seeds"] = config.pop("documentation_sources")
        for seed in config["seeds"]["documents"]:
            seed["local_path"] = str((original.parent / seed["local_path"]).resolve())
        config["seeds"]["documents"].append(
            {**config["seeds"]["documents"][0], "source_id": "second-source-fixture"}
        )
        config["storage"] = {"local_root": str(self.home / "tables")}
        self.config = self.home / "config.json"
        self.config.write_text(json.dumps(config))
        for module in ("dataset_annotation",):
            self.stack.enter_context(
                patch(f"codebench_pipeline.{module}.infer_typed", side_effect=provider_response)
            )
        with redirect_stdout(io.StringIO()):
            self.prepared = execute_build(self.config, self.home)
        self.spec = load_build_config(self.config, self.home)
        self.instance = self.stack.enter_context(dataset_instance(self.home))
        self.data = DatasetDataService(self.home, self.instance)
        self.query = DatasetQueryRequest(dataset_id=self.spec.dataset_id)
        self.workflow = ReviewWorkflow(
            self.home, source_stage(), SourceReviewSource(self.home, self.instance)
        )

    def documents(self) -> TableResponse:
        result = self.data.query("source_documents", self.query)
        assert isinstance(result, TableResponse)
        return result

    def test_sources_load_without_review_and_unchanged_build_reuses_outputs(self) -> None:
        self.assertIn("source_documents", self.prepared)
        self.assertIn("human_review_queue", self.prepared)
        self.assertNotIn("human_source_decisions", INPUT_STAGES)
        self.assertEqual(
            {str(row.values["source_id"]) for row in self.documents().page.rows},
            {seed.source_id for seed in self.spec.catalog.documents},
        )
        plan = plan_dataset(
            self.spec, stage_configs(self.spec), self.instance, home=self.home
        )
        self.assertEqual(plan.selected, ())
        self.assertIsNone(plan.waiting_for)
        self.assertEqual(
            self.instance.fetch_materializations(
                AssetRecordsFilter(asset_key=AssetKey(["datasets", "human_source_decisions"])),
                limit=1,
            ).records,
            [],
        )
        with redirect_stdout(io.StringIO()):
            self.assertEqual(execute_registered_build(self.spec.dataset_id, self.home), {})

    def test_inventory_revision_stales_and_rebinds_identical_source_rows(self) -> None:
        _, before = inventory_authorization(
            self.home, self.instance, self.spec.dataset_id, "dataset"
        )
        updated = self.spec.model_copy(
            update={"catalog": self.spec.catalog.model_copy(update={"collection_revision": "refresh"})}
        )
        DatasetRegistry(self.home).register(updated)
        self.assertEqual(self.documents().publication_state, "stale")
        with redirect_stdout(io.StringIO()):
            rebuilt = execute_registered_build(self.spec.dataset_id, self.home)
        self.assertIn("source_documents", rebuilt)
        _, after = inventory_authorization(
            self.home, self.instance, self.spec.dataset_id, "dataset"
        )
        self.assertNotEqual(before, after)
        stored = self.data.read_current(self.spec.dataset_id, "source_documents")
        assert isinstance(stored.value, pd.DataFrame)
        self.assertEqual(frame_inventory_authorization(stored.value, "dataset"), after)
        self.assertIsNone(stored.publication_state)

    def test_declared_source_list_controls_acquisition_without_source_inference(self) -> None:
        selected = self.spec.catalog.documents[:1]
        DatasetRegistry(self.home).register(self.spec.model_copy(update={
            "catalog": self.spec.catalog.model_copy(update={
                "documents": selected, "collection_revision": "manifest-selection"
            })
        }))
        with redirect_stdout(io.StringIO()):
            execute_registered_build(self.spec.dataset_id, self.home)
        self.assertEqual(
            [row.values["source_id"] for row in self.documents().page.rows],
            [selected[0].source_id],
        )

    def test_source_review_retraction_does_not_invalidate_inventory_outputs(self) -> None:
        view = self.workflow.view(self.query)
        for identity in view.batch.snapshot.candidate_ids:
            self.workflow.decide(
                DecisionRequest(
                    dataset_id=self.spec.dataset_id,
                    snapshot_id=view.batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=view.batch.batch_id,
                        generation=view.batch.generation,
                        candidate_id=identity,
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="historical fixture reviewer",
                    ),
                )
            )
        view = self.workflow.view(self.query)
        self.workflow.complete(
            CompletionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=view.batch.snapshot.snapshot_id,
                expected=view.batch,
                reviewer="historical fixture reviewer",
            )
        )
        completed = self.workflow.view(self.query)
        self.workflow.retract(
            RetractionRequest(
                dataset_id=self.spec.dataset_id,
                snapshot_id=completed.batch.snapshot.snapshot_id,
                expected=completed.batch,
                candidate_id=completed.batch.snapshot.candidate_ids[0],
                expected_row_revision=1,
                reviewer="historical fixture reviewer",
            )
        )
        self.assertIsNone(self.documents().publication_state)
        self.assertEqual(
            plan_dataset(self.spec, stage_configs(self.spec), self.instance, home=self.home).selected,
            (),
        )

    def test_inventory_change_during_compute_prevents_output_commit(self) -> None:
        previous = self.data.read_current(self.spec.dataset_id, "source_documents").reference
        updated = self.spec.model_copy(
            update={
                "snapshots": self.spec.snapshots.model_copy(
                    update={"maximum_bytes": self.spec.snapshots.maximum_bytes + 1}
                )
            }
        )
        DatasetRegistry(self.home).register(updated)

        def changed_inventory(
            catalog: DataFrame[SourceCatalogTable],
            config: SnapshotConfig,
            *,
            cache: Path | None = None,
            evidence: Path | None = None,
        ) -> DataFrame[SourceDocumentsTable]:
            frame = snapshot_documents(catalog, config, cache=cache, evidence=evidence)
            DatasetRegistry(self.home).register(
                updated.model_copy(
                    update={
                        "catalog": updated.catalog.model_copy(update={"collection_revision": "concurrent"})
                    }
                )
            )
            return frame

        with patch(
            "codebench_pipeline.dataset_assets_sources.snapshot_documents", side_effect=changed_inventory
        ):
            result = build_definitions(self.home).resolve_job_def("build_dataset").execute_in_process(
                instance=self.instance,
                partition_key=self.spec.dataset_id,
                asset_selection=[AssetKey(["datasets", "source_documents"])],
                raise_on_error=False,
            )
        self.assertFalse(result.success)
        self.assertEqual(
            self.data.read_current(self.spec.dataset_id, "source_documents").reference, previous
        )

    def test_inventory_binding_cannot_cross_collection_branches(self) -> None:
        _, binding = inventory_authorization(
            self.home, self.instance, self.spec.dataset_id, "dataset"
        )
        frame = pd.DataFrame()
        frame.attrs[inventory_binding("documentation")] = binding.model_dump(mode="json")
        with self.assertRaises(HumanReviewError) as raised:
            frame_inventory_authorization(frame, "documentation")
        self.assertEqual(raised.exception.code, "stale")
