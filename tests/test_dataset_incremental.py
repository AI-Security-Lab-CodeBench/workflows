import csv
import io
import json
import tempfile
import unittest
from collections.abc import Iterable
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import BaseModel
from test_review_release import attest_fixture_release

from codebench_pipeline.collection_config import SourceDeclaration
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION
from codebench_pipeline.dataset_cli import execute_registered_build
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_records import LabelDecision
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_source_models import SourceSelection
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.keenable_policy import KeenableDeclaration
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.runtime_instance import dataset_instance


def provider_response(
    schema: type[BaseModel], system: str, prompt: str, model: ModelConfig, out: Path
) -> BaseModel:
    payload = json.loads(prompt[prompt.index("{") :])
    if schema is SourceSelection:
        return SourceSelection.model_validate(
            {
                "sources": [
                    {
                        "source_id": row["source_id"],
                        "decision": "selected",
                        "rationale": "Inspected example.",
                        "analysis": "The inspected source contains in-scope command examples.",
                        "confidence": "high",
                    }
                    for row in payload["sources"]
                ]
            }
        )
    if issubclass(schema, LabelDecision):
        candidate = payload["candidate"]
        risk = payload["allowed_risk_ids"][0]
        return LabelDecision.model_validate(
            {
                "candidate_id": candidate["candidate_id"],
                "decision": "accept",
                "category_id": risk.split(".")[0],
                "risk_id": risk,
                "severity": "low",
                "class": "allow",
                "description": "Inspects repository state.",
                "util": candidate["util"],
                "category_confidence": "medium",
                "risk_confidence": "high",
                "severity_confidence": "high",
                "class_confidence": "high",
                "rationale": "Documented inspection.",
                "documentation_urls": [payload["documentation"][0]["url"]],
            }
        )
    raise AssertionError(f"Unexpected provider schema: {schema}")


class IncrementalDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.fixture = LabelingFixture(self, self.home, category_confidence="medium")
        self.spec: DatasetBuildSpec = self.fixture.spec
        self.cycle = NativeDatasetCycle(self.home)
        self.first = self.build()

    def build(self) -> dict[str, bool]:
        DatasetRegistry(self.home).register(self.spec)
        with redirect_stdout(io.StringIO()):
            execute_registered_build(self.spec.dataset_id, self.home)
        with dataset_instance(self.home) as instance:
            return self.cycle.drain(instance, partition=self.spec.dataset_id)

    def update_labeler(self) -> None:
        annotation = self.spec.annotation
        model = annotation.model.model_copy(update={"timeout_seconds": annotation.model.timeout_seconds + 1})
        self.spec = self.spec.model_copy(
            update={"annotation": annotation.model_copy(update={"model": model})}
        )

    def materializations(self, dataset: str, stages: Iterable[str] | None = None) -> dict[str, int]:
        with dataset_instance(self.home) as instance:
            return {
                stage: instance.fetch_materializations(
                    AssetRecordsFilter(
                        asset_key=AssetKey(["datasets", *stage.split("/")]), asset_partitions=[dataset]
                    ),
                    limit=1,
                )
                .records[0]
                .storage_id
                for stage in (self.first if stages is None else stages)
            }

    def assert_boundary_changes(self, before: dict[str, int], changed: set[str]) -> None:
        after = self.materializations(self.spec.dataset_id, before)
        for stage in before:
            with self.subTest(stage=stage):
                if stage in changed:
                    self.assertNotEqual(after[stage], before[stage])
                else:
                    self.assertEqual(after[stage], before[stage])

    def test_documentation_refresh_rebuilds_consumers_and_reuses_sibling_evidence(self) -> None:
        before = self.materializations(
            self.spec.dataset_id,
            (
                "command_candidates",
                "documentation/capture_documentation",
                "documentation/command_candidates",
                "keenable/command_candidates",
                "merged_command_candidates",
                "filtered_commands",
                "labeling_attempts",
                "human_review_queue",
            ),
        )
        collection = self.spec.declared_collection
        assert collection is not None
        declaration = collection.documentation_crawl.declaration
        assert isinstance(declaration, RepositoryBranchDeclaration)
        revised = SourceDeclaration.from_declaration(
            declaration.model_copy(update={"refresh_revision": "new-documentation-scope"})
        )
        self.spec = self.spec.model_copy(
            update={"declared_collection": collection.model_copy(update={"documentation_crawl": revised})}
        )
        self.build()
        self.assert_boundary_changes(
            before,
            {
                "documentation/capture_documentation",
                "documentation/command_candidates",
                "merged_command_candidates",
                "filtered_commands",
                "labeling_attempts",
                "human_review_queue",
            },
        )
        self.assertEqual(self.build(), {})

    def test_source_revision_preserves_rules_and_refreshes_source_bound_labeling(self) -> None:
        dataset_id = self.spec.dataset_id
        before = self.materializations(
            dataset_id,
            (
                "command_candidates",
                "documentation/command_candidates",
                "keenable/command_candidates",
                "merged_command_candidates",
                "filtered_commands",
                "labeling_attempts",
                "human_review_queue",
            ),
        )
        with dataset_instance(self.home) as instance:
            rules = (
                DatasetDataService(self.home, instance).read_current(dataset_id, "category_rules").reference
            )
        collection = self.spec.declared_collection
        assert collection is not None
        declaration = collection.existing_datasets.declaration
        assert isinstance(declaration, RepositoryBranchDeclaration)
        revised = SourceDeclaration.from_declaration(
            declaration.model_copy(update={"refresh_revision": "dataset-source-refresh"})
        )
        self.spec = self.spec.model_copy(
            update={"declared_collection": collection.model_copy(update={"existing_datasets": revised})}
        )
        self.build()
        self.assert_boundary_changes(
            before,
            {
                "command_candidates",
                "merged_command_candidates",
                "filtered_commands",
                "labeling_attempts",
                "human_review_queue",
            },
        )
        with dataset_instance(self.home) as instance:
            refreshed = DatasetDataService(self.home, instance).read_current(dataset_id, "category_rules")
        self.assertEqual(refreshed.reference.digest, rules.digest)

    def test_labeler_and_policy_changes_reuse_filtered_command_evidence(self) -> None:
        stages = (
            "command_candidates",
            "documentation/command_candidates",
            "keenable/command_candidates",
            "filtered_commands",
            "labeling_attempts",
            "human_review_queue",
        )
        with dataset_instance(self.home) as instance:
            self.assertEqual(self.cycle.tick(instance), ())
            self.update_labeler()
            DatasetRegistry(self.home).register(self.spec)
            self.assertEqual(self.cycle.tick(instance), ())

        self.spec = self.spec.model_copy(update={"revision": "label-settings-update"})
        rules = self.spec.rules.model_copy(
            update={
                "policy": self.spec.rules.policy + " Check evidence carefully.",
                "version": "policy-update",
            }
        )
        for change, spec in (
            ("labeler", self.spec),
            ("policy", self.spec.model_copy(update={"rules": rules})),
        ):
            with self.subTest(change=change):
                before = self.materializations(self.spec.dataset_id, stages)
                inference_before = len(self.fixture.remote_rows)
                self.spec = spec
                self.build()
                self.assert_boundary_changes(before, {"labeling_attempts", "human_review_queue"})
                self.assertGreater(len(self.fixture.remote_rows), inference_before)
        self.assertEqual(self.build(), {})

    def test_publication_cap_replaces_completion_without_changing_review_rows(self) -> None:
        dataset_id = self.spec.dataset_id
        store = HumanReviewStore(self.home)
        with dataset_instance(self.home) as instance, store.transaction() as tx:
            data = DatasetDataService(self.home, instance)
            before = data.read_current(dataset_id, "human_review_queue").value
            original = tx.get_current_batch(dataset_id, "annotations")
            self.assertTrue(original.snapshot.candidate_ids)
            for candidate_id in original.snapshot.candidate_ids:
                tx.save_decision(
                    original.snapshot,
                    DecisionChange(
                        batch_id=original.batch_id,
                        generation=original.generation,
                        candidate_id=candidate_id,
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="synthetic QA",
                    ),
                )
            token = tx.complete(
                original.snapshot, tx.get_current_batch(dataset_id, "annotations"), "synthetic QA"
            )
            self.assertEqual(tx.authorize_completion(original.snapshot, token), token)

        self.spec = self.spec.model_copy(
            update={"publication": self.spec.publication.model_copy(update={"limit_per_risk": 1})}
        )
        self.assertEqual(set(self.build()), {"human_review_queue"})
        with dataset_instance(self.home) as instance:
            run = instance.get_runs(limit=1)[0]
            data = DatasetDataService(self.home, instance)
            after = data.read_current(dataset_id, "human_review_queue").value
            snapshots = ReviewSnapshots.from_environment(self.home, data)
            self.assertEqual(snapshots.current(dataset_id).manifest.publication.limit_per_risk, 1)
        assert isinstance(before, pd.DataFrame) and isinstance(after, pd.DataFrame)
        pd.testing.assert_frame_equal(before, after)
        with store.transaction(dataset_id=dataset_id) as tx:
            fresh = tx.get_current_batch(dataset_id, "annotations")
            self.assertEqual(fresh.snapshot.candidate_ids, original.snapshot.candidate_ids)
            self.assertEqual(fresh.snapshot.record_fingerprints, original.snapshot.record_fingerprints)
            self.assertNotEqual(fresh.snapshot.snapshot_id, original.snapshot.snapshot_id)
            self.assertNotEqual(fresh.batch_id, original.batch_id)
            self.assertIsNone(fresh.completion)
            with self.assertRaises(HumanReviewError) as rejected:
                tx.authorize_completion(fresh.snapshot, token)
            self.assertEqual(rejected.exception.code, "stale")
            decisions = tx.list_decisions(fresh.batch_id)
            self.assertEqual(
                {row.candidate_id for row in decisions if row.decision == "approve"},
                set(original.snapshot.candidate_ids),
            )
            self.assertIsNone(tx.last_publication(dataset_id))
            updated_token = tx.complete(fresh.snapshot, fresh, "synthetic QA")
        self.assertEqual(run.job_name, "__ASSET_JOB")
        with dataset_instance(self.home) as instance:
            runner = ReleaseRunner(self.home, instance)
            with self.assertRaises(HumanReviewError) as rejected:
                runner(token)
            self.assertEqual(rejected.exception.code, "stale")
            attest_fixture_release(self.home, instance, dataset_id)
            published = runner(updated_token)
        payload = ObjectStore(self.spec.storage).read(published.csv_reference)
        (row,) = csv.DictReader(io.StringIO(payload.decode()))
        self.assertEqual(row["human_reviewed"], "true")
        self.assertIn(row["command"], before["command"].tolist())

    def test_registered_metadata_revision_reuses_review_queue_and_authority(self) -> None:
        before = self.materializations(self.spec.dataset_id)
        store = HumanReviewStore(self.home)
        with store.reader() as transaction:
            original = transaction.get_current_batch(self.spec.dataset_id, "annotations")
        self.spec = self.spec.model_copy(update={"revision": "metadata-only"})
        with patch(
            "codebench_pipeline.batch_labeling.guarded_complete",
            side_effect=AssertionError("Inference reran"),
        ):
            self.assertEqual(self.build(), {})
        self.assertEqual(self.materializations(self.spec.dataset_id), before)
        with dataset_instance(self.home) as instance:
            data = DatasetDataService(self.home, instance)
            queue = data.read_current(self.spec.dataset_id, "human_review_queue").value
            assert isinstance(queue, pd.DataFrame)
            with store.reader() as transaction:
                batch = transaction.get_current_batch(self.spec.dataset_id, "annotations")
            self.assertEqual(
                (batch.batch_id, batch.snapshot.snapshot_id),
                (original.batch_id, original.snapshot.snapshot_id),
            )
            self.assertEqual(
                queue.attrs[REVIEW_PREPARATION],
                {"batch_id": batch.batch_id, "snapshot_id": batch.snapshot.snapshot_id},
            )
            runs_before = len(instance.get_runs())
        self.assertEqual(self.build(), {})
        self.assertEqual(self.materializations(self.spec.dataset_id), before)
        with dataset_instance(self.home) as instance:
            self.assertEqual(len(instance.get_runs()), runs_before + 1)
            self.assertEqual(instance.get_runs(limit=1)[0].job_name, "refresh_dataset_configuration")

    def test_unchanged_refresh_does_not_retry_failed_native_labeling_run(self) -> None:
        before = self.materializations(self.spec.dataset_id)
        self.update_labeler()
        with patch(
            "codebench_pipeline.batch_labeling.guarded_complete",
            side_effect=RuntimeError("Provider rejected this request"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Provider rejected this request"):
                self.build()
        self.assertEqual(self.materializations(self.spec.dataset_id), before)
        with patch(
            "codebench_pipeline.batch_labeling.guarded_complete",
            side_effect=AssertionError("Refresh retried a failed run instead of leaving it for native retry"),
        ):
            self.assertEqual(self.build(), {})
        self.assertEqual(self.materializations(self.spec.dataset_id), before)

    def test_adding_and_changing_category_leaves_existing_category_events_untouched(self) -> None:
        original = self.spec
        identity = original.dataset_id
        before = self.materializations(identity)
        collection = original.declared_collection
        assert collection is not None
        keenable = collection.keenable_declaration
        assert isinstance(keenable, KeenableDeclaration)
        dataset_id = "another_category"
        revised = SourceDeclaration.from_declaration(keenable.model_copy(update={"dataset": dataset_id}))
        rules = original.rules.model_copy(
            update={
                "category_id": "CAT-9",
                "risks": [
                    rule.model_copy(update={"id": f"CAT-9.{index}"})
                    for index, rule in enumerate(original.rules.risks, 1)
                ],
            }
        )
        self.spec = original.model_copy(
            update={
                "dataset_id": dataset_id,
                "rules": rules,
                "config_directory": None,
                "declared_collection": collection.model_copy(update={"keenable_use_cases": revised}),
            }
        )
        self.build()
        boundaries = (
            "merged_command_candidates",
            "filtered_commands",
            "labeling_attempts",
            "human_review_queue",
        )
        new_category = self.materializations(dataset_id, boundaries)
        with dataset_instance(self.home) as instance:
            rules_output = DatasetDataService(self.home, instance).read_current(dataset_id, "category_rules")
        self.assertEqual(rules_output.value, rules)
        self.update_labeler()
        self.build()
        self.assert_boundary_changes(new_category, {"labeling_attempts", "human_review_queue"})
        self.assertEqual(self.materializations(identity), before)
        self.spec = original
        self.assertEqual(self.build(), {})
