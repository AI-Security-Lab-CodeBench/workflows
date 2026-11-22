import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from unittest.mock import patch

import pandas as pd
from configuration_fixture import example_config, seed_prepared_snapshot
from dagster import DagsterInstance, build_sensor_context
from guarded_labeling_fixture import DOCS, LIVE_URL, LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import JsonValue, TypeAdapter
from test_declared_dataset_graph import fixture_spec
from test_repository import RepositoryFixture, repository_branch_fixture, repository_config_fixture

from codebench_pipeline.collection_config import SourceDeclaration
from codebench_pipeline.data_view_models import DatasetQueryRequest, DatasetViewError, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, asset_storage_key
from codebench_pipeline.dataframe_schemas import model_records
from codebench_pipeline.dataset_catalog import build_catalog
from codebench_pipeline.dataset_config import (
    CatalogConfig,
    DatasetBuildSpec,
    FilteringConfig,
    require_declared_annotation,
)
from codebench_pipeline.dataset_finalization import (
    FINALIZATION_BINDING,
    authorize_finalization_output,
    available_candidates,
    merge_candidates,
)
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.grounding_sources import CandidateEvidence
from codebench_pipeline.grounding_tables import GuardedLabeling, verify_live_support
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.labeling_runtime import LabelingRuntime
from codebench_pipeline.labeling_stage_settings import LabelingSources
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.repository_etl import FencedLinesOptions, MarkdownRepositoryParser
from codebench_pipeline.repository_receipts import RepositoryRun
from codebench_pipeline.repository_tree import raw_url, tree_url
from codebench_pipeline.review_api_models import DecisionRequest, EvidenceTarget
from codebench_pipeline.review_artifact_admission import ReviewArtifact, checked_review_artifacts
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, GuardedLabelingConfig
from codebench_pipeline.storage import IntegrityError


class IndependentBranchDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.registry = DatasetRegistry(self.home)

    def refresh(self, cycle: NativeDatasetCycle, instance: DagsterInstance, dataset_id: str) -> None:
        instance.add_dynamic_partitions("dataset_builds", [dataset_id])
        result = cycle.definitions.resolve_job_def("refresh_dataset_configuration").execute_in_process(
            instance=instance, partition_key=dataset_id
        )
        self.assertTrue(result.success)

    def test_runtime_corpus_metadata_cannot_bypass_admission_as_documentation(self) -> None:
        fixture = example_config()
        catalog = fixture.documentation_sources
        assert catalog is not None
        corpus = catalog.documents[0].model_copy(
            update={"source_type": "human_command_corpus", "provenance_type": "human_collected"}
        )
        configured = CatalogConfig(documents=[corpus])
        with self.assertRaisesRegex(IntegrityError, "Dataset corpora cannot enter"):
            build_catalog(configured, fixture.rules, self.home / "attempt", branch="documentation")

    def test_review_snapshot_rejects_disagreement_in_verified_source_bindings(self) -> None:
        spec = LabelingFixture(self, self.home, native=True, category_confidence="medium").spec
        self.registry.register(spec)
        cycle = NativeDatasetCycle(self.home)
        with dataset_instance(self.home) as instance:
            self.refresh(cycle, instance, spec.dataset_id)
            cycle.drain(instance, partition=spec.dataset_id)
            snapshots = ReviewSnapshots.from_environment(self.home, DatasetDataService(self.home, instance))
            original = snapshots.current(spec.dataset_id)

            def disagreeing_source_binding(
                manager: DataFrameIOManager,
                instance: DagsterInstance,
                registered: DeclaredGlobalConfig,
                *,
                include_queue: bool,
            ) -> dict[str, ReviewArtifact]:
                artifacts = checked_review_artifacts(
                    manager, instance, registered, include_queue=include_queue
                )
                stage = "documentation/command_candidates"
                artifact = artifacts[stage]
                snapshot = artifact.snapshot
                artifacts[stage] = replace(
                    artifact,
                    snapshot=replace(
                        snapshot, reference=snapshot.reference.model_copy(update={"digest": "0" * 64})
                    ),
                )
                return artifacts

            with patch(
                "codebench_pipeline.review_snapshots.checked_review_artifacts",
                side_effect=disagreeing_source_binding,
            ):
                with self.assertRaisesRegex(DatasetViewError, "different collection inputs") as stale:
                    snapshots.capture(spec.dataset_id, tuple(spec.publication.review_columns))
                self.assertEqual(stale.exception.code, "not_ready")
            self.assertEqual(snapshots.current(spec.dataset_id).snapshot, original.snapshot)

    def test_documentation_job_needs_no_dataset_discovery_or_source_approval(self) -> None:
        fixture = LabelingFixture(self, self.home, native=True)
        spec = fixture.spec
        self.registry.register(spec)
        with dataset_instance(self.home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            result = (
                build_definitions(self.home)
                .resolve_job_def("build_documentation")
                .execute_in_process(instance=instance, partition_key=spec.dataset_id)
            )
            self.assertTrue(result.success)
            data = DatasetDataService(self.home, instance)
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            reviewed = data.query("documentation/command_candidates", query)
            assert isinstance(reviewed, TableResponse)
            self.assertEqual([row.values["command"] for row in reviewed.page.rows], ["docker ps --all"])
            for stage in ("enumerate_corpus_files", "keenable/command_candidates", "labeling_attempts"):
                with self.assertRaises(DatasetViewError) as missing:
                    data.query(stage, query)
                self.assertEqual(missing.exception.code, "not_materialized")
            fixture.inference.assert_not_called()
        with HumanReviewStore(self.home).reader() as transaction:
            with self.assertRaises(HumanReviewError) as missing:
                transaction.get_current_batch(spec.dataset_id, "sources")
            self.assertEqual(missing.exception.code, "not_found")

    def test_declared_branches_prepare_without_source_human_completion(self) -> None:
        fixture = LabelingFixture(
            self, self.home, native=True, limit_per_risk=4, category_confidence="medium"
        )
        spec = fixture.spec
        self.registry.register(spec)
        cycle = NativeDatasetCycle(self.home)
        with dataset_instance(self.home) as instance:
            self.refresh(cycle, instance, spec.dataset_id)
            cycle.drain(instance, partition=spec.dataset_id)
            sensor = cycle.definitions.resolve_sensor_def("dataset_release_requests")
            with build_sensor_context(
                instance=instance, repository_def=cycle.definitions.get_repository_def()
            ) as context:
                self.assertFalse(sensor.evaluate_tick(context).run_requests)
        with dataset_instance(self.home) as instance:
            data = DatasetDataService(self.home, instance)
            filtered = data.read_current(spec.dataset_id, "filtered_commands").value
            assert isinstance(filtered, pd.DataFrame)
            eligible = filtered.loc[filtered["filter_decision"] == "eligible"]
            self.assertCountEqual(fixture.remote_rows, eligible["candidate_id"].tolist())
            self.assertEqual(
                {
                    (identity.split(":")[0], command)
                    for identity, command in zip(eligible["source_id"], eligible["command"], strict=True)
                },
                {
                    ("dataset", "kubectl get pods"),
                    ("dataset", "docker compose ps"),
                    ("documentation", "docker ps --all"),
                    ("keenable", "terraform plan"),
                },
            )
        with HumanReviewStore(self.home).reader() as transaction:
            with self.assertRaises(HumanReviewError) as missing:
                transaction.get_current_batch(spec.dataset_id, "sources")
            self.assertEqual(missing.exception.code, "not_found")
            batch = transaction.get_current_batch(spec.dataset_id, "annotations")
            self.assertEqual(batch.state, "open")
        fixture.inference.reset_mock()
        with dataset_instance(self.home) as instance:
            self.assertEqual(cycle.tick(instance), ())
        with HumanReviewStore(self.home).reader() as transaction:
            repeated = transaction.get_current_batch(spec.dataset_id, "annotations")
            self.assertEqual(
                (repeated.snapshot.snapshot_id, repeated.state), (batch.snapshot.snapshot_id, "open")
            )
        fixture.inference.assert_not_called()

    def test_configured_empty_branch_retains_its_digest(self) -> None:
        fixture = LabelingFixture(
            self,
            self.home,
            native=True,
            documentation_corpus=b"# Docker\n\nNo command examples.\n",
            category_confidence="medium",
        )
        spec = fixture.spec
        self.registry.register(spec)
        cycle = NativeDatasetCycle(self.home)
        with dataset_instance(self.home) as instance:
            self.refresh(cycle, instance, spec.dataset_id)
            cycle.drain(instance, prepare_only=True, partition=spec.dataset_id)
            data = DatasetDataService(self.home, instance)
            candidates = data.read_current(spec.dataset_id, "documentation/command_candidates")
            merged = data.read_current(spec.dataset_id, "merged_command_candidates").value
            assert isinstance(candidates.value, pd.DataFrame)
            assert isinstance(merged, pd.DataFrame)
            self.assertEqual(len(candidates.value), 0)
            self.assertEqual(
                merged.attrs[FINALIZATION_BINDING]["documentation"],
                {"state": "configured", "digest": candidates.reference.digest},
            )
            self.assertEqual(
                {identity.split(":")[0] for identity in merged["candidate_id"]}, {"dataset", "keenable"}
            )

    def test_labeling_without_documentation_capture_keeps_candidate_provenance(self) -> None:
        fixture = LabelingFixture(self, self.home, documentation_corpus=b"# Docker\n\nNo command examples.\n")
        spec = fixture.spec
        mapped = GuardedLabelingConfig.from_spec(spec)
        settings = mapped.labeling_attempts.batch
        paths = require_declared_annotation(spec.annotation).require_capacity()
        batch = settings.bind_capacity(paths.state_directory, paths.expected_model_path)
        run = RepositoryRun(self.home, spec.dataset_id, "synthetic-filter")
        commands = fixture.filtered()
        execution = LabelingRuntime.for_home(self.home).execution(
            mapped.labeling_attempts.model.api_key_env, settings.grounding
        )
        labeling = GuardedLabeling(mapped, batch, execution)
        attempts = labeling.attempts(commands, self.home / "attempts")
        verify_live_support(attempts, settings.grounding)
        labels = model_records(attempts, LabelingAttempt)
        self.assertEqual(
            {row.command: row.status for row in labels},
            {"kubectl get pods": "success", "docker compose ps": "success", "terraform plan": "success"},
        )
        self.assertEqual({doc.url for row in labels for doc in row.documentation}, {LIVE_URL})
        self.assertEqual(
            {request["url"] for name, request in fixture.live_calls if name == "fetch_page_content"},
            {LIVE_URL},
        )
        collection = spec.declared_collection
        assert collection is not None
        provenance = CandidateEvidence.from_frame(attempts, run, LabelingSources.from_collection(collection))
        self.assertEqual(set(provenance.repositories), {"dataset", "documentation"})
        originals = {row.candidate_id: row for row in model_records(commands, CandidateRecord)}
        for row in labels:
            original = originals[row.candidate_id]
            capture = provenance.resolve(original)
            self.assertEqual(
                (row.source_id, row.source_url, row.source_position),
                (original.source_id, capture.url, original.source_position),
            )
            self.assertNotEqual(capture.url, LIVE_URL)

    def test_configured_waiting_branch_cannot_authorize_shared_processing(self) -> None:
        fixture = LabelingFixture(self, self.home, native=True)
        spec = fixture.spec
        self.registry.register(spec)
        with dataset_instance(self.home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            result = (
                build_definitions(self.home)
                .resolve_job_def("build_documentation")
                .execute_in_process(instance=instance, partition_key=spec.dataset_id)
            )
            self.assertTrue(result.success)
            manager = DataFrameIOManager(storage=spec.storage, review_home=str(self.home))
            registered = DeclaredGlobalConfig.from_spec(spec)
            frames, binding = available_candidates(manager, instance, registered)
            self.assertEqual(binding["dataset"], {"state": "waiting"})
            self.assertEqual(binding["keenable"], {"state": "waiting"})
            self.assertEqual(frames["dataset"].attrs["branch_state"], "waiting")
            merged = merge_candidates(frames["dataset"], frames["documentation"], frames["keenable"])
            merged.attrs[FINALIZATION_BINDING] = binding
            with self.assertRaises(HumanReviewError) as waiting:
                authorize_finalization_output(manager, instance, registered, merged, complete=True)
            self.assertEqual(waiting.exception.code, "pending")
            fixture.inference.assert_not_called()

    def test_empty_prepared_branches_remain_readable_but_cannot_build(self) -> None:
        fixture = LabelingFixture(self, self.home)
        archived, revision = seed_prepared_snapshot(
            self.home, fixture_spec(self.home), example_config().documentation
        )
        self.assertEqual(self.registry.get_snapshot(archived.dataset_id, revision=revision), archived)
        self.assertFalse(archived.catalog.configured)
        self.assertIsNone(archived.documentation_catalog)
        with self.assertRaisesRegex(IntegrityError, "Only declared"):
            self.registry.register(archived)
        with (
            dataset_instance(self.home) as instance,
            self.assertRaisesRegex(IntegrityError, "read-only"),
        ):
            self.refresh(NativeDatasetCycle(self.home), instance, archived.dataset_id)
        fixture.inference.assert_not_called()

    def test_declared_build_finalizes_empty_eligible_input_without_inference(self) -> None:
        fixture = LabelingFixture(self, self.home, native=True)
        spec = fixture.spec.model_copy(update={"filtering": FilteringConfig(maximum_command_characters=1)})
        self.registry.register(spec)
        cycle = NativeDatasetCycle(self.home)
        with dataset_instance(self.home) as instance:
            self.refresh(cycle, instance, spec.dataset_id)
            cycle.drain(instance, partition=spec.dataset_id)
        fixture.inference.assert_not_called()
        with dataset_instance(self.home) as instance:
            data = DatasetDataService(self.home, instance)
            merged = data.read_current(spec.dataset_id, "merged_command_candidates").value
            attempts = data.read_current(spec.dataset_id, "labeling_attempts").value
            assert isinstance(merged, pd.DataFrame)
            assert isinstance(attempts, pd.DataFrame)
            self.assertEqual(len(merged), 4)
            self.assertTrue(attempts.empty)
        with HumanReviewStore(self.home).reader() as transaction:
            batch = transaction.get_current_batch(spec.dataset_id, "annotations")
            self.assertEqual(batch.snapshot.candidate_ids, ())
            self.assertIsNotNone(batch.completion)
            self.assertEqual(transaction.list_decisions(batch.batch_id), ())
            self.assertIsNone(transaction.last_publication(spec.dataset_id))

    def test_added_documentation_source_reinfers_all_rows_and_preserves_human_decision(self) -> None:
        fixture = LabelingFixture(
            self, self.home, native=True, limit_per_risk=6, category_confidence="medium"
        )
        spec = fixture.spec
        self.registry.register(spec)
        cycle = NativeDatasetCycle(self.home)
        query = DatasetQueryRequest(dataset_id=spec.dataset_id)
        with dataset_instance(self.home) as instance:
            self.refresh(cycle, instance, spec.dataset_id)
            cycle.drain(instance, partition=spec.dataset_id)
            data = DatasetDataService(self.home, instance)
            service = ReviewService(self.home, data, ReleaseRunner(self.home, instance))
            initial = service.view(query)
            identity = next(
                row.review_record_id
                for row in initial.page.rows
                if row.values["command"] == "kubectl get pods"
            )
            before = data.read_current(spec.dataset_id, "labeling_attempts").value
            assert isinstance(before, pd.DataFrame)
            service.decide(
                DecisionRequest(
                    dataset_id=spec.dataset_id,
                    snapshot_id=initial.batch.snapshot.snapshot_id,
                    change=DecisionChange(
                        batch_id=initial.batch.batch_id,
                        generation=initial.batch.generation,
                        candidate_id=identity,
                        expected_row_revision=0,
                        decision="approve",
                        reviewer="fixture human",
                        notes="Keep unchanged evidence.",
                    ),
                )
            )
        path = "docs/source/markdown/podman-ps.1.md"
        origin = repository_config_fixture().documentation[2]
        added = RepositoryFixture(origin.model_copy(update={"included_paths": (path,)}))
        added.file(
            path,
            b"Display all containers, including stopped ones:\n```bash\ndocker ps --all\n```\n",
        )
        collection = spec.declared_collection
        assert collection is not None
        docs = repository_branch_fixture("documentation", (fixture.repositories[2], added))
        original, extension = docs.origins
        parser = extension.parser
        assert isinstance(parser, MarkdownRepositoryParser)
        extension = extension.model_copy(
            update={
                "parser": parser.model_copy(
                    update={"options": FencedLinesOptions(mode="fenced_lines", allowed_utilities=("docker",))}
                )
            }
        )
        docs = docs.model_copy(update={"origins": (original, extension)})
        changed = spec.model_copy(
            update={
                "declared_collection": collection.model_copy(
                    update={"documentation_crawl": SourceDeclaration.from_declaration(docs)}
                )
            }
        )
        self.registry.register(changed)
        fixture.remote_rows.clear()
        added_bytes = {raw_url(added.origin, path): body for path, body in added.bodies.items()}
        added_bytes[tree_url(added.origin)] = added.response()

        def expanded_wire(url: str, limit: int) -> bytes:
            if url in added_bytes:
                self.assertLessEqual(len(added_bytes[url]), limit)
                return added_bytes[url]
            return fixture.repository_wire(url, limit)

        with (
            dataset_instance(self.home) as instance,
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes", side_effect=expanded_wire
            ),
        ):
            self.refresh(cycle, instance, changed.dataset_id)
            cycle.drain(instance, partition=changed.dataset_id)
        with dataset_instance(self.home) as instance:
            data = DatasetDataService(self.home, instance)
            service = ReviewService(self.home, data, ReleaseRunner(self.home, instance))
            after = data.read_current(spec.dataset_id, "labeling_attempts").value
            assert isinstance(after, pd.DataFrame)
            previous = {row.candidate_id: row for row in model_records(before, LabelingAttempt)}
            observed = {row.candidate_id: row for row in model_records(after, LabelingAttempt)}
            (added_id,) = observed.keys() - previous.keys()
            self.assertTrue(added_id.startswith("documentation:"))
            self.assertCountEqual(fixture.remote_rows, observed)
            for candidate_id, attempt in previous.items():
                self.assertNotEqual(
                    observed[candidate_id].row_input_fingerprint, attempt.row_input_fingerprint
                )
            current = service.view(query)
            self.assertNotEqual(current.batch.snapshot.snapshot_id, initial.batch.snapshot.snapshot_id)
            carried = next(row for row in current.decisions if row.candidate_id == identity)
            self.assertEqual(
                (carried.decision, carried.notes, carried.row_revision, carried.carried_from_batch_id),
                ("approve", "Keep unchanged evidence.", 1, initial.batch.batch_id),
            )
            self.assertEqual(current.batch.state, "open")
            self.assertIsNone(current.batch.completion)

    def test_waiting_branch_never_substitutes_its_historical_reviewed_output(self) -> None:
        fixture = LabelingFixture(self, self.home, native=True, category_confidence="medium")
        spec = fixture.spec
        self.registry.register(spec)
        cycle = NativeDatasetCycle(self.home)
        with dataset_instance(self.home) as instance:
            self.refresh(cycle, instance, spec.dataset_id)
            cycle.drain(instance, partition=spec.dataset_id)
            data = DatasetDataService(self.home, instance)
            prior = data.read_current(spec.dataset_id, "proposed_annotations")
            ObjectStore(spec.storage).write(
                asset_storage_key(("datasets", "reviewed_annotations"), spec.dataset_id),
                prior.reference.model_dump_json().encode(),
            )
            archived, revision = seed_prepared_snapshot(self.home, spec, example_config().documentation)
            self.assertEqual(data.specification(spec.dataset_id), archived)
            self.assertEqual(
                data.read_current(spec.dataset_id, "reviewed_annotations").reference, prior.reference
            )
            changed = self.with_changed_branch(fixture, "existing_datasets")
            self.registry.register(changed)
            self.assertEqual(self.registry.get_snapshot(spec.dataset_id, revision=revision), archived)
            manager = DataFrameIOManager(storage=spec.storage, review_home=str(self.home))
            frames, binding = available_candidates(manager, instance, DeclaredGlobalConfig.from_spec(changed))
            self.assertEqual(binding["dataset"], {"state": "stale"})
            self.assertTrue(frames["dataset"].empty)
            docs = data.read_current(spec.dataset_id, "documentation/command_candidates")
            self.assertEqual(
                binding["documentation"], {"state": "configured", "digest": docs.reference.digest}
            )
            with patch.object(
                fixture.inference, "side_effect", AssertionError("Historical view ran inference")
            ):
                self.assertEqual(
                    data.read_current(spec.dataset_id, "reviewed_annotations").reference, prior.reference
                )

    def with_changed_branch(
        self, fixture: LabelingFixture, field: Literal["existing_datasets", "documentation_crawl"]
    ) -> DatasetBuildSpec:
        spec = fixture.spec
        collection = spec.declared_collection
        assert collection is not None
        source = (
            collection.existing_datasets if field == "existing_datasets" else collection.documentation_crawl
        )
        declaration = source.declaration
        assert isinstance(declaration, RepositoryBranchDeclaration)
        changed = declaration.model_copy(update={"refresh_revision": "changed-fixture"})
        return spec.model_copy(
            update={
                "declared_collection": collection.model_copy(
                    update={field: SourceDeclaration.from_declaration(changed)}
                )
            }
        )

    def test_dataset_changes_preserve_documentation_but_documentation_changes_stale_it(self) -> None:
        padding = b"Fixture context for this example. " * 5 + b"\n\n"
        example = DOCS.split(b"\n\n", 1)[1]
        fixture = LabelingFixture(
            self,
            self.home,
            native=True,
            category_confidence="medium",
            documentation_corpus=b"# Docker\n\n" + (padding + example + b"\n" + padding) * 2,
        )
        spec = fixture.spec
        self.registry.register(spec)
        cycle = NativeDatasetCycle(self.home)
        fetched_at = datetime(2026, 1, 1, tzinfo=UTC)
        with (
            dataset_instance(self.home) as instance,
            patch("codebench_pipeline.live_grounding.datetime", wraps=datetime) as grounding_clock,
        ):
            grounding_clock.now.return_value = fetched_at
            self.refresh(cycle, instance, spec.dataset_id)
            cycle.drain(instance, partition=spec.dataset_id)
            data = DatasetDataService(self.home, instance)
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            service = ReviewService(self.home, data, ReleaseRunner(self.home, instance))
            view = service.view(query)
            identity = next(
                row.review_record_id for row in view.page.rows if row.values["command"] == "docker ps --all"
            )
            evidence = service.evidence(
                EvidenceTarget(
                    dataset_id=spec.dataset_id, batch_id=view.batch.batch_id, candidate_id=identity
                )
            )
            record = TypeAdapter(dict[str, JsonValue])
            survivor = record.validate_python(evidence.fields["reviewed_annotation"])
            (duplicate_evidence,) = TypeAdapter(list[dict[str, JsonValue]]).validate_python(
                evidence.fields["duplicate_evidence"]
            )
            duplicate = record.validate_python(duplicate_evidence["reviewed_annotation"])
            self.assertEqual(
                evidence.fields["duplicates"],
                [
                    {
                        "survivor_id": identity,
                        "duplicate_id": duplicate["candidate_id"],
                        "normalized_command": "docker ps --all",
                        "reason": "exact_normalized_command_and_annotation",
                    }
                ],
            )
            self.assertNotEqual(duplicate["candidate_id"], identity)
            self.assertNotEqual(duplicate["source_position"], survivor["source_position"])
            different_provenance = {"candidate_id", "source_position"}
            self.assertEqual(
                {key: value for key, value in survivor.items() if key not in different_provenance},
                {key: value for key, value in duplicate.items() if key not in different_provenance},
            )
            self.assertEqual(duplicate_evidence["source_document"], evidence.fields["source_document"])
            proposed = data.read_current(spec.dataset_id, "proposed_annotations").value
            assert isinstance(proposed, pd.DataFrame)
            row_documentation = record.validate_python(proposed.attrs["row_documentation"])
            duplicate_id = TypeAdapter(str).validate_python(duplicate["candidate_id"])
            self.assertEqual(row_documentation[identity], row_documentation[duplicate_id])
            (document,) = TypeAdapter(list[dict[str, JsonValue]]).validate_python(row_documentation[identity])
            self.assertEqual(document["retrieved_at"], fetched_at.isoformat())
            before = data.query("documentation/command_candidates", query)
            assert isinstance(before, TableResponse)
            self.assertIsNone(before.publication_state)
            self.registry.register(self.with_changed_branch(fixture, "existing_datasets"))
            after = data.query("documentation/command_candidates", query)
            assert isinstance(after, TableResponse)
            self.assertIsNone(after.publication_state)
            self.assertEqual(after.snapshot_id, before.snapshot_id)
            corpus = data.query("deduplicated_annotations", query)
            assert isinstance(corpus, TableResponse)
            self.assertEqual(corpus.publication_state, "stale")
            self.registry.register(self.with_changed_branch(fixture, "documentation_crawl"))
            stale = data.query("documentation/command_candidates", query)
            assert isinstance(stale, TableResponse)
            self.assertEqual(stale.publication_state, "stale")
