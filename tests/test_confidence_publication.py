import csv
import io
import json
import tempfile
import unittest
from contextlib import ExitStack
from functools import partial
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from dagster import AssetKey, build_input_context, build_sensor_context
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pydantic import JsonValue, TypeAdapter
from test_dataset_rules import make_candidate, make_label, make_rules
from test_review_release import attest_fixture_release

from codebench_pipeline.batch_labeling_protocol import SubmittedLabel
from codebench_pipeline.data_view_models import DatasetQueryRequest, DatasetViewError, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import REVIEW_PREPARATION, DataFrameIOManager
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_export import prepare_dataset
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration, run_registered
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.human_review import DecisionChange, HumanReviewError
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.publication_authorization import authorize_publication
from codebench_pipeline.release_runner import ReleaseRunner
from codebench_pipeline.release_stage_settings import PublicationTaxonomy
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    PublicationTarget,
    RetractionRequest,
)
from codebench_pipeline.review_service import ReviewService
from codebench_pipeline.review_snapshots import ReviewSnapshots
from codebench_pipeline.runtime_instance import dataset_instance


def mixed_confidence_reply(
    fixture: LabelingFixture,
    url: str,
    body: bytes,
    key: str | None,
    timeout: float,
    capacity: CapacityConfig,
    out: Path,
) -> HTTPResponse:
    response = fixture.infer(url, body, key, timeout, capacity, out)
    envelope = TypeAdapter(dict[str, JsonValue]).validate_json(response.body)
    (choice,) = TypeAdapter(list[dict[str, JsonValue]]).validate_python(envelope["choices"])
    message = TypeAdapter(dict[str, JsonValue]).validate_python(choice["message"])
    (call,) = TypeAdapter(list[dict[str, JsonValue]]).validate_python(message["tool_calls"])
    function = TypeAdapter(dict[str, str]).validate_python(call["function"])
    if function["name"] != "finalize":
        return response
    submitted = SubmittedLabel.model_validate_json(function["arguments"])
    row = submitted.outcome
    if row.label is not None and row.label.util == "docker":
        row = row.model_copy(update={"label": row.label.model_copy(update={"category_confidence": "medium"})})
    function["arguments"] = SubmittedLabel(outcome=row).model_dump_json(by_alias=True)
    return HTTPResponse(
        json.dumps(
            {
                **envelope,
                "choices": [
                    {**choice, "message": {**message, "tool_calls": [{**call, "function": function}]}}
                ],
            }
        ).encode(),
        response.request_id,
    )


class ConfidencePublicationTests(unittest.TestCase):
    def test_each_uncertain_judgment_requires_human_approval_and_marks_the_result(self) -> None:
        rules = make_rules()
        taxonomy = PublicationTaxonomy.from_rules(rules)
        candidate = make_candidate()
        for field in (
            "category_confidence",
            "risk_confidence",
            "severity_confidence",
            "class_confidence",
        ):
            for confidence in ("low", "medium", "high"):
                with self.subTest(field=field, confidence=confidence):
                    label = make_label(candidate, rules)
                    label = label.model_copy(update={field: confidence})
                    pending = prepare_dataset([candidate], [label], taxonomy, 250)
                    if confidence == "high":
                        self.assertEqual(
                            [(row["command"], row["human_reviewed"]) for row in pending.commands],
                            [(candidate.command, "false")],
                        )
                        self.assertEqual(pending.human_review_ids, ())
                    else:
                        self.assertEqual(pending.commands, [])
                        self.assertEqual(pending.human_review_ids, (candidate.candidate_id,))
                    accepted = prepare_dataset(
                        [candidate],
                        [label],
                        taxonomy,
                        250,
                        approved_ids=frozenset({candidate.candidate_id}),
                    )
                    self.assertEqual(
                        [row["human_reviewed"] for row in accepted.commands],
                        ["false" if confidence == "high" else "true"],
                    )

    def test_confident_rejection_is_neither_automatic_inclusion_nor_uncertain_review(self) -> None:
        rules = make_rules()
        candidate = make_candidate()
        publication = prepare_dataset(
            [candidate],
            [make_label(candidate, rules).model_copy(update={"decision": "reject"})],
            rules,
            250,
        )
        self.assertEqual(publication.commands, [])
        self.assertEqual(publication.human_review_ids, ())
        self.assertEqual(publication.eligible_ids, ())

    def test_human_approval_cannot_admit_machine_ineligible_uncertain_labels(self) -> None:
        rules = make_rules()
        candidate = make_candidate()
        for decision, documentation in (("reject", ["https://example.org/docs"]), ("needs_review", [])):
            with self.subTest(decision=decision, documentation=documentation):
                label = make_label(candidate, rules).model_copy(
                    update={
                        "decision": decision,
                        "class_confidence": "medium",
                        "documentation_urls": documentation,
                    }
                )
                publication = prepare_dataset(
                    [candidate],
                    [label],
                    rules,
                    250,
                    approved_ids=frozenset({candidate.candidate_id}),
                )
                self.assertEqual(publication.human_review_ids, (candidate.candidate_id,))
                self.assertEqual(publication.eligible_ids, ())
                self.assertEqual(publication.commands, [])
                self.assertEqual(publication.pending[0]["human_reviewed"], "false")

    def test_supported_needs_review_labels_require_explicit_human_resolution(self) -> None:
        rules = make_rules()
        candidate = make_candidate()
        label = make_label(candidate, rules).model_copy(update={"decision": "needs_review"})
        pending = prepare_dataset([candidate], [label], rules, 250)
        self.assertEqual(pending.commands, [])
        self.assertEqual(pending.human_review_ids, (candidate.candidate_id,))
        self.assertEqual(pending.eligible_ids, (candidate.candidate_id,))
        approved = prepare_dataset(
            [candidate], [label], rules, 250, approved_ids=frozenset({candidate.candidate_id})
        )
        self.assertEqual([row["human_reviewed"] for row in approved.commands], ["true"])
        self.assertEqual(label.decision, "needs_review")

    def test_only_uncertain_rows_wait_for_human_review_and_retraction_stales_release(self) -> None:
        with ExitStack() as stack:
            home = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            fixture = LabelingFixture(self, home, native=True, limit_per_risk=4)
            spec = fixture.spec
            DatasetRegistry(home).register(spec)
            fixture.inference.side_effect = partial(mixed_confidence_reply, fixture)
            instance = stack.enter_context(dataset_instance(home))
            refresh_registered_configuration(spec.dataset_id, home)
            cycle = NativeDatasetCycle(home)
            first = cycle.drain(instance, partition=spec.dataset_id)
            self.assertIn("documentation/capture_documentation", first)
            prepared = cycle.drain(instance, partition=spec.dataset_id)
            self.assertIn("human_review_queue", first)
            self.assertEqual(prepared, {})
            data = DatasetDataService(home, instance)
            service = ReviewService(home, data, ReleaseRunner(home, instance))
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            view = service.view(query)
            self.assertEqual(view.counts.pending, 2)
            self.assertEqual(
                {str(row.values["command"]) for row in view.page.rows},
                {
                    "docker compose ps",
                    "docker ps --all",
                },
            )
            self.assertIsNone(view.publication)
            selected = next(
                row.review_record_id for row in view.page.rows if row.values["command"] == "docker ps --all"
            )
            for row in view.decisions:
                service.decide(
                    DecisionRequest(
                        dataset_id=spec.dataset_id,
                        snapshot_id=view.batch.snapshot.snapshot_id,
                        change=DecisionChange(
                            batch_id=view.batch.batch_id,
                            generation=view.batch.generation,
                            candidate_id=row.candidate_id,
                            expected_row_revision=row.row_revision,
                            decision="approve" if row.candidate_id == selected else "reject",
                            reviewer="fixture human",
                        ),
                    )
                )
            saved = service.view(query)
            request = CompletionRequest(
                dataset_id=spec.dataset_id,
                snapshot_id=saved.batch.snapshot.snapshot_id,
                expected=saved.batch,
                reviewer="fixture human",
            )
            with self.assertRaises(HumanReviewError) as pending:
                service.complete(request)
            self.assertEqual(pending.exception.code, "pending")
            attest_fixture_release(home, instance, spec.dataset_id)
            completed = service.continue_review(
                request.model_copy(update={"expected": service.view(query).batch})
            )
            assert completed.publication is not None
            union_csv = service.download(
                PublicationTarget(
                    dataset_id=spec.dataset_id, publication_id=completed.publication.publication_id
                )
            )
            self.assertEqual(
                {
                    (row["command"], row["human_reviewed"])
                    for row in csv.DictReader(io.StringIO(union_csv.decode()))
                },
                {
                    ("kubectl get pods", "false"),
                    ("terraform plan", "false"),
                    ("docker ps --all", "true"),
                },
            )
            with HumanReviewStore(home).transaction() as transaction:
                resolved = service.snapshots.current(spec.dataset_id)
                authorize_publication(resolved, completed.publication.authorization, transaction)
            current = data.query("dataset_release", query)
            assert isinstance(current, TableResponse)
            self.assertEqual(current.page.total_rows, 3)
            self.assertEqual(current.publication_state, "published")
            final_view = service.view(query)
            service.retract(
                RetractionRequest(
                    dataset_id=spec.dataset_id,
                    snapshot_id=final_view.batch.snapshot.snapshot_id,
                    expected=final_view.batch,
                    candidate_id=selected,
                    expected_row_revision=1,
                    reviewer="fixture human",
                )
            )
            stale_release = data.query("dataset_release", query)
            assert isinstance(stale_release, TableResponse)
            self.assertEqual(stale_release.publication_state, "stale")
            self.assertEqual(stale_release.page.total_rows, 3)
            self.assertIsNone(service.view(query).publication)
            self.assertEqual(
                service.download(
                    PublicationTarget(
                        dataset_id=spec.dataset_id,
                        publication_id=completed.publication.publication_id,
                    )
                ),
                union_csv,
            )

    def test_native_build_and_release_sensor_publish_empty_queue_without_cli(self) -> None:
        with ExitStack() as stack:
            home = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            spec = LabelingFixture(self, home, native=True, limit_per_risk=4).spec
            DatasetRegistry(home).register(spec)
            instance = stack.enter_context(dataset_instance(home))
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            definitions = build_definitions(home)
            built = definitions.resolve_job_def("build_dataset").execute_in_process(
                instance=instance, partition_key=spec.dataset_id, run_config=dataset_run_config(spec)
            )
            self.assertTrue(built.success)
            sensor = definitions.resolve_sensor_def("dataset_release_requests")
            context = stack.enter_context(
                build_sensor_context(instance=instance, repository_def=definitions.get_repository_def())
            )
            self.assertFalse(sensor.evaluate_tick(context).run_requests)
            attest_fixture_release(home, instance, spec.dataset_id)
            requests = sensor.evaluate_tick(context).run_requests
            assert requests is not None
            (request,) = requests
            self.assertEqual(request.partition_key, spec.dataset_id)
            released = definitions.resolve_job_def("release_dataset").execute_in_process(
                instance=instance, partition_key=request.partition_key, run_config=request.run_config
            )
            self.assertTrue(released.success)
            self.assertFalse(sensor.evaluate_tick(context).run_requests)
            result = DatasetDataService(home, instance).query(
                "dataset_release", DatasetQueryRequest(dataset_id=spec.dataset_id)
            )
            assert isinstance(result, TableResponse)
            self.assertEqual(result.publication_state, "published")
            self.assertEqual(result.page.total_rows, 4)
            self.assertEqual([row.values["human_reviewed"] for row in result.page.rows], ["false"] * 4)

    def test_all_high_confidence_build_publishes_without_human_decisions_and_reuses_release(self) -> None:
        with ExitStack() as stack:
            home = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            spec = LabelingFixture(self, home, native=True, limit_per_risk=4).spec
            DatasetRegistry(home).register(spec)
            built = run_registered(spec.dataset_id, home).materialized_stages
            self.assertNotIn("dataset_release", built)
            instance = stack.enter_context(dataset_instance(home))
            attest_fixture_release(home, instance, spec.dataset_id)
            with HumanReviewStore(home).reader() as transaction:
                token = transaction.get_current_batch(spec.dataset_id).completion
            assert token is not None
            published = ReleaseRunner(home, instance)(token)
            self.assertEqual(
                {
                    "/".join(record.asset_materialization.asset_key.path[1:])
                    for record in instance.get_records_for_run(published.dagster_run_id).records
                    if record.asset_materialization is not None
                },
                {
                    "global_config",
                    "map_dataset_release_config",
                    "human_review_decisions",
                    "dataset_release",
                },
            )
            data = DatasetDataService(home, instance)
            query = DatasetQueryRequest(dataset_id=spec.dataset_id)
            released = data.query("dataset_release", query)
            assert isinstance(released, TableResponse)
            self.assertEqual(released.publication_state, "published")
            self.assertEqual(
                {(row.values["command"], row.values["human_reviewed"]) for row in released.page.rows},
                {
                    ("docker compose ps", "false"),
                    ("docker ps --all", "false"),
                    ("kubectl get pods", "false"),
                    ("terraform plan", "false"),
                },
            )
            with patch.object(data, "read_current", side_effect=AssertionError("Unexpected upstream read")):
                resolved = ReviewSnapshots.from_environment(home, data).current(spec.dataset_id)
            self.assertEqual(
                set(resolved.manifest.references),
                set(built)
                - {"human_review_queue", "human_review_decisions", "dataset_release", "labeling_failures"},
            )
            reference = resolved.manifest.references["documentation/command_candidates"]
            manager = DataFrameIOManager(storage=spec.storage)
            with build_input_context(
                asset_key=AssetKey(["datasets", "documentation", "command_candidates"]),
                partition_key=spec.dataset_id,
                asset_partitions_def=DATASET_PARTITIONS,
                instance=instance,
            ) as context:
                manager.publish_reference(context, reference.model_copy(update={"digest": "0" * 64}))
                try:
                    with self.assertRaises(DatasetViewError) as changed:
                        ReviewSnapshots.from_environment(home, data).authoritative(spec.dataset_id)
                    self.assertEqual(changed.exception.code, "not_ready")
                finally:
                    manager.publish_reference(context, reference)
            self.assertNotIn("reviewed_annotations", resolved.manifest.references)
            label_stage = "proposed_annotations"
            metadata = TypeAdapter(dict[str, JsonValue]).validate_python(
                resolved.manifest.evidence["labeling_attempts"]
            )
            receipts = TypeAdapter(dict[str, str]).validate_python(metadata["receipts"])
            labeled = data.query(label_stage, query)
            assert isinstance(labeled, TableResponse)
            identities = [
                TypeAdapter(str).validate_python(row.values["candidate_id"]) for row in labeled.page.rows
            ]
            self.assertEqual(
                {path for path in receipts if path.startswith("label/") and path.endswith("/receipt.json")},
                {f"label/{sha256(identity.encode()).hexdigest()}/receipt.json" for identity in identities},
            )
            store = ObjectStore(spec.storage)
            scheduled: list[str] = []
            for path, uri in receipts.items():
                if path.startswith("ranked-packets/") and path.endswith("/label-packets.json"):
                    key = "/".join(uri.rsplit("/", 2)[-2:])
                    packet = TypeAdapter(dict[str, JsonValue]).validate_json(store.read(key))
                    for batch_ids in TypeAdapter(list[list[str]]).validate_python(packet["packets"]):
                        scheduled.extend(batch_ids)
            self.assertCountEqual(scheduled, identities)
            with HumanReviewStore(home).reader() as transaction:
                publication = transaction.last_publication(spec.dataset_id)
            assert publication is not None
            sidecar = store.read(f"{publication.bundle_reference}/review/reviewed.jsonl")
            exported = [json.loads(line) for line in sidecar.splitlines()]
            self.assertEqual(
                [set(row) & {"review", "review_confidence", "review_verdict"} for row in exported],
                [set(), set(), set(), set()],
            )
            with HumanReviewStore(home).reader() as transaction:
                batch = transaction.get_current_batch(spec.dataset_id, "annotations")
                self.assertEqual(batch.snapshot.candidate_ids, ())
                self.assertEqual(transaction.list_decisions(batch.batch_id), ())
            queue = data.read_current(spec.dataset_id, "human_review_queue").value
            assert isinstance(queue, pd.DataFrame)
            self.assertTrue(queue.empty)
            self.assertEqual(
                queue.attrs[REVIEW_PREPARATION],
                {"batch_id": batch.batch_id, "snapshot_id": batch.snapshot.snapshot_id},
            )
            before = len(instance.get_runs())
            self.assertEqual(NativeDatasetCycle(home).drain(instance, partition=spec.dataset_id), {})
            self.assertEqual(ReleaseRunner(home, instance)(token), publication)
            self.assertEqual(len(instance.get_runs()), before)
