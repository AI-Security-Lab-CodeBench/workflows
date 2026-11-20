import json
import os
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import ExitStack
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow
import pyarrow.parquet as parquet
from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetRecordsFilter,
    DagsterEvent,
    DagsterInstance,
    JobDefinition,
    JobExecutionResult,
    OutputContext,
    ReexecutionOptions,
    RetryRequested,
    build_sensor_context,
    execute_job,
    in_process_executor,
    reconstructable,
)
from dagster._core.storage.tags import PARTITION_NAME_TAG, RESUME_RETRY_TAG
from guarded_labeling_fixture import LabelingFixture
from native_dataset_cycle import NativeDatasetCycle
from pandera.errors import SchemaError, SchemaErrors
from pydantic import JsonValue, TypeAdapter
from test_batch_labeling import tool_reply
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules
from test_review_release import attest_fixture_release

from codebench_pipeline.admitted_tables import resolve_labeling_completion
from codebench_pipeline.batch_labeling import BatchExecution, SubmittedLabels
from codebench_pipeline.command_embeddings import encode_commands
from codebench_pipeline.config_assets import configured_selection
from codebench_pipeline.data_view_models import DatasetQueryRequest, TableResponse
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager, StorageConfig, frame_bytes, read_frame
from codebench_pipeline.dataframe_schemas import (
    FilteredCommandsTable,
    LabelingAttemptsTable,
    SourceDocumentsTable,
    frame_records,
    model_records,
    validated_frame,
)
from codebench_pipeline.dataset_config import PublicationConfig, require_guarded_annotation
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_tables import publication
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.finalization_table_io import FinalizationTableIO
from codebench_pipeline.grounding_tables import GuardedLabeling, LabelingEvidenceInputs
from codebench_pipeline.grounding_tools import GroundingCredentials
from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, label_attempt
from codebench_pipeline.labeling_tables import failures_from_attempts, proposed_from_attempts
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.repository_receipts import RepositoryRun
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import GuardedLabelingConfig


def unsupported_plan_reply(name: str, arguments: dict[str, JsonValue]) -> HTTPResponse:
    if name == "submit_labels":
        submitted = SubmittedLabels.model_validate(arguments)
        arguments = {
            "outcomes": [
                row.model_copy(
                    update={
                        "label": None,
                        "evidence_ids": [],
                        "grounding_quotes": {},
                        "grounding_rationale": "",
                        "failure_code": "grounding_missing",
                        "failure_detail": "Fixture provider did not establish the plan semantics.",
                    }
                ).model_dump(mode="json", by_alias=True)
                if row.label is not None and row.label.util == "terraform"
                else row.model_dump(mode="json", by_alias=True)
                for row in submitted.outcomes
            ]
        }
    return tool_reply(name, arguments)


def _labeling_job() -> JobDefinition:
    return (
        build_definitions(Path(os.environ["CODEBENCH_HOME"]))
        .resolve_job_def("build_dataset")
        .with_executor_def(in_process_executor)
    )


class LabelingTableTests(unittest.TestCase):
    def refresh(self, cycle: NativeDatasetCycle, instance: DagsterInstance, dataset_id: str) -> None:
        instance.add_dynamic_partitions("dataset_builds", [dataset_id])
        result = cycle.definitions.resolve_job_def("refresh_dataset_configuration").execute_in_process(
            instance=instance, partition_key=dataset_id
        )
        self.assertTrue(result.success)

    def test_native_labeling_retry_resumes_but_explicit_rematerialization_reinfers_all_rows(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        fixture = LabelingFixture(self, home)
        spec = fixture.spec
        batch = require_guarded_annotation(spec.annotation).model_copy(update={"batch_size": 1})
        spec = spec.model_copy(
            update={"annotation": spec.annotation.model_copy(update={"batch": batch, "workers": 1})}
        )
        DatasetRegistry(home).register(spec)
        cycle = NativeDatasetCycle(home)
        with dataset_instance(home) as instance:
            self.refresh(cycle, instance, spec.dataset_id)
            preparation = cycle.drain(instance, prepare_only=True, partition=spec.dataset_id)
            self.assertIn("filtered_commands", preparation)

        requests: list[str] = []
        interrupted = False
        step_retry = False

        def infer(
            url: str, body: bytes, key: str | None, timeout: float, capacity: CapacityConfig, path: Path
        ) -> HTTPResponse:
            nonlocal interrupted, step_retry
            messages = json.loads(body)["messages"]
            if len(messages) == 2:
                (request,) = json.loads(messages[1]["content"])["records"]
                requests.append(str(request["candidate"]["candidate_id"]))
                if len(requests) == 2 and step_retry:
                    step_retry = False
                    raise RetryRequested(max_retries=1)
                if len(requests) == 2 and not interrupted:
                    interrupted = True
                    raise CapacityUnavailable("Fixture pause after the first complete conversation")
            return fixture.infer(url, body, key, timeout, capacity, path)

        asset_key = AssetKey(["datasets", "labeling_attempts"])
        selection = configured_selection([asset_key])
        with (
            dataset_instance(home) as instance,
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=infer),
            patch.dict(os.environ, {"CODEBENCH_HOME": str(home)}),
        ):
            job = _labeling_job()
            native_job = reconstructable(_labeling_job)

            def retry_failed(run_id: str) -> JobExecutionResult:
                return execute_job(
                    native_job,
                    instance=instance,
                    reexecution_options=ReexecutionOptions.from_failure(run_id, instance),
                    tags={PARTITION_NAME_TAG: spec.dataset_id, RESUME_RETRY_TAG: "true"},
                    raise_on_error=False,
                )

            def materialize_labeling():
                return job.execute_in_process(
                    instance=instance,
                    partition_key=spec.dataset_id,
                    asset_selection=selection,
                    run_config=dataset_run_config(spec),
                    raise_on_error=False,
                )

            failed = materialize_labeling()
            self.assertFalse(failed.success)
            self.assertFalse(failed.asset_materializations_for_node("datasets__labeling_attempts"))
            first, later = requests
            requests.clear()
            fresh = materialize_labeling()
            self.assertTrue(fresh.success)
            self.assertEqual(requests[:2], [first, later])
            fresh_requests = list(requests)

            requests.clear()
            recovered = retry_failed(failed.run_id)
            self.assertTrue(recovered.success)
            self.assertNotEqual(recovered.run_id, failed.run_id)
            (recovered_event,) = recovered.asset_materializations_for_node("datasets__labeling_attempts")
            (failure,) = [
                item.asset_observation
                for item in instance.fetch_observations(
                    AssetRecordsFilter(asset_key=asset_key, asset_partitions=[spec.dataset_id]), limit=10
                ).records
                if item.run_id == failed.run_id and item.asset_observation is not None
            ]
            failed_attempt_id = failure.metadata["inference_attempt"].value
            signature = recovered_event.metadata["input_fingerprint"].value
            prefix = f"{spec.dataset_id}/labeling_attempts/{signature}/{failed_attempt_id}"
            storage = DataFrameIOManager(storage=DatasetRegistry(home).storage(spec.dataset_id))
            retained = json.loads(storage.read_evidence(f"{prefix}/receipts.json"))
            self.assertTrue(retained)
            self.assertEqual(retained, failure.metadata["receipts"].value)
            for name in retained:
                self.assertTrue(storage.read_evidence(f"{prefix}/{name}"))
            self.assertEqual(
                json.loads(storage.read_evidence(f"{prefix}/failure.json")),
                {"kind": "failed_step", "attempt": failed_attempt_id},
            )
            self.assertFalse(storage.exists(f"{prefix}/evidence-index.json"))
            self.assertEqual(requests, fresh_requests[1:])
            requested = {key for request in cycle.tick(instance) for key in request.asset_selection or ()}
            self.assertIn(AssetKey(["datasets", "proposed_annotations"]), requested)
            self.assertNotIn(asset_key, requested)

            candidates = set(fresh_requests)
            requests.clear()
            explicit = execute_job(
                native_job,
                instance=instance,
                reexecution_options=ReexecutionOptions(failed.run_id),
                tags={PARTITION_NAME_TAG: spec.dataset_id},
                raise_on_error=False,
            )
            self.assertTrue(explicit.success)
            self.assertCountEqual(requests, candidates)
            (event,) = explicit.asset_materializations_for_node("datasets__labeling_attempts")
            self.assertFalse(event.metadata["cache_hit"].value)

            def interrupt_publication(
                _manager: FinalizationTableIO, context: OutputContext, _table: pd.DataFrame
            ) -> Iterator[DagsterEvent]:
                yield DagsterEvent.asset_materialization(
                    context.step_context,
                    AssetMaterialization(
                        asset_key=context.asset_key,
                        partition=context.asset_partition_key,
                        metadata=event.metadata,
                    ),
                )
                raise RuntimeError("Fixture interruption after a draft materialization")

            requests.clear()
            with patch.object(FinalizationTableIO, "handle_output", new=interrupt_publication):
                draft = materialize_labeling()
            self.assertFalse(draft.success)
            self.assertCountEqual(requests, candidates)
            (raw,) = instance.fetch_materializations(
                AssetRecordsFilter(asset_key=asset_key, asset_partitions=[spec.dataset_id]), limit=1
            ).records
            self.assertEqual(raw.run_id, draft.run_id)
            completion = resolve_labeling_completion(instance, asset_key, spec.dataset_id)
            assert completion is not None
            record, reference = completion
            self.assertEqual(record.run_id, explicit.run_id)
            self.assertEqual(reference.digest, event.metadata["codebench/content_digest"].value)

            requests.clear()
            recovered_publication = retry_failed(draft.run_id)
            self.assertTrue(recovered_publication.success)
            self.assertEqual(requests, [])

            step_retry = True
            retried_in_place = materialize_labeling()
            self.assertTrue(retried_in_place.success)
            self.assertEqual(requests, [first, later, *fresh_requests[1:]])

    def test_pipeline_uses_bound_grounding_credentials_without_reload_or_retaining_echo(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        fixture = LabelingFixture(self, home)
        key = "synthetic-grounding-secret"
        selector = "FIXTURE_GROUNDING_SELECTOR"
        batch = require_guarded_annotation(fixture.spec.annotation)
        grounding = batch.grounding.model_copy(update={"context7_api_key_env": selector})
        annotation = fixture.spec.annotation.model_copy(
            update={"batch": batch.model_copy(update={"grounding": grounding})}
        )
        spec = fixture.spec.model_copy(update={"annotation": annotation})
        dataset_id = DatasetRegistry(home).register(spec)
        echoed = False

        def infer(
            url: str,
            body: bytes,
            model_key: str | None,
            timeout: float,
            capacity: CapacityConfig,
            path: Path,
        ) -> HTTPResponse:
            nonlocal echoed
            self.assertNotIn(key.encode(), body, "Grounding credential reached the model request")
            if not echoed:
                echoed = True
                stack.enter_context(patch.dict("os.environ", {selector: ""}))
                return tool_reply("unexpected_tool", {"echo": key})
            return fixture.infer(url, body, model_key, timeout, capacity, path)

        with (
            ExitStack() as stack,
            patch.dict("os.environ", {selector: key}),
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=infer),
            patch.object(
                GroundingCredentials, "from_environment", side_effect=AssertionError("Ambient fallback")
            ),
        ):
            cycle = NativeDatasetCycle(home)
            with dataset_instance(home) as instance:
                self.refresh(cycle, instance, dataset_id)
                materialized = cycle.drain(instance, partition=dataset_id)

        self.assertTrue(echoed)
        self.assertIn("labeling_attempts", materialized)
        with dataset_instance(home) as instance:
            attempts = DatasetDataService(home, instance).read_current(dataset_id, "labeling_attempts").value
        assert isinstance(attempts, pd.DataFrame)
        self.assertEqual(set(attempts["status"]), {"success"})
        for path in Path(spec.storage.local_root).rglob("*"):
            if path.is_file():
                self.assertFalse(
                    key.encode() in path.read_bytes(), f"Grounding credential retained in {path.name}"
                )

    def test_materialized_failure_is_persisted_but_excluded_from_proposals_and_release(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            fixture = LabelingFixture(self, home, limit_per_risk=4)
            spec = fixture.spec
            DatasetRegistry(home).register(spec)
            with (
                patch("guarded_labeling_fixture.tool_reply", side_effect=unsupported_plan_reply),
                dataset_instance(home) as instance,
            ):
                cycle = NativeDatasetCycle(home)
                self.refresh(cycle, instance, spec.dataset_id)
                built = cycle.drain(instance, partition=spec.dataset_id)
                for stage in ("labeling_failures", "proposed_annotations"):
                    self.assertIn(stage, built)
                self.assertNotIn("dataset_release", built)
                inference_calls = fixture.inference.call_count
                sensor = cycle.definitions.resolve_sensor_def("dataset_release_requests")
                with build_sensor_context(
                    instance=instance, repository_def=cycle.definitions.get_repository_def()
                ) as context:
                    data = DatasetDataService(home, instance)
                    query = DatasetQueryRequest(dataset_id=spec.dataset_id)
                    self.assertFalse(sensor.evaluate_tick(context).run_requests)
                    attest_fixture_release(home, instance, spec.dataset_id)
                    requests = sensor.evaluate_tick(context).run_requests
                    assert requests is not None
                    (request,) = requests
                    self.assertEqual(request.partition_key, spec.dataset_id)
                    result = cycle.definitions.resolve_job_def("release_dataset").execute_in_process(
                        instance=instance,
                        partition_key=request.partition_key,
                        run_config=request.run_config,
                        tags=request.tags,
                    )
                    self.assertTrue(result.success)
                    self.assertFalse(sensor.evaluate_tick(context).run_requests)
                    failures = data.query("labeling_failures", query)
                    proposals = data.query("proposed_annotations", query)
                    released = data.query("dataset_release", query)
                    assert isinstance(failures, TableResponse)
                    assert isinstance(proposals, TableResponse)
                    assert isinstance(released, TableResponse)
                    self.assertEqual(failures.page.total_rows, 1)
                    self.assertEqual(
                        [
                            (row.values["command"], row.values["failure_code"], row.values["label"])
                            for row in failures.page.rows
                        ],
                        [("terraform plan", "grounding_missing", None)],
                    )
                    expected = {"docker compose ps", "docker ps --all", "kubectl get pods"}
                    self.assertEqual(proposals.page.total_rows, 3)
                    self.assertEqual({str(row.values["command"]) for row in proposals.page.rows}, expected)
                    self.assertEqual(released.publication_state, "published")
                    self.assertEqual(released.page.total_rows, 3)
                    self.assertEqual(
                        {
                            (str(row.values["command"]), str(row.values["human_reviewed"]))
                            for row in released.page.rows
                        },
                        {(command, "false") for command in expected},
                    )
                    capture_value = data.read_current(
                        spec.dataset_id, "documentation/capture_documentation"
                    ).value
                    attempts_value = data.read_current(spec.dataset_id, "labeling_attempts").value
                    commands_value = data.read_current(spec.dataset_id, "filtered_commands").value
                    assert isinstance(capture_value, pd.DataFrame)
                    assert isinstance(attempts_value, pd.DataFrame)
                    assert isinstance(commands_value, pd.DataFrame)
                    capture = SourceDocumentsTable.validate(capture_value)
                    attempts = LabelingAttemptsTable.validate(attempts_value)
                    self.assertCountEqual(fixture.remote_rows, attempts["candidate_id"].tolist())
                    commands = FilteredCommandsTable.validate(commands_value)
                    mapped = GuardedLabelingConfig.from_spec(spec)
                    scoped_batch = mapped.labeling_attempts.batch
                    capacity = require_guarded_annotation(spec.annotation).capacity
                    evidence = LabelingEvidenceInputs(
                        RepositoryRun(home, spec.dataset_id, "labeling-replay"),
                        mapped.collection,
                        scoped_batch.grounding,
                        capture,
                    )
                    evidence.verify(attempts)
                    labeling = GuardedLabeling(
                        mapped,
                        evidence,
                        scoped_batch.bind_capacity(capacity.state_directory, capacity.expected_model_path),
                        BatchExecution(None, encode_commands),
                    )
                    self.assertEqual(fixture.inference.call_count, inference_calls)
                    before_replay = len(fixture.remote_rows)
                    replay = labeling.attempts(
                        FilteredCommandsTable.validate(commands.loc[commands["command"].isin(expected)]),
                        home / "labeling-replay",
                    )
                    evidence.verify(replay)
                    original_rows = {
                        row.candidate_id: row
                        for row in model_records(
                            attempts.loc[attempts["status"] == "success"], LabelingAttempt
                        )
                    }
                    replay_rows = {row.candidate_id: row for row in model_records(replay, LabelingAttempt)}
                    self.assertCountEqual(fixture.remote_rows[before_replay:], original_rows)
                    self.assertEqual(replay_rows.keys(), original_rows.keys())
                    for candidate_id, original in original_rows.items():
                        repeated = replay_rows[candidate_id]
                        self.assertNotEqual(repeated.row_input_fingerprint, original.row_input_fingerprint)
                        self.assertEqual(
                            repeated.model_dump(exclude={"row_input_fingerprint"}),
                            original.model_dump(exclude={"row_input_fingerprint"}),
                        )
                    inference_calls = fixture.inference.call_count
                self.assertEqual(cycle.tick(instance), ())
                self.assertEqual(fixture.inference.call_count, inference_calls)

    def test_failed_row_is_inspectable_but_cannot_become_a_released_command(self) -> None:
        rules = make_rules()
        valid, unsupported = make_candidate("valid"), make_candidate("unsupported")
        attempts = [
            label_attempt(valid, make_label(valid, rules), documentation(), "a" * 64, "label/valid"),
            failed_attempt(
                unsupported, "grounding_missing", "No matching evidence.", [], "b" * 64, "label/failed"
            ),
        ]
        table = validated_frame(
            [
                {**attempt.model_dump(mode="json"), "source_weight": weight}
                for attempt, weight in zip(attempts, [0.7, 0.2], strict=True)
            ],
            LabelingAttemptsTable,
        )
        proposals = proposed_from_attempts(table, rules)
        failures = failures_from_attempts(table)
        released = publication(proposals, rules, PublicationConfig())
        self.assertEqual(proposals["candidate_id"].tolist(), ["valid"])
        self.assertEqual(failures["candidate_id"].tolist(), ["unsupported"])
        self.assertEqual(frame_records(failures)[0]["command"], unsupported.command)
        self.assertEqual(proposals["source_weight"].tolist(), [0.7])
        self.assertEqual(failures["source_weight"].tolist(), [0.2])
        self.assertEqual(len(released.commands), 1)
        self.assertEqual(proposals.attrs["row_documentation"]["valid"][0]["url"], documentation()[0].url)

    def test_all_failed_attempts_preserve_empty_success_schema_and_failure_reasons(self) -> None:
        candidate = make_candidate("unsupported")
        attempt = failed_attempt(
            candidate, "tool_budget_exhausted", "Still unsupported.", [], "a" * 64, "label/1"
        )
        table = validated_frame([attempt.model_dump(mode="json")], LabelingAttemptsTable)
        proposals = proposed_from_attempts(table, make_rules())
        failures = failures_from_attempts(table)
        self.assertEqual(proposals["candidate_id"].tolist(), [])
        self.assertIn("class", proposals.columns)
        self.assertEqual(failures["failure_code"].tolist(), ["tool_budget_exhausted"])
        self.assertEqual(failures["label"].tolist(), [None])

    def test_nested_attempt_types_survive_parquet_with_no_successes_or_rows(self) -> None:
        candidate = make_candidate("one")
        docs = documentation()
        success = label_attempt(candidate, make_label(candidate, make_rules()), docs, "a" * 64, "label/1")
        failure = failed_attempt(candidate, "missing", "No docs.", [], "b" * 64, "label/2")
        for attempts in ([success], [failure], []):
            with self.subTest(attempts=attempts):
                frame = validated_frame(
                    [attempt.model_dump(mode="json") for attempt in attempts], LabelingAttemptsTable
                )
                frame.attrs["provenance"] = {"fixture": "labeling"}
                payload = frame_bytes(frame)
                physical = parquet.read_schema(BytesIO(payload))
                self.assertIsInstance(physical.field("label").type, pyarrow.StructType)
                excerpts = physical.field("documentation").type
                assert isinstance(excerpts, pyarrow.ListType)
                self.assertIsInstance(excerpts.value_type, pyarrow.StructType)
                restored = read_frame(payload)
                self.assertEqual(
                    model_records(LabelingAttemptsTable.validate(restored), LabelingAttempt), attempts
                )
                self.assertEqual(restored.attrs, frame.attrs)
                self.assertEqual(frame_bytes(restored), payload)

    def test_historical_attempt_is_typed_on_read_without_rewriting_stored_evidence(self) -> None:
        candidate = make_candidate("one")
        docs = documentation()
        label = make_label(candidate, make_rules())
        expected = label_attempt(candidate, label, docs, "a" * 64, "label/1")
        historical = pd.DataFrame(
            [
                {
                    **expected.model_dump(mode="json", exclude={"label", "documentation"}),
                    "label_json": label.model_dump_json(by_alias=True),
                    "documentation_json": TypeAdapter(list[SemanticSnippet]).dump_json(docs).decode(),
                    "source_weight": 0.123456789012345,
                }
            ]
        )
        historical.attrs["provenance"] = {"receipt": "original"}
        with tempfile.TemporaryDirectory() as root:
            manager = DataFrameIOManager(storage=StorageConfig(local_root=root))
            reference = manager.persist_value(historical)
            store = ObjectStore(manager.storage)
            key = f"objects/{reference.digest}.parquet"
            original = store.read(key)
            restored = manager.read_version(reference)
            assert isinstance(restored, pd.DataFrame)
            self.assertEqual(model_records(restored, LabelingAttempt), [expected])
            self.assertEqual(restored["source_weight"].tolist(), [0.123456789012345])
            self.assertEqual(restored.attrs, historical.attrs)
            self.assertEqual(store.read(key), original)
            self.assertIn("documentation_json", read_frame(original))

    def test_attempt_table_rejects_unknown_fields_before_storage_conversion(self) -> None:
        candidate = make_candidate("one")
        label, docs = make_label(candidate, make_rules()), documentation()
        attempt = label_attempt(candidate, label, docs, "a" * 64, "label/1")
        row = attempt.model_dump(mode="json")
        invalid_rows: tuple[dict[str, JsonValue], ...] = (
            {**row, "unexpected": True},
            {**row, "label": {**label.model_dump(mode="json"), "unexpected": True}},
            {**row, "documentation": [{**docs[0].model_dump(mode="json"), "unexpected": True}]},
        )
        for invalid in invalid_rows:
            with self.subTest(invalid=invalid), self.assertRaises((SchemaError, SchemaErrors)):
                validated_frame([invalid], LabelingAttemptsTable)
