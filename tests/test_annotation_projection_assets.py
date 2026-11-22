import ast
import hashlib
import json
import unittest
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
from traceback import extract_stack
from unittest.mock import MagicMock, patch

import httpx
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetRecordsFilter,
    DagsterInstance,
    ExecuteInProcessResult,
    IOManagerDefinition,
    OutputContext,
    RetryRequested,
    asset,
    materialize,
)
from mcp import ClientSession
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter, ValidationError
from test_batch_labeling import (
    QUOTE,
    URL,
    batch_config,
    live_mcp_fixture,
    remote_model,
    submitted,
    tool_replies,
    tool_reply,
    unsupported,
)
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.batch_labeling import (
    BatchExecution,
    LabelingGeneration,
    prepare_batch_rows,
    run_prepared_batches,
)
from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    RegisteredConfig,
    configuration_assets,
    mapping_key,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataframe_schemas import LabelingAttemptsTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.dataset_assets_annotations import annotation_assets
from codebench_pipeline.dataset_assets_sources import category_rules
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_engine import implementation_signature
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded
from codebench_pipeline.finalization_publication import TableOutputAdmission
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO
from codebench_pipeline.finalization_table_io import finalization_table_io
from codebench_pipeline.grounding_tools import GroundingCredentials
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.keenable_mcp import SearchHit, SearchResult
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.labeling_outcomes import LabelingAttempt, failed_attempt, label_attempt
from codebench_pipeline.labeling_stage_settings import ProposalRules
from codebench_pipeline.labeling_tables import failures_from_attempts, proposed_from_attempts
from codebench_pipeline.object_store import ObjectStore
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.stage_implementation import ImplementationSource, processing_versions
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import TypedConfig

PROPOSED = AssetKey(["datasets", "proposed_annotations"])
FAILURES = AssetKey(["datasets", "labeling_failures"])
PROJECTIONS = {PROPOSED, FAILURES}
SOURCE = "codebench_pipeline/dataset_assets_annotations.py"
LEGACY_WRAPPERS = """\
def proposed_annotations(
    context: AssetExecutionContext,
    labeling_attempts: DataFrame[LabelingAttemptsTable],
    category_rules: CategoryRules,
    registered: RegisteredConfig,
    dataset_io_manager: ResourceParam[DataFrameIOManager],
) -> Output[DataFrame[ProposedAnnotationsTable]]:
    require_registered_rules(registered, category_rules)
    return cached_table(
        context,
        dataset_io_manager,
        TypedConfig(),
        {"attempts": labeling_attempts, "category_rules": category_rules},
        ProposedAnnotationsTable,
        lambda evidence: proposed_from_attempts(labeling_attempts, category_rules),
        registered=registered,
    )

def labeling_failures(
    context: AssetExecutionContext,
    labeling_attempts: DataFrame[LabelingAttemptsTable],
    registered: RegisteredConfig,
    dataset_io_manager: ResourceParam[DataFrameIOManager],
) -> Output[DataFrame[LabelingFailuresTable]]:
    return cached_table(
        context,
        dataset_io_manager,
        TypedConfig(),
        {"attempts": labeling_attempts},
        LabelingFailuresTable,
        lambda evidence: failures_from_attempts(labeling_attempts),
        registered=registered,
    )
"""


class AnnotationProjectionAssetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(self.home).model_copy(update={"rules": make_rules()})
        self.registered = RegisteredConfig.from_spec(spec)
        self.partition = spec.dataset_id
        self.instance = self.enterContext(DagsterInstance.local_temp(str(self.home)))
        self.instance.add_dynamic_partitions("dataset_builds", [self.partition])
        self.io = RegisteredDatasetTableIO(storage=spec.storage, review_home=str(self.home))
        self.io.bind_review_store(MagicMock(spec=HumanReviewStore))
        binding = {"dataset": {"state": "configured", "digest": "0" * 64}}
        self.enterContext(
            patch("codebench_pipeline.dataset_collection_binding.finalization_binding", return_value=binding)
        )

        def run_configuration(_run_id: str, _dataset_id: str) -> RegisteredConfig:
            return self.registered

        self.enterContext(
            patch.object(DataFrameIOManager, "run_configuration", side_effect=run_configuration)
        )
        self.enterContext(
            patch(
                "codebench_pipeline.review_output_state.registered_materialization_stale", return_value=False
            )
        )
        rules = self.registered.rules
        first, second = (make_candidate(name, rules.risks[0].id) for name in ("supported", "unsupported"))
        outcomes = (
            label_attempt(first, make_label(first, rules), documentation(), "a" * 64, "label/supported"),
            failed_attempt(second, "grounding_missing", "No proof.", [], "b" * 64, "label/unsupported"),
        )
        self.attempts = validated_frame(
            [row.model_dump(mode="json") for row in outcomes], LabelingAttemptsTable
        )
        self.attempts.attrs[FINALIZATION_BINDING] = binding

    def materialize_projections(self) -> ExecuteInProcessResult:
        @asset(key=GLOBAL_CONFIG, partitions_def=DATASET_PARTITIONS, io_manager_key="config_run_io_manager")
        def global_config() -> RegisteredConfig:
            return self.registered

        @asset(
            key_prefix=["datasets"],
            partitions_def=DATASET_PARTITIONS,
            io_manager_key="dataset_io_manager",
            dagster_type=TABLE_TYPES[LabelingAttemptsTable],
        )
        def labeling_attempts() -> DataFrame[LabelingAttemptsTable]:
            return self.attempts

        proposed_config = mapping_key("proposed_annotations")
        mapped = next(item for item in configuration_assets(self.home) if item.key == proposed_config)
        projections = [definition for definition in annotation_assets() if definition.key in PROJECTIONS]
        return materialize(
            [global_config, category_rules, mapped, labeling_attempts, *projections],
            resources={
                "config_run_io_manager": ConfigRunIOManager(home=str(self.home)),
                "dataset_io_manager": IOManagerDefinition(resource_fn=lambda _: self.io),
                FINALIZATION_TABLE_IO: finalization_table_io,
            },
            instance=self.instance,
            partition_key=self.partition,
        )

    def test_terminal_provider_failure_projects_but_a_later_grounded_label_does_not(self) -> None:
        obj = TypeAdapter(dict[str, JsonValue])
        objects = TypeAdapter(list[dict[str, JsonValue]])
        string = TypeAdapter(str)
        search_calls: Counter[str] = Counter()

        def reply(_url: str, body: bytes, *_: object) -> HTTPResponse:
            messages = objects.validate_python(obj.validate_json(body)["messages"])
            row = obj.validate_json(string.validate_python(messages[1]["content"]))
            observations = [
                obj.validate_json(string.validate_python(message["content"]))
                for message in messages
                if message["role"] == "tool"
            ]
            identity = string.validate_python(obj.validate_python(row["candidate"])["candidate_id"])
            if not observations:
                return tool_reply("query", {"query": "manual", "candidate_ids": ["wrong-row"]})
            self.assertEqual(observations[0]["error"], "invalid_candidate_scope")
            if len(observations) == 1:
                return tool_replies(
                    ("query", {"query": f"terminal {identity}", "candidate_ids": [identity]}),
                    ("query", {"query": f"discovery {identity}", "candidate_ids": [identity]}),
                )
            self.assertEqual(observations[1]["error"], "provider_http_status")
            self.assertNotIn("status", observations[1])
            self.assertNotIn("provider_error", observations[1])
            self.assertNotIn("error", observations[2])
            self.assertNotIn(b"private provider response", body)
            self.assertNotIn(b"retry_after_seconds", body)
            if identity == "unresolved":
                self.assertEqual(observations[2]["discovery"], [])
                return tool_reply("finalize", {"outcome": unsupported(row)})
            if len(observations) == 3:
                self.assertEqual(
                    observations[2]["discovery"], [{"url": URL, "snippet": "Operation documentation"}]
                )
                return tool_reply("page_fetch", {"url": URL, "candidate_ids": [identity]})
            page = obj.validate_python(observations[3]["page"])
            evidence_id = string.validate_python(obj.validate_python(page["evidence_ids"])[identity])
            return tool_reply("finalize", {"outcome": submitted(row, [evidence_id])})

        async def search(_session: ClientSession, query: str, _limits: tuple[int, int]) -> SearchResult:
            search_calls[query] += 1
            if query == "discovery unresolved":
                return SearchResult(results=())
            if query == "discovery recovered" and search_calls[query] > 1:
                return SearchResult(results=(SearchHit(url=URL, snippet="Operation documentation"),))
            status = 429 if query.startswith("terminal ") else 500
            request = httpx.Request("POST", "https://example.org/provider")
            response = httpx.Response(status, request=request, headers={"Retry-After": "0"})
            raise httpx.HTTPStatusError("private provider response", request=request, response=response)

        rules = self.registered.rules
        candidates = [make_candidate(identity, rules.risks[0].id) for identity in ("unresolved", "recovered")]
        scratch = self.home / "synthetic-labeling"
        config = batch_config(scratch).model_copy(
            update={
                "inference_mode": "plain",
                "capacity": None,
                "maximum_grounding_turns": 3,
                "row_concurrency": 1,
            }
        )
        model = remote_model()
        execution = BatchExecution(None, GroundingCredentials("fixture-projection-key"))
        generation = LabelingGeneration(
            LabelingCheckpointCache(self.home / "cache", self.home / "operations"), "a" * 64
        )
        prepared = prepare_batch_rows(candidates, rules, model, config, implementation="fixture-projection")

        def label_rows(rows: list[CandidateRecord], out: Path) -> list[LabelingAttempt]:
            return run_prepared_batches(
                rows, prepared, rules, model, config, out, execution, 1_048_576, generation
            )

        with (
            live_mcp_fixture({URL: QUOTE}),
            patch("codebench_pipeline.live_grounding.search_snippets", side_effect=search),
            patch("codebench_pipeline.batch_labeling.plain_complete", side_effect=reply) as infer,
        ):
            outcomes = label_rows(candidates, scratch)
            infer.reset_mock()
            resumed = self.home / "resumed"
            self.assertEqual(label_rows(candidates[1:], resumed), outcomes[1:])
            infer.assert_not_called()
        self.assertEqual(
            search_calls,
            {
                "terminal unresolved": 4,
                "terminal recovered": 4,
                "discovery unresolved": 1,
                "discovery recovered": 2,
            },
        )
        incident_path = (scratch / outcomes[1].attempt_evidence_reference).with_name(
            "provider-incidents.json"
        )
        incidents = json.loads(incident_path.read_bytes())
        self.assertEqual(list(incidents), ["2"])
        self.assertEqual(
            [(item["tool"], item["attempt"], item["error"]["status"]) for item in incidents["2"]],
            [("search", 1, 500)],
        )
        self.assertEqual(
            (resumed / incident_path.relative_to(scratch)).read_bytes(), incident_path.read_bytes()
        )
        for outcome in outcomes:
            receipt_path = (scratch / outcome.attempt_evidence_reference).with_name("grounding.json")
            receipts = json.loads(receipt_path.read_bytes())["provider_receipts"]
            self.assertEqual(
                [receipt["error"] for receipt in receipts[:2]],
                ["invalid_candidate_scope", "provider_http_status"],
            )
            self.assertEqual(
                receipts[1]["provider_error"],
                {"code": "provider_http_status", "status": 429, "retry_after_seconds": 0},
            )
        binding = self.attempts.attrs[FINALIZATION_BINDING]
        self.attempts = validated_frame(
            [outcome.model_dump(mode="json") for outcome in outcomes], LabelingAttemptsTable
        )
        self.attempts.attrs[FINALIZATION_BINDING] = binding
        run = self.materialize_projections()
        failures = run.output_for_node(FAILURES.to_python_identifier())
        self.assertEqual(failures["candidate_id"].tolist(), ["unresolved"])
        self.assertEqual(failures["failure_code"].tolist(), ["provider_http_status"])
        self.assertEqual(
            failures["attempt_evidence_reference"].tolist(), [outcomes[0].attempt_evidence_reference]
        )
        self.assertEqual(
            run.output_for_node(PROPOSED.to_python_identifier())["candidate_id"].tolist(), ["recovered"]
        )

    def test_projection_compute_avoids_storage_and_cache_hits_keep_receipts(self) -> None:
        original_read, original_write = ObjectStore.read, ObjectStore.write

        def reject_compute_access() -> None:
            if any(
                Path(frame.filename).name == Path(SOURCE).name
                and frame.name in {"proposed_annotations", "labeling_failures"}
                for frame in extract_stack()
            ):
                self.fail("Annotation projection compute reached durable storage")

        def read(store: ObjectStore, key: str) -> bytes:
            reject_compute_access()
            return original_read(store, key)

        def write(store: ObjectStore, key: str, data: bytes, *, replace: bool = False) -> str:
            reject_compute_access()
            return original_write(store, key, data, replace=replace)

        with (
            patch.object(ObjectStore, "read", read),
            patch.object(ObjectStore, "write", write),
            patch(
                "codebench_pipeline.dataset_assets_annotations.proposed_from_attempts",
                wraps=proposed_from_attempts,
            ) as proposed,
            patch(
                "codebench_pipeline.dataset_assets_annotations.failures_from_attempts",
                wraps=failures_from_attempts,
            ) as failures,
        ):
            first = self.materialize_projections()
            second = self.materialize_projections()
        proposed.assert_called_once()
        failures.assert_called_once()
        for key, expected in ((PROPOSED, "supported"), (FAILURES, "unsupported")):
            node = key.to_python_identifier()
            self.assertEqual(first.output_for_node(node)["candidate_id"].tolist(), [expected])
            new = first.asset_materializations_for_node(node)[0].metadata
            reused = second.asset_materializations_for_node(node)[0].metadata
            self.assertIs(new["cache_hit"].value, False)
            self.assertIs(reused["cache_hit"].value, True)
            self.assertEqual(reused["evidence"], new["evidence"])
            index = TypeAdapter(dict[str, JsonValue]).validate_python(new["evidence"].value)
            cache_key = f"{self.partition}/{key.path[-1]}/{new['input_fingerprint'].value}"
            self.assertEqual(index["receipts"], {})
            self.assertEqual(self.io.read_evidence(f"{cache_key}/{index['attempt']}/receipts.json"), b"{}")
        self.assertFalse(
            any(event.asset_key in PROJECTIONS for event in second.all_events if event.is_asset_observation)
        )

    def test_proposal_cache_tracks_routing_but_not_labeling_prose(self) -> None:
        node = PROPOSED.to_python_identifier()
        first = self.materialize_projections()
        original = first.asset_materializations_for_node(node)[0].metadata
        rules = self.registered.rules
        prose = rules.model_copy(update={"policy": "Revised instructions for inference only."})
        self.registered = self.registered.model_copy(update={"rules": prose})
        reused = self.materialize_projections()
        cached = reused.asset_materializations_for_node(node)[0].metadata
        self.assertIs(cached["cache_hit"].value, True)
        self.assertEqual(cached["input_fingerprint"], original["input_fingerprint"])
        self.assertEqual(cached["evidence"], original["evidence"])

        risk = rules.risks[0]
        rerouted = risk.model_copy(update={"command_patterns": [*risk.command_patterns, r"^probe other$"]})
        changed = prose.model_copy(update={"risks": [rerouted, *rules.risks[1:]]})
        self.registered = self.registered.model_copy(update={"rules": changed})
        rebuilt = self.materialize_projections()
        revised = rebuilt.asset_materializations_for_node(node)[0].metadata
        self.assertIs(revised["cache_hit"].value, False)
        self.assertNotEqual(revised["input_fingerprint"], original["input_fingerprint"])
        self.assertEqual(frame_bytes(rebuilt.output_for_node(node)), frame_bytes(first.output_for_node(node)))

    def test_proposal_mapping_rejects_inference_policy_in_persisted_payload(self) -> None:
        rules = self.registered.rules
        scope = ProposalRules.from_rules(rules)
        payload = {**scope.model_dump(mode="json"), "policy": rules.policy}
        with self.assertRaises(ValidationError) as rejected:
            ProposalRules.model_validate_json(json.dumps(payload), strict=True)
        self.assertEqual(
            [(error["loc"], error["type"]) for error in rejected.exception.errors()],
            [(("policy",), "extra_forbidden")],
        )

    def retain_legacy_projections(self) -> dict[AssetKey, tuple[str, str, bytes]]:
        source = ImplementationSource()
        module = ast.parse(source.read(SOURCE))
        factory = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == "annotation_assets"
        )
        wrappers = {
            node.name: node for node in ast.parse(LEGACY_WRAPPERS).body if isinstance(node, ast.FunctionDef)
        }
        factory.body = [
            wrappers.get(node.name, node) if isinstance(node, ast.FunctionDef) else node
            for node in factory.body
        ]
        module.body.extend(
            ast.parse(
                "from codebench_pipeline.config_assets import require_registered_rules\nfrom codebench_pipeline.typed_config import TypedConfig"
            ).body
        )
        old_source = ast.unparse(module).encode()
        config = TypedConfig()
        old: dict[AssetKey, tuple[str, str, bytes]] = {}
        for key in (PROPOSED, FAILURES):
            stage = key.path[-1]
            rules = self.registered.rules
            inputs = (
                {"attempts": self.attempts, "category_rules": rules}
                if key == PROPOSED
                else {"attempts": self.attempts}
            )
            table = (
                proposed_from_attempts(self.attempts, ProposalRules.from_rules(rules))
                if key == PROPOSED
                else failures_from_attempts(self.attempts)
            )
            retained = {SOURCE: old_source}

            def read(name: str) -> bytes:
                if name not in retained:
                    retained[name] = source.read(name)
                return retained[name]

            scoped = processing_versions(stage, read)
            for name in (
                "codebench_pipeline/dataset_engine.py",
                "codebench_pipeline/stage_implementation.py",
            ):
                scoped[name] = hashlib.sha256(read(name)).hexdigest()
            implementation = {name: hashlib.sha256(payload).hexdigest() for name, payload in retained.items()}
            for name, payload in retained.items():
                self.io.write_evidence(f"implementation/{implementation[name]}/{name}", payload)
            signature = implementation_signature(stage, config, inputs, scoped)
            cache_key = f"{self.partition}/{stage}/{signature}"
            self.io.cached_frame(cache_key, lambda: table)
            index = {
                "attempt": "0" * 32,
                "receipts": {},
                "config": config.model_dump(mode="json"),
                "implementation": implementation,
                "stage_implementation": scoped,
            }
            self.io.write_evidence(f"{cache_key}/{'0' * 32}/receipts.json", b"{}")
            original_index = json.dumps(index, indent=2).encode()
            self.io.write_evidence(f"{cache_key}/evidence-index.json", original_index)
            self.io.write_evidence(f"{cache_key}/stage-config.json", config.model_dump_json().encode())
            self.instance.report_runless_asset_event(
                AssetMaterialization(
                    key,
                    partition=self.partition,
                    metadata={
                        "input_fingerprint": signature,
                        "evidence": index,
                        "codebench/content_digest": hashlib.sha256(frame_bytes(table)).hexdigest(),
                    },
                )
            )
            old[key] = (signature, cache_key, original_index)
        return old

    def test_historical_projection_entries_recompute_without_rewriting_receipt_bytes(self) -> None:
        old = self.retain_legacy_projections()
        run = self.materialize_projections()
        for key, (signature, cache_key, original_index) in old.items():
            metadata = run.asset_materializations_for_node(key.to_python_identifier())[0].metadata
            self.assertIs(metadata["cache_hit"].value, False)
            index = TypeAdapter(dict[str, JsonValue]).validate_python(metadata["evidence"].value)
            self.assertNotIn("reused_from", index)
            self.assertNotEqual(metadata["input_fingerprint"].value, signature)
            self.assertEqual(self.io.read_evidence(f"{cache_key}/evidence-index.json"), original_index)
            self.assertEqual(self.io.read_evidence(f"{cache_key}/{'0' * 32}/receipts.json"), b"{}")

    def test_current_cache_rejects_altered_content(self) -> None:
        run = self.materialize_projections()
        metadata = run.asset_materializations_for_node(PROPOSED.to_python_identifier())[0].metadata
        digest = TypeAdapter(str).validate_python(metadata["codebench/content_digest"].value)
        alternate = proposed_from_attempts(
            self.attempts.head(0), ProposalRules.from_rules(self.registered.rules)
        )
        self.io.object_store().write(f"objects/{digest}.parquet", frame_bytes(alternate), replace=True)

        with self.assertRaisesRegex(IntegrityError, "Asset reference content mismatch"):
            self.materialize_projections()

    def test_failed_output_publication_is_not_reused_on_retry(self) -> None:
        original = RegisteredDatasetTableIO.handle_output

        def fail_proposed(
            manager: RegisteredDatasetTableIO,
            context: OutputContext,
            value: pd.DataFrame | CategoryRules,
            *,
            admission: TableOutputAdmission | None = None,
        ) -> None:
            if context.asset_key == PROPOSED:
                raise IntegrityError("Synthetic publication rejection")
            original(manager, context, value, admission=admission)

        with (
            patch.object(RegisteredDatasetTableIO, "handle_output", fail_proposed),
            self.assertRaisesRegex(IntegrityError, "Synthetic publication rejection"),
        ):
            self.materialize_projections()
        retry = self.materialize_projections()
        metadata = retry.asset_materializations_for_node(PROPOSED.to_python_identifier())[0].metadata
        self.assertIs(metadata["cache_hit"].value, False)

    def test_successful_step_retry_does_not_admit_previous_attempt_materialization(self) -> None:
        key = AssetKey("attempt_lifecycle_fixture")

        @asset(key=key)
        def attempted(context: AssetExecutionContext) -> int:
            if context.retry_number == 0:
                context.log_event(AssetMaterialization(key, metadata={"draft": True}))
                raise RetryRequested(max_retries=1)
            return 1

        result = materialize([attempted], instance=self.instance)
        self.assertTrue(result.success)
        records = self.instance.fetch_materializations(AssetRecordsFilter(asset_key=key), limit=2).records
        admitted, draft = records
        self.assertEqual(admitted.event_log_entry.run_id, draft.event_log_entry.run_id)
        self.assertTrue(
            materialization_attempt_succeeded(self.instance, admitted, output_name="result", retry_number=1)
        )
        self.assertFalse(materialization_attempt_succeeded(self.instance, draft, output_name="result"))
        self.assertFalse(materialization_attempt_succeeded(self.instance, admitted, output_name="result"))
        self.assertFalse(
            materialization_attempt_succeeded(self.instance, admitted, output_name="other", retry_number=1)
        )
        self.instance.store_event(admitted.event_log_entry)
        self.assertFalse(
            materialization_attempt_succeeded(self.instance, admitted, output_name="result", retry_number=1)
        )

    def test_current_cache_rejects_altered_attempt_receipt(self) -> None:
        run = self.materialize_projections()
        metadata = run.asset_materializations_for_node(PROPOSED.to_python_identifier())[0].metadata
        index = TypeAdapter(dict[str, JsonValue]).validate_python(metadata["evidence"].value)
        key = f"{self.partition}/{PROPOSED.path[-1]}/{metadata['input_fingerprint'].value}"
        receipt = f"{key}/{index['attempt']}/receipts.json"
        self.io.object_store().write(
            f"evidence/{hashlib.sha256(receipt.encode()).hexdigest()}",
            b'{"unexpected":"alteration"}',
            replace=True,
        )
        with self.assertRaises(IntegrityError):
            self.materialize_projections()

    def test_current_cache_rejects_changed_evidence_index_bytes(self) -> None:
        run = self.materialize_projections()
        metadata = run.asset_materializations_for_node(PROPOSED.to_python_identifier())[0].metadata
        index = TypeAdapter(dict[str, JsonValue]).validate_python(metadata["evidence"].value)
        key = f"{self.partition}/{PROPOSED.path[-1]}/{metadata['input_fingerprint'].value}"
        address = f"{key}/{index['attempt']}/evidence-index.json"
        payload = self.io.read_evidence(address)
        self.io.object_store().write(
            f"evidence/{hashlib.sha256(address.encode()).hexdigest()}", payload + b"\n", replace=True
        )

        with self.assertRaisesRegex(IntegrityError, "evidence index digest differs"):
            self.materialize_projections()
