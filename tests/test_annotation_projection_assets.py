import ast
import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from traceback import extract_stack
from unittest.mock import MagicMock, patch

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
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import GLOBAL_CONFIG, ConfigRunIOManager, RegisteredConfig
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataframe_schemas import LabelingAttemptsTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.dataset_assets_annotations import annotation_assets
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_engine import implementation_signature
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded
from codebench_pipeline.finalization_publication import TableOutputAdmission
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO
from codebench_pipeline.finalization_table_io import finalization_table_io
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.labeling_outcomes import failed_attempt, label_attempt
from codebench_pipeline.labeling_tables import failures_from_attempts, proposed_from_attempts
from codebench_pipeline.object_store import ObjectStore
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
        self.enterContext(patch.object(DataFrameIOManager, "run_configuration", return_value=self.registered))
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

        @asset(
            key_prefix=["datasets"], partitions_def=DATASET_PARTITIONS, io_manager_key="dataset_io_manager"
        )
        def category_rules() -> CategoryRules:
            return self.registered.rules

        projections = [definition for definition in annotation_assets() if definition.key in PROJECTIONS]
        return materialize(
            [global_config, labeling_attempts, category_rules, *projections],
            resources={
                "config_run_io_manager": ConfigRunIOManager(home=str(self.home)),
                "dataset_io_manager": IOManagerDefinition(resource_fn=lambda _: self.io),
                FINALIZATION_TABLE_IO: finalization_table_io,
            },
            instance=self.instance,
            partition_key=self.partition,
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

    def retain_legacy_projections(
        self, *, corrupt_attempt: AssetKey | None = None
    ) -> dict[AssetKey, tuple[str, str, bytes]]:
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
                proposed_from_attempts(self.attempts, rules)
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
            self.io.write_evidence(
                f"{cache_key}/{'0' * 32}/receipts.json",
                b'{"unexpected":"alteration"}' if key == corrupt_attempt else b"{}",
            )
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

    def test_historical_projection_with_altered_receipt_is_not_reused(self) -> None:
        _, cache_key, _ = self.retain_legacy_projections(corrupt_attempt=PROPOSED)[PROPOSED]
        run = self.materialize_projections()
        metadata = run.asset_materializations_for_node(PROPOSED.to_python_identifier())[0].metadata
        self.assertIs(metadata["cache_hit"].value, False)
        self.assertEqual(
            self.io.read_evidence(f"{cache_key}/{'0' * 32}/receipts.json"), b'{"unexpected":"alteration"}'
        )

    def test_current_cache_rejects_altered_content(self) -> None:
        run = self.materialize_projections()
        metadata = run.asset_materializations_for_node(PROPOSED.to_python_identifier())[0].metadata
        digest = TypeAdapter(str).validate_python(metadata["codebench/content_digest"].value)
        alternate = proposed_from_attempts(self.attempts.head(0), self.registered.rules)
        self.io.object_store().write(f"objects/{digest}.parquet", frame_bytes(alternate), replace=True)

        with self.assertRaisesRegex(IntegrityError, "Asset reference content mismatch"):
            self.materialize_projections()

    def test_legacy_projection_misses_when_a_current_only_helper_changes(self) -> None:
        self.retain_legacy_projections()
        original_read = ImplementationSource.read
        helper = "codebench_pipeline/finalization_table_asset.py"
        before = (
            b"table = compute(loaded.attempts) if loaded.cached is None else schema.validate(loaded.cached)"
        )
        after = b"table = compute(loaded.attempts.head(0)) if loaded.cached is None else schema.validate(loaded.cached)"

        def read(source: ImplementationSource, name: str) -> bytes:
            payload = original_read(source, name)
            if name == helper:
                self.assertIn(before, payload)
                return payload.replace(before, after)
            return payload

        with patch.object(ImplementationSource, "read", read):
            run = self.materialize_projections()
        metadata = run.asset_materializations_for_node(PROPOSED.to_python_identifier())[0].metadata
        self.assertIs(metadata["cache_hit"].value, False)

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
