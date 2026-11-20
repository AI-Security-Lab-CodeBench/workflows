import ast
import hashlib
import json
import unittest
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest.mock import MagicMock, patch

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetRecordsFilter,
    DagsterInstance,
    ExecuteInProcessResult,
    IOManagerDefinition,
    asset,
    build_input_context,
    materialize,
)
from pydantic import JsonValue, TypeAdapter
from test_dataset_rules import make_candidate, make_label
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    RegisteredConfig,
    configuration_assets,
    mapping_key,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataframe_schemas import ProposedAnnotationsTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_config import DeduplicationPolicy
from codebench_pipeline.dataset_engine import implementation_signature
from codebench_pipeline.dataset_finalization import category_eligible_annotations, deduplicated_annotations
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.finalization_publication import (
    FINALIZATION_PUBLICATION_METADATA,
    FinalizationPublicationV1,
)
from codebench_pipeline.finalization_table_asset import (
    FINALIZATION_TABLE_IO,
    Annotations,
    FinalizationTable,
    finalization_table_assets,
)
from codebench_pipeline.finalization_table_io import finalization_table_io
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.stage_implementation import ImplementationSource, processing_versions
from codebench_pipeline.typed_config import TypedConfig

DEDUP = AssetKey(["datasets", "deduplicated_annotations"])
ELIGIBILITY = AssetKey(["datasets", "category_eligible_annotations"])
LEGACY_DEDUP = """\
def deduplicated_annotations(
    context: AssetExecutionContext,
    proposed_annotations: DataFrame[ProposedAnnotationsTable],
    registered: RegisteredConfig,
    dataset_io_manager: ResourceParam[DataFrameIOManager],
) -> Output[DataFrame[ProposedAnnotationsTable]]:
    def compute(evidence: Path) -> DataFrame[ProposedAnnotationsTable]:
        frame = deduplicate_annotations(proposed_annotations)
        (evidence / "duplicates.json").write_text(json.dumps(frame.attrs["duplicates"]), encoding="utf-8")
        return frame

    return cached_table(
        context,
        dataset_io_manager,
        TypedConfig(),
        {"annotations": proposed_annotations},
        ProposedAnnotationsTable,
        compute,
        registered=registered,
    )
"""
LEGACY_ELIGIBILITY = """\
def category_eligible_annotations(
    context: AssetExecutionContext,
    deduplicated_annotations: DataFrame[ProposedAnnotationsTable],
    category_rules: CategoryRules,
    registered: RegisteredConfig,
    dataset_io_manager: ResourceParam[DataFrameIOManager],
) -> Output[DataFrame[ProposedAnnotationsTable]]:
    require_registered_rules(registered, category_rules)

    def compute(evidence: Path) -> DataFrame[ProposedAnnotationsTable]:
        frame = filter_category(deduplicated_annotations, category_rules)
        (evidence / "category-exclusions.json").write_text(
            json.dumps(frame.attrs["category_exclusions"]), encoding="utf-8"
        )
        return frame

    return cached_table(
        context,
        dataset_io_manager,
        TypedConfig(),
        {"annotations": deduplicated_annotations, "category_rules": category_rules},
        ProposedAnnotationsTable,
        compute,
        registered=registered,
    )
"""


class FinalizationTableAssetTests(unittest.TestCase):
    def setUp(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        self.home = home
        spec = fixture_spec(home).model_copy(
            update={"deduplication": DeduplicationPolicy(normalize_safe_whitespace=False)}
        )
        self.registered = RegisteredConfig.from_spec(spec)
        self.partition = spec.dataset_id
        self.instance = self.enterContext(DagsterInstance.local_temp(str(home)))
        self.instance.add_dynamic_partitions("dataset_builds", [self.partition])
        self.io = RegisteredDatasetTableIO(storage=spec.storage, review_home=str(home))
        review = MagicMock(spec=HumanReviewStore)
        self.io.bind_review_store(review)
        self.binding = {"dataset": {"state": "configured", "digest": "0" * 64}}
        self.authorization = self.enterContext(
            patch(
                "codebench_pipeline.dataset_collection_binding.finalization_binding",
                return_value=self.binding,
            )
        )
        self.enterContext(patch.object(DataFrameIOManager, "run_configuration", return_value=self.registered))
        self.enterContext(
            patch(
                "codebench_pipeline.review_output_state.registered_materialization_stale", return_value=False
            )
        )
        rules = self.registered.rules
        self.input = validated_frame(
            [
                {
                    **candidate.model_dump(mode="json"),
                    **make_label(candidate, rules).model_dump(mode="json", by_alias=True),
                }
                for candidate in (make_candidate(name, rules.risks[0].id) for name in ("row-a", "row-b"))
            ],
            ProposedAnnotationsTable,
        )
        self.input.attrs[FINALIZATION_BINDING] = self.binding
        self.calls = 0
        self.eligibility_calls = 0
        self.fail_compute = False
        self.stale_on_publish = False

    def dedup(self, table: Annotations, policy: DeduplicationPolicy) -> FinalizationTable:
        self.calls += 1
        if self.fail_compute:
            raise ValueError("synthetic compute failure")
        result = deduplicated_annotations(table, policy)
        if self.stale_on_publish:
            self.authorization.return_value = {"dataset": {"state": "stale"}}
        return result

    def eligible(self, table: Annotations, category_id: str) -> FinalizationTable:
        self.eligibility_calls += 1
        return category_eligible_annotations(table, category_id)

    def run_tables(self, *, eligibility: bool = False) -> ExecuteInProcessResult:
        @asset(key=GLOBAL_CONFIG, partitions_def=DATASET_PARTITIONS, io_manager_key="config_run_io_manager")
        def global_config() -> RegisteredConfig:
            return self.registered

        @asset(
            key_prefix=["datasets"],
            partitions_def=DATASET_PARTITIONS,
            io_manager_key="dataset_io_manager",
            dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        )
        def proposed_annotations() -> Annotations:
            return self.input

        dedup, eligible = finalization_table_assets(self.dedup, self.eligible)
        tables = (dedup, eligible) if eligibility else (dedup,)
        mapping_keys = {mapping_key(table.key.path[-1]) for table in tables}
        return materialize(
            [
                global_config,
                proposed_annotations,
                *(item for item in configuration_assets(self.home) if item.key in mapping_keys),
                *tables,
            ],
            resources={
                "config_run_io_manager": ConfigRunIOManager(home=str(self.home)),
                "dataset_io_manager": IOManagerDefinition(resource_fn=lambda _: self.io),
                FINALIZATION_TABLE_IO: finalization_table_io,
            },
            instance=self.instance,
            partition_key=self.partition,
            raise_on_error=False,
        )

    def test_cache_hit_reuses_table_and_evidence_without_recomputing_or_recording_attempt(self) -> None:
        first = self.run_tables(eligibility=True)
        second = self.run_tables(eligibility=True)
        self.assertTrue(first.success)
        self.assertTrue(second.success)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.eligibility_calls, 1)
        for key, filename in ((DEDUP, "duplicates.json"), (ELIGIBILITY, "category-exclusions.json")):
            node = key.to_python_identifier()
            new = first.asset_materializations_for_node(node)[0].metadata
            reused = second.asset_materializations_for_node(node)[0].metadata
            self.assertIs(new["cache_hit"].value, False)
            self.assertIs(reused["cache_hit"].value, True)
            self.assertEqual(reused["evidence"], new["evidence"])
            index = TypeAdapter(dict[str, JsonValue]).validate_python(new["evidence"].value)
            evidence_key = f"{self.partition}/{key.path[-1]}/{new['input_fingerprint'].value}"
            evidence = self.io.read_evidence(f"{evidence_key}/{index['attempt']}/{filename}")
            receipts = TypeAdapter(dict[str, str]).validate_python(index["receipts"])
            self.assertTrue(receipts[filename])
            self.assertEqual(
                json.loads(evidence),
                []
                if key == ELIGIBILITY
                else [
                    {
                        "duplicate_id": "row-b",
                        "survivor_id": "row-a",
                        "normalized_command": "probe inspect example",
                        "reason": "exact_command_and_annotation",
                    }
                ],
            )
        self.assertFalse(
            any(
                event.asset_key in {DEDUP, ELIGIBILITY} and event.is_asset_observation
                for event in second.all_events
            )
        )

    def test_failed_compute_records_diagnostics_without_materializing(self) -> None:
        self.fail_compute = True
        failed = self.run_tables()
        self.assertFalse(failed.success)
        self.assertEqual(self.calls, 1)
        self.assertFalse(failed.asset_materializations_for_node(DEDUP.to_python_identifier()))
        observations = self.instance.fetch_observations(
            AssetRecordsFilter(asset_key=DEDUP, asset_partitions=[self.partition]), limit=10
        ).records
        self.assertEqual(len(observations), 1)
        observation = observations[0].asset_observation
        assert observation is not None
        attempt = observation.metadata["inference_attempt"].value
        signature = self.io.stage_signature(
            DEDUP.path[-1],
            self.registered.scopes.deduplicated_annotations.deduplication,
            {"annotations": self.input},
        )
        key = f"{self.partition}/{DEDUP.path[-1]}/{signature}"
        self.assertEqual(self.io.read_evidence(f"{key}/{attempt}/receipts.json"), b"{}")
        self.assertEqual(
            json.loads(self.io.read_evidence(f"{key}/{attempt}/failure.json")),
            {"kind": "failed_step", "attempt": attempt},
        )
        self.assertEqual(observations[0].event_log_entry.run_id, failed.run_id)
        self.assertFalse(self.io.exists(f"{key}/evidence-index.json"))
        self.fail_compute = False
        retry = self.run_tables()
        self.assertTrue(retry.success)
        self.assertIs(
            retry.asset_materializations_for_node(DEDUP.to_python_identifier())[0]
            .metadata["cache_hit"]
            .value,
            False,
        )

    def test_publish_time_staleness_blocks_output_even_after_compute(self) -> None:
        self.stale_on_publish = True
        failed = self.run_tables()
        self.assertFalse(failed.success)
        self.assertEqual(self.calls, 1)
        self.assertFalse(failed.asset_materializations_for_node(DEDUP.to_python_identifier()))
        with (
            build_input_context(
                asset_key=DEDUP,
                partition_key=self.partition,
                asset_partitions_def=DATASET_PARTITIONS,
                instance=self.instance,
            ) as context,
            self.assertRaises(FileNotFoundError),
        ):
            self.io.current_reference(context)
        self.assertIn(
            "Collection inputs changed during finalization",
            str(failed.failure_data_for_node(DEDUP.to_python_identifier())),
        )

    def test_current_cache_rejects_corrupted_sidecar_without_recomputing(self) -> None:
        published = self.run_tables()
        self.assertTrue(published.success)
        node = DEDUP.to_python_identifier()
        original = published.asset_materializations_for_node(node)[0]
        publication = FinalizationPublicationV1.model_validate_json(
            json.dumps(original.metadata[FINALIZATION_PUBLICATION_METADATA].value)
        )
        prefix = (
            f"{self.partition}/{DEDUP.path[-1]}/{publication.input_signature}/{publication.artifact_attempt}"
        )
        index = self.io.read_evidence(f"{prefix}/evidence-index.json")
        key = hashlib.sha256(f"{prefix}/duplicates.json".encode()).hexdigest()
        self.io.object_store().write(f"evidence/{key}", b'["corrupted sidecar"]', replace=True)

        rejected = self.run_tables()

        self.assertFalse(rejected.success)
        self.assertEqual(self.calls, 1)
        self.assertFalse(rejected.asset_materializations_for_node(node))
        self.assertIn("sidecar differs", str(rejected.failure_data_for_node(node)))
        self.assertEqual(self.io.read_evidence(f"{prefix}/evidence-index.json"), index)
        latest = self.instance.fetch_materializations(
            AssetRecordsFilter(asset_key=DEDUP, asset_partitions=[self.partition]), limit=1
        ).records[0]
        self.assertEqual(latest.asset_materialization, original)
        with build_input_context(
            asset_key=DEDUP,
            partition_key=self.partition,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as context:
            self.assertEqual(self.io.current_reference(context), publication.output)

    def retain_legacy(
        self,
        asset_key: AssetKey,
        wrapper: str,
        inputs: Mapping[str, Annotations | CategoryRules],
        old: FinalizationTable,
        filename: str,
    ) -> tuple[str, str, bytes, bytes]:
        stage = asset_key.path[-1]
        source = ImplementationSource()
        finalization = "codebench_pipeline/dataset_finalization.py"
        text = source.read(finalization).decode()
        root = next(
            node for node in ast.parse(text).body if isinstance(node, ast.FunctionDef) and node.name == stage
        )
        assert root.end_lineno is not None
        lines = text.splitlines(keepends=True)
        old_source = "".join(lines[: root.lineno - 1]) + dedent(wrapper) + "".join(lines[root.end_lineno :])
        old_source += (
            "\nfrom codebench_pipeline.config_assets import RegisteredConfig, require_registered_rules\n"
        )
        old_source += "from codebench_pipeline.typed_config import TypedConfig\n"
        retained = {finalization: old_source.encode()}

        def read(name: str) -> bytes:
            if name not in retained:
                retained[name] = source.read(name)
            return retained[name]

        scoped = processing_versions(stage, read)
        for module in ("dataset_engine", "stage_implementation"):
            name = f"codebench_pipeline/{module}.py"
            scoped[name] = hashlib.sha256(read(name)).hexdigest()
        versions = {name: hashlib.sha256(payload).hexdigest() for name, payload in retained.items()}
        for name, payload in retained.items():
            self.io.write_evidence(f"implementation/{versions[name]}/{name}", payload)
        config = TypedConfig()
        signature = implementation_signature(stage, config, inputs, scoped)
        key = f"{self.partition}/{stage}/{signature}"
        self.io.cached_frame(key, lambda: old.table)
        attempt = "0" * 32
        sidecar = json.dumps(old.evidence).encode()
        receipts = {filename: self.io.write_evidence(f"{key}/{attempt}/{filename}", sidecar)}
        evidence = {
            "attempt": attempt,
            "receipts": receipts,
            "config": config.model_dump(mode="json"),
            "implementation": versions,
            "stage_implementation": scoped,
        }
        self.io.write_evidence(f"{key}/stage-config.json", config.model_dump_json().encode())
        old_index = json.dumps(evidence).encode()
        self.io.write_evidence(f"{key}/evidence-index.json", old_index)
        self.instance.report_runless_asset_event(
            AssetMaterialization(
                asset_key,
                partition=self.partition,
                metadata={
                    "input_fingerprint": signature,
                    "evidence": evidence,
                    "codebench/content_digest": hashlib.sha256(frame_bytes(old.table)).hexdigest(),
                },
            )
        )
        return signature, key, old_index, sidecar

    def test_runless_legacy_receipts_remain_readable_but_are_not_reused(self) -> None:
        dedup = deduplicated_annotations(self.input, DeduplicationPolicy())
        rules = self.registered.rules
        eligibility = category_eligible_annotations(dedup.table, rules.category_id)
        entries = (
            (DEDUP, LEGACY_DEDUP, {"annotations": self.input}, dedup, "duplicates.json"),
            (
                ELIGIBILITY,
                LEGACY_ELIGIBILITY,
                {"annotations": dedup.table, "category_rules": rules},
                eligibility,
                "category-exclusions.json",
            ),
        )
        previous = [self.retain_legacy(*entry) for entry in entries]
        result = self.run_tables(eligibility=True)
        self.assertTrue(result.success)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.eligibility_calls, 1)
        for (asset_key, _, _, old, filename), (_, key, index, sidecar) in zip(entries, previous, strict=True):
            metadata = result.asset_materializations_for_node(asset_key.to_python_identifier())[0].metadata
            self.assertIs(metadata["cache_hit"].value, False)
            self.assertEqual(self.io.read_evidence(f"{key}/evidence-index.json"), index)
            self.assertEqual(self.io.read_evidence(f"{key}/{'0' * 32}/{filename}"), sidecar)
            historical, hit = self.io.cached_frame(key, lambda: self.fail("Historical table is missing"))
            self.assertTrue(hit)
            self.assertEqual(frame_bytes(historical), frame_bytes(old.table))
