import ast
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from dagster import AssetExecutionContext, Output, StaticPartitionsDefinition, asset, materialize
from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataframe_schemas import SourceCatalogTable, validated_frame
from codebench_pipeline.dataset_asset_types import TABLE_TYPES
from codebench_pipeline.dataset_config import ExtractionConfig
from codebench_pipeline.dataset_engine import (
    cached_table,
    code_versions,
    implementation_signature,
    receipt_source_versions,
    stage_signature,
    verified_legacy_table,
)
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.stage_cache_compatibility import legacy_signature_matches
from codebench_pipeline.stage_implementation import STAGE_ROOTS, processing_runtime, processing_versions
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import TypedConfig

ROOT = Path(__file__).resolve().parents[1]


class StageImplementationTests(unittest.TestCase):
    def test_review_driver_changes_preserve_processing_dependencies(self) -> None:
        base = {
            "project": {"dependencies": ["pandas>=3"]},
            "package": [
                {"name": "pandas", "version": "3"},
                {"name": "jsonschema", "version": "4"},
                {"name": "codebench", "dependencies": []},
            ],
        }
        review = {
            "project": {"dependencies": ["pandas>=3", "psycopg>=3", "jsonschema>=4"]},
            "package": [
                {"name": "pandas", "version": "3"},
                {"name": "jsonschema", "version": "4"},
                {"name": "codebench", "dependencies": [{"name": "psycopg"}, {"name": "jsonschema"}]},
                {"name": "psycopg", "version": "3.3"},
            ],
        }
        changed = {
            **base,
            "package": [
                {"name": "pandas", "version": "4"},
                {"name": "jsonschema", "version": "4"},
                {"name": "codebench", "dependencies": []},
            ],
        }
        self.assertEqual(processing_runtime(base), processing_runtime(review))
        self.assertNotEqual(processing_runtime(base), processing_runtime(changed))
        shared_upgrade = {
            **base,
            "package": [
                {"name": "pandas", "version": "3"},
                {"name": "jsonschema", "version": "5"},
                {"name": "codebench", "dependencies": []},
            ],
        }
        self.assertNotEqual(processing_runtime(base), processing_runtime(shared_upgrade))

    def test_downstream_graph_and_review_edits_preserve_upstream_signatures(self) -> None:
        original = Path.read_bytes
        changes = {
            "dataset_finalization.py": b"\ndef downstream_review_only():\n    return 'new stage'\n",
            "dataset_tables.py": b"\ndef downstream_publication_only():\n    return 'new review'\n",
            "dataset_plan.py": b"\nDOWNSTREAM_GRAPH_REVISION = 2\n",
            "dataset_branches.py": b"\nFINALIZATION_STAGES = (*FINALIZATION_STAGES, 'new_stage')\n",
            "review_snapshots.py": b"\nDOWNSTREAM_REVIEW_REVISION = 2\n",
        }
        before = {stage: stage_signature(stage, TypedConfig(), {}) for stage in STAGE_ROOTS}

        def changed(path: Path) -> bytes:
            payload = original(path) + changes.get(path.name, b"")
            if path.name in {"dataset_tables.py", "dataset_export.py", "review_policy.py"}:
                tree = ast.parse(payload)
                for node in tree.body:
                    if isinstance(node, ast.FunctionDef) and node.name in {
                        "review_queue",
                        "prepare_dataset",
                        "requires_human_review",
                    }:
                        node.body = [ast.Return(value=ast.Constant(value=None))]
                return ast.unparse(tree).encode()
            return payload

        with patch.object(Path, "read_bytes", changed):
            after = {stage: stage_signature(stage, TypedConfig(), {}) for stage in STAGE_ROOTS}

        self.assertNotEqual(after["human_review_queue"], before["human_review_queue"])
        self.assertEqual(
            {stage: value for stage, value in after.items() if stage != "human_review_queue"},
            {stage: value for stage, value in before.items() if stage != "human_review_queue"},
        )

    def test_changed_processing_invalidates_affected_but_not_unrelated_stages(self) -> None:
        original = Path.read_bytes
        before = {
            stage: stage_signature(stage, TypedConfig(), {})
            for stage in ("deduplicated_annotations", "command_candidates")
        }

        def changed(path: Path) -> bytes:
            payload = original(path)
            if path.name == "dataset_finalization.py":
                return payload.replace(b'.strip(" ")', b".strip()")
            return payload

        with patch.object(Path, "read_bytes", changed):
            after = {stage: stage_signature(stage, TypedConfig(), {}) for stage in before}

        self.assertNotEqual(after["deduplicated_annotations"], before["deduplicated_annotations"])
        self.assertEqual(after["command_candidates"], before["command_candidates"])

    def test_input_and_configuration_changes_still_invalidate(self) -> None:
        config = ExtractionConfig()
        frame = pd.DataFrame({"command": ["git status"]})
        before = stage_signature("command_candidates", config, {"source": frame})

        self.assertNotEqual(
            before,
            stage_signature(
                "command_candidates", config, {"source": pd.DataFrame({"command": ["git diff"]})}
            ),
        )
        self.assertNotEqual(
            before,
            stage_signature("command_candidates", TypedConfig(), {"source": frame}),
        )

    def test_legacy_equivalent_receipts_reuse_cache_without_processing(self) -> None:
        self.check_legacy("downstream", accepted=True)

    def test_cached_table_retains_scoped_files_and_reuses_after_unrelated_edit(self) -> None:
        stage = "source_catalog"
        config = TypedConfig()
        source = "codebench_pipeline/stage_configuration.py"
        original = Path.read_bytes
        with tempfile.TemporaryDirectory() as temporary:
            manager = DataFrameIOManager(storage=StorageConfig(local_root=temporary))

            @asset(
                key=["datasets", stage],
                partitions_def=StaticPartitionsDefinition(["fixture"]),
                dagster_type=TABLE_TYPES[SourceCatalogTable],
            )
            def catalog(context: AssetExecutionContext) -> Output[DataFrame[SourceCatalogTable]]:
                return cached_table(
                    context,
                    manager,
                    config,
                    {},
                    SourceCatalogTable,
                    lambda _: validated_frame([], SourceCatalogTable),
                )

            run = materialize([catalog], partition_key="fixture")
            (first,) = run.asset_materializations_for_node("datasets__source_catalog")
            evidence = first.metadata["evidence"].value
            expected = hashlib.sha256((ROOT / source).read_bytes()).hexdigest()
            self.assertEqual(evidence["implementation"][source], expected)
            self.assertEqual(
                manager.read_evidence(f"implementation/{expected}/{source}"), (ROOT / source).read_bytes()
            )
            self.assertFalse(first.metadata["cache_hit"].value)
            self.assertTrue(
                legacy_signature_matches(
                    manager, stage, config, {}, first.metadata["input_fingerprint"].value, evidence
                )
            )

            def changed(path: Path) -> bytes:
                payload = original(path)
                return payload + b"\nUNRELATED_REVISION = 2\n" if path.name == "dataset_plan.py" else payload

            with patch.object(Path, "read_bytes", changed):
                repeated = materialize([catalog], partition_key="fixture")
            (second,) = repeated.asset_materializations_for_node("datasets__source_catalog")
            self.assertTrue(second.metadata["cache_hit"].value)
            self.assertEqual(second.metadata["input_fingerprint"], first.metadata["input_fingerprint"])
            self.assertEqual(second.metadata["evidence"], first.metadata["evidence"])

    def test_scoped_receipt_reuses_unchanged_processing_after_runtime_boundary_change(self) -> None:
        stage = "category_eligible_annotations"
        current = code_versions(stage)
        implementation = receipt_source_versions(current)
        runtime = "codebench_pipeline/stage_implementation.py"
        historical_runtime = (ROOT / runtime).read_bytes() + b"\n"
        historical_digest = hashlib.sha256(historical_runtime).hexdigest()
        implementation[runtime] = historical_digest
        scoped = {**current, runtime: historical_digest}
        inputs = {"annotations": pd.DataFrame({"command": ["git status"]})}
        config = TypedConfig()
        recorded = implementation_signature(stage, config, inputs, scoped)
        evidence: dict[str, JsonValue] = {
            "implementation": {name: value for name, value in implementation.items()},
            "stage_implementation": {name: value for name, value in scoped.items()},
        }
        with tempfile.TemporaryDirectory() as temporary:
            manager = DataFrameIOManager(storage=StorageConfig(local_root=temporary))
            with self.assertRaises(FileNotFoundError):
                legacy_signature_matches(manager, stage, config, inputs, recorded, evidence)
            for name, expected in implementation.items():
                payload = historical_runtime if name == runtime else (ROOT / name).read_bytes()
                manager.write_evidence(f"implementation/{expected}/{name}", payload)
            incomplete: dict[str, JsonValue] = {
                **evidence,
                "implementation": {name: value for name, value in code_versions().items()},
            }
            self.assertFalse(legacy_signature_matches(manager, stage, config, inputs, recorded, incomplete))
            self.assertTrue(legacy_signature_matches(manager, stage, config, inputs, recorded, evidence))
            changed = {"annotations": pd.DataFrame({"command": ["git diff"]})}
            self.assertFalse(legacy_signature_matches(manager, stage, config, changed, recorded, evidence))

    def test_legacy_changed_processing_is_rejected(self) -> None:
        self.check_legacy("processing", accepted=False)

    def test_legacy_changed_cache_frame_processing_is_rejected(self) -> None:
        self.check_legacy("engine", accepted=False)

    def test_legacy_changed_configuration_is_rejected(self) -> None:
        self.check_legacy("configuration", accepted=False)

    def test_legacy_missing_source_receipt_is_rejected(self) -> None:
        self.check_legacy("missing", accepted=False)

    def test_legacy_corrupted_source_receipt_is_rejected(self) -> None:
        self.check_legacy("corrupted", accepted=False)

    def test_legacy_missing_cache_never_starts_processing(self) -> None:
        self.check_legacy("cache_missing", accepted=False)

    def test_legacy_cache_must_match_original_materialization_content(self) -> None:
        self.check_legacy("content", accepted=False)

    def check_legacy(self, change: str, *, accepted: bool) -> None:
        stage = "deduplicated_annotations"
        config = TypedConfig()
        inputs = {"annotations": pd.DataFrame({"command": ["git status"]})}
        implementation = receipt_source_versions(code_versions(stage))
        sources = {name: (ROOT / name).read_bytes() for name in implementation}
        filename = "codebench_pipeline/dataset_finalization.py"
        if change == "processing":
            sources[filename] = sources[filename].replace(b'.strip(" ")', b".strip()")
        else:
            sources[filename] += b"\ndef legacy_review_only():\n    return 'old review'\n"
        if change == "engine":
            engine = "codebench_pipeline/dataset_engine.py"
            sources[engine] = sources[engine].replace(
                b'frame.attrs["codebench_source_inventory"]',
                b'frame.attrs["different_inventory_binding"]',
            )
        implementation = {name: hashlib.sha256(payload).hexdigest() for name, payload in sources.items()}
        signature = implementation_signature(stage, config, inputs, implementation)
        key = f"synthetic/{stage}/{signature}"
        frame = pd.DataFrame({"command": ["git status"], "class": ["allow"]})
        frame.attrs["original_inference_receipt"] = "immutable-label-receipt"
        with tempfile.TemporaryDirectory(prefix="stage-implementation-") as temporary:
            manager = DataFrameIOManager(storage=StorageConfig(local_root=temporary))
            if change != "cache_missing":
                manager.cached_frame(key, lambda: frame)
            manager.write_evidence(f"{key}/stage-config.json", config.model_dump_json().encode())
            manager.write_evidence(
                f"{key}/evidence-index.json",
                json.dumps({"config": config.model_dump(mode="json"), "receipts": {}}).encode(),
            )
            required = processing_versions(stage, lambda name: sources[name])
            names = {name.split(":")[0] for name in required}
            for name in names:
                if change == "missing" and name == filename:
                    continue
                payload = sources[name]
                if change == "corrupted" and name == filename:
                    payload += b"\n"
                manager.write_evidence(f"implementation/{implementation[name]}/{name}", payload)
            requested = ExtractionConfig() if change == "configuration" else config
            content_digest = (
                "0" * 64 if change == "content" else hashlib.sha256(frame_bytes(frame)).hexdigest()
            )
            if accepted:
                result = verified_legacy_table(
                    manager,
                    key,
                    stage=stage,
                    config=requested,
                    inputs=inputs,
                    implementation=implementation,
                    content_digest=content_digest,
                )
                pd.testing.assert_frame_equal(result, frame)
                self.assertEqual(result.attrs, frame.attrs)
                self.assertFalse(legacy_signature_matches(manager, stage, config, inputs, signature))
                manager.write_evidence(
                    f"legacy-implementations/{signature}.json", json.dumps(implementation).encode()
                )
                self.assertTrue(legacy_signature_matches(manager, stage, config, inputs, signature))
                self.assertFalse(
                    legacy_signature_matches(manager, stage, ExtractionConfig(), inputs, signature)
                )
                self.assertFalse(
                    legacy_signature_matches(
                        manager,
                        stage,
                        config,
                        {"annotations": pd.DataFrame({"command": ["git diff"]})},
                        signature,
                    )
                )
            else:
                with self.assertRaises((IntegrityError, FileNotFoundError)):
                    verified_legacy_table(
                        manager,
                        key,
                        stage=stage,
                        config=requested,
                        inputs=inputs,
                        implementation=implementation,
                        content_digest=content_digest,
                    )


if __name__ == "__main__":
    unittest.main()
