import ast
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from dagster import AssetKey
from guarded_labeling_fixture import LabelingFixture
from pydantic import JsonValue, TypeAdapter
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.config_assets import configured_selection
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataset_branches import DECLARED_SOURCE_STAGES
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_engine import (
    code_versions,
    implementation_signature,
    receipt_source_versions,
    stage_signature,
    verified_legacy_table,
)
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.stage_implementation import (
    STAGE_ROOTS,
    ImplementationSource,
    ProcessingChanged,
    ProcessingReceiptIncomplete,
    processing_runtime,
    processing_versions,
    verify_processing,
)
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import TypedConfig

ROOT = Path(__file__).resolve().parents[1]


class StageImplementationTests(unittest.TestCase):
    def test_installed_source_rejects_escaped_names_and_links(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root = home / "installed"
        package = root / "codebench_pipeline"
        package.mkdir(parents=True)
        external = home / "outside.py"
        external.write_bytes(b"outside implementation")
        (package / "linked.py").symlink_to(external)
        (root / "linked-parent").symlink_to(home, target_is_directory=True)
        (package / "_internal.py").write_bytes(b"installed implementation")
        source = ImplementationSource(root)

        self.assertEqual(source.read("codebench_pipeline/_internal.py"), b"installed implementation")
        self.assertEqual(
            source.digest("codebench_pipeline/_internal.py"),
            hashlib.sha256(b"installed implementation").hexdigest(),
        )
        self.assertTrue(source.is_file("codebench_pipeline/_internal.py"))
        self.assertFalse(source.is_file("codebench_pipeline/missing.py"))
        for name in (
            str(external),
            "../outside.py",
            "linked-parent/outside.py",
            "codebench_pipeline/linked.py",
        ):
            with self.subTest(name=name):
                with self.assertRaises(IntegrityError):
                    source.read(name)
                with self.assertRaises(IntegrityError):
                    source.is_file(name)

    def test_package_matching_remains_flat_and_rejects_a_linked_package(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root = home / "installed"
        package = root / "codebench_pipeline"
        package.mkdir(parents=True)
        (package / "_internal.py").touch()
        (package / "non-source.txt").touch()
        source = ImplementationSource(root)

        self.assertEqual(source.package_matches("*.py"), ("codebench_pipeline/_internal.py",))
        with self.assertRaises(IntegrityError):
            source.package_matches("../*.py")
        external = package.rename(home / "outside-package")
        package.symlink_to(external, target_is_directory=True)
        with self.assertRaises(IntegrityError):
            source.package_matches("*.py")

    def test_review_driver_changes_preserve_processing_dependencies(self) -> None:
        base: dict[str, JsonValue] = {
            "project": {"dependencies": ["pandas>=3"]},
            "package": [
                {"name": "pandas", "version": "3"},
                {"name": "jsonschema", "version": "4"},
                {"name": "codebench", "dependencies": []},
            ],
        }
        review: dict[str, JsonValue] = {
            "project": {"dependencies": ["pandas>=3", "psycopg>=3", "jsonschema>=4"]},
            "package": [
                {"name": "pandas", "version": "3"},
                {"name": "jsonschema", "version": "4"},
                {"name": "codebench", "dependencies": [{"name": "psycopg"}, {"name": "jsonschema"}]},
                {"name": "psycopg", "version": "3.3"},
            ],
        }
        changed: dict[str, JsonValue] = {
            **base,
            "package": [
                {"name": "pandas", "version": "4"},
                {"name": "jsonschema", "version": "4"},
                {"name": "codebench", "dependencies": []},
            ],
        }
        self.assertEqual(processing_runtime(base), processing_runtime(review))
        self.assertNotEqual(processing_runtime(base), processing_runtime(changed))
        shared_upgrade: dict[str, JsonValue] = {
            **base,
            "package": [
                {"name": "pandas", "version": "3"},
                {"name": "jsonschema", "version": "5"},
                {"name": "codebench", "dependencies": []},
            ],
        }
        self.assertNotEqual(processing_runtime(base), processing_runtime(shared_upgrade))

    def test_downstream_graph_and_review_edits_preserve_upstream_signatures(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        configs = stage_configs(fixture_spec(home))
        original = ImplementationSource.read
        changes = {
            "dataset_finalization.py": b"\ndef downstream_review_only():\n    return 'new stage'\n",
            "dataset_tables.py": b"\ndef downstream_publication_only():\n    return 'new review'\n",
            "definitions.py": b"\nDOWNSTREAM_GRAPH_REVISION = 2\n",
            "dataset_dependencies.py": b"\nRELEASE_STAGES = (*RELEASE_STAGES, 'new_stage')\n",
            "review_snapshots.py": b"\nDOWNSTREAM_REVIEW_REVISION = 2\n",
        }
        before = {stage: stage_signature(stage, configs[stage], {}) for stage in STAGE_ROOTS}

        def changed(source: ImplementationSource, name: str) -> bytes:
            filename = Path(name).name
            payload = original(source, name) + changes.get(filename, b"")
            if filename in {"dataset_tables.py", "dataset_export.py", "review_policy.py"}:
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

        with patch.object(ImplementationSource, "read", changed):
            after = {stage: stage_signature(stage, configs[stage], {}) for stage in STAGE_ROOTS}

        self.assertNotEqual(after["human_review_queue"], before["human_review_queue"])
        self.assertEqual(
            {stage: value for stage, value in after.items() if stage != "human_review_queue"},
            {stage: value for stage, value in before.items() if stage != "human_review_queue"},
        )

    def test_changed_processing_invalidates_affected_but_not_unrelated_stages(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        configs = stage_configs(fixture_spec(home))
        original = ImplementationSource.read
        before = {
            stage: stage_signature(stage, configs[stage], {})
            for stage in ("deduplicated_annotations", "command_candidates")
        }

        def changed(source: ImplementationSource, name: str) -> bytes:
            payload = original(source, name)
            if name == "codebench_pipeline/dataset_finalization.py":
                return payload.replace(b'.strip(" ")', b".strip()")
            return payload

        with patch.object(ImplementationSource, "read", changed):
            after = {stage: stage_signature(stage, configs[stage], {}) for stage in before}

        self.assertNotEqual(after["deduplicated_annotations"], before["deduplicated_annotations"])
        self.assertEqual(after["command_candidates"], before["command_candidates"])

    def test_declared_collection_versions_ignore_inspector_but_track_capture_processing(self) -> None:
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        configs = stage_configs(fixture_spec(home))
        stages = [
            stage
            for stage in configs
            if stage.rsplit("/", 1)[-1] in DECLARED_SOURCE_STAGES or stage == "labeling_attempts"
        ]
        original = ImplementationSource.read
        before = {stage: stage_signature(stage, configs[stage], {}) for stage in stages}

        for file, target, replacement, expected in (
            (
                "dataset_artifacts.py",
                b"No persisted artifact binding",
                b"No artifact binding for this request",
                set[str](),
            ),
            (
                "repository.py",
                b'"retrieved_at": capture.retrieved_at',
                b'"retrieved_at": "timestamp omitted"',
                {"capture_corpus_files", "documentation/capture_documentation"},
            ),
        ):
            with self.subTest(file=file):
                filename = f"codebench_pipeline/{file}"
                self.assertIn(target, original(ImplementationSource(), filename))

                def changed(source: ImplementationSource, name: str) -> bytes:
                    payload = original(source, name)
                    return payload.replace(target, replacement) if name == filename else payload

                with patch.object(ImplementationSource, "read", changed):
                    invalidated = {
                        stage
                        for stage in stages
                        if stage_signature(stage, configs[stage], {}) != before[stage]
                    }
                self.assertEqual(invalidated, expected)

    def test_input_and_configuration_changes_still_invalidate(self) -> None:
        config = FilteringConfig()
        frame = pd.DataFrame({"command": ["git status"]})
        before = stage_signature("filtered_commands", config, {"command_candidates": frame})

        self.assertNotEqual(
            before,
            stage_signature(
                "filtered_commands", config, {"command_candidates": pd.DataFrame({"command": ["git diff"]})}
            ),
        )
        self.assertNotEqual(
            before,
            stage_signature(
                "filtered_commands",
                FilteringConfig(maximum_command_characters=1),
                {"command_candidates": frame},
            ),
        )

    def test_historical_table_preserves_original_content_and_receipt(self) -> None:
        self.check_legacy("downstream")

    def test_cached_table_retains_scoped_files_and_reuses_after_unrelated_edit(self) -> None:
        stage = "command_candidates"
        key = AssetKey(["datasets", "documentation", stage])
        capture_key = AssetKey(["datasets", "documentation", "capture_documentation"])
        source = "codebench_pipeline/declared_source_assets.py"
        original = ImplementationSource.read
        home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        spec = LabelingFixture(self, home).spec
        dataset_id = DatasetRegistry(home).register(spec)
        manager = DataFrameIOManager(storage=spec.storage)
        job = build_definitions(home).resolve_job_def("build_documentation")

        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            run = job.execute_in_process(instance=instance, partition_key=dataset_id)
            (first,) = run.asset_materializations_for_node(key.to_python_identifier())
            (capture,) = run.asset_materializations_for_node(capture_key.to_python_identifier())
            evidence = TypeAdapter(dict[str, JsonValue]).validate_python(first.metadata["evidence"].value)
            implementation = TypeAdapter(dict[str, str]).validate_python(evidence["implementation"])
            expected = hashlib.sha256((ROOT / source).read_bytes()).hexdigest()
            self.assertEqual(implementation[source], expected)
            self.assertEqual(
                manager.read_evidence(f"implementation/{expected}/{source}"), (ROOT / source).read_bytes()
            )
            self.assertFalse(first.metadata["cache_hit"].value)
            verify_processing(stage, implementation, manager.read_evidence)

            def changed(installed: ImplementationSource, name: str) -> bytes:
                payload = original(installed, name)
                return (
                    payload.replace(b"No persisted artifact binding", b"No artifact binding for this request")
                    if name == "codebench_pipeline/dataset_artifacts.py"
                    else payload
                )

            with patch.object(ImplementationSource, "read", changed):
                repeated = job.execute_in_process(
                    instance=instance,
                    partition_key=dataset_id,
                    asset_selection=configured_selection([capture_key, key]),
                )
            (second,) = repeated.asset_materializations_for_node(key.to_python_identifier())
            (repeated_capture,) = repeated.asset_materializations_for_node(capture_key.to_python_identifier())
            self.assertTrue(second.metadata["cache_hit"].value)
            self.assertEqual(second.metadata["input_fingerprint"], first.metadata["input_fingerprint"])
            self.assertEqual(second.metadata["evidence"], first.metadata["evidence"])
            self.assertTrue(repeated_capture.metadata["cache_hit"].value)
            self.assertEqual(
                repeated_capture.metadata["input_fingerprint"], capture.metadata["input_fingerprint"]
            )
            self.assertEqual(repeated_capture.metadata["evidence"], capture.metadata["evidence"])

    def test_historical_table_rejects_invalid_receipts(self) -> None:
        cases = (
            ("processing", ProcessingChanged, "processing implementation differs"),
            ("engine", ProcessingChanged, "processing implementation differs"),
            ("configuration", IntegrityError, "fingerprint does not match"),
            ("inputs", IntegrityError, "fingerprint does not match"),
            ("incomplete", ProcessingReceiptIncomplete, "implementation receipt is missing"),
            ("missing", FileNotFoundError, None),
            ("corrupted", IntegrityError, "implementation receipt changed"),
            ("cache_missing", IntegrityError, "cache is missing; processing was not started"),
            ("content", IntegrityError, "cache differs from the recorded materialization"),
        )
        for change, error, message in cases:
            with self.subTest(change=change):
                assertion = self.assertRaisesRegex(error, message) if message else self.assertRaises(error)
                with assertion:
                    self.check_legacy(change)

    def check_legacy(self, change: str) -> None:
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
            original = b"frame.attrs = {**shared_inputs[0].attrs, **frame.attrs}"
            self.assertIn(original, sources[engine])
            sources[engine] = sources[engine].replace(
                original,
                b"frame.attrs = {**frame.attrs, **shared_inputs[0].attrs}",
            )
        implementation = {name: hashlib.sha256(payload).hexdigest() for name, payload in sources.items()}
        if change == "incomplete":
            del implementation[filename]
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
                if change in {"missing", "incomplete"} and name == filename:
                    continue
                payload = sources[name]
                if change == "corrupted" and name == filename:
                    payload += b"\n"
                manager.write_evidence(f"implementation/{implementation[name]}/{name}", payload)
            requested = FilteringConfig() if change == "configuration" else config
            requested_inputs = (
                {"annotations": pd.DataFrame({"command": ["git diff"]})} if change == "inputs" else inputs
            )
            content_digest = (
                "0" * 64 if change == "content" else hashlib.sha256(frame_bytes(frame)).hexdigest()
            )
            result = verified_legacy_table(
                manager,
                key,
                stage=stage,
                config=requested,
                inputs=requested_inputs,
                implementation=implementation,
                content_digest=content_digest,
            )
            pd.testing.assert_frame_equal(result, frame)
            self.assertEqual(result.attrs, frame.attrs)


if __name__ == "__main__":
    unittest.main()
