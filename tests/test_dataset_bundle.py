from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from configuration_fixture import example_config, source_bytes
from dagster import DagsterInstance, validate_run_config
from pydantic import JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.dataframe_schemas import FilteredCommandsTable, validated_frame
from codebench_pipeline.dataset_catalog import build_catalog
from codebench_pipeline.dataset_config import catalog_for_branch, load_build_config, snapshots_for_branch
from codebench_pipeline.dataset_documentation import collect_documentation
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_plan import plan_dataset, unavailable_candidates
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.source_manifests import DocumentationManifest, SourceManifest
from codebench_pipeline.source_transform_contract import SourceTransformConfig
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import IntegrityError

JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


def write_bundle(root: Path, **updates: JsonValue) -> Path:
    fixture = example_config()
    assert fixture.rules is not None
    contents: dict[str, JsonValue] = {
        "dataset_id": fixture.dataset_id,
        "revision": "bundle-fixture",
        "finalization": {
            "rules": fixture.rules.model_dump(mode="json"),
            "documentation": fixture.documentation.model_dump(mode="json"),
            "annotation": {"model": fixture.models.labeler.model_dump(mode="json")},
        },
        **updates,
    }
    path = root / "bundle.json"
    path.write_text(JSON_OBJECT.dump_json(contents).decode(), encoding="utf-8")
    return path


class DatasetBundleTests(unittest.TestCase):
    def test_branch_settings_reach_launch_configs_and_branch_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset: dict[str, JsonValue] = {
                "snapshot": {"maximum_bytes": 2001, "workers": 2},
                "adapters": {"sample_characters": 311},
                "extraction": {"context_characters": 41},
            }
            docs: dict[str, JsonValue] = {
                "snapshot": {"maximum_bytes": 2002, "workers": 3},
                "adapters": {"sample_characters": 312},
                "extraction": {"context_characters": 42},
            }
            spec = load_build_config(
                write_bundle(root, existing_datasets=dataset, documentation_crawl=docs), root
            )
            run_config = dataset_run_config(spec, root)
            resolved = validate_run_config(build_definitions(root).get_job_def("build_dataset"), run_config)
            ops = resolved["ops"]
            for prefix, values in (
                ("datasets__", (2001, 2, 311, 41)),
                ("datasets__documentation__", (2002, 3, 312, 42)),
            ):
                expected_bytes, workers, sample, context = values
                self.assertEqual(ops[prefix + "source_documents"]["config"]["maximum_bytes"], expected_bytes)
                self.assertEqual(ops[prefix + "source_documents"]["config"]["workers"], workers)
                self.assertEqual(ops[prefix + "document_adapters"]["config"]["sample_characters"], sample)
                self.assertEqual(ops[prefix + "command_candidates"]["config"]["context_characters"], context)
            changed = load_build_config(
                write_bundle(
                    root,
                    existing_datasets=dataset,
                    documentation_crawl={
                        **docs,
                        "extraction": {"context_characters": 99},
                    },
                ),
                root,
            )
            self.assertEqual(
                unavailable_candidates(spec, "dataset", "waiting").attrs,
                unavailable_candidates(changed, "dataset", "waiting").attrs,
            )
            self.assertNotEqual(
                unavailable_candidates(spec, "documentation", "waiting").attrs,
                unavailable_candidates(changed, "documentation", "waiting").attrs,
            )

    def test_registry_and_stage_evidence_survive_manifest_removal(self) -> None:
        fixture = example_config()
        assert fixture.documentation_sources is not None
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inventory = root / "manifests"
            inventory.mkdir()
            module = root / "custom.py"
            module.write_bytes(b"def transform(rows, context):\n    return []\n")
            (inventory / "source.md").write_bytes(source_bytes())
            source = fixture.documentation_sources.documents[0].model_copy(
                update={
                    "transform": SourceTransformConfig(
                        module_path="../custom.py",
                        sha256=hashlib.sha256(module.read_bytes()).hexdigest(),
                        entrypoint="transform",
                        trusted=True,
                    ),
                }
            )
            sources_file = inventory / "sources.json"
            sources_file.write_text(
                SourceManifest(
                    category_id="CAT-99",
                    revision="inventory-v2",
                    sources=[source],
                    evidence={"origin": "fixture inventory"},
                ).model_dump_json(),
                encoding="utf-8",
            )
            snippets_file = inventory / "snippets.json"
            snippets_file.write_text(
                DocumentationManifest(
                    category_id="CAT-99",
                    revision="references-v2",
                    snippets=fixture.documentation.snippets,
                ).model_dump_json(),
                encoding="utf-8",
            )
            path = write_bundle(root, documentation_crawl={"sources": {"manifest": "manifests/sources.json"}})
            raw = JSON_OBJECT.validate_json(path.read_bytes())
            finalization = JSON_OBJECT.validate_python(raw["finalization"])
            finalization["documentation"] = {"manifest": "manifests/snippets.json"}
            finalization["rules"] = {
                **JSON_OBJECT.validate_python(finalization["rules"]),
                "base_prompt": "  Preserve this exact prompt.\n",
            }
            path.write_bytes(JSON_OBJECT.dump_json({**raw, "finalization": finalization}))
            spec = load_build_config(path, root)
            config = catalog_for_branch(spec, "documentation")
            signature = stage_signature("source_catalog", config, {"category_rules": spec.rules})
            inline_source = {
                **source.model_dump(mode="json"),
                "local_path": "manifests/source.md",
                "transform": {
                    **JSON_OBJECT.validate_python(source.model_dump(mode="json")["transform"]),
                    "module_path": "custom.py",
                },
            }
            inline = load_build_config(
                write_bundle(root, documentation_crawl={"sources": {"documents": [inline_source]}}),
                root,
            )
            self.assertEqual(catalog_for_branch(inline, "documentation").documents, config.documents)
            registry = DatasetRegistry(root)
            registry.register(spec)
            path.unlink()
            sources_file.unlink()
            snippets_file.unlink()

            registered = registry.get(spec.dataset_id)
            self.assertEqual(registered.rules.base_prompt, "  Preserve this exact prompt.\n")
            catalog = catalog_for_branch(registered, "documentation")
            self.assertEqual(
                stage_signature("source_catalog", catalog, {"category_rules": registered.rules}),
                signature,
            )
            self.assertEqual(catalog.documents[0].local_path, str(inventory / "source.md"))
            transform = catalog.documents[0].transform
            assert transform is not None
            self.assertEqual(transform.module_path, str(module))
            self.assertEqual(snapshots_for_branch(registered, "documentation").documents, catalog.documents)
            with DagsterInstance.ephemeral() as instance:
                self.assertEqual(
                    plan_dataset(registered, stage_configs(registered), instance, home=root).unconfigured,
                    ("dataset",),
                )
            build_catalog(catalog, registered.rules, root / "catalog-evidence", branch="documentation")
            retained = SourceManifest.model_validate_json(
                (root / "catalog-evidence/source-manifest.json").read_bytes()
            )
            self.assertEqual(retained.evidence, {"origin": "fixture inventory"})
            self.assertEqual(retained.revision, "inventory-v2")
            table = collect_documentation(
                FilteredCommandsTable.validate(validated_frame([], FilteredCommandsTable)),
                registered.rules,
                registered.documentation,
                root / "documentation-evidence",
            )
            self.assertEqual(
                table["snippet"].tolist(), [seed.snippet for seed in fixture.documentation.snippets]
            )

    def test_bundle_rejects_version_selectors_competing_inputs_and_foreign_category(self) -> None:
        fixture = example_config()
        assert fixture.documentation_sources is not None
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for updates in (
                {"version": 1},
                {"version": 2},
                {"documentation_crawl": {"snapshot": {"workers": "2"}}},
                {
                    "documentation_crawl": {
                        "sources": {
                            "manifest": "unused.json",
                            "documents": [fixture.documentation_sources.documents[0].model_dump()],
                        }
                    }
                },
            ):
                with self.subTest(updates=updates), self.assertRaises(ValidationError):
                    load_build_config(write_bundle(root, **updates), root)
            (root / "foreign.json").write_text(
                SourceManifest(category_id="CAT-98", revision="foreign", sources=[]).model_dump_json(),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(IntegrityError, "another category"):
                load_build_config(
                    write_bundle(root, existing_datasets={"sources": {"manifest": "foreign.json"}}), root
                )
