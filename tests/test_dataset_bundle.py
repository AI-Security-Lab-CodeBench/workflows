from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from configuration_fixture import example_config, seed_prepared_snapshot, source_bytes
from dagster import validate_run_config
from pydantic import ValidationError
from test_declared_dataset_graph import BUNDLE, declared_labeling_fixture

from codebench_pipeline.dataset_catalog import build_catalog
from codebench_pipeline.dataset_collection_binding import unavailable_candidates
from codebench_pipeline.dataset_config import CatalogConfig, catalog_for_branch, load_build_config
from codebench_pipeline.dataset_documentation_config import BundleDocumentation
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.source_manifests import (
    DocumentationManifest,
    SourceManifest,
    load_source_manifest,
    resolve_source_paths,
)
from codebench_pipeline.source_transform_contract import SourceTransformConfig
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.storage import IntegrityError


def write_declared_bundle(root: Path) -> Path:
    shutil.copytree(BUNDLE.parent.parent, root)
    path = root / "datasets" / BUNDLE.name
    payload = json.loads(path.read_bytes())
    payload["labeling"] = declared_labeling_fixture(root).model_dump(mode="json")
    payload["storage"] = {"backend": "s3", "bucket": "fixture-artifacts"}
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class DatasetBundleTests(unittest.TestCase):
    def test_branch_settings_reach_mapping_and_branch_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = write_declared_bundle(root / "business")
            collection = bundle.parent.parent / "categories/cat3/collection"
            for name, maximum, workers in (
                ("existing-datasets", 2001, 2),
                ("documentation-crawl", 2002, 3),
            ):
                path = collection / f"{name}.json"
                payload = json.loads(path.read_bytes())
                payload.update(maximum_file_bytes=maximum, capture_workers=workers)
                path.write_text(json.dumps(payload), encoding="utf-8")

            spec = load_build_config(bundle, root)
            run_config = dataset_run_config(spec).to_config_dict()
            job = build_definitions(root).resolve_job_def("build_dataset")
            validate_run_config(job, run_config)
            self.assertEqual(set(run_config["ops"]), {"datasets__global_config"})
            scopes = DeclaredGlobalConfig.from_spec(spec).scopes
            for mapped, maximum, workers in (
                (scopes.existing_datasets, 2001, 2),
                (scopes.documentation_crawl, 2002, 3),
            ):
                repository = mapped.collection.repository_config
                self.assertEqual(repository.maximum_file_bytes, maximum)
                self.assertEqual(repository.workers, workers)

            payload["maximum_file_bytes"] = 2999
            path.write_text(json.dumps(payload), encoding="utf-8")
            changed = load_build_config(bundle, root)
            original_config = DeclaredGlobalConfig.from_spec(spec)
            changed_config = DeclaredGlobalConfig.from_spec(changed)
            self.assertEqual(
                unavailable_candidates(original_config, "dataset", "waiting").attrs,
                unavailable_candidates(changed_config, "dataset", "waiting").attrs,
            )
            self.assertNotEqual(
                unavailable_candidates(original_config, "documentation", "waiting").attrs,
                unavailable_candidates(changed_config, "documentation", "waiting").attrs,
            )

    def test_archived_registry_and_stage_evidence_survive_manifest_removal(self) -> None:
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
                    )
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
            manifest = load_source_manifest(sources_file, "CAT-99")
            catalog = CatalogConfig(
                documents=manifest.sources,
                collection_revision=manifest.revision,
                manifest_header_json=manifest.model_dump_json(exclude={"sources"}),
            )
            self.assertEqual(catalog.documents, [resolve_source_paths(source, inventory)])
            documentation = BundleDocumentation(manifest=snippets_file.name).resolve(inventory, "CAT-99")
            bundle = write_declared_bundle(root / "business")
            active = load_build_config(bundle, root)
            rules = fixture.rules.model_copy(update={"base_prompt": "  Preserve this exact prompt.\n"})
            archived, revision = seed_prepared_snapshot(
                root,
                active.model_copy(update={"rules": rules, "documentation_catalog": catalog}),
                documentation,
            )
            signature = stage_signature("source_catalog", catalog, {"category_rules": rules})
            bundle.unlink()
            sources_file.unlink()
            snippets_file.unlink()

            registry = DatasetRegistry(root)
            registered = registry.get_snapshot(archived.dataset_id, revision=revision)
            with self.assertRaisesRegex(IntegrityError, "read-only"):
                registry.get(archived.dataset_id)
            with self.assertRaisesRegex(IntegrityError, "read-only"):
                refresh_registered_configuration(registered.dataset_id, root)
            self.assertEqual(registered.rules.base_prompt, "  Preserve this exact prompt.\n")
            catalog = catalog_for_branch(registered, "documentation")
            self.assertEqual(catalog.collection_revision, "inventory-v2")
            assert registered.documentation is not None
            self.assertEqual(registered.documentation.revision, "references-v2")
            self.assertEqual(
                stage_signature("source_catalog", catalog, {"category_rules": registered.rules}), signature
            )
            self.assertEqual(catalog.documents[0].local_path, str(inventory / "source.md"))
            transform = catalog.documents[0].transform
            assert transform is not None
            self.assertEqual(transform.module_path, str(module))
            build_catalog(catalog, registered.rules, root / "catalog-evidence", branch="documentation")
            retained = SourceManifest.model_validate_json(
                (root / "catalog-evidence/source-manifest.json").read_bytes()
            )
            self.assertEqual(retained.evidence, {"origin": "fixture inventory"})
            self.assertEqual(retained.revision, "inventory-v2")
            self.assertEqual(registered.documentation.snippets, fixture.documentation.snippets)

    def test_bundle_rejects_version_selectors_prepared_inputs_and_foreign_partition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = write_declared_bundle(root / "business")
            original = json.loads(path.read_bytes())
            for updates in (
                {"version": 1},
                {"version": 2},
                {"documentation_crawl": {"snapshot": {"workers": "2"}}},
                {"documentation_crawl": {"sources": {"manifest": "unused.json", "documents": []}}},
                {"category_id": "CAT-98"},
            ):
                with self.subTest(updates=updates), self.assertRaises(ValidationError):
                    path.write_text(json.dumps({**original, **updates}), encoding="utf-8")
                    load_build_config(path, root)
            path.write_text(json.dumps({**original, "dataset_id": "foreign_partition"}), encoding="utf-8")
            with self.assertRaisesRegex(IntegrityError, "different dataset partition"):
                load_build_config(path, root)
