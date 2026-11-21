from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from configuration_fixture import FixtureCategory, example_config, seed_prepared_snapshot, source_bytes
from pydantic import JsonValue, ValidationError
from test_dataset_bundle import write_declared_bundle
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.business_config import DEFAULT_BUSINESS_CONFIG, load_business_rules
from codebench_pipeline.dataset_catalog import provided_documents
from codebench_pipeline.dataset_config import (
    CatalogConfig,
    DocumentationConfig,
    SnapshotConfig,
    catalog_for_branch,
    load_build_config,
    snapshots_for_branch,
)
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_seed_config import RiskSearchSeeds, SearchSeeds, SourceSeed
from codebench_pipeline.dataset_tables import documentation_table
from codebench_pipeline.repository_branch_config import RepositoryBranchDeclaration
from codebench_pipeline.source_manifests import resolve_source_paths
from codebench_pipeline.stage_configuration import declared_source_config
from codebench_pipeline.storage import IntegrityError


class DatasetConfigurationTests(unittest.TestCase):
    def test_deployment_routes_admit_deployments_without_reclassifying_deletions(self) -> None:
        rules = load_business_rules(DEFAULT_BUSINESS_CONFIG)
        for command, deployment in (
            ("serverless deploy", True),
            ("serverless remove", False),
            ("aws cloudformation deploy --stack-name example", True),
            ("aws cloudformation delete-stack --stack-name example", False),
            ("docker compose up", True),
            ("docker compose down --volumes", False),
        ):
            with self.subTest(command=command):
                self.assertEqual("CAT-3.5" in rules.matching_risks(command), deployment)

    def test_archived_documentation_inventory_remains_independent_of_empty_dataset_inventory(self) -> None:
        for category in ("CAT-99", "CAT-98"):
            with self.subTest(category=category), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture = example_config(category)
                supplied = fixture.documentation_sources
                assert supplied is not None
                (root / "source.md").write_bytes(source_bytes(category))
                resolved = [resolve_source_paths(source, root) for source in supplied.documents]
                catalog = CatalogConfig(
                    documents=resolved, collection_revision=supplied.revision, model=fixture.models.selector
                )
                spec, _ = seed_prepared_snapshot(
                    root,
                    fixture_spec(root).model_copy(
                        update={"rules": fixture.rules, "documentation_catalog": catalog}
                    ),
                    fixture.documentation,
                )
                archived = DatasetRegistry(root).get_snapshot(spec.dataset_id)
                self.assertEqual(catalog_for_branch(archived, "dataset").documents, [])
                self.assertEqual(catalog_for_branch(archived, "dataset").mode, "provided_sources")
                self.assertEqual(snapshots_for_branch(archived, "dataset").documents, [])
                stored = catalog_for_branch(archived, "documentation")
                self.assertEqual(stored.collection_revision, supplied.revision)
                self.assertEqual(stored.model, fixture.models.selector)
                self.assertEqual(snapshots_for_branch(archived, "documentation").documents, stored.documents)
                for original, actual in zip(supplied.documents, stored.documents, strict=True):
                    self.assertEqual(actual.local_path, str(root / "source.md"))
                    self.assertEqual(
                        actual.model_dump(exclude={"local_path"}), original.model_dump(exclude={"local_path"})
                    )
                    self.assertEqual(actual.to_source_document().sha256, actual.sha256)

    def test_missing_documentation_declaration_fails_instead_of_producing_an_empty_branch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = write_declared_bundle(root / "business")
            path = bundle.parent.parent / "categories/cat3/collection/documentation-crawl.json"
            path.unlink()
            with self.assertRaises(FileNotFoundError):
                load_build_config(bundle, root)

    def test_online_documentation_source_search_is_rejected_as_pipeline_input(self) -> None:
        search = SearchSeeds(searches=[RiskSearchSeeds(risk="CAT-99.1", queries=["Git examples"])])
        with self.assertRaises(ValidationError):
            DocumentationConfig(search_request=search)

    def test_documentation_inventory_rejects_corpora_and_undocumented_origins(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            spec = fixture_spec(Path(temporary))
        collection = spec.declared_collection
        assert collection is not None
        declaration = collection.documentation_crawl.declaration
        assert isinstance(declaration, RepositoryBranchDeclaration)
        original = declaration.model_dump(mode="json")
        for source_type, provenance in (
            ("community_command_corpus", "human_authored"),
            ("community_command_corpus", "human_collected"),
            ("community_command_corpus", "community_documentation"),
            ("agent_results", "community_documentation"),
            ("sft_examples", "community_documentation"),
            ("synthetic_examples", "community_documentation"),
            ("generated_examples", "community_documentation"),
            ("maintainer_documentation", None),
            ("maintainer_documentation", "unknown"),
            ("maintainer_documentation", "human_authored"),
        ):
            with self.subTest(source_type=source_type, provenance=provenance):
                origin, *remaining = original["origins"]
                changed = {**origin, "source_type": source_type, "provenance_type": provenance}
                with self.assertRaises((ValidationError, IntegrityError)):
                    RepositoryBranchDeclaration.model_validate_json(
                        json.dumps({**original, "origins": [changed, *remaining]})
                    ).runtime("documentation")
        self.assertTrue(
            all(
                origin.source_type == "community_command_corpus"
                for origin in collection.repository_config("dataset").origins("dataset")
            )
        )

    def test_local_pinned_source_and_taxonomy_support_multiple_categories(self) -> None:
        cases: tuple[tuple[FixtureCategory, str, str], ...] = (
            ("CAT-99", "git-status.md", "git status"),
            ("CAT-98", "terraform-destroy.md", "terraform destroy -auto-approve"),
        )
        for category, filename, command in cases:
            with self.subTest(category=category), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture = example_config(category)
                assert fixture.documentation_sources is not None
                raw = source_bytes(category)
                (root / filename).write_bytes(raw)
                source = resolve_source_paths(
                    fixture.documentation_sources.documents[0].model_copy(update={"local_path": filename}),
                    root,
                )
                document = provided_documents([source])[0]
                documentation = documentation_table(fixture.documentation)
                self.assertEqual(set(root.iterdir()), {root / filename})
                self.assertEqual(document.local_path, root / filename)
                self.assertEqual(document.sha256, hashlib.sha256(raw).hexdigest())
                self.assertIn(command, document.local_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    set(documentation["url"]), {item.url for item in fixture.documentation.snippets}
                )
                self.assertEqual(fixture.rules.matching_risks(command), [f"{category}.1"])

    def test_declared_stage_knobs_and_category_rules_are_pinned_at_registration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            file = write_declared_bundle(root / "business")
            payload = json.loads(file.read_bytes())
            payload["filtering"] = {"maximum_command_characters": 765}
            payload["release"] = {"release_cap_per_risk": 13}
            file.write_text(json.dumps(payload))
            spec = load_build_config(file, root)
            registry = DatasetRegistry(root)
            registry.register(spec)
            file.unlink()
            restored = registry.get(spec.dataset_id)
        self.assertIn("CAT-3.5", restored.rules.matching_risks("docker compose up"))
        self.assertNotIn("CAT-3.5", restored.rules.matching_risks("docker compose down --volumes"))
        self.assertEqual(restored.filtering.maximum_command_characters, 765)
        self.assertEqual(restored.publication.limit_per_risk, 13)

    def test_registry_and_labeler_revisions_do_not_change_fixed_acquisition_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = write_declared_bundle(root / "business")
            declaration = bundle.parent.parent / "categories/cat3/keenable/declaration.json"
            settings = json.loads(declaration.read_bytes())
            settings["search"]["queries"] = ["Container inventory examples"]
            declaration.write_text(json.dumps(settings))
            first = load_build_config(bundle, root)
            payload = json.loads(bundle.read_bytes())
            payload["revision_label"] = "new-registry-version"
            payload["labeling"]["model"]["model"] = "another/labeler"
            bundle.write_text(json.dumps(payload))
            second = load_build_config(bundle, root)
            dataset = bundle.parent.parent / "categories/cat3/collection/existing-datasets.json"
            payload = json.loads(dataset.read_bytes())
            payload["refresh_revision"] = "refresh-2"
            dataset.write_text(json.dumps(payload))
            third = load_build_config(bundle, root)
        for branch in ("dataset", "documentation", "keenable"):
            self.assertEqual(declared_source_config(first, branch), declared_source_config(second, branch))
        revised = declared_source_config(third, "dataset")
        self.assertNotEqual(
            revised.declaration_digest, declared_source_config(second, "dataset").declaration_digest
        )
        self.assertEqual(
            revised.repository_config.origins("dataset"),
            declared_source_config(second, "dataset").repository_config.origins("dataset"),
        )
        for branch in ("documentation", "keenable"):
            self.assertEqual(declared_source_config(second, branch), declared_source_config(third, branch))

    def test_incompatible_modes_duplicate_ids_and_unknown_fields_are_rejected(self) -> None:
        fixture = example_config()
        assert fixture.documentation_sources is not None
        seed: SourceSeed = fixture.documentation_sources.documents[0]
        search = SearchSeeds(searches=[RiskSearchSeeds(risk="CAT-99.1", queries=["Git examples"])])
        cases: list[dict[str, JsonValue]] = [
            {"search_request": search.model_dump()},
            {
                "mode": "provided_sources",
                "documents": [seed.model_dump()],
                "search_request": search.model_dump(),
            },
            {"mode": "keenable"},
            {"mode": "keenable", "documents": [seed.model_dump()], "search_request": search.model_dump()},
            {"mode": "provided_sources", "documents": [seed.model_dump(), seed.model_dump()]},
        ]
        for values in cases:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                CatalogConfig.model_validate(values)
        with self.assertRaises(ValidationError):
            SnapshotConfig(documents=[seed, seed])
        with self.assertRaises(ValidationError):
            SearchSeeds.model_validate({**search.model_dump(), "max_chars": "4000"})
        with self.assertRaises(ValidationError):
            SearchSeeds(searches=search.searches * 2)
        with self.assertRaises(ValidationError):
            RiskSearchSeeds(risk="CAT-99.1", queries=[" "])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = write_declared_bundle(root / "business")
            original = json.loads(path.read_bytes())
            for update in (
                {"rules_file": "external.json"},
                {"seeds": {"mode": "keenable", "search_request": search.model_dump()}},
                {"documentation_sources": {"mode": "keenable", "search_request": search.model_dump()}},
                {"seeds": {"documents": [seed.model_dump()], "manifest": "old.jsonl"}},
            ):
                with self.subTest(update=update), self.assertRaises(ValidationError):
                    path.write_text(json.dumps({**original, **update}))
                    load_build_config(path, root)
