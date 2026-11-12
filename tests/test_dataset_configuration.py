from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from configuration_fixture import FixtureCategory, example_config, source_bytes, write_fixture
from dagster import Config
from pydantic import JsonValue, ValidationError

from codebench_pipeline.business_config import DEFAULT_BUSINESS_CONFIG, load_business_rules
from codebench_pipeline.dataframe_schemas import SourceCatalogTable, validated_frame
from codebench_pipeline.dataset_catalog import catalog_row, provided_documents
from codebench_pipeline.dataset_config import (
    CatalogConfig,
    DatasetFileConfig,
    DatasetSeeds,
    DocumentationConfig,
    SnapshotConfig,
    catalog_for_branch,
    load_build_config,
    snapshots_for_branch,
)
from codebench_pipeline.dataset_documents import snapshot_documents
from codebench_pipeline.dataset_seed_config import (
    RiskSearchSeeds,
    SearchSeeds,
)
from codebench_pipeline.dataset_source_models import SourceChoice
from codebench_pipeline.dataset_tables import documentation_table
from codebench_pipeline.storage import IntegrityError


def write_config(root: Path, config: DatasetFileConfig) -> Path:
    path = root / "dataset.json"
    path.write_text(config.model_dump_json(), encoding="utf-8")
    return path


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

    def test_example_documentation_inventories_are_independent_of_empty_dataset_inventories(self) -> None:
        for category in ("CAT-99", "CAT-98"):
            with self.subTest(category=category), tempfile.TemporaryDirectory() as temporary:
                path = write_fixture(Path(temporary), category)
                config = example_config(category)
                assert config.documentation_sources is not None
                spec = load_build_config(path, Path(temporary))
                catalog = catalog_for_branch(spec, "documentation")
                snapshots = snapshots_for_branch(spec, "documentation")
                self.assertEqual(catalog_for_branch(spec, "dataset").documents, [])
                self.assertEqual(catalog_for_branch(spec, "dataset").mode, "provided_sources")
                self.assertEqual(snapshots_for_branch(spec, "dataset").documents, [])
                self.assertEqual(catalog.collection_revision, config.documentation_sources.revision)
                self.assertEqual(catalog.model, config.models.selector)
                self.assertEqual(snapshots.documents, catalog.documents)
                for supplied, resolved in zip(
                    config.documentation_sources.documents, catalog.documents, strict=True
                ):
                    assert supplied.local_path is not None
                    self.assertEqual(resolved.local_path, str((path.parent / supplied.local_path).resolve()))
                    self.assertEqual(
                        resolved.model_dump(exclude={"local_path"}),
                        supplied.model_dump(exclude={"local_path"}),
                    )
                    resolved.to_source_document()

    def test_absent_documentation_inventory_supplies_only_empty_nonselected_configuration(self) -> None:
        original = example_config()
        config = original.model_copy(update={"documentation_sources": None})
        with tempfile.TemporaryDirectory() as temporary:
            spec = load_build_config(write_config(Path(temporary), config))
        self.assertIsNone(spec.documentation_catalog)
        self.assertEqual(
            catalog_for_branch(spec, "documentation"),
            CatalogConfig(mode="provided_sources", model=config.models.selector),
        )
        self.assertEqual(snapshots_for_branch(spec, "documentation").documents, [])
        self.assertEqual(DatasetSeeds(mode="provided_sources").documents, [])

    def test_online_documentation_source_search_is_rejected_as_pipeline_input(self) -> None:
        search = SearchSeeds(searches=[RiskSearchSeeds(risk="CAT-99.1", queries=["Git examples"])])
        with self.assertRaises(ValidationError):
            DatasetSeeds.model_validate({"mode": "keenable", "search_request": search.model_dump()})

    def test_documentation_inventory_rejects_corpora_and_undocumented_origins(self) -> None:
        config = example_config()
        assert config.documentation_sources is not None
        original = config.documentation_sources.documents[0].model_dump()
        for source_type, provenance in (
            ("bash_dataset", "human_authored"),
            ("command_corpus", "human_collected"),
            ("command_benchmark", "human_authored"),
            ("agent_results", "community_documentation"),
            ("sft_examples", "community_documentation"),
            ("synthetic_examples", "community_documentation"),
            ("generated_examples", "community_documentation"),
            ("maintainer_documentation", None),
            ("maintainer_documentation", "unknown"),
            ("maintainer_documentation", "human_authored"),
        ):
            with self.subTest(source_type=source_type, provenance=provenance):
                values = {
                    **original,
                    "source_type": source_type,
                    "provenance_type": provenance,
                }
                with self.assertRaises(ValidationError):
                    DatasetFileConfig.model_validate(
                        {
                            **config.model_dump(),
                            "documentation_sources": {"mode": "provided_sources", "documents": [values]},
                        }
                    )
                if source_type in {"bash_dataset", "command_corpus", "command_benchmark"}:
                    dataset = DatasetFileConfig.model_validate(
                        {
                            **config.model_dump(),
                            "seeds": {"mode": "provided_sources", "documents": [values]},
                        }
                    )
                    self.assertEqual(dataset.seeds.documents[0].source_type, source_type)

    def test_bounded_inspection_capture_cannot_substitute_for_a_source_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            spec = load_build_config(write_fixture(Path(temporary)), Path(temporary))
            document = provided_documents(catalog_for_branch(spec, "documentation").documents)[0]
            row = catalog_row(document, None, "bounded inspection")
            row["captured_content"] = "git status"
            catalog = validated_frame([row], SourceCatalogTable)
            with self.assertRaises(IntegrityError):
                snapshot_documents(catalog, SnapshotConfig())

    def test_single_config_and_raw_bytes_supply_both_categories(self) -> None:
        cases: tuple[tuple[FixtureCategory, str, str], ...] = (
            ("CAT-99", "git-status.md", "git status"),
            ("CAT-98", "terraform-destroy.md", "terraform destroy -auto-approve"),
        )
        for category, filename, command in cases:
            with self.subTest(category=category), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                original = example_config(category)
                assert original.documentation_sources is not None
                raw = source_bytes(category)
                (root / filename).write_bytes(raw)
                seed = original.documentation_sources.documents[0].model_copy(update={"local_path": filename})
                config = original.model_copy(
                    update={"seeds": original.seeds.model_copy(update={"documents": [seed]})}
                )
                path = write_config(root, config)

                spec = load_build_config(path, root / "runtime")
                document = provided_documents(spec.catalog.documents)[0]
                catalog = validated_frame(
                    [
                        catalog_row(
                            document,
                            SourceChoice(
                                source_id=document.source_id,
                                decision="selected",
                                rationale="Inspected maintainer source.",
                                analysis="Relevant command examples occur in the inspected source.",
                                confidence="high",
                            ),
                            raw.decode(),
                        )
                    ],
                    SourceCatalogTable,
                )
                snapshots = snapshot_documents(catalog, spec.snapshots)
                documentation = documentation_table(spec.documentation)

                self.assertEqual(set(root.iterdir()), {path, root / filename})
                self.assertEqual(document.local_path, (root / filename).resolve())
                self.assertEqual(document.sha256, hashlib.sha256(raw).hexdigest())
                self.assertEqual(snapshots.iloc[0]["content"], raw.decode())
                self.assertIn(command, snapshots.iloc[0]["content"])
                self.assertEqual(
                    set(documentation["url"]),
                    {snippet.url for snippet in config.documentation.snippets},
                )
                self.assertEqual(spec.rules.matching_risks(command), [f"{category}.1"])

    def test_category_patterns_and_stage_knobs_are_loaded_only_from_initial_file(self) -> None:
        config = example_config()
        risk = config.rules.risks[0].model_copy(
            update={"id": "CAT-91.1", "command_patterns": [r"^git\s+diff\b"]}
        )
        rules = config.rules.model_copy(update={"category_id": "CAT-91", "risks": [risk]})
        config = config.model_copy(
            update={
                "rules": rules,
                "snapshot_maximum_bytes": 4321,
                "adapters": "provided",
                "adapter_sample_characters": 321,
                "extraction": config.extraction.model_copy(update={"context_characters": 17}),
                "filtering": config.filtering.model_copy(update={"maximum_command_characters": 765}),
                "publication": config.publication.model_copy(update={"limit_per_risk": 13}),
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            spec = load_build_config(write_config(Path(temporary), config))

        self.assertEqual(spec.rules.matching_risks("git diff --stat"), ["CAT-91.1"])
        self.assertEqual(spec.rules.matching_risks("git status"), [])
        self.assertEqual(spec.snapshots.maximum_bytes, 4321)
        self.assertEqual(spec.documentation_snapshots.maximum_bytes, 4321)
        self.assertEqual((spec.adapters.mode, spec.adapters.sample_characters), ("provided", 321))
        self.assertEqual(spec.extraction.context_characters, 17)
        self.assertEqual(spec.filtering.maximum_command_characters, 765)
        self.assertEqual(spec.publication.limit_per_risk, 13)
        self.assertEqual(example_config().rules.category_id, "CAT-99")

    def test_registry_revision_and_reviewer_do_not_change_acquisition_configuration(self) -> None:
        original = example_config()
        assert original.models.reviewer is not None
        changed = original.model_copy(
            update={
                "revision": "new-registry-version",
                "rules": original.rules.model_copy(update={"version": "new-category-version"}),
                "models": original.models.model_copy(
                    update={
                        "reviewer": original.models.reviewer.model_copy(update={"model": "another/reviewer"})
                    }
                ),
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = load_build_config(write_config(root, original), root)
            second = load_build_config(write_config(root, changed), root)
            refreshed = changed.model_copy(
                update={"seeds": changed.seeds.model_copy(update={"revision": "refresh-2"})}
            )
            third = load_build_config(write_config(root, refreshed), root)

        self.assertEqual(first.catalog, second.catalog)
        self.assertEqual(first.snapshots, second.snapshots)
        self.assertEqual(first.documentation_catalog, second.documentation_catalog)
        self.assertEqual(first.documentation_snapshots, second.documentation_snapshots)
        self.assertIsNone(first.review)
        self.assertIsNone(second.review)
        self.assertEqual(third.catalog.collection_revision, "refresh-2")
        self.assertEqual(third.catalog.documents, second.catalog.documents)

    def test_online_dataset_discovery_is_rejected_even_with_valid_search_settings(self) -> None:
        search = SearchSeeds(searches=[RiskSearchSeeds(risk="CAT-99.1", queries=["Git examples"])])
        with self.assertRaises(ValidationError):
            DatasetSeeds(mode="provided_sources", search_request=search)

    def test_incompatible_modes_duplicate_ids_and_unknown_fields_are_rejected(self) -> None:
        config = example_config()
        assert config.documentation_sources is not None
        seed = config.documentation_sources.documents[0]
        search = SearchSeeds(
            searches=[RiskSearchSeeds(risk="CAT-99.1", queries=["Git maintainer command examples"])]
        )
        cases: list[dict[str, JsonValue]] = [
            {
                "mode": "provided_sources",
                "documents": [seed.model_dump()],
                "search_request": search.model_dump(),
            },
            {"mode": "keenable"},
            {"mode": "keenable", "documents": [seed.model_dump()], "search_request": search.model_dump()},
            {"mode": "provided_sources", "documents": [seed.model_dump(), seed.model_dump()]},
            {"mode": "provided_sources", "documents": [seed.model_dump()], "manifest": "old.jsonl"},
        ]
        for values in cases:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                DatasetSeeds.model_validate(values)
        with self.assertRaises(ValidationError):
            SnapshotConfig(documents=[seed, seed])
        with self.assertRaises(ValidationError):
            DatasetFileConfig.model_validate({**config.model_dump(), "rules_file": "external.json"})
        with self.assertRaises(ValidationError):
            SearchSeeds.model_validate({**search.model_dump(), "max_chars": "4000"})
        with self.assertRaises(ValidationError):
            SearchSeeds(searches=search.searches * 2)
        with self.assertRaises(ValidationError):
            RiskSearchSeeds(risk="CAT-99.1", queries=[" "])
        for field in ("seeds", "documentation_sources"):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                DatasetFileConfig.model_validate(
                    {
                        **config.model_dump(),
                        field: {
                            "mode": "keenable",
                            "search_request": {
                                "searches": [{"risk": "CAT-3.1", "queries": ["Terraform maintainer docs"]}]
                            },
                        },
                    }
                )

    def test_inline_stage_configuration_has_dagster_compatible_nested_fields(self) -> None:
        config = example_config()
        for config_type in (CatalogConfig, SnapshotConfig, DocumentationConfig):
            with self.subTest(config_type=config_type):
                self.assertIsInstance(config_type.to_fields_dict(), dict)
        assert config.documentation_sources is not None
        self.assertIsInstance(config.documentation_sources.documents[0], Config)
        self.assertIsInstance(config.documentation.snippets[0], Config)
        self.assertIsInstance(SearchSeeds.to_fields_dict(), dict)
