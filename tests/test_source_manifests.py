import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import example_config, seed_prepared_snapshot, source_bytes
from test_dataset_rules import make_candidate
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_config import CatalogConfig, DatasetBuildSpec
from codebench_pipeline.dataset_extract import filter_candidates
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.source_manifests import (
    DocumentationManifest,
    SourceManifest,
    manifest_documentation,
    manifest_sources,
    resolve_source_paths,
    validate_transformations,
)
from codebench_pipeline.source_transform_contract import SourceTransformConfig
from codebench_pipeline.storage import IntegrityError


class SourceManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        fixture = example_config()
        assert fixture.documentation_sources is not None
        (self.root / "source.md").write_bytes(source_bytes())
        self.source = resolve_source_paths(fixture.documentation_sources.documents[0], self.root)
        self.catalog = CatalogConfig(documents=[self.source])
        self.documentation = fixture.documentation
        self.spec = fixture_spec(self.root).model_copy(update={"rules": fixture.rules})

    def test_registry_reuses_validation_without_sharing_mutable_values(self) -> None:
        registry = DatasetRegistry(self.root)
        archived, revision = seed_prepared_snapshot(
            self.root,
            self.spec.model_copy(update={"documentation_catalog": self.catalog}),
            self.documentation,
        )
        with patch.object(
            DatasetBuildSpec, "model_validate_json", wraps=DatasetBuildSpec.model_validate_json
        ) as validate:
            first = registry.get_snapshot(archived.dataset_id, revision=revision)
            first.rules.risks.clear()
            assert first.documentation_catalog is not None
            first.documentation_catalog.documents[0].transformations.append("caller-only")
            second = DatasetRegistry(self.root).get_snapshot(archived.dataset_id, revision=revision)
        self.assertEqual(validate.call_count, 1)
        self.assertEqual(second.rules.risks, self.spec.rules.risks)
        assert second.documentation_catalog is not None
        self.assertEqual(second.documentation_catalog.documents[0].transformations, [])

    def test_source_manifest_cache_tracks_bytes_not_timestamps(self) -> None:
        path = self.root / "cached-sources.json"
        manifest = SourceManifest(category_id="CAT-99", revision="first", sources=[self.source])
        path.write_text(manifest.model_dump_json())
        original = path.stat()
        with patch.object(
            SourceManifest, "model_validate_json", wraps=SourceManifest.model_validate_json
        ) as validate:
            first = manifest_sources(path, "CAT-99")
            first[0].transformations.append("caller-only")
            self.assertEqual(manifest_sources(path, "CAT-99")[0].transformations, [])
            self.assertEqual(validate.call_count, 1)
            changed = self.source.model_copy(update={"source_id": self.source.source_id[:-1] + "x"})
            path.write_text(manifest.model_copy(update={"sources": [changed]}).model_dump_json())
            os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
            self.assertEqual(manifest_sources(path, "CAT-99")[0].source_id, changed.source_id)
            self.assertEqual(validate.call_count, 2)

    def test_historical_registry_does_not_resolve_superseded_manifests(self) -> None:
        sources = self.root / "live-sources.json"
        current_source = self.source.model_copy(update={"source_id": "current-source"})
        sources.write_text(
            SourceManifest(
                category_id="CAT-99", revision="current", sources=[current_source]
            ).model_dump_json()
        )
        semantics = self.root / "live-semantics.json"
        current_snippet = self.documentation.snippets[0].model_copy(
            update={"snippet": "Current documented operation."}
        )
        semantics.write_text(
            DocumentationManifest(
                category_id="CAT-99", revision="current", snippets=[current_snippet]
            ).model_dump_json()
        )
        stored, revision = seed_prepared_snapshot(
            self.root,
            self.spec.model_copy(
                update={"documentation_catalog": self.catalog.model_copy(update={"manifest": str(sources)})}
            ),
            self.documentation.model_copy(update={"manifest": str(semantics)}),
        )
        registry = DatasetRegistry(self.root)
        self.assertEqual(manifest_sources(sources, "CAT-99")[0].source_id, "current-source")
        self.assertEqual(
            manifest_documentation(semantics, "CAT-99")[0].snippet, "Current documented operation."
        )
        sources.unlink()
        semantics.unlink()
        historical = registry.get_snapshot(stored.dataset_id, revision=revision)
        self.assertEqual(historical.documentation_catalog, stored.documentation_catalog)
        self.assertEqual(historical.documentation, stored.documentation)
        with self.assertRaisesRegex(IntegrityError, "read-only"):
            registry.get(stored.dataset_id)

    def test_documentation_manifest_cache_does_not_share_subcommands(self) -> None:
        path = self.root / "cached-semantics.json"
        manifest = DocumentationManifest(
            category_id="CAT-99", revision=self.root.name, snippets=self.documentation.snippets
        )
        path.write_text(manifest.model_dump_json())
        with patch.object(
            DocumentationManifest, "model_validate_json", wraps=DocumentationManifest.model_validate_json
        ) as validate:
            first = manifest_documentation(path, "CAT-99")
            first[0].subcommands.clear()
            second = manifest_documentation(path, "CAT-99")
        self.assertEqual(validate.call_count, 1)
        self.assertEqual(second[0].subcommands, self.documentation.snippets[0].subcommands)
        path.write_text(manifest.model_copy(update={"category_id": "CAT-98"}).model_dump_json())
        with self.assertRaises(IntegrityError):
            manifest_documentation(path, "CAT-99")

    def test_cached_manifest_resolves_changed_symlink_targets(self) -> None:
        assert self.source.local_path is not None
        original = Path(self.source.local_path)
        alternate = self.root / "alternate.md"
        alternate.write_bytes(original.read_bytes())
        link = self.root / "capture.md"
        link.symlink_to(original)
        path = self.root / "linked-sources.json"
        path.write_text(
            SourceManifest(
                category_id="CAT-99",
                revision="first",
                sources=[self.source.model_copy(update={"local_path": "capture.md"})],
            ).model_dump_json()
        )
        first = manifest_sources(path, "CAT-99")
        link.unlink()
        link.symlink_to(alternate)
        second = manifest_sources(path, "CAT-99")
        self.assertEqual(first[0].local_path, str(original.resolve()))
        self.assertEqual(second[0].local_path, str(alternate.resolve()))

    def test_cached_manifest_still_rejects_changed_transformation_bytes_on_validation(self) -> None:
        module = self.root / "trusted.py"
        module.write_text("def transform(rows, context):\n    return []\n")
        transform = SourceTransformConfig(
            module_path=str(module),
            sha256=hashlib.sha256(module.read_bytes()).hexdigest(),
            entrypoint="transform",
            trusted=True,
        )
        path = self.root / "transformation-sources.json"
        path.write_text(
            SourceManifest(
                category_id="CAT-99",
                revision="fixture",
                sources=[self.source.model_copy(update={"transform": transform})],
            ).model_dump_json()
        )
        validate_transformations(manifest_sources(path, "CAT-99"))
        module.write_text("raise RuntimeError('changed module')\n")
        with self.assertRaises(IntegrityError):
            validate_transformations(manifest_sources(path, "CAT-99"))

    def test_noncommand_tokens_with_trailing_slashes_remain_inspectable_and_are_filtered(self) -> None:
        commands = [
            ("//", "//"),
            ("https://example.org/reference/", "https://example.org/reference/"),
            ("git status", "git"),
        ]
        candidates = validated_frame(
            [
                make_candidate(str(index))
                .model_copy(update={"command": command, "util": utility})
                .model_dump(mode="json")
                for index, (command, utility) in enumerate(commands)
            ],
            CommandCandidatesTable,
        )
        filtered = filter_candidates(candidates, self.spec.rules, self.spec.filtering)
        self.assertEqual(list(filtered["command"]), [command for command, _ in commands])
        self.assertEqual(list(filtered["util"]), ["//", "https://example.org/reference/", "git"])
        self.assertEqual(
            list(filtered["filter_reason"]),
            ["outside_configured_command_families", "outside_configured_command_families", ""],
        )

    def test_manifest_category_and_paths_are_bound_and_file_changes_are_resolved(self) -> None:
        manifest_path = self.root / "sources.json"
        manifest = SourceManifest(
            category_id="CAT-99",
            revision="first",
            sources=[self.source.model_copy(update={"local_path": "source.md"})],
        )
        manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")
        self.assertEqual(manifest_sources(manifest_path, "CAT-99"), [self.source])
        changed_source = manifest.sources[0].model_copy(update={"source_id": "new-source"})
        manifest_path.write_text(
            manifest.model_copy(update={"sources": [changed_source]}).model_dump_json(), encoding="utf-8"
        )
        (current,) = manifest_sources(manifest_path, "CAT-99")
        self.assertEqual(current.source_id, "new-source")
        self.assertEqual(current.local_path, str(self.root / "source.md"))
        manifest_path.write_text(
            manifest.model_copy(update={"category_id": "CAT-98"}).model_dump_json(), encoding="utf-8"
        )
        with self.assertRaises(IntegrityError):
            manifest_sources(manifest_path, "CAT-99")
