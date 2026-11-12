import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

from configuration_fixture import write_fixture

from codebench_pipeline.dataframe_io import frame_bytes, read_frame
from codebench_pipeline.dataframe_schemas import SourceDocumentsTable
from codebench_pipeline.dataset_catalog import build_catalog
from codebench_pipeline.dataset_config import SnapshotConfig, load_build_config
from codebench_pipeline.dataset_documents import document_adapters, snapshot_documents
from codebench_pipeline.dataset_extract import extract_candidates, filter_candidates
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_seed_config import SourceSeed
from codebench_pipeline.source_manifests import SourceManifest
from codebench_pipeline.source_retrieval import retrieve_source
from codebench_pipeline.source_transform_contract import SourceTransformConfig
from codebench_pipeline.storage import IntegrityError


class SourceResponse(io.BytesIO):
    def __init__(self, url: str, body: bytes) -> None:
        super().__init__(body)
        self.url = url

    def geturl(self) -> str:
        return self.url


class SourceManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = write_fixture(self.root)
        self.spec = load_build_config(self.config, self.root)
        assert self.spec.documentation_catalog is not None
        self.source = self.spec.documentation_catalog.documents[0]

    def remote(self, body: bytes) -> SourceSeed:
        return SourceSeed.model_validate({
            **self.source.model_dump(),
            "local_path": None,
            "sha256": hashlib.sha256(body).hexdigest(),
        })

    def test_retrieval_keeps_full_body_beyond_inspection_limits_and_reuses_verified_bytes(self) -> None:
        body = b"# reference\n" + b"ordinary source text\n" * 4000 + b"`git status --show-stash`\n"
        source = self.remote(body)
        with patch(
            "codebench_pipeline.source_retrieval.urlopen",
            return_value=SourceResponse(source.url, body),
        ):
            first = retrieve_source(source, len(body), self.root / "cache", self.root / "receipts")
        with patch(
            "codebench_pipeline.source_retrieval.urlopen",
            side_effect=AssertionError("An unchanged source must not be fetched twice"),
        ):
            second = retrieve_source(source, len(body), self.root / "cache", None)
        self.assertEqual(first.content.encode(), body)
        self.assertEqual(second.content.encode(), body)
        self.assertEqual(second.sha256, hashlib.sha256(body).hexdigest())
        self.assertFalse(first.cache_hit)
        self.assertTrue(second.cache_hit)

    def test_wrong_hash_and_oversized_sources_never_enter_the_source_cache(self) -> None:
        body = b"git status\n"
        for maximum, received in ((len(body), b"git diff\n"), (len(body) - 1, body)):
            with self.subTest(maximum=maximum, received=received):
                source = self.remote(body)
                cache = self.root / str(maximum)
                with patch(
                    "codebench_pipeline.source_retrieval.urlopen",
                    return_value=SourceResponse(source.url, received),
                ), self.assertRaises(IntegrityError):
                    retrieve_source(source, maximum, cache, None)
                self.assertEqual(list(cache.rglob("body")), [])

    def test_transport_recovery_preserves_attempt_evidence_and_verified_body(self) -> None:
        body = b"git status\n"
        source = self.remote(body)
        evidence = self.root / "receipts"
        with patch(
            "codebench_pipeline.source_retrieval.urlopen",
            side_effect=[URLError("temporary DNS failure"), SourceResponse(source.url, body)],
        ):
            result = retrieve_source(source, 100, self.root / "cache", evidence)
        self.assertEqual(result.content.encode(), body)
        identity = hashlib.sha256(source.source_id.encode()).hexdigest()
        self.assertEqual(json.loads((evidence / f"{identity}.json").read_text())["download_attempts"], 2)
        self.assertEqual(
            json.loads((evidence / f"{identity}.attempt-1.json").read_text())["error_type"], "URLError"
        )

    def test_transport_recovery_is_bounded_and_never_caches_failed_content(self) -> None:
        source = self.remote(b"git status\n")
        with patch(
            "codebench_pipeline.source_retrieval.urlopen",
            side_effect=URLError("unavailable host"),
        ) as fetch, self.assertRaises(URLError):
            retrieve_source(source, 100, self.root / "cache", self.root / "receipts")
        self.assertEqual(fetch.call_count, 3)
        self.assertEqual(list((self.root / "cache").rglob("body")), [])
        self.assertEqual(len(list((self.root / "receipts").glob("*.json"))), 3)

    def test_git_blob_identity_is_checked_independently_of_sha256(self) -> None:
        body = b"git status\n"
        source = SourceSeed.model_validate({
            **self.source.model_dump(),
            "local_path": None,
            "sha256": None,
            "git_blob_sha1": hashlib.sha1(b"blob 11\0" + body, usedforsecurity=False).hexdigest(),
        })
        with patch(
            "codebench_pipeline.source_retrieval.urlopen",
            return_value=SourceResponse(source.url, body),
        ):
            result = retrieve_source(source, 100, None, None)
        self.assertEqual(result.sha256, hashlib.sha256(body).hexdigest())
        changed = source.model_copy(update={"git_blob_sha1": "0" * 40})
        with patch(
            "codebench_pipeline.source_retrieval.urlopen",
            return_value=SourceResponse(source.url, body),
        ), self.assertRaises(IntegrityError):
            retrieve_source(changed, 100, None, None)

    def test_persisted_source_metadata_preserves_trusted_extraction_and_provenance(self) -> None:
        module = Path(__file__).resolve().parents[1] / "transforms" / "tldr.py"
        transform = SourceTransformConfig(
            module_path=str(module),
            sha256=hashlib.sha256(module.read_bytes()).hexdigest(),
            entrypoint="transform",
            trusted=True,
        )
        source = self.source.model_copy(
            update={"transform": transform, "transformations": ["source_prepared"]}
        )
        assert self.spec.documentation_catalog is not None
        config = self.spec.documentation_catalog.model_copy(update={"documents": [source]})
        catalog = build_catalog(config, self.spec.rules, self.root, branch="documentation")
        documents = snapshot_documents(catalog, SnapshotConfig(documents=[source]))
        restored = SourceDocumentsTable.validate(read_frame(frame_bytes(documents)))
        adapters = document_adapters(restored, self.spec.adapters, self.root)
        candidates = extract_candidates(restored, adapters, self.spec.extraction)
        self.assertEqual(list(candidates["command"]), ["git status", "git status --show-stash"])
        expected = ["source_prepared", f"trusted_python:{transform.sha256}:transform"]
        self.assertEqual([markers[:2] for markers in candidates["transformations"]], [expected, expected])

    def test_noncommand_tokens_with_trailing_slashes_remain_inspectable_and_are_filtered(self) -> None:
        body = b"//\nhttps://example.org/reference/\ngit status\n"
        assert self.source.local_path is not None
        Path(self.source.local_path).write_bytes(body)
        source = self.source.model_copy(
            update={"format": "text", "sha256": hashlib.sha256(body).hexdigest()}
        )
        assert self.spec.documentation_catalog is not None
        config = self.spec.documentation_catalog.model_copy(update={"documents": [source]})
        catalog = build_catalog(config, self.spec.rules, self.root, branch="documentation")
        documents = snapshot_documents(catalog, SnapshotConfig(documents=[source]))
        adapters = document_adapters(documents, self.spec.adapters, self.root)
        candidates = extract_candidates(documents, adapters, self.spec.extraction)
        filtered = filter_candidates(candidates, self.spec.rules, self.spec.filtering)
        self.assertEqual(list(filtered["command"]), body.decode().splitlines())
        self.assertEqual(list(filtered["util"]), ["//", "https://example.org/reference/", "git"])
        self.assertEqual(
            list(filtered["filter_reason"]),
            ["outside_configured_command_families", "outside_configured_command_families", ""],
        )

    def test_manifest_category_and_paths_are_bound_and_registered_changes_are_resolved(self) -> None:
        manifest_path = self.root / "sources.json"
        manifest = SourceManifest(
            category_id="CAT-99", revision="first", sources=[self.source]
        )
        manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")
        config = json.loads(self.config.read_text())
        config["models"] = {"labeler": config["models"]["labeler"]}
        config["documentation_sources"] = {"manifest": str(manifest_path)}
        self.config.write_text(json.dumps(config), encoding="utf-8")
        initial = load_build_config(self.config, self.root)
        DatasetRegistry(self.root).register(initial)
        changed_source = self.source.model_copy(update={"source_id": "new-source"})
        manifest_path.write_text(
            manifest.model_copy(update={"sources": [changed_source]}).model_dump_json(), encoding="utf-8"
        )
        current = DatasetRegistry(self.root).get(initial.dataset_id)
        assert current.documentation_catalog is not None
        self.assertEqual([source.source_id for source in current.documentation_catalog.documents], ["new-source"])
        self.assertEqual(current.documentation_snapshots.documents, current.documentation_catalog.documents)
        manifest_path.write_text(
            manifest.model_copy(update={"category_id": "CAT-98"}).model_dump_json(), encoding="utf-8"
        )
        with self.assertRaises(IntegrityError):
            DatasetRegistry(self.root).get(initial.dataset_id)
