from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from test_repository import CONFIG, TLDR, RepositoryFixture

from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.repository import (
    RepositoryRun,
    load_repository_config,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_acquisition import CAPTURED_SOURCES
from codebench_pipeline.repository_receipts import read_receipt
from codebench_pipeline.storage import IntegrityError


class LegacyRepositoryCaptureTests(TestCase):
    def setUp(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        self.run_context = RepositoryRun(home, "migration-fixture", "run")
        self.legacy_root = Path(self.enterContext(TemporaryDirectory()))
        (home / "source-downloads").symlink_to(self.legacy_root, target_is_directory=True)
        shipped = load_repository_config(CONFIG)
        fixture = RepositoryFixture(shipped.documentation[0])
        fixture.file("pages/common/docker.md", TLDR)
        self.blob = fixture.entries["pages/common/docker.md"][2]
        self.config = shipped.model_copy(update={"documentation": (fixture.declaration(),)})
        responses = iter((fixture.response(), fixture.bodies["LICENSE"]))
        with patch("codebench_pipeline.repository_acquisition.fetch_repository_bytes", side_effect=responses):
            self.catalog = repository_catalog(self.config, "documentation", self.run_context)
        self.enterContext(
            patch(
                "codebench_pipeline.repository_acquisition.fetch_repository_bytes",
                side_effect=AssertionError("Legacy migration must not use HTTPS"),
            )
        )

    def test_cache_import_checks_sha_and_git_blob_without_mutating_original(self) -> None:
        key = f"{self.blob}/body"
        store = ObjectStore(StorageConfig(local_root=str(self.legacy_root), prefix="sources"))
        store.write(key, TLDR)
        prepared_cache = self.legacy_root / "sources" / key
        before = {path: path.stat().st_mtime_ns for path in prepared_cache.rglob("*")}

        result = repository_documents(self.catalog, self.config, self.run_context)

        receipt = read_receipt(result, "source_documents", self.run_context)
        (capture,) = CAPTURED_SOURCES.validate_python(receipt.observations["captures"])
        self.assertEqual((capture.acquired_via, capture.response_status), ("verified_prepared_cache", None))
        self.assertEqual(capture.sha256, hashlib.sha256(TLDR).hexdigest())
        self.assertEqual((self.run_context.root / capture.capture_key).read_bytes(), TLDR)
        self.assertEqual({path: path.stat().st_mtime_ns for path in prepared_cache.rglob("*")}, before)

    def test_migration_refuses_incomplete_wrong_sized_or_corrupt_legacy_objects(self) -> None:
        missing = None
        wrong = b"x" * len(TLDR)
        for content, pointer, reason in (
            (missing, hashlib.sha256(TLDR).hexdigest(), "Incomplete legacy cache object"),
            (b"short", hashlib.sha256(TLDR).hexdigest(), "incorrect type or size"),
            (wrong, hashlib.sha256(TLDR).hexdigest(), "Legacy cache object SHA-256 differs"),
            (wrong, hashlib.sha256(wrong).hexdigest(), "Repository capture Git blob differs"),
        ):
            with self.subTest(reason=reason), TemporaryDirectory(dir=self.legacy_root) as destination:
                directory = Path(destination) / "sources" / self.blob / "body"
                (directory / pointer).mkdir(parents=True)
                (directory / "current").write_text(pointer, encoding="ascii")
                if content is not None:
                    (directory / pointer / "data").write_bytes(content)
                alias = self.run_context.dataset_home / "source-downloads"
                alias.unlink()
                alias.symlink_to(destination, target_is_directory=True)

                with self.assertRaisesRegex(IntegrityError, reason):
                    repository_documents(self.catalog, self.config, self.run_context)

                self.assertFalse((self.run_context.root / "blobs").exists())
                self.assertEqual(len(list((self.run_context.root / "receipts").glob("*.json"))), 1)

    def test_migration_never_follows_a_legacy_object_member_symlink(self) -> None:
        key = f"{self.blob}/body"
        checksum = hashlib.sha256(TLDR).hexdigest()
        directory = self.legacy_root / "sources" / key / checksum
        directory.mkdir(parents=True)
        (directory.parent / "current").write_text(checksum, encoding="ascii")
        outside = self.run_context.dataset_home / "outside.bin"
        outside.write_bytes(TLDR)
        (directory / "data").symlink_to(outside)

        with self.assertRaises(OSError):
            repository_documents(self.catalog, self.config, self.run_context)

        self.assertFalse((self.run_context.root / "blobs").exists())
