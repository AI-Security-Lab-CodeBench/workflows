import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
from typing import Final
from unittest import TestCase
from unittest.mock import patch

from codebench_pipeline.collection_stage_settings import RepositoryCaptureOrigin
from codebench_pipeline.repository_acquisition import CaptureRuntime, CatalogSource, capture_sources
from codebench_pipeline.repository_receipts import RepositoryRun, publish_bytes
from codebench_pipeline.repository_tree import git_blob_sha1
from codebench_pipeline.storage import IntegrityError

CAPTURE_ORIGIN: Final = RepositoryCaptureOrigin(
    origin_id="synthetic", repository="example/fixture", commit="a" * 40
)
CAPTURE_BODY: Final = b"# Synthetic capture\n"
CAPTURE_SOURCE: Final = CatalogSource(
    origin_id=CAPTURE_ORIGIN.origin_id,
    source_id="synthetic-source",
    path="fixture.md",
    git_blob_sha1=git_blob_sha1(CAPTURE_BODY),
    size=len(CAPTURE_BODY),
)
CAPTURE_KEY: Final = f"blobs/{CAPTURE_SOURCE.git_blob_sha1}"
CAPTURE_DIGEST: Final = hashlib.sha256(CAPTURE_BODY).hexdigest()
FETCH: Final = "codebench_pipeline.repository_acquisition.fetch_repository_bytes"


class RepositoryReceiptIOTests(TestCase):
    def setUp(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        self.run_storage = RepositoryRun(home / "dataset", "cat3", "run")
        self.outside = home / "outside"
        self.outside.mkdir()
        self.run_storage.root.mkdir(parents=True)

    def test_rejects_reads_through_linked_directory_and_leaf(self) -> None:
        external = self.outside / "receipt.json"
        external.write_bytes(b"outside")
        (self.run_storage.root / "linked-directory").symlink_to(self.outside, target_is_directory=True)
        (self.run_storage.root / "linked-leaf").symlink_to(external)

        for key in ("linked-directory/receipt.json", "linked-leaf"):
            with self.subTest(key=key):
                with self.assertRaises(IntegrityError):
                    self.run_storage.read_evidence(key)
                with self.assertRaises(IntegrityError):
                    self.run_storage.read_capture(key, 64)

    def test_neither_reuses_nor_creates_files_through_symlinks(self) -> None:
        external = self.outside / "receipt.json"
        external.write_bytes(b"same bytes")
        root = self.run_storage.root
        (root / "linked-directory").symlink_to(self.outside, target_is_directory=True)
        (root / "linked-leaf").symlink_to(external)
        (root / "dangling-leaf").symlink_to(self.outside / "missing.json")

        for key in (
            "linked-directory/receipt.json",
            "linked-directory/new/file",
            "linked-leaf",
            "dangling-leaf",
        ):
            with self.subTest(key=key), self.assertRaises(IntegrityError):
                publish_bytes(root / key, b"same bytes")

        self.assertEqual(list(self.outside.iterdir()), [external])
        self.assertEqual(external.read_bytes(), b"same bytes")
        self.assertTrue((root / "linked-leaf").is_symlink())
        self.assertTrue((root / "dangling-leaf").is_symlink())

    def test_reusing_an_ordinary_file_preserves_its_inode_and_bytes(self) -> None:
        key = "receipts/receipt.json"
        target = self.run_storage.root / key
        self.run_storage.write_evidence(key, b"immutable")
        inode = target.stat().st_ino

        self.run_storage.write_evidence(key, b"immutable")

        self.assertEqual(target.stat().st_ino, inode)
        self.assertEqual(self.run_storage.read_evidence(key), b"immutable")
        self.assertEqual(list(target.parent.iterdir()), [target])

    def test_concurrent_identical_writes_reuse_winner_and_conflicts_do_not_replace_it(self) -> None:
        path = self.run_storage.root / "receipts" / "concurrent.json"
        payloads = (b"first", b"first", b"second", b"second")
        barrier = Barrier(len(payloads))

        def publish(payload: bytes) -> bool:
            barrier.wait(timeout=5)
            try:
                publish_bytes(path, payload)
            except IntegrityError:
                return False
            return True

        with ThreadPoolExecutor(max_workers=len(payloads)) as executor:
            accepted = tuple(executor.map(publish, payloads))

        retained = path.read_bytes()
        self.assertIn(retained, payloads)
        self.assertEqual(accepted, tuple(payload == retained for payload in payloads))
        self.assertEqual(list(path.parent.iterdir()), [path])


class PinnedCaptureRestartTests(TestCase):
    def setUp(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        self.initial = RepositoryRun(home, "synthetic", "initial")
        self.restart = RepositoryRun(home, "synthetic", "restart")

    def test_restart_reuses_complete_blob_without_fetching_or_replacing_it(self) -> None:
        sources = (CAPTURE_SOURCE,)
        with patch(FETCH, return_value=CAPTURE_BODY):
            capture_sources(sources, CAPTURE_ORIGIN, CaptureRuntime(64, 1, self.initial, None))
        blob = self.initial.root / CAPTURE_KEY
        inode = blob.stat().st_ino

        with patch(FETCH, side_effect=AssertionError("restart fetched a complete blob")):
            ((capture, text),) = capture_sources(
                sources, CAPTURE_ORIGIN, CaptureRuntime(64, 1, self.restart, None)
            )

        self.assertEqual(capture.acquired_via, "repository_cache")
        self.assertEqual(capture.capture_key, CAPTURE_KEY)
        self.assertEqual(capture.sha256, CAPTURE_DIGEST)
        self.assertEqual(text, CAPTURE_BODY.decode())
        self.assertEqual(blob.stat().st_ino, inode)

    def test_restart_fetches_blob_when_only_checksum_was_published(self) -> None:
        def interrupt_blob_publication(path: Path, body: bytes) -> None:
            if path == self.initial.root / CAPTURE_KEY:
                raise KeyboardInterrupt
            publish_bytes(path, body)

        sources = (CAPTURE_SOURCE,)
        with (
            patch(FETCH, return_value=CAPTURE_BODY),
            patch(
                "codebench_pipeline.repository_receipts.publish_bytes", side_effect=interrupt_blob_publication
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            capture_sources(sources, CAPTURE_ORIGIN, CaptureRuntime(64, 1, self.initial, None))
        checksum = self.initial.root / f"{CAPTURE_KEY}.sha256"
        self.assertEqual(checksum.read_bytes(), CAPTURE_DIGEST.encode())
        self.assertFalse((self.initial.root / CAPTURE_KEY).exists())

        with patch(FETCH, return_value=CAPTURE_BODY) as fetch:
            ((capture, text),) = capture_sources(
                sources, CAPTURE_ORIGIN, CaptureRuntime(64, 1, self.restart, None)
            )

        fetch.assert_called_once()
        self.assertEqual(capture.acquired_via, "https")
        self.assertEqual(text, CAPTURE_BODY.decode())
        self.assertEqual(self.restart.read_evidence(CAPTURE_KEY), CAPTURE_BODY)

    def test_restart_rejects_corrupt_persisted_capture_without_fetching(self) -> None:
        corrupt_body = b"x" * len(CAPTURE_BODY)
        cases = (
            ("SHA-256 differs", corrupt_body, CAPTURE_DIGEST.encode()),
            ("Git blob differs", corrupt_body, hashlib.sha256(corrupt_body).hexdigest().encode()),
        )
        for index, (error, body, digest) in enumerate(cases):
            with self.subTest(error=error):
                initial = RepositoryRun(self.initial.dataset_home, f"corruption-{index}", "initial")
                restart = RepositoryRun(initial.dataset_home, initial.dataset_id, "restart")
                sources = (CAPTURE_SOURCE,)
                with patch(FETCH, return_value=CAPTURE_BODY):
                    capture_sources(sources, CAPTURE_ORIGIN, CaptureRuntime(64, 1, initial, None))
                blob = initial.root / CAPTURE_KEY
                sidecar = initial.root / f"{CAPTURE_KEY}.sha256"
                blob.write_bytes(body)
                sidecar.write_bytes(digest)

                with (
                    patch(FETCH, side_effect=AssertionError("restart fetched over corrupt evidence")),
                    self.assertRaisesRegex(IntegrityError, error),
                ):
                    capture_sources(sources, CAPTURE_ORIGIN, CaptureRuntime(64, 1, restart, None))

                self.assertEqual(blob.read_bytes(), body)
                self.assertEqual(sidecar.read_bytes(), digest)
