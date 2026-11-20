from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
from unittest import TestCase

from codebench_pipeline.repository_receipts import RepositoryRun, publish_bytes
from codebench_pipeline.storage import IntegrityError


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
