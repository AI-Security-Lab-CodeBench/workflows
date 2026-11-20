from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.storage import IntegrityError


class LocalObjectStoreTests(TestCase):
    def setUp(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        self.root = home / "store"
        self.outside = home / "outside"
        self.outside.mkdir()
        self.store = ObjectStore(StorageConfig(local_root=str(self.root)))

    def test_read_refuses_links_at_the_pointer_revision_and_payload(self) -> None:
        for layer in ("pointer", "revision", "payload"):
            with self.subTest(layer=layer):
                data = layer.encode()
                self.store.write(layer, data)
                directory = self.root / "datasets" / layer
                revision = directory / sha256(data)
                target = {
                    "pointer": directory / "current",
                    "revision": revision,
                    "payload": revision / "data",
                }[layer]
                external = target.rename(self.outside / layer)
                target.symlink_to(external, target_is_directory=layer == "revision")

                with self.assertRaises(IntegrityError):
                    self.store.read(layer)

    def test_write_refuses_a_linked_storage_root_without_creating_outside(self) -> None:
        self.root.symlink_to(self.outside, target_is_directory=True)

        with self.assertRaises(IntegrityError):
            self.store.write("new", b"outside")

        self.assertEqual(list(self.outside.iterdir()), [])

    def test_absent_objects_are_distinct_from_damaged_objects(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.store.read("missing-directory")

        directory = self.root / "datasets" / "current-state"
        directory.mkdir(parents=True)
        (directory / ".lock").touch()
        with self.assertRaises(FileNotFoundError):
            self.store.read("current-state")

        revision = directory / sha256(b"body")
        revision.mkdir()
        with self.assertRaisesRegex(IntegrityError, "Missing completion metadata"):
            self.store.read("current-state")

        pointer = directory / "current"
        pointer.write_bytes(b"invalid digest")
        with self.assertRaisesRegex(IntegrityError, "Invalid completion metadata"):
            self.store.read("current-state")

        pointer.write_text(revision.name, encoding="ascii")
        with self.assertRaisesRegex(IntegrityError, "Incomplete object"):
            self.store.read("current-state")

        (revision / "data").write_bytes(b"wrong bytes")
        with self.assertRaisesRegex(IntegrityError, "Object content mismatch"):
            self.store.read("current-state")

    def test_immutable_reuse_and_replacement_preserve_retained_versions(self) -> None:
        uri = self.store.write("name with spaces", b"first")
        directory = self.root / "datasets" / "name with spaces"
        version = directory / sha256(b"first") / "data"
        inode = version.stat().st_ino

        self.assertEqual(self.store.write("name with spaces", b"first"), uri)
        with self.assertRaisesRegex(IntegrityError, "Immutable object already exists"):
            self.store.write("name with spaces", b"second")
        self.assertEqual(self.store.read("name with spaces"), b"first")

        self.store.write("name with spaces", b"second", replace=True)
        self.store.write("name with spaces", b"first", replace=True)

        self.assertEqual(version.stat().st_ino, inode)
        self.assertEqual(self.store.read("name with spaces"), b"first")
        self.assertEqual((directory / sha256(b"second") / "data").read_bytes(), b"second")
        self.assertEqual(
            {path.name for path in directory.iterdir()}, {"current", sha256(b"first"), sha256(b"second")}
        )

    def test_comparing_a_retained_revision_refuses_its_linked_payload(self) -> None:
        self.store.write("object", b"prior")
        self.store.write("object", b"head", replace=True)
        old = self.root / "datasets" / "object" / sha256(b"prior") / "data"
        external = old.rename(self.outside / "prior")
        old.symlink_to(external)

        with self.assertRaises(IntegrityError):
            self.store.write("object", b"prior", replace=True)

        self.assertEqual(self.store.read("object"), b"head")
        self.assertEqual(external.read_bytes(), b"prior")
