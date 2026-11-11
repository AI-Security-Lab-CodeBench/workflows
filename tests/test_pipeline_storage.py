from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pydantic import JsonValue

from codebench_pipeline.models import DatasetSpec
from codebench_pipeline.storage import (
    ArtifactRequest,
    ArtifactStore,
    IntegrityError,
    Registry,
    digest,
    exclusive,
    load_artifact,
)


class ArtifactStorageTests(unittest.TestCase):
    def test_second_dataset_and_restart_preserve_completed_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = ArtifactStore(root)
            first = DatasetSpec(
                dataset_id="first", revision="1", documents=root / "docs", semantics=root / "docs"
            )
            second = first.model_copy(update={"dataset_id": "second"})
            calls: list[str] = []

            def build(destination: Path) -> dict[str, JsonValue]:
                calls.append("computed")
                (destination / "result.txt").write_text("preserved data", encoding="utf-8")
                return {"records": 1}

            original, cached = store.materialize(ArtifactRequest(first, "extraction", {}), build)
            self.assertFalse(cached)
            original_hash = digest(original.path / "_artifact.json")
            store.materialize(ArtifactRequest(second, "extraction", {}), build)
            recovered, cached = ArtifactStore(root).materialize(
                ArtifactRequest(first, "extraction", {}), build
            )

            self.assertTrue(cached)
            self.assertEqual(calls, ["computed", "computed"])
            self.assertEqual(recovered, original)
            self.assertEqual(digest(original.path / "_artifact.json"), original_hash)

    def test_modified_or_extra_artifact_files_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = DatasetSpec(
                dataset_id="first", revision="1", documents=root / "docs", semantics=root / "docs"
            )

            def build(destination: Path) -> dict[str, JsonValue]:
                (destination / "result.txt").write_text("original", encoding="utf-8")
                return {}

            artifact, _ = ArtifactStore(root).materialize(ArtifactRequest(spec, "filtering", {}), build)
            (artifact.path / "result.txt").write_text("tampered", encoding="utf-8")

            with self.assertRaisesRegex(IntegrityError, "content mismatch"):
                load_artifact(artifact.path / "_artifact.json")

    def test_failed_build_is_not_published_and_can_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = DatasetSpec(
                dataset_id="first", revision="1", documents=root / "docs", semantics=root / "docs"
            )
            request = ArtifactRequest(spec, "labels", {})

            def fail(destination: Path) -> dict[str, JsonValue]:
                (destination / "partial.txt").write_text("partial", encoding="utf-8")
                raise RuntimeError("interrupted")

            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                ArtifactStore(root).materialize(request, fail)
            self.assertEqual(list(root.rglob("_artifact.json")), [])
            artifact, cached = ArtifactStore(root).materialize(request, lambda path: {"complete": True})
            self.assertFalse(cached)
            self.assertEqual(artifact.metadata, {"complete": True})

    def test_registry_keeps_versions_and_rejects_simultaneous_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            registry = Registry(root)
            spec = DatasetSpec(
                dataset_id="first", revision="1", documents=root / "docs", semantics=root / "docs"
            )
            registry.register(spec)
            registry.register(spec.model_copy(update={"revision": "2"}))

            self.assertEqual(registry.get("first").revision, "2")
            self.assertEqual(len(list((root / "registry/first").glob("*.json"))), 2)
            with exclusive(root / "registry/first/registration.lock"):
                with self.assertRaisesRegex(IntegrityError, "Another run owns"):
                    registry.register(spec)


if __name__ == "__main__":
    unittest.main()
