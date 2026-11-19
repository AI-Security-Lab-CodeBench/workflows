import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pydantic import JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.dataset_config import DatasetBuildSpec, load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.declared_dataset_bundle import DeclaredDatasetBundle
from codebench_pipeline.storage import IntegrityError, fingerprint

BUNDLE = Path("business-configs/datasets/cat3.json").resolve()
JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


def explicit_labeling(home: Path) -> dict[str, JsonValue]:
    return {
        "model": {
            "provider": "openai_compatible",
            "model": "fixture/guarded",
            "base_url": "http://fixture.invalid/v1",
        },
        "max_request_bytes": 16384,
        "batch": {
            "backend_revision": "fixture-backend-1",
            "grounding": {"allowed_domains": ["example.org"]},
            "capacity": {
                "state_directory": str(home / "capacity"),
                "expected_model_path": "/fixture/model",
            },
        },
    }


def authoring(**changes: JsonValue) -> DeclaredDatasetBundle:
    return DeclaredDatasetBundle.model_validate({**JSON_OBJECT.validate_json(BUNDLE.read_bytes()), **changes})


def historical_snapshot(spec: DatasetBuildSpec, home: Path) -> tuple[Path, str]:
    payload = JSON_OBJECT.validate_json(spec.model_dump_json())
    annotation = payload["annotation"]
    assert isinstance(annotation, dict)
    batch = annotation["batch"]
    assert isinstance(batch, dict)
    batch["retry_revision"] = "archived-retry"
    annotation["batch"] = {
        **{key: value for key, value in batch.items() if key not in {"grounding", "capacity"}},
        "inference_mode": "plain",
        "grounding": batch["grounding"],
        "capacity": batch["capacity"],
    }
    publication = payload["publication"]
    assert isinstance(publication, dict)
    publication["review_columns"] = ["risk_id"]
    snapshot = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    version = fingerprint(snapshot)
    directory = DatasetRegistry(home).root / spec.dataset_id
    directory.mkdir(parents=True)
    path = directory / f"{version}.json"
    path.write_text(snapshot, encoding="utf-8")
    (directory / "current").write_text(version, encoding="ascii")
    return path, version


class DeclaredDatasetAuthoringTests(TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(TemporaryDirectory()))

    def test_authored_scopes_normalize_to_registered_settings(self) -> None:
        bundle = authoring(
            revision_label="registration-2026",
            labeling=explicit_labeling(self.home),
            storage={
                "backend": "local",
                "root_directory": "artifacts",
                "key_prefix": "review",
                "preview_rows": 3,
            },
            filtering={"maximum_command_characters": 4096, "quarantine_dynamic_shell": False},
            merge={"keenable_weight": 0.2},
            review={"review_columns": ["command", "severity"]},
            release={"release_cap_per_risk": 13},
        )
        spec = bundle.resolve(BUNDLE.parent, home=self.home)

        self.assertEqual(spec.revision, "registration-2026")
        self.assertEqual(spec.annotation.max_request_bytes, 16384)
        self.assertEqual(spec.filtering.maximum_command_characters, 4096)
        self.assertFalse(spec.filtering.quarantine_dynamic_shell)
        assert spec.declared_collection is not None
        self.assertEqual(spec.declared_collection.merge_keenable_weight, 0.2)
        self.assertEqual(spec.publication.review_columns, ["command", "severity"])
        self.assertEqual(spec.publication.limit_per_risk, 13)
        self.assertEqual(spec.storage.local_root, str(BUNDLE.parent / "artifacts"))
        self.assertEqual((spec.storage.prefix, spec.storage.sample_rows), ("review", 3))

    def test_labeling_and_storage_inherit_independently(self) -> None:
        inherited = authoring(
            labeling={**explicit_labeling(self.home), "max_request_bytes": 8192},
            storage={"backend": "s3", "bucket": "fixture-artifacts", "key_prefix": "old"},
        ).resolve(BUNDLE.parent, home=self.home)
        registration = DatasetRegistry(self.home).root / inherited.dataset_id
        registration.mkdir(parents=True)
        payload = inherited.model_dump_json()
        version = fingerprint(payload)
        (registration / f"{version}.json").write_text(payload, encoding="utf-8")
        (registration / "current").write_text(version, encoding="ascii")

        both = load_build_config(BUNDLE, self.home)
        self.assertEqual((both.annotation, both.storage), (inherited.annotation, inherited.storage))
        only_labeling = authoring(storage=None).resolve(BUNDLE.parent, home=self.home)
        self.assertEqual(only_labeling.annotation.max_request_bytes, 8192)
        self.assertEqual(only_labeling.storage.local_root, str(self.home / "dataframes"))
        self.assertEqual(only_labeling.storage.backend, "local")
        only_storage = authoring(labeling=explicit_labeling(self.home)).resolve(BUNDLE.parent, home=self.home)
        self.assertEqual(only_storage.annotation.max_request_bytes, 16384)
        self.assertEqual(only_storage.storage, inherited.storage)

    def test_storage_only_inheritance_requires_its_own_registration(self) -> None:
        bundle = authoring(labeling=explicit_labeling(self.home))
        with self.assertRaisesRegex(IntegrityError, "requires existing registered storage"):
            bundle.resolve(BUNDLE.parent, home=self.home)

    def test_current_schema_rejects_legacy_or_mixed_authoring(self) -> None:
        labeling = explicit_labeling(self.home)
        batch = labeling["batch"]
        assert isinstance(batch, dict)
        changes: tuple[tuple[str, JsonValue, str], ...] = (
            ("labeling", {**labeling, "batch": {**batch, "inference_mode": "plain"}}, "literal_error"),
            ("labeling", {**labeling, "batch": {**batch, "retry_revision": "2"}}, "extra_forbidden"),
            ("labeling", {**labeling, "maximum_context_characters": 8192}, "extra_forbidden"),
            ("labeling", {**labeling, "from_registration": True}, "extra_forbidden"),
            (
                "storage",
                {"from_registration": True, "backend": "s3", "bucket": "fixture-artifacts"},
                "extra_forbidden",
            ),
            (
                "storage",
                {"backend": "s3", "bucket": "fixture-artifacts", "root_directory": "artifacts"},
                "extra_forbidden",
            ),
            ("storage", {"from_registration": 1}, "from_registration"),
            ("review", {"review_columns": ["severity"]}, "review_command_identity"),
        )
        for key, value, error_type in changes:
            with self.subTest(field=key, value=value):
                with self.assertRaises(ValidationError) as raised:
                    authoring(**{key: value})
                self.assertIn(error_type, {error["type"] for error in raised.exception.errors()})

    def test_legacy_snapshot_replays_but_plain_transport_cannot_be_inherited(self) -> None:
        bundle = authoring(labeling=explicit_labeling(self.home), storage=None)
        spec = bundle.resolve(BUNDLE.parent, home=self.home)
        path, version = historical_snapshot(spec, self.home)
        original = path.read_bytes()

        replay = DatasetRegistry(self.home).get_snapshot(spec.dataset_id, revision=version)
        self.assertEqual(replay.model_dump_json().encode(), original)
        self.assertEqual(replay.annotation.max_request_bytes, 16384)
        self.assertEqual(replay.publication.review_columns, ["risk_id"])
        assert replay.annotation.batch is not None
        self.assertEqual(replay.annotation.batch.retry_revision, "archived-retry")
        self.assertEqual(replay.annotation.batch.inference_mode, "plain")
        with self.assertRaisesRegex(ValueError, "requires guarded inference transport"):
            authoring(storage=None).resolve(BUNDLE.parent, home=self.home)
