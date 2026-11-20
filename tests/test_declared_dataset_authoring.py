import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from dagster import AssetRecordsFilter, DagsterInstance, materialize
from dagster._core.definitions.data_version import (
    extract_data_provenance_from_entry,
    extract_data_version_from_entry,
)
from pydantic import JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.config_assets import (
    CONFIG_DIRECTORY,
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    configuration_assets,
    mapping_key,
)
from codebench_pipeline.config_directory import ConfigDirectorySnapshot
from codebench_pipeline.dataset_config import DatasetBuildSpec, load_build_config
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.declared_dataset_bundle import DeclaredDatasetBundle
from codebench_pipeline.object_store import StorageConfig
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


def write_snapshot(payload: str, home: Path, dataset_id: str) -> tuple[Path, str]:
    version = fingerprint(payload)
    directory = DatasetRegistry(home).root / dataset_id
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{version}.json"
    path.write_text(payload, encoding="utf-8")
    (directory / "current").write_text(version, encoding="ascii")
    return path, version


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
    return write_snapshot(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")), home, spec.dataset_id
    )


class DeclaredDatasetAuthoringTests(TestCase):
    def setUp(self) -> None:
        self.home = Path(self.enterContext(TemporaryDirectory()))

    def test_native_directory_asset_observes_live_changes_without_registration(self) -> None:
        root = self.home / "authoring"
        shutil.copytree(
            BUNDLE.parents[1], root, ignore=shutil.ignore_patterns("manifests", "reference-examples")
        )
        bundle = authoring(
            labeling=explicit_labeling(self.home), storage={"backend": "s3", "bucket": "fixture-artifacts"}
        )
        spec = bundle.resolve(root / "datasets", home=self.home)
        registry_path, revision = write_snapshot(spec.model_dump_json(), self.home, spec.dataset_id)
        assets = [
            item
            for item in configuration_assets(self.home, authoring_root=root)
            if item.key in {CONFIG_DIRECTORY, GLOBAL_CONFIG}
        ]
        manager = ConfigRunIOManager(home=str(self.home))
        relative = Path("categories/cat3/instructions.md")
        instructions = root / relative
        with DagsterInstance.local_temp(str(self.home)) as instance:
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])

            def materialize_directory() -> tuple[str, str]:
                result = materialize(
                    assets,
                    selection=[CONFIG_DIRECTORY],
                    instance=instance,
                    partition_key=spec.dataset_id,
                    resources={"config_run_io_manager": manager},
                )
                event = instance.get_latest_materialization_event(CONFIG_DIRECTORY)
                assert event is not None
                version = extract_data_version_from_entry(event)
                assert version is not None
                return result.run_id, version.value

            def materialize_registered() -> str:
                result = materialize(
                    assets,
                    selection=[GLOBAL_CONFIG],
                    instance=instance,
                    partition_key=spec.dataset_id,
                    resources={"config_run_io_manager": manager},
                )
                return result.run_id

            first, original_version = materialize_directory()
            _, unchanged_version = materialize_directory()
            self.assertEqual(unchanged_version, original_version)
            materialize_registered()
            global_event = instance.get_latest_materialization_event(GLOBAL_CONFIG)
            assert global_event is not None
            provenance = extract_data_provenance_from_entry(global_event)
            assert provenance is not None
            (directory_event,) = instance.fetch_materializations(
                AssetRecordsFilter(asset_key=CONFIG_DIRECTORY, asset_partitions=[spec.dataset_id]), limit=1
            ).records
            self.assertEqual(provenance.input_storage_ids[CONFIG_DIRECTORY], directory_event.storage_id)

            addition = root / "categories/cat3/collection/extra.json"
            addition.write_bytes(b"unsupported and unread")
            with self.assertRaisesRegex(IntegrityError, r"Unsupported entries.*extra\.json"):
                materialize_directory()
            addition.unlink()
            _, restored_version = materialize_directory()
            self.assertEqual(restored_version, original_version)

            changed_bytes = instructions.read_bytes() + b"\r\n"
            instructions.write_bytes(changed_bytes)
            edited, edited_version = materialize_directory()
            self.assertNotEqual(edited_version, original_version)
            with self.assertRaisesRegex(IntegrityError, "Observed configuration directory changed"):
                materialize_registered()

            instructions.unlink()
            removed, removed_version = materialize_directory()
            self.assertNotIn(removed_version, {original_version, edited_version})
            instructions.write_bytes(changed_bytes)
            _, restored_edited_version = materialize_directory()
            self.assertEqual(restored_edited_version, edited_version)

            self.assertEqual(DatasetRegistry(self.home).current_revision(spec.dataset_id), revision)
            self.assertEqual(registry_path.read_text(), spec.model_dump_json())
            shutil.rmtree(root)
            captured = manager.directory(edited)
            self.assertEqual(captured.digest, edited_version)
            self.assertEqual(captured.read(relative), changed_bytes)
            self.assertEqual(manager.directory(first), spec.config_directory)
            self.assertNotIn(relative.as_posix(), manager.directory(removed).files)
            changed = bundle.resolve(root / "datasets", home=self.home, observed_directory=captured)
            self.assertEqual(changed.config_directory, captured)
            with self.assertRaisesRegex(FileNotFoundError, "Required configuration file is missing"):
                bundle.resolve(
                    root / "datasets", home=self.home, observed_directory=manager.directory(removed)
                )
            with self.assertRaisesRegex(IntegrityError, "Observed configuration directory changed"):
                materialize_registered()
            write_snapshot(changed.model_dump_json(), self.home, changed.dataset_id)
            self.assertEqual(manager.registered(materialize_registered()).config_directory, captured)

    def test_config_directory_records_exact_bytes_and_ignores_unrelated_files(self) -> None:
        root = self.home / "authoring"
        shutil.copytree(
            BUNDLE.parents[1], root, ignore=shutil.ignore_patterns("manifests", "reference-examples")
        )
        bundle = authoring(
            labeling=explicit_labeling(self.home), storage={"backend": "s3", "bucket": "fixture-artifacts"}
        )
        original = bundle.resolve(BUNDLE.parent, home=self.home)
        (root / ".env").write_text("SECRET=must-not-be-captured", encoding="utf-8")
        neighboring_category = root / "categories/cat2"
        neighboring_category.mkdir()
        (neighboring_category / "category.json").write_text("unrelated category", encoding="utf-8")
        unchanged = bundle.resolve(root / "datasets", home=self.home)
        self.assertEqual(unchanged, original)
        directory = original.config_directory
        assert directory is not None
        self.assertEqual(directory.scope, "dataset-authoring/CAT-3")
        self.assertNotIn(str(root), directory.model_dump_json())
        self.assertNotIn("must-not-be-captured", directory.model_dump_json())

        instructions = root / "categories/cat3/instructions.md"
        changed_bytes = instructions.read_bytes() + b"\r\n"
        instructions.write_bytes(changed_bytes)
        changed = bundle.resolve(root / "datasets", home=self.home)
        assert changed.config_directory is not None
        self.assertNotEqual(changed.config_directory.digest, directory.digest)
        self.assertEqual(
            changed.config_directory.read(Path("categories/cat3/instructions.md")), changed_bytes
        )

        instructions.unlink()
        with self.assertRaisesRegex(FileNotFoundError, "Required configuration file is missing"):
            bundle.resolve(root / "datasets", home=self.home)
        instructions.symlink_to(BUNDLE.parents[1] / "categories/cat3/instructions.md")
        with self.assertRaisesRegex(IntegrityError, "Linked local evidence"):
            bundle.resolve(root / "datasets", home=self.home)

    def test_active_asset_and_mapping_consume_persisted_capture_after_source_removal(self) -> None:
        root = self.home / "authoring"
        shutil.copytree(
            BUNDLE.parents[1], root, ignore=shutil.ignore_patterns("manifests", "reference-examples")
        )
        bundle = authoring(
            labeling=explicit_labeling(self.home),
            storage={"backend": "s3", "bucket": "fixture-artifacts"},
            merge={"keenable_source_weight": 0.19},
        )
        spec = bundle.resolve(root / "datasets", home=self.home)
        assert spec.config_directory is not None
        path, _ = write_snapshot(spec.model_dump_json(), self.home, spec.dataset_id)
        registry_bytes = path.read_bytes()
        instructions = root / "categories/cat3/instructions.md"
        instructions.write_bytes(instructions.read_bytes() + b"\r\n")
        changed = bundle.resolve(root / "datasets", home=self.home)
        assert changed.config_directory is not None
        shutil.rmtree(root)
        mapped = mapping_key("merged_command_candidates")
        definitions = [
            item
            for item in configuration_assets(self.home)
            if item.key in {CONFIG_DIRECTORY, GLOBAL_CONFIG, mapped}
        ]
        manager = ConfigRunIOManager(home=str(self.home))
        with DagsterInstance.local_temp(str(self.home)) as instance:
            instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
            versions: list[str] = []
            for expected in (spec, spec, changed):
                if expected is changed:
                    write_snapshot(changed.model_dump_json(), self.home, changed.dataset_id)
                result = materialize(
                    definitions,
                    selection=[GLOBAL_CONFIG, mapped],
                    instance=instance,
                    partition_key=spec.dataset_id,
                    resources={"config_run_io_manager": manager},
                )
                self.assertTrue(result.success)
                event = instance.get_latest_materialization_event(GLOBAL_CONFIG)
                assert event is not None
                version = extract_data_version_from_entry(event)
                assert version is not None
                versions.append(version.value)
                event_data = event.asset_materialization
                assert event_data is not None and expected.config_directory is not None
                self.assertEqual(
                    event_data.metadata["config_directory_digest"].value, expected.config_directory.digest
                )
                self.assertEqual(
                    manager.registered(result.run_id).config_directory, expected.config_directory
                )
                self.assertEqual(
                    result.output_for_node(mapped.to_python_identifier()).keenable_source_weight,
                    0.19,
                )
            self.assertEqual(versions[0], versions[1])
            self.assertNotEqual(versions[1], versions[2])
        self.assertEqual(path.read_bytes(), registry_bytes)

    def test_authored_scopes_normalize_to_registered_settings(self) -> None:
        bundle = authoring(
            revision_label="registration-2026",
            labeling=explicit_labeling(self.home),
            storage={
                "backend": "s3",
                "bucket": "fixture-artifacts",
                "endpoint_url": "https://objects.fixture.invalid",
                "region": "fixture-region-1",
                "access_key_env": "FIXTURE_ACCESS_KEY",
                "secret_key_env": "FIXTURE_SECRET_KEY",
                "key_prefix": "review",
                "preview_rows": 3,
            },
            filtering={"maximum_command_characters": 4096, "quarantine_dynamic_shell": False},
            merge={"keenable_source_weight": 0.2},
            review={"review_columns": ["command", "severity"]},
            release={"release_cap_per_risk": 13},
        )
        spec = bundle.resolve(BUNDLE.parent, home=self.home)

        self.assertEqual(spec.revision, "registration-2026")
        self.assertEqual(spec.annotation.max_request_bytes, 16384)
        self.assertEqual(spec.filtering.maximum_command_characters, 4096)
        self.assertFalse(spec.filtering.quarantine_dynamic_shell)
        assert spec.declared_collection is not None
        self.assertEqual(spec.declared_collection.merge_config.keenable_source_weight, 0.2)
        self.assertEqual(spec.publication.review_columns, ["command", "severity"])
        self.assertEqual(spec.publication.limit_per_risk, 13)
        self.assertEqual(
            spec.storage,
            StorageConfig(
                backend="s3",
                bucket="fixture-artifacts",
                endpoint_url="https://objects.fixture.invalid",
                region="fixture-region-1",
                access_key_env="FIXTURE_ACCESS_KEY",
                secret_key_env="FIXTURE_SECRET_KEY",
                prefix="review",
                sample_rows=3,
            ),
        )

    def test_current_registration_rejects_capture_from_another_category(self) -> None:
        current = authoring(
            labeling=explicit_labeling(self.home), storage={"backend": "s3", "bucket": "fixture-artifacts"}
        ).resolve(BUNDLE.parent, home=self.home)
        assert current.config_directory is not None
        foreign = ConfigDirectorySnapshot(
            category_id="CAT-2",
            files={
                name.replace("categories/cat3/", "categories/cat2/"): body
                for name, body in current.config_directory.files.items()
            },
        )
        edited = current.model_copy(update={"config_directory": foreign})
        with self.assertRaisesRegex(ValidationError, "scope differs from the declared dataset category"):
            DatasetRegistry(self.home).register(edited)

    def test_labeling_and_storage_inherit_independently(self) -> None:
        inherited = authoring(
            labeling={**explicit_labeling(self.home), "max_request_bytes": 8192},
            storage={
                "backend": "s3",
                "bucket": "fixture-artifacts",
                "endpoint_url": "https://inherited.fixture.invalid",
                "key_prefix": "old",
            },
        ).resolve(BUNDLE.parent, home=self.home)
        write_snapshot(inherited.model_dump_json(), self.home, inherited.dataset_id)

        both = load_build_config(BUNDLE, self.home)
        self.assertEqual((both.annotation, both.storage), (inherited.annotation, inherited.storage))
        only_labeling = authoring(storage={"backend": "s3", "bucket": "replacement-artifacts"}).resolve(
            BUNDLE.parent, home=self.home
        )
        self.assertEqual(only_labeling.annotation.max_request_bytes, 8192)
        self.assertEqual(only_labeling.storage.bucket, "replacement-artifacts")
        self.assertIsNone(only_labeling.storage.endpoint_url)
        only_storage = authoring(labeling=explicit_labeling(self.home)).resolve(BUNDLE.parent, home=self.home)
        self.assertEqual(only_storage.annotation.max_request_bytes, 16384)
        self.assertEqual(only_storage.storage, inherited.storage)

    def test_storage_only_inheritance_requires_its_own_registration(self) -> None:
        bundle = authoring(labeling=explicit_labeling(self.home))
        with self.assertRaisesRegex(IntegrityError, "requires existing registered storage"):
            bundle.resolve(BUNDLE.parent, home=self.home)

    def test_local_history_remains_readable_but_cannot_supply_new_declared_storage(self) -> None:
        bundle = authoring(
            labeling=explicit_labeling(self.home), storage={"backend": "s3", "bucket": "new-artifacts"}
        )
        spec = bundle.resolve(BUNDLE.parent, home=self.home)
        legacy = spec.model_copy(update={"storage": StorageConfig(local_root=str(self.home / "archived"))})
        path, revision = write_snapshot(legacy.model_dump_json(), self.home, legacy.dataset_id)
        before = path.read_bytes()
        registry = DatasetRegistry(self.home)

        with self.assertRaisesRegex(IntegrityError, "Registered storage is local"):
            load_build_config(BUNDLE, self.home)
        with self.assertRaisesRegex(IntegrityError, "Registered storage is local"):
            authoring(labeling=explicit_labeling(self.home)).resolve(BUNDLE.parent, home=self.home)
        self.assertEqual(bundle.resolve(BUNDLE.parent, home=self.home).storage, spec.storage)
        self.assertEqual(registry.get_snapshot(legacy.dataset_id, revision=revision), legacy)
        self.assertEqual(registry.storage(legacy.dataset_id, revision=revision), legacy.storage)
        self.assertEqual(path.read_bytes(), before)

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
            ("storage", {"backend": "local", "root_directory": "artifacts"}, "literal_error"),
            ("storage", None, "model_type"),
            ("storage", {"from_registration": 1}, "from_registration"),
            ("review", {"review_columns": ["severity"]}, "review_command_identity"),
        )
        for key, value, error_type in changes:
            with self.subTest(field=key, value=value):
                with self.assertRaises(ValidationError) as raised:
                    authoring(**{key: value})
                self.assertIn(error_type, {error["type"] for error in raised.exception.errors()})
        payload = JSON_OBJECT.validate_json(BUNDLE.read_bytes())
        del payload["storage"]
        with self.assertRaises(ValidationError) as raised:
            DeclaredDatasetBundle.model_validate(payload)
        self.assertTrue(all(error["loc"][0] == "storage" for error in raised.exception.errors()))

    def test_legacy_snapshot_replays_but_plain_transport_cannot_be_inherited(self) -> None:
        storage: dict[str, JsonValue] = {"backend": "s3", "bucket": "fixture-artifacts"}
        bundle = authoring(labeling=explicit_labeling(self.home), storage=storage)
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
            authoring(storage=storage).resolve(BUNDLE.parent, home=self.home)
