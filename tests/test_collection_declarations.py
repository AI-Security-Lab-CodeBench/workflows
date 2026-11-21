import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pydantic import JsonValue, TypeAdapter, ValidationError
from test_declared_dataset_graph import fixture_spec
from test_keenable import permission
from test_keenable import source_declaration as historical_keenable_fixture
from test_repository import COLLECTION, repository_config_fixture

from codebench_pipeline.collection_config import (
    CollectionMergeConfig,
    DeclaredCollection,
    DeclaredSourceStageConfig,
    SourceDeclaration,
)
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_registry import DatasetRegistry, read_registered_spec
from codebench_pipeline.keenable_policy import KeenableDeclaration
from codebench_pipeline.repository_branch_config import (
    DeclaredRepositoryOrigin,
    RepositoryBranchConfig,
    RepositoryBranchDeclaration,
    load_repository_branch,
)
from codebench_pipeline.repository_config import RepositoryBranch
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import IntegrityError, fingerprint


class CollectionDeclarationTests(TestCase):
    def test_historical_labeling_budget_reads_without_changing_registered_bytes(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        payload = spec.model_dump(mode="json")
        batch = payload["annotation"]["batch"]
        batch.pop("grounding_calls_per_command")
        batch["maximum_grounding_rounds"] = 2
        wire = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        revision = fingerprint(wire)
        directory = home / "dataset-specifications" / spec.dataset_id
        directory.mkdir(parents=True)
        stored = directory / f"{revision}.json"
        stored.write_text(wire, encoding="utf-8")
        (directory / "current").write_text(revision, encoding="ascii")

        restored = DatasetRegistry(home).get_snapshot(spec.dataset_id)

        assert restored.annotation.batch is not None
        self.assertEqual(restored.annotation.batch.grounding_calls_per_command, 1.5)
        self.assertEqual(stored.read_text(encoding="utf-8"), wire)
        with self.assertRaises(ValidationError):
            DatasetBuildSpec.model_validate_json(wire)

    def test_registration_resolves_independent_path_free_repository_declarations(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        collection = spec.declared_collection
        assert collection is not None

        cases: tuple[tuple[RepositoryBranch, str, str], ...] = (
            ("dataset", "cheat", "tldr"),
            ("documentation", "tldr", "cheat"),
        )
        for branch, origin, sibling in cases:
            with self.subTest(branch=branch):
                source = collection.for_branch(branch)
                declaration = source.model_dump_json()
                ids = {item.origin_id for item in collection.repository_config(branch).origins(branch)}
                self.assertIn(origin, ids)
                self.assertNotIn(sibling, ids)
                self.assertNotIn("source_list", declaration)
                self.assertNotIn("module_path", declaration)
        self.assertEqual(collection.keenable_declaration.dataset, spec.dataset_id)

    def test_current_registration_preserves_the_source_envelope_and_registered_digest(self) -> None:
        spec = fixture_spec(Path(self.enterContext(TemporaryDirectory())))
        collection = spec.declared_collection
        assert collection is not None
        wire = spec.model_dump_json()

        restored = read_registered_spec(wire.encode())

        self.assertEqual(restored.model_dump_json(), wire)
        for branch in ("dataset", "documentation", "keenable"):
            source = collection.for_branch(branch)
            definition = source.declaration
            payload: dict[str, JsonValue] = definition.model_dump(mode="json")
            self.assertEqual(
                source.model_dump(mode="json"), {"declaration": payload, "digest": fingerprint(payload)}
            )

    def test_mapped_stage_rejects_a_valid_declaration_for_the_wrong_branch(self) -> None:
        spec = fixture_spec(Path(self.enterContext(TemporaryDirectory())))
        collection = spec.declared_collection
        assert collection is not None
        cases: tuple[tuple[Branch, SourceDeclaration, str], ...] = (
            ("keenable", collection.existing_datasets, "do not have Keenable acquisition"),
            ("dataset", collection.keenable_use_cases, "does not have repository acquisition"),
            ("documentation", collection.existing_datasets, "differs from the documentation branch"),
        )
        for branch, source, reason in cases:
            with self.subTest(branch=branch):
                wire = json.dumps({"branch": branch, "source": source.model_dump(mode="json")})
                with self.assertRaisesRegex(IntegrityError, reason):
                    DeclaredSourceStageConfig.model_validate_json(wire, strict=True)

    def test_parser_config_rejects_unreviewed_pins_and_local_paths(self) -> None:
        origin = load_repository_branch(COLLECTION / "existing-datasets.json").origins[0]
        payload = origin.model_dump()
        parser = origin.parser.model_dump()

        for change, error_type in (
            ({"parser_id": "unknown-parser"}, "union_tag_invalid"),
            ({"sha256": "0" * 64}, "parser_pin"),
            ({"module_path": "private/parser.py"}, "extra_forbidden"),
        ):
            with self.subTest(change=change):
                with self.assertRaises(ValidationError) as raised:
                    DeclaredRepositoryOrigin.model_validate({**payload, "parser": {**parser, **change}})
                self.assertEqual(raised.exception.errors()[0]["type"], error_type)

    def test_authoring_rejects_options_that_do_not_belong_to_the_selected_parser_or_mode(self) -> None:
        cheatsheets, markdown, *_ = load_repository_branch(COLLECTION / "existing-datasets.json").origins
        for template, options, field, error_type in (
            (cheatsheets, {"typo": 100}, "typo", "extra_forbidden"),
            (cheatsheets, {"context_characters": True}, "context_characters", "int_type"),
            (markdown, {"allowed_utilities": ["kubectl"]}, "options", "union_tag_not_found"),
            (
                markdown,
                {"mode": "fenced_lines", "allowed_utilities": ["kubectl"], "inline_usage": True},
                "inline_usage",
                "extra_forbidden",
            ),
            (markdown, {"mode": "literal", "utility": "kubectl"}, "utility", "literal_error"),
            (
                markdown,
                {"mode": "literal", "utility": "pulumi", "inline_usage": "true"},
                "inline_usage",
                "bool_type",
            ),
        ):
            with self.subTest(options=options):
                payload = template.model_dump()
                payload["parser"]["options"] = options
                with self.assertRaises(ValidationError) as raised:
                    DeclaredRepositoryOrigin.model_validate(payload)
                error = raised.exception.errors()[0]
                self.assertEqual((error["loc"][-1], error["type"]), (field, error_type))

    def test_authoring_rejects_removed_settings_at_their_old_boundaries(self) -> None:
        origin = load_repository_branch(COLLECTION / "existing-datasets.json").origins[0]
        cases: tuple[tuple[str, JsonValue, str], ...] = (
            ("source_weights", {"cheat": 0.8}, "branch"),
            ("trust_weight", 0.8, "origin"),
            ("workers", 4, "branch"),
            ("use", "retain_and_quote_attributed_examples", "origin"),
            ("format", "text", "origin"),
            ("batch_size", 17, "parser"),
            ("options_json", "{}", "parser"),
        )
        for field, value, scope in cases:
            with self.subTest(field=field):
                parser_payload: dict[str, JsonValue] = origin.parser.model_dump(mode="json")
                origin_payload: dict[str, JsonValue] = {
                    **origin.model_dump(mode="json"),
                    "parser": parser_payload,
                }
                declaration: dict[str, JsonValue] = {
                    "refresh_revision": "reviewed",
                    "origins": [origin_payload],
                }
                {"branch": declaration, "origin": origin_payload, "parser": parser_payload}[scope][field] = (
                    value
                )

                with self.assertRaises(ValidationError) as raised:
                    RepositoryBranchDeclaration.model_validate(declaration)

                error = raised.exception.errors()[0]
                self.assertEqual((error["loc"][-1], error["type"]), (field, "extra_forbidden"))

    def test_merge_configuration_rejects_invalid_weights_and_tier_inversions(self) -> None:
        collection = fixture_spec(Path(self.enterContext(TemporaryDirectory()))).declared_collection
        assert collection is not None
        for changed, reason in (
            ({"keenable_source_weight": 0.55}, "above Keenable"),
            ({"repository_origin_weights": {"cheat": 0.9}}, "above Keenable"),
            ({"repository_origin_weights": {"new": 0.55}}, "declared repository origins"),
            *(
                ({"repository_origin_weights": {"cheat": value}}, reason)
                for value, reason in (
                    (float("nan"), "finite"),
                    (float("inf"), "finite"),
                    (-0.01, "greater than or equal"),
                    (1.01, "less than or equal"),
                    (True, "valid number"),
                    ("0.55", "valid number"),
                )
            ),
        ):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValidationError, reason):
                merge = CollectionMergeConfig.model_validate(
                    {**collection.merge_config.model_dump(), **changed}
                )
                DeclaredCollection.model_validate(
                    {
                        "existing_datasets": collection.existing_datasets,
                        "documentation_crawl": collection.documentation_crawl,
                        "keenable_use_cases": collection.keenable_use_cases,
                        "merge": merge,
                    }
                )

    def test_reweighting_changes_only_the_merge_stage_configuration(self) -> None:
        spec = fixture_spec(Path(self.enterContext(TemporaryDirectory())))
        collection = spec.declared_collection
        assert collection is not None
        original = collection.merge_config
        changed = original.model_copy(
            update={
                "repository_origin_weights": {**original.repository_origin_weights, "cheat": 0.51},
                "keenable_source_weight": 0.24,
            }
        )
        revised = spec.model_copy(
            update={"declared_collection": collection.model_copy(update={"merge": changed})}
        )

        before, after = stage_configs(spec), stage_configs(revised)

        self.assertEqual(
            {stage for stage in before if before[stage] != after[stage]}, {"merged_command_candidates"}
        )

    def test_path_free_historical_snapshot_retains_its_options_and_digest(self) -> None:
        original = repository_config_fixture().dataset[0]
        old_options = '{ "context_characters": 7 }'
        payload: dict[str, JsonValue] = original.model_dump(mode="json")
        payload["transform"] = {
            "parser_id": "cheatsheets_front_matter",
            "sha256": original.transform.sha256,
            "options_json": old_options,
            "batch_size": 17,
        }
        historical: dict[str, JsonValue] = {
            "revision": "historical",
            "origins": [payload],
            "source_weights": {"cheat": 0.59},
        }
        expected_digest = fingerprint(historical)
        wire = TypeAdapter(dict[str, JsonValue]).dump_json(
            {"declaration": historical, "digest": expected_digest}
        )
        restored = SourceDeclaration.model_validate_json(wire, strict=True)
        runtime = restored.repository_config("dataset")
        (origin,) = runtime.origins("dataset")

        self.assertEqual(restored.digest, expected_digest)
        self.assertIsInstance(restored.declaration, RepositoryBranchConfig)
        self.assertEqual(restored.model_dump_json().encode(), wire)
        self.assertEqual(origin.transform.options_json, old_options)
        self.assertEqual(origin.transform.batch_size, 17)

    def test_historical_weighted_declaration_round_trips_as_read_only_evidence(self) -> None:
        template = load_repository_branch(COLLECTION / "existing-datasets.json")
        origin: dict[str, JsonValue] = {**template.origins[0].model_dump(mode="json"), "trust_weight": 0.57}
        declaration: dict[str, JsonValue] = {"refresh_revision": "historical", "origins": [origin]}
        expected_digest = fingerprint(declaration)
        wire = TypeAdapter(dict[str, JsonValue]).dump_json(
            {"declaration": declaration, "digest": expected_digest}
        )

        restored = SourceDeclaration.model_validate_json(wire)

        self.assertEqual(restored.model_dump_json().encode(), wire)
        self.assertEqual(restored.repository_config("dataset").origins("dataset")[0].origin_id, "cheat")
        with self.assertRaisesRegex(IntegrityError, "re-register with merge ranking"):
            restored.require_current("dataset")

        current = fixture_spec(Path(self.enterContext(TemporaryDirectory()))).declared_collection
        assert current is not None
        registered = current.model_copy(update={"merge": None, "existing_datasets": restored})
        registered.require_current_declarations()
        self.assertEqual(registered.merge_config.weight("dataset", "cheat"), 0.57)
        self.assertNotIn("trust_weight", registered.for_branch("dataset").model_dump_json())
        self.assertEqual(registered.existing_datasets.model_dump_json().encode(), wire)

    def test_historical_raw_keenable_retains_omitted_defaults_and_original_key_order(self) -> None:
        source_scoped = historical_keenable_fixture()
        page_scoped = KeenableDeclaration(
            dataset=source_scoped.dataset,
            generation="historical-page-scope",
            generation_review=source_scoped.generation_review,
            cases=source_scoped.cases,
            permissions=(permission(),),
        )
        for policy, omitted in ((page_scoped, "revision"), (source_scoped, "permissions")):
            with self.subTest(revision=policy.revision):
                normalized: dict[str, JsonValue] = policy.model_dump(mode="json")
                historical = {key: value for key, value in reversed(normalized.items()) if key != omitted}
                expected_digest = fingerprint(historical)
                wire = TypeAdapter(dict[str, JsonValue]).dump_json(
                    {"declaration": historical, "digest": expected_digest}
                )

                restored = SourceDeclaration.model_validate_json(wire, strict=True)

                self.assertIsInstance(restored.declaration, KeenableDeclaration)
                self.assertIs(restored.keenable_config(), restored.declaration)
                self.assertEqual(restored.keenable_config().declaration_sha256, policy.declaration_sha256)
                self.assertEqual(restored.model_dump_json().encode(), wire)
                with self.assertRaisesRegex(ValidationError, "digest does not match"):
                    SourceDeclaration.model_validate(
                        {"declaration": {**historical, "generation": "changed"}, "digest": expected_digest}
                    )

    def test_path_bearing_registered_snapshot_is_readable_but_requires_reregistration(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        snapshot = spec.model_dump(mode="json")
        policy: dict[str, JsonValue] = repository_config_fixture().model_dump(mode="json")
        del policy["workers"]
        repository = {"declaration": policy, "digest": fingerprint(policy)}
        legacy_path = "/prior/deployment/repositories.json"
        snapshot["declared_collection"]["existing_datasets"] = {"source_list": legacy_path, **repository}
        snapshot["declared_collection"]["documentation_crawl"] = {"source_list": legacy_path, **repository}

        restored = read_registered_spec(json.dumps(snapshot).encode()).declared_collection
        assert restored is not None
        self.assertEqual(restored.existing_datasets.source_list, legacy_path)
        self.assertEqual(
            restored.existing_datasets.model_dump(mode="json"),
            snapshot["declared_collection"]["existing_datasets"],
        )
        with self.assertRaisesRegex(IntegrityError, "re-register the path-free"):
            DeclaredSourceStageConfig(branch="dataset", source=restored.existing_datasets)
