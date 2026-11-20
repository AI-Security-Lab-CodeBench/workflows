from pathlib import Path, PurePosixPath, PureWindowsPath
from shutil import copy2, copytree, rmtree
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import test_declared_dataset_graph as declared_graph
from dagster import AssetKey, DagsterInvalidConfigError
from pydantic import JsonValue, TypeAdapter, ValidationError
from test_keenable import PEER, PLAN, SOURCE_BODY, result

from codebench_pipeline.collection_config import CollectionMergeConfig
from codebench_pipeline.collection_stage_settings import COLLECTION_OPERATIONS, RepositoryCatalogSettings
from codebench_pipeline.config_assets import (
    CONFIG_DIRECTORY,
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    RegisteredConfig,
    collection_mapping_key,
    configured_selection,
    mapping_key,
)
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataset_config import DeduplicationPolicy, FilteringConfig, PublicationConfig
from codebench_pipeline.dataset_dependencies import preparation_stages
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.declared_dataset_bundle import DeclaredDatasetBundle
from codebench_pipeline.keenable_transport import DirectResponse
from codebench_pipeline.labeling_stage_settings import ProposalRules
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import (
    CategoryEligibilityConfig,
    FilterConfig,
    GuardedLabelingConfig,
    ReviewQueueConfig,
    stage_configs,
)
from codebench_pipeline.storage import IntegrityError


class RegisteredConfigMaterializationTests(TestCase):
    def test_named_build_uses_registered_directory_input_without_live_capture(self) -> None:
        for preapproved in (False, True):
            with self.subTest(preapproved=preapproved), TemporaryDirectory() as temporary:
                home = Path(temporary)
                authored = home / "business-configs"
                bundle = authored / "datasets" / declared_graph.BUNDLE.name
                template = declared_graph.BUNDLE.parent.parent
                bundle.parent.mkdir(parents=True)
                copy2(declared_graph.BUNDLE, bundle)
                copytree(template / "categories/cat3", authored / "categories/cat3")
                copytree(template / "labeling", authored / "labeling")
                seeded = self.enterContext(
                    declared_graph.collection_fixture(declared_graph.fixture_spec(home, bundle))
                )
                sources = seeded.declared_collection
                assert sources is not None
                category = authored / "categories/cat3"
                for relative, source in (
                    ("collection/existing-datasets.json", sources.existing_datasets),
                    ("collection/documentation-crawl.json", sources.documentation_crawl),
                    ("keenable/declaration.json", sources.keenable_use_cases),
                ):
                    (category / relative).write_text(source.declaration.model_dump_json(), encoding="utf-8")
                authored_bundle = DeclaredDatasetBundle.model_validate_json(bundle.read_bytes())
                bundle.write_text(
                    authored_bundle.model_copy(update={"merge": sources.merge_config}).model_dump_json(),
                    encoding="utf-8",
                )
                spec = declared_graph.fixture_spec(home, bundle)
                spec = spec.model_copy(
                    update={
                        "filtering": FilteringConfig(maximum_command_characters=1),
                        "config_directory": spec.config_directory if preapproved else None,
                    }
                )
                registry = DatasetRegistry(home)
                dataset_id = registry.register(spec)
                definitions = build_definitions(home)
                build = definitions.resolve_job_def("build_dataset")
                with (
                    dataset_instance(home) as instance,
                    patch.dict(
                        "os.environ",
                        {"CODEBENCH_CONFIG_ROOT": str(authored), "KEENABLE_API_KEY": "fixture-key"},
                    ),
                    patch(
                        "codebench_pipeline.keenable.request_direct",
                        side_effect=(result(PLAN), DirectResponse(SOURCE_BODY, PEER)),
                    ),
                ):
                    instance.add_dynamic_partitions("dataset_builds", [dataset_id])
                    if preapproved:
                        observe = definitions.resolve_job_def("observe_dataset_configuration")
                        capture = observe.execute_in_process(instance=instance, partition_key=dataset_id)
                        self.assertEqual(
                            {event.asset_key for event in capture.get_asset_materialization_events()},
                            {CONFIG_DIRECTORY},
                        )

                    offline = authored.with_name("offline")
                    authored.rename(offline)
                    run = build.execute_in_process(instance=instance, partition_key=dataset_id)
                    materialized = {event.asset_key for event in run.get_asset_materialization_events()}
                    self.assertTrue(
                        {
                            GLOBAL_CONFIG,
                            AssetKey(["datasets", "filtered_commands"]),
                            AssetKey(["datasets", "labeling_attempts"]),
                            AssetKey(["datasets", "human_review_queue"]),
                            AssetKey(["datasets", "labeling_failures"]),
                        }
                        <= materialized,
                        materialized,
                    )
                    self.assertNotIn(CONFIG_DIRECTORY, materialized)
                    applied = ConfigRunIOManager(home=str(home)).registered(run.run_id)
                    self.assertEqual(applied.config_directory, spec.config_directory)
                    if not preapproved:
                        with self.assertRaisesRegex(IntegrityError, "does not authorize a live"):
                            definitions.resolve_job_def("observe_dataset_configuration").execute_in_process(
                                instance=instance, partition_key=dataset_id
                            )
                    if preapproved:
                        offline.rename(authored)
                        instructions = category / "instructions.md"
                        instructions.write_bytes(instructions.read_bytes() + b"\nNew unapproved policy.\n")
                        changed = definitions.resolve_job_def(
                            "observe_dataset_configuration"
                        ).execute_in_process(instance=instance, partition_key=dataset_id)
                        self.assertEqual(
                            {event.asset_key for event in changed.get_asset_materialization_events()},
                            {CONFIG_DIRECTORY},
                        )
                        rmtree(authored)
                        with self.assertRaisesRegex(
                            IntegrityError, "Observed configuration directory changed"
                        ):
                            build.execute_in_process(instance=instance, partition_key=dataset_id)

    def assert_no_local_paths(self, value: JsonValue, location: tuple[str, ...] = ()) -> None:
        match value:
            case dict():
                for key, child in value.items():
                    self.assert_no_local_paths(child, (*location, key))
            case list():
                for child in value:
                    self.assert_no_local_paths(child, location)
            case str():
                if location[-2:] == ("grounding", "pinned_library_ids"):
                    self.assertRegex(value, r"^/[\w.-]+/[\w.-]+$")
                    return
                path = value.strip()
                self.assertFalse(
                    path.lower().startswith("file:")
                    or PurePosixPath(path).is_absolute()
                    or bool(PureWindowsPath(path).drive)
                    or path.startswith(("./", "../", "~/", ".\\", "..\\", "~\\")),
                    f"Local path in {'.'.join(location)}: {path!r}",
                )
            case None | bool() | int() | float():
                return

    def test_materializes_registered_scopes_and_filter_without_authored_files(self) -> None:
        root = Path(self.enterContext(TemporaryDirectory()))
        home = root / "runtime"
        authored = root / "business-configs"
        bundle = authored / "datasets" / declared_graph.BUNDLE.name
        bundle.parent.mkdir(parents=True)
        copy2(declared_graph.BUNDLE, bundle)
        template = declared_graph.BUNDLE.parent.parent
        copytree(template / "categories" / "cat3", authored / "categories" / "cat3")
        copytree(template / "labeling", authored / "labeling")
        spec = self.enterContext(declared_graph.collection_fixture(declared_graph.fixture_spec(home, bundle)))
        batch = spec.annotation.batch
        assert batch is not None
        provider_id = "/fixture/command-reference"
        grounding = batch.grounding.model_copy(update={"pinned_library_ids": [provider_id]})
        spec = spec.model_copy(
            update={
                "annotation": spec.annotation.model_copy(
                    update={"batch": batch.model_copy(update={"grounding": grounding})}
                ),
                "filtering": FilteringConfig(maximum_command_characters=1),
                "deduplication": DeduplicationPolicy(normalize_safe_whitespace=False),
                "publication": PublicationConfig(review_columns=["command", "severity"]),
            }
        )
        dataset_id = DatasetRegistry(home).register(spec)
        rmtree(authored)

        shared_contracts = {
            "merged_command_candidates": CollectionMergeConfig,
            "filtered_commands": FilterConfig,
            "labeling_attempts": GuardedLabelingConfig,
            "proposed_annotations": ProposalRules,
            "deduplicated_annotations": DeduplicationPolicy,
            "category_eligible_annotations": CategoryEligibilityConfig,
            "human_review_queue": ReviewQueueConfig,
        }
        contracts = {
            **{mapping_key(stage): (stage, contract) for stage, contract in shared_contracts.items()},
            **{
                collection_mapping_key(operation.branch, operation.stage): (
                    operation.name,
                    operation.contract,
                )
                for operation in COLLECTION_OPERATIONS
            },
        }
        rules = AssetKey(["datasets", "category_rules"])
        filtered = AssetKey(["datasets", "filtered_commands"])
        stages = preparation_stages()
        selection = configured_selection(
            AssetKey(["datasets", *stage.split("/")])
            for stage in stages[: stages.index("filtered_commands") + 1]
        )
        selection = list(dict.fromkeys([*selection, *contracts]))
        definitions = build_definitions(home)
        graph = definitions.resolve_asset_graph()
        self.assertEqual(
            {key for key in graph.get_all_asset_keys() if graph.get(key).group_name == "configuration"},
            {CONFIG_DIRECTORY, GLOBAL_CONFIG, rules},
        )
        self.assertNotIn(CONFIG_DIRECTORY, selection)
        self.assertEqual(graph.get(GLOBAL_CONFIG).parent_keys, {CONFIG_DIRECTORY})
        self.assertEqual(graph.get(rules).parent_keys, {GLOBAL_CONFIG})
        job = definitions.resolve_job_def("build_dataset")

        with dataset_instance(home) as instance:
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            with (
                patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
                patch(
                    "codebench_pipeline.keenable.request_direct",
                    side_effect=(result(PLAN), DirectResponse(SOURCE_BODY, PEER)),
                ),
            ):
                run = job.execute_in_process(
                    instance=instance, partition_key=dataset_id, asset_selection=selection
                )
            self.assertTrue(run.success)
            events = run.get_asset_materialization_events()
            self.assertCountEqual([event.asset_key for event in events], selection)
            stored_run = instance.get_run_by_id(run.run_id)
            assert stored_run is not None
            json_object = TypeAdapter(dict[str, JsonValue])
            launch = json_object.validate_python(stored_run.run_config)
            ops = json_object.validate_python(launch["ops"])
            root_op = GLOBAL_CONFIG.to_python_identifier()
            with self.subTest(boundary="launch"):
                self.assertEqual(set(ops), {root_op})
                self.assert_no_local_paths(launch)

            artifact_root = home / "configuration-runs" / run.run_id
            registered = RegisteredConfig.model_validate_json(
                artifact_root.joinpath(*GLOBAL_CONFIG.path).read_bytes()
            )
            self.assertEqual((registered.dataset_id, registered.rules), (dataset_id, spec.rules))
            self.assert_no_local_paths(registered.model_dump(mode="json"))
            expected = stage_configs(registered)
            collection_stages = {operation.name for operation in COLLECTION_OPERATIONS}
            for key, (stage, contract) in contracts.items():
                with self.subTest(stage=stage):
                    payload = artifact_root.joinpath(*key.path).read_bytes()
                    mapped = contract.model_validate_json(payload, strict=True)
                    self.assert_no_local_paths(mapped.model_dump(mode="json"))
                    self.assertEqual(mapped, expected[stage])
                    if stage in collection_stages:
                        for field in (b'"category_rules":', b'"module_path":', b'"source_list":'):
                            self.assertNotIn(field, payload)
                        with self.assertRaisesRegex(ValidationError, "category_rules"):
                            contract.model_validate_json(
                                json_object.dump_json(
                                    {
                                        **json_object.validate_json(payload),
                                        "category_rules": "unrelated policy",
                                    }
                                ),
                                strict=True,
                            )
                    if isinstance(mapped, RepositoryCatalogSettings):
                        source = registered.source_config(mapped.branch).repository_config
                        self.assertEqual(mapped.declaration_digest, source.branch_digest(mapped.branch))
                        self.assertEqual(
                            [
                                (origin.commit, origin.tree_digest, origin.license)
                                for origin in mapped.origins
                            ],
                            [
                                (origin.commit, origin.tree_digest, origin.license)
                                for origin in source.origins(mapped.branch)
                            ],
                        )
            self.assertEqual(registered.scopes.filtered_commands.filtered_commands, spec.filtering)
            self.assertEqual(registered.scopes.deduplicated_annotations.deduplication, spec.deduplication)
            self.assertEqual(registered.scopes.human_review_queue.human_review_queue, spec.publication)
            self.assertEqual(
                GuardedLabelingConfig.model_validate_json(
                    artifact_root.joinpath(*mapping_key("labeling_attempts").path).read_bytes()
                ).labeling_attempts.batch.grounding.pinned_library_ids,
                [provider_id],
            )
            published = DatasetDataService(home, instance).read_current(dataset_id, "filtered_commands").value
            assert isinstance(published, pd.DataFrame)
            self.assertCountEqual(
                list(
                    zip(
                        published["command"],
                        published["filter_decision"],
                        published["filter_reason"],
                        strict=True,
                    )
                ),
                [
                    (command, "needs_review", "oversized_or_control_character")
                    for command in ("docker compose ps --all", "docker ps --all", "terraform plan")
                ],
            )

            override = {
                **launch,
                "ops": {
                    **ops,
                    filtered.to_python_identifier(): {"config": {"maximum_command_characters": 32768}},
                },
            }
            with self.assertRaisesRegex(
                DagsterInvalidConfigError, "maximum_command_characters|filtered_commands"
            ):
                job.execute_in_process(
                    instance=instance,
                    partition_key=dataset_id,
                    asset_selection=selection,
                    run_config=override,
                )
