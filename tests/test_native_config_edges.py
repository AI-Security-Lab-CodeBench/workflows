from collections.abc import Callable, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

import pandas as pd
from configuration_fixture import write_fixture
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Config,
    DataVersion,
    Output,
    OutputContext,
    build_input_context,
)
from dagster._core.definitions.decorators.op_decorator import DecoratedOpFunction
from dagster._core.execution.context.system import StepExecutionContext
from pandera.typing import DataFrame
from pydantic import BaseModel, ValidationError
from test_dataset_rules import make_candidate, make_rules
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    REGISTERED_CONFIG_TYPE,
    ConfigRunIOManager,
    GlobalConfig,
    mapping_key,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    FilteredCommandsTable,
    validated_frame,
)
from codebench_pipeline.dataset_config import FilteringConfig, load_build_config
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import (
    DeclaredGlobalConfig,
    FilterConfig,
    stage_configs,
)
from codebench_pipeline.storage import IntegrityError


class ConfigArtifactIOTests(TestCase):
    def test_loads_prepared_global_json_from_existing_runs(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = load_build_config(write_fixture(home), home)
        path = home.joinpath("configuration-runs", "run-a", *GLOBAL_CONFIG.path)
        path.parent.mkdir(parents=True)
        path.write_text(spec.model_dump_json(), encoding="utf-8")
        context = build_input_context(
            asset_key=GLOBAL_CONFIG,
            step_context=Mock(spec=StepExecutionContext, run_id="run-a"),
            dagster_type=REGISTERED_CONFIG_TYPE,
        )

        self.assertEqual(ConfigRunIOManager(home=str(home)).load_input(context), spec)

    def test_loads_existing_scope_json_as_its_domain_type(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = load_build_config(write_fixture(home), home)
        key = mapping_key("filtered_commands")
        definition = next(
            asset
            for asset in build_definitions(home).assets or ()
            if isinstance(asset, AssetsDefinition) and key in asset.keys
        )
        step = Mock(spec=StepExecutionContext, run_id="run-a")
        context = build_input_context(
            asset_key=key, step_context=step, dagster_type=definition.op.output_defs[0].dagster_type
        )
        path = home.joinpath("configuration-runs", "run-a", *key.path)
        path.parent.mkdir(parents=True)
        settings = FilterConfig(category_rules=spec.rules, filtered_commands=spec.filtering)
        path.write_text(settings.model_dump_json(), encoding="utf-8")
        manager = ConfigRunIOManager(home=str(home))

        loaded = manager.load_input(context)

        self.assertEqual(loaded, settings)
        path.write_text(
            settings.model_dump_json().replace('"filtered_commands":{', '"filtered_commands":{"unknown":1,'),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValidationError, "filtered_commands.unknown"):
            manager.load_input(context)
        step.run_id = "run-b"
        with self.assertRaisesRegex(IntegrityError, "Missing configuration .* in this run"):
            manager.load_input(context)

    def test_declared_global_round_trip_keeps_pins_and_omits_runtime_paths(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        launch = GlobalConfig.for_spec(spec)
        self.assertEqual(launch.snapshot, "")
        self.assertNotEqual(launch.expected_revision, "")
        config = DeclaredGlobalConfig.from_spec(spec)
        manager = ConfigRunIOManager(home=str(home))
        output = Mock(spec=OutputContext, asset_key=GLOBAL_CONFIG, run_id="run-a")
        context = build_input_context(
            asset_key=GLOBAL_CONFIG,
            step_context=Mock(spec=StepExecutionContext, run_id="run-a"),
            dagster_type=REGISTERED_CONFIG_TYPE,
        )

        manager.handle_output(output, config)

        path = home.joinpath("configuration-runs", "run-a", *GLOBAL_CONFIG.path)
        serialized = path.read_text(encoding="utf-8")
        for forbidden in (
            "source_list",
            "expected_model_path",
            "state_directory",
            "retry_revision",
            "maximum_context_characters",
            str(home),
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertIn('"max_request_bytes":65536', serialized)
        self.assertEqual(manager.load_input(context), config)
        path.write_text(
            serialized.replace('"max_request_bytes":65536', '"max_request_bytes":"65536"'),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValidationError, r"labeling_attempts\.max_request_bytes"):
            manager.load_input(context)


class NativeConfigEdgesTests(TestCase):
    def test_configuration_only_job_persists_active_settings_without_processing(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        prepared = load_build_config(write_fixture(home), home)
        for spec, extra_scopes in (
            (prepared, {"labeling_documentation"}),
            (fixture_spec(home), {"keenable_use_cases", "merged_command_candidates"}),
        ):
            with self.subTest(declared=spec.has_declared_collection):
                registry = DatasetRegistry(home)
                dataset_id = registry.register(spec)
                definitions = build_definitions(home, specification=spec)
                job = definitions.resolve_job_def("refresh_dataset_configuration")
                expected = {
                    GLOBAL_CONFIG,
                    AssetKey(["datasets", "category_rules"]),
                    *(
                        mapping_key(scope)
                        for scope in {
                            "existing_datasets",
                            "documentation_crawl",
                            "filtered_commands",
                            "labeling_attempts",
                            "human_review_queue",
                            *extra_scopes,
                        }
                    ),
                }
                with dataset_instance(home) as instance:
                    instance.add_dynamic_partitions("dataset_builds", [dataset_id])
                    result = job.execute_in_process(instance=instance, partition_key=dataset_id)
                    self.assertEqual(
                        {event.asset_key for event in result.get_asset_materialization_events()}, expected
                    )
                self.assertTrue(result.success)
                for key in expected - {AssetKey(["datasets", "category_rules"])}:
                    self.assertTrue(home.joinpath("configuration-runs", result.run_id, *key.path).is_file())

    def test_filter_rejects_conflicting_op_config_before_cache_in_both_graphs(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        key = AssetKey(["datasets", "filtered_commands"])
        strict_filter = FilteringConfig(maximum_command_characters=1)
        rules = make_rules()
        candidates = validated_frame(
            [
                make_candidate()
                .model_copy(update={"command": "probe inspect example"})
                .model_dump(mode="json")
            ],
            CommandCandidatesTable,
        )

        def compute_without_persistence(
            context: AssetExecutionContext,
            storage: DataFrameIOManager,
            config: Config,
            inputs: Mapping[str, pd.DataFrame | CategoryRules],
            schema: type[FilteredCommandsTable],
            compute: Callable[[Path], DataFrame[FilteredCommandsTable]],
        ) -> Output[DataFrame[FilteredCommandsTable]]:
            return Output(
                schema.validate(compute(home)),
                data_version=DataVersion(stage_signature(context.asset_key.path[-1], config, inputs)),
            )

        for spec in (load_build_config(write_fixture(home), home), fixture_spec(home)):
            with self.subTest(declared=spec.has_declared_collection):
                spec = spec.model_copy(update={"filtering": strict_filter, "rules": rules})
                scope = FilterConfig(category_rules=spec.rules, filtered_commands=spec.filtering)
                definition = build_definitions(home, specification=spec).get_assets_def(key)
                op = definition.op.compute_fn
                assert isinstance(op, DecoratedOpFunction)
                context = Mock(spec=AssetExecutionContext, asset_key=key)
                storage = DataFrameIOManager(storage=spec.storage)
                with patch(
                    "codebench_pipeline.dataset_assets_sources.cached_table",
                    side_effect=compute_without_persistence,
                ) as cache:
                    with self.assertRaisesRegex(IntegrityError, "filtered_commands.*same-run mapped config"):
                        op.decorated_fn(context, FilteringConfig(), candidates, scope, storage)
                    cache.assert_not_called()

                    result: Output[DataFrame[FilteredCommandsTable]] = op.decorated_fn(
                        context, strict_filter, candidates, scope, storage
                    )
                self.assertEqual(result.value["filter_decision"].to_list(), ["needs_review"])
                self.assertEqual(result.value["filter_reason"].to_list(), ["oversized_or_control_character"])
                self.assertEqual(
                    result.data_version,
                    DataVersion(
                        stage_signature(
                            "filtered_commands",
                            stage_configs(spec)["filtered_commands"],
                            {"command_candidates": candidates, "category_rules": spec.rules},
                        )
                    ),
                )

    def test_config_edges_use_matching_domain_types_in_both_graphs(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        for spec in (load_build_config(write_fixture(home), home), fixture_spec(home)):
            definitions = build_definitions(home, specification=spec)
            assets = {
                asset.key: asset
                for asset in definitions.assets or ()
                if isinstance(asset, AssetsDefinition) and len(asset.keys) == 1
            }
            configs = {
                key: definition.op.output_defs[0].dagster_type
                for key, definition in assets.items()
                if key == GLOBAL_CONFIG or key.path[-1].startswith("map_")
            }
            for key, definition in assets.items():
                for name, dependency in definition.keys_by_input_name.items():
                    if dependency in configs:
                        with self.subTest(
                            declared=spec.has_declared_collection, asset=key.to_user_string(), input=name
                        ):
                            edge_type = definition.op.input_def_named(name).dagster_type
                            self.assertEqual(edge_type.key, configs[dependency].key)
                            if dependency != GLOBAL_CONFIG:
                                self.assertTrue(issubclass(edge_type.typing_type, BaseModel))

    def test_declared_consumers_only_depend_on_their_active_config(self) -> None:
        with TemporaryDirectory() as home:
            graph = build_definitions(
                Path(home), specification=fixture_spec(Path(home))
            ).resolve_asset_graph()

        branch_mappings = (
            (
                (),
                "existing_datasets",
                ("enumerate_corpus_files", "capture_corpus_files", "command_candidates"),
            ),
            (("documentation",), "documentation_crawl", ("capture_documentation", "command_candidates")),
            (
                ("keenable",),
                "keenable_use_cases",
                ("discover_use_cases", "capture_origin_pages", "command_candidates"),
            ),
        )
        shared = (
            "merged_command_candidates",
            "filtered_commands",
            "labeling_attempts",
            "human_review_queue",
        )
        expected = {
            AssetKey(["datasets", *prefix, stage]): AssetKey(["datasets", f"map_{scope}_config"])
            for prefix, scope, branch_stages in branch_mappings
            for stage in branch_stages
        } | {AssetKey(["datasets", stage]): AssetKey(["datasets", f"map_{stage}_config"]) for stage in shared}
        rules = AssetKey(["datasets", "category_rules"])
        global_config = AssetKey(["datasets", "global_config"])
        all_keys = graph.get_all_asset_keys()
        audit = AssetKey(["datasets", "bashguard_syntax_audit"])
        policy = {
            AssetKey(["datasets", name])
            for name in ("proposed_annotations", "category_eligible_annotations", "dataset_release")
        }
        no_config = {
            AssetKey(["datasets", name])
            for name in ("labeling_failures", "deduplicated_annotations", "human_review_decisions")
        }
        self.assertEqual(
            set(all_keys),
            set(expected) | set(expected.values()) | policy | no_config | {rules, global_config, audit},
        )
        self.assertEqual(graph.get(rules).parent_keys, {global_config})

        for stage, mapping in expected.items():
            with self.subTest(stage=stage.to_user_string()):
                parents = graph.get(stage).parent_keys
                self.assertEqual(parents & set(expected.values()), {mapping})
                self.assertNotIn(rules, parents)
        configs = set(expected.values()) | {global_config, rules}
        for stage in policy:
            self.assertEqual(graph.get(stage).parent_keys & configs, {global_config, rules})
        for stage in no_config:
            self.assertEqual(graph.get(stage).parent_keys & configs, set())
