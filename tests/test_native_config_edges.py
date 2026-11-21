import os
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from dagster import (
    AssetKey,
    AssetsDefinition,
    DagsterInstance,
    IOManagerDefinition,
    MetadataValue,
    Output,
    OutputContext,
    asset,
    build_input_context,
    materialize,
)
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
    RegisteredConfig,
    configuration_assets,
    mapping_key,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import CommandCandidatesTable, validated_frame
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, RULES_TYPE, TABLE_TYPES
from codebench_pipeline.dataset_assets_sources import filtered_commands
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_extract import filter_candidates
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO
from codebench_pipeline.finalization_table_io import finalization_table_io
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.labeling_stage_settings import RiskRoute
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import (
    DeclaredGlobalConfig,
    FilterConfig,
)
from codebench_pipeline.storage import IntegrityError


class ConfigArtifactIOTests(TestCase):
    def test_rejects_unmapped_registration_json_as_same_run_global_config(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        path = home.joinpath("configuration-runs", "run-a", *GLOBAL_CONFIG.path)
        path.parent.mkdir(parents=True)
        path.write_text(spec.model_dump_json(), encoding="utf-8")
        context = build_input_context(
            asset_key=GLOBAL_CONFIG,
            step_context=Mock(spec=StepExecutionContext, run_id="run-a"),
            dagster_type=REGISTERED_CONFIG_TYPE,
        )

        with self.assertRaisesRegex(ValidationError, "scopes"):
            ConfigRunIOManager(home=str(home)).load_input(context)

    def test_loads_existing_scope_json_as_its_domain_type(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
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
        settings = FilterConfig(
            filtered_commands=spec.filtering,
            routes=tuple(RiskRoute.from_rule(risk) for risk in spec.rules.risks),
        )
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
    def test_run_output_keeps_using_review_store_bound_before_computation(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        dataset_id = DatasetRegistry(home).register(fixture_spec(home))
        job = build_definitions(home).resolve_job_def("refresh_dataset_configuration")
        write_configuration = ConfigRunIOManager.handle_output

        with dataset_instance(home) as instance, ExitStack() as stack:

            def record_configuration(
                manager: ConfigRunIOManager, context: OutputContext, value: BaseModel
            ) -> None:
                write_configuration(manager, context, value)
                if context.asset_key == GLOBAL_CONFIG:
                    stack.enter_context(patch.dict(os.environ, {"CODEBENCH_REVIEW_DATABASE_URL": ""}))

            stack.enter_context(patch.object(ConfigRunIOManager, "handle_output", record_configuration))
            instance.add_dynamic_partitions("dataset_builds", [dataset_id])
            result = job.execute_in_process(instance=instance, partition_key=dataset_id)

        self.assertEqual(len(result.asset_materializations_for_node("datasets__category_rules")), 1)

    def test_filter_uses_mapped_settings_for_decisions_and_fingerprint(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        key = AssetKey(["datasets", "filtered_commands"])
        upstream = AssetKey(["datasets", "merged_command_candidates"])
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
        spec = fixture_spec(home).model_copy(update={"filtering": strict_filter, "rules": rules})
        registered = RegisteredConfig.from_spec(spec)
        binding = {"dataset": {"state": "configured", "digest": "0" * 64}}
        candidates.attrs[FINALIZATION_BINDING] = binding
        store = RegisteredDatasetTableIO(storage=spec.storage, review_home=str(home))
        store.bind_review_store(MagicMock(spec=HumanReviewStore))
        instance = self.enterContext(DagsterInstance.local_temp(str(home)))
        instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])

        @asset(key=GLOBAL_CONFIG, partitions_def=DATASET_PARTITIONS, io_manager_key="config_run_io_manager")
        def global_config() -> RegisteredConfig:
            return registered

        @asset(
            key=AssetKey(["datasets", "category_rules"]),
            partitions_def=DATASET_PARTITIONS,
            io_manager_key="dataset_io_manager",
            dagster_type=RULES_TYPE,
        )
        def category_rules() -> CategoryRules:
            return rules

        @asset(
            key=upstream,
            partitions_def=DATASET_PARTITIONS,
            io_manager_key="dataset_io_manager",
            dagster_type=TABLE_TYPES[CommandCandidatesTable],
        )
        def merged_command_candidates() -> Output[DataFrame[CommandCandidatesTable]]:
            settings = registered.scopes.merged_command_candidates.merged_command_candidates
            return Output(
                candidates,
                metadata={"evidence": MetadataValue.json({"config": settings.model_dump(mode="json")})},
            )

        mapped = next(
            item for item in configuration_assets(home) if item.key == mapping_key("filtered_commands")
        )
        assets = [global_config, category_rules, mapped, merged_command_candidates, filtered_commands]
        resources = {
            "config_run_io_manager": ConfigRunIOManager(home=str(home)),
            "dataset_io_manager": IOManagerDefinition.hardcoded_io_manager(store),
            FINALIZATION_TABLE_IO: finalization_table_io,
        }

        def run_configuration(run_id: str, dataset_id: str) -> RegisteredConfig:
            return ConfigRunIOManager(home=str(home)).registered(run_id)

        with (
            patch.object(DataFrameIOManager, "run_configuration", side_effect=run_configuration),
            patch("codebench_pipeline.dataset_collection_binding.finalization_binding", return_value=binding),
            patch(
                "codebench_pipeline.review_output_state.registered_materialization_stale", return_value=False
            ),
            patch(
                "codebench_pipeline.dataset_assets_sources.filter_candidates",
                wraps=filter_candidates,
            ) as compute,
        ):
            first = materialize(assets, resources=resources, instance=instance, partition_key=spec.dataset_id)
            reused = materialize(
                assets, resources=resources, instance=instance, partition_key=spec.dataset_id
            )
            compute.assert_called_once()
            registered = RegisteredConfig.from_spec(spec.model_copy(update={"filtering": FilteringConfig()}))
            control = materialize(
                assets, resources=resources, instance=instance, partition_key=spec.dataset_id
            )
        node = key.to_python_identifier()
        result = first.output_for_node(node)
        self.assertEqual(result["filter_decision"].to_list(), ["needs_review"])
        self.assertEqual(result["filter_reason"].to_list(), ["oversized_or_control_character"])
        self.assertEqual(control.output_for_node(node)["filter_decision"].to_list(), ["eligible"])
        for run, expected_hit in ((first, False), (reused, True), (control, False)):
            metadata = run.asset_materializations_for_node(node)[0].metadata
            self.assertIs(metadata["cache_hit"].value, expected_hit)
            self.assertEqual(run.output_for_node(node).attrs[FINALIZATION_BINDING], binding)
        strict_metadata = first.asset_materializations_for_node(node)[0].metadata
        self.assertNotEqual(
            strict_metadata["input_fingerprint"],
            control.asset_materializations_for_node(node)[0].metadata["input_fingerprint"],
        )

    def test_registration_rejects_metadata_only_configuration_bypasses(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        settings = AssetKey("future_settings")

        @asset(key=settings, group_name="configuration")
        def future_settings() -> str:
            return "settings"

        @asset(group_name="future_processing")
        def future_consumer() -> str:
            return "output"

        mapped = mapping_key("filtered_commands")
        for parent, reason in (
            (settings, "future_consumer bypasses scoped configuration: future_settings"),
            (mapped, f"{mapped.to_user_string()} must belong to the group of future_consumer"),
        ):
            bypass = future_consumer.map_asset_specs(lambda spec: spec.merge_attributes(deps=[parent]))
            with (
                self.subTest(parent=parent),
                patch(
                    "codebench_pipeline.definitions.configuration_assets",
                    return_value=[*configuration_assets(home), future_settings],
                ),
                patch("codebench_pipeline.definitions.bashguard_syntax_audit", bypass),
                self.assertRaisesRegex(IntegrityError, reason),
            ):
                build_definitions(home)
