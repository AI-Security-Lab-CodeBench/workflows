import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import (
    AssetIn,
    AssetKey,
    DagsterInstance,
    Definitions,
    IOManagerDefinition,
    JobDefinition,
    MetadataValue,
    Output,
    PartitionKeyRange,
    ReexecutionOptions,
    StaticPartitionsDefinition,
    asset,
    build_output_context,
    build_reconstructable_job,
    define_asset_job,
    execute_job,
    in_process_executor,
)
from dagster._core.storage.tags import PARTITION_NAME_TAG
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.config_assets import (
    GLOBAL_CONFIG,
    ConfigRunIOManager,
    RegisteredConfig,
    mapped_input,
    mapping_key,
    registered_input,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_config import FilteringConfig
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_storage_io import RegisteredDatasetTableIO
from codebench_pipeline.human_review_store import HumanReviewStore
from codebench_pipeline.object_store import StorageConfig
from codebench_pipeline.stage_configuration import FilterConfig
from codebench_pipeline.storage import fingerprint

TABLE = AssetKey("retry_table")
MAPPING = mapping_key("filtered_commands")
CONFIG_RESULT = AssetKey("retry_config_result")
TABLE_RESULT = AssetKey("retry_table_result")


def retry_job(directory: str, with_registration: bool = False) -> JobDefinition:
    home = Path(directory)
    spec = fixture_spec(home)
    partitions = StaticPartitionsDefinition([spec.dataset_id])
    tags = {PARTITION_NAME_TAG: spec.dataset_id}
    storage = StorageConfig(local_root=str(home / "tables"))
    tables = DataFrameIOManager(storage=storage)
    if with_registration:
        tables = RegisteredDatasetTableIO(
            storage=spec.storage,
            review_home=str(home),
            registered_revision=DatasetRegistry(home).current_revision(spec.dataset_id),
        )
        tables.bind_review_store(HumanReviewStore(home))

    @asset(key=GLOBAL_CONFIG, partitions_def=partitions, io_manager_key="config_io")
    def global_config() -> RegisteredConfig:
        return RegisteredConfig.from_spec(spec)

    @asset(
        key=MAPPING,
        partitions_def=partitions,
        io_manager_key="config_io",
        ins={"registered": registered_input()},
    )
    def config(registered: RegisteredConfig) -> FilterConfig:
        value = int((home / "source").read_text(encoding="utf-8"))
        return FilterConfig(
            category_rules=registered.rules,
            filtered_commands=FilteringConfig(maximum_command_characters=value),
        )

    @asset(key=TABLE, partitions_def=partitions, io_manager_key="tables")
    def table() -> pd.DataFrame:
        value = int((home / "source").read_text(encoding="utf-8"))
        return pd.DataFrame({"value": [value]})

    @asset(
        key=CONFIG_RESULT,
        partitions_def=partitions,
        ins={"settings": mapped_input("filtered_commands", FilterConfig)},
    )
    def config_result(settings: FilterConfig) -> Output[int]:
        if (home / "fail").exists():
            raise RuntimeError("Fixture stopped after loading configuration")
        value = settings.filtered_commands.maximum_command_characters
        return Output(value, metadata={"value": value})

    @asset(key=TABLE_RESULT, partitions_def=partitions, ins={"rows": AssetIn(key=TABLE)})
    def table_result(rows: pd.DataFrame) -> Output[int]:
        if (home / "fail").exists():
            raise RuntimeError("Fixture stopped after loading the table")
        value = int(str(rows["value"].iat[0]))
        return Output(value, metadata={"value": value})

    definitions = Definitions(
        assets=[global_config, config, table, config_result, table_result],
        jobs=[define_asset_job("retry_io", executor_def=in_process_executor, run_tags=tags)],
        resources={
            "config_io": ConfigRunIOManager(home=str(home)),
            "tables": IOManagerDefinition.hardcoded_io_manager(tables),
        },
    )
    return definitions.resolve_job_def("retry_io")


class NativeRetryIOTests(unittest.TestCase):
    def test_retry_loads_parent_config_and_table_after_another_run_materializes(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        instance = self.enterContext(DagsterInstance.local_temp(str(home)))
        spec = fixture_spec(home)
        registration = home / "dataset-specifications" / spec.dataset_id
        registration.mkdir(parents=True)
        (registration / "current").write_text(fingerprint(spec.model_dump_json()), encoding="ascii")
        job = build_reconstructable_job(
            __name__,
            "retry_job",
            reconstructable_args=(str(home),),
            reconstructable_kwargs={"with_registration": True},
        )
        source = home / "source"
        failure = home / "fail"
        source.write_text("101", encoding="utf-8")
        failure.touch()
        failed = execute_job(job, instance)
        self.assertFalse(failed.success)
        self.assertEqual(
            {event.step_key for event in failed.get_step_failure_events()},
            {"retry_config_result", "retry_table_result"},
        )
        failure.unlink()
        source.write_text("202", encoding="utf-8")
        self.assertTrue(execute_job(job, instance).success)

        retry = execute_job(
            job, instance, reexecution_options=ReexecutionOptions.from_failure(failed.run_id, instance)
        )
        self.assertTrue(retry.success)
        self.assertEqual(
            {event.step_key for event in retry.all_events if event.is_step_start},
            {"retry_config_result", "retry_table_result"},
        )
        for key in (CONFIG_RESULT, TABLE_RESULT):
            [materialization] = retry.asset_materializations_for_node(key.to_python_identifier())
            self.assertEqual(materialization.metadata["value"], MetadataValue.int(101))

    def test_selected_load_rejects_a_pointer_without_matching_dagster_provenance(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        instance = self.enterContext(DagsterInstance.local_temp(str(home)))
        job = build_reconstructable_job(__name__, "retry_job", reconstructable_args=(str(home),))
        (home / "source").write_text("101", encoding="utf-8")
        self.assertTrue(execute_job(job, instance).success)
        self.assertTrue(execute_job(job, instance, asset_selection=[TABLE_RESULT]).success)
        manager = DataFrameIOManager(storage=StorageConfig(local_root=str(home / "tables")))
        replacement = manager.persist_value(pd.DataFrame({"value": [202]}))
        dataset_id = fixture_spec(home).dataset_id
        context = build_output_context(
            asset_key=TABLE,
            partition_key=dataset_id,
            asset_partitions_def=StaticPartitionsDefinition([dataset_id]),
            asset_partition_key_range=PartitionKeyRange(dataset_id, dataset_id),
        )
        manager.publish_reference(context, replacement)

        selected = execute_job(job, instance, asset_selection=[TABLE_RESULT])

        [failure] = selected.get_step_failure_events()
        error = failure.step_failure_data.error
        self.assertIsNotNone(error)
        assert error is not None
        self.assertIsNotNone(error.cause)
        assert error.cause is not None
        self.assertEqual(error.cause.cls_name, "IntegrityError")
        self.assertIn("differs from its recorded materialization", error.cause.message)
