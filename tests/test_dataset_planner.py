import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from dagster import (
    AssetSelection,
    AutomationCondition,
    Config,
    DataVersion,
    Definitions,
    Output,
    StaticPartitionsDefinition,
    asset,
    evaluate_automation_conditions,
    materialize,
)
from dagster._core.definitions.data_version import StaleStatus

from codebench_pipeline.dataset_provenance import provenance_resolver
from codebench_pipeline.runtime_instance import dataset_instance


class DatasetPlannerTests(unittest.TestCase):
    def test_native_status_uses_persisted_data_versions_for_the_selected_partition(self) -> None:
        partitions = StaticPartitionsDefinition(["first", "second"])

        class UpstreamConfig(Config):
            version: str = "original"

        @asset(partitions_def=partitions)
        def upstream(config: UpstreamConfig) -> Output[str]:
            return Output(config.version, data_version=DataVersion(config.version))

        @asset(partitions_def=partitions)
        def downstream(upstream: str) -> str:
            return upstream

        assets = [upstream, downstream]
        with TemporaryDirectory() as directory, dataset_instance(Path(directory)) as instance:
            with patch("codebench_pipeline.definitions.defs", Definitions(assets=assets)):
                for partition in ("first", "second"):
                    materialize(assets, instance=instance, partition_key=partition)
                for version, expected in (
                    ("original", StaleStatus.FRESH),
                    ("changed", StaleStatus.STALE),
                ):
                    with self.subTest(version=version):
                        materialize(
                            assets,
                            instance=instance,
                            partition_key="first",
                            selection=AssetSelection.assets(upstream),
                            run_config={"ops": {"upstream": {"config": {"version": version}}}},
                        )
                        resolver = provenance_resolver(instance)
                        self.assertEqual(resolver.get_status(downstream.key, "first"), expected)
                        self.assertEqual(resolver.get_status(downstream.key, "second"), StaleStatus.FRESH)

    def test_native_condition_waits_for_missing_transitive_partition_inputs(self) -> None:
        partitions = StaticPartitionsDefinition(["first", "second"])
        ready = AutomationCondition.missing() & ~AutomationCondition.any_deps_missing()

        @asset(partitions_def=partitions)
        def evidence() -> str:
            return "captured"

        @asset(partitions_def=partitions, automation_condition=ready)
        def mapping(evidence: str) -> str:
            return evidence

        @asset(partitions_def=partitions, automation_condition=ready)
        def consumer(mapping: str) -> str:
            return mapping

        assets = [evidence, mapping, consumer]
        definitions = Definitions(assets=assets)
        with TemporaryDirectory() as directory, dataset_instance(Path(directory)) as instance:
            blocked = evaluate_automation_conditions(defs=definitions, instance=instance)
            self.assertEqual(blocked.get_requested_partitions(consumer.key), set())
            materialize(assets, instance=instance, partition_key="first", selection=[evidence])
            requested = evaluate_automation_conditions(
                defs=definitions, instance=instance, cursor=blocked.cursor
            )
            self.assertEqual(requested.get_requested_partitions(mapping.key), {"first"})
            self.assertEqual(requested.get_requested_partitions(consumer.key), {"first"})
