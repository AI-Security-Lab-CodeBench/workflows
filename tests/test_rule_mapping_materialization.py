import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from dagster import AssetKey
from dagster._core.definitions.data_version import extract_data_version_from_entry
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.config_assets import GLOBAL_CONFIG, ConfigRunIOManager, mapping_key
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.runtime_instance import dataset_instance

FILTER = mapping_key("filtered_commands")
LABELING = mapping_key("labeling_attempts")
PROPOSAL = mapping_key("proposed_annotations")
ELIGIBILITY = mapping_key("category_eligible_annotations")
REVIEW = mapping_key("human_review_queue")
RELEASE = mapping_key("dataset_release")
RULE_MAPPINGS = (FILTER, LABELING, PROPOSAL, ELIGIBILITY, REVIEW, RELEASE)


class RuleMappingMaterializationTests(TestCase):
    def test_rule_mapping_versions_change_only_when_projected_values_change(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = fixture_spec(home)
        rules = spec.rules
        prose = rules.model_copy(update={"policy": "Revised fixture labeling instructions."})
        risk, *others = rules.risks
        routed = rules.model_copy(
            update={"risks": [risk.model_copy(update={"command_patterns": ["^fixture-report$"]}), *others]}
        )
        registry = DatasetRegistry(home)
        definitions = build_definitions(home)
        graph = definitions.resolve_asset_graph()
        job = definitions.get_implicit_global_asset_job_def()
        instance = self.enterContext(dataset_instance(home))
        instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        self.enterContext(patch.dict(os.environ, {"CODEBENCH_CONFIG_ROOT": str(home / "absent")}))

        def materialize(revision: DatasetBuildSpec) -> tuple[str, dict[AssetKey, str]]:
            registry.register(revision)
            result = job.execute_in_process(
                instance=instance,
                partition_key=revision.dataset_id,
                asset_selection=[GLOBAL_CONFIG, *RULE_MAPPINGS],
                run_config=dataset_run_config(revision),
            )
            self.assertEqual(
                {event.asset_key for event in result.get_asset_materialization_events()},
                {GLOBAL_CONFIG, *RULE_MAPPINGS},
            )
            versions: dict[AssetKey, str] = {}
            for key in RULE_MAPPINGS:
                consumer = AssetKey(["datasets", key.path[-1].removeprefix("map_").removesuffix("_config")])
                producer_type = graph.get(key).assets_def.op.output_defs[0].dagster_type
                consumer_type = (
                    graph.get(consumer).assets_def.op.input_def_named("mapped_config").dagster_type
                )
                self.assertIs(producer_type, consumer_type)
                path = home.joinpath("configuration-runs", result.run_id, *key.path)
                persisted = consumer_type.typing_type.model_validate_json(path.read_bytes(), strict=True)
                self.assertEqual(persisted, result.output_for_node(key.to_python_identifier()))
                record = instance.get_latest_materialization_event(key)
                assert record is not None
                version = extract_data_version_from_entry(record)
                assert version is not None
                versions[key] = version.value
            return result.run_id, versions

        original_id, original = materialize(spec)
        _, prose_only = materialize(spec.model_copy(update={"rules": prose}))
        routed_id, routing_only = materialize(spec.model_copy(update={"rules": routed}))

        self.assertEqual({key for key in original if original[key] != prose_only[key]}, {LABELING})
        self.assertEqual(
            {key for key in original if original[key] != routing_only[key]},
            {FILTER, LABELING, PROPOSAL, REVIEW, RELEASE},
        )
        filter_path = home.joinpath("configuration-runs", routed_id, *FILTER.path)
        filter_value = json.loads(filter_path.read_bytes())
        self.assertEqual(set(filter_value), {"filtered_commands", "routes"})
        self.assertEqual(filter_value["routes"][0], {"id": risk.id, "command_patterns": ["^fixture-report$"]})
        original_evidence = ConfigRunIOManager(home=str(home)).registered(original_id)
        self.assertEqual(original_evidence.rules, rules)
