import tempfile
import unittest
from pathlib import Path

from configuration_fixture import write_fixture
from dagster import AssetKey

from codebench_pipeline.config_assets import configured_selection
from codebench_pipeline.dataset_config import catalog_for_branch, load_build_config
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.storage import IntegrityError


class ConfigAssetPolicyTests(unittest.TestCase):
    def test_selective_catalog_uses_pinned_policy_and_requires_same_run_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            original = load_build_config(write_fixture(home), home)
            pinned = original.model_copy(
                update={"rules": original.rules.model_copy(update={"category_name": "Pinned fixture policy"})}
            )
            DatasetRegistry(home).register(original)
            catalog = AssetKey(["datasets", "documentation", "source_catalog"])
            rules = AssetKey(["datasets", "category_rules"])
            job = build_definitions(home).resolve_job_def("build_dataset")

            with dataset_instance(home) as instance:
                instance.add_dynamic_partitions("dataset_builds", [original.dataset_id])
                result = job.execute_in_process(
                    instance=instance,
                    partition_key=original.dataset_id,
                    asset_selection=configured_selection([rules, catalog]),
                    run_config=dataset_run_config(pinned, home),
                )
                (materialization,) = result.asset_materializations_for_node(catalog.to_python_identifier())
                self.assertEqual(
                    materialization.metadata["input_fingerprint"].value,
                    stage_signature(
                        "source_catalog",
                        catalog_for_branch(pinned, "documentation"),
                        {"category_rules": pinned.rules},
                    ),
                )

                with self.assertRaisesRegex(IntegrityError, "Missing configuration.*map_documentation_crawl"):
                    job.execute_in_process(
                        instance=instance,
                        partition_key=original.dataset_id,
                        asset_selection=[catalog],
                        run_config=dataset_run_config(original, home),
                    )
