from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd
from dagster import AssetKey
from dagster._core.events import LoadedInputData
from guarded_labeling_fixture import LabelingFixture
from test_declared_dataset_graph import with_keenable_declaration

from codebench_pipeline.collection_config import KeenableSnippetDeclaration
from codebench_pipeline.config_assets import mapping_key
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.keenable_policy import KeenableSnippetSettings
from codebench_pipeline.runtime_instance import dataset_instance


class NativeProcessingInputTests(TestCase):
    def test_selected_merge_consumes_only_dagster_inputs_and_keeps_history(self) -> None:
        home = Path(self.enterContext(TemporaryDirectory()))
        spec = LabelingFixture(self, home).spec
        spec = with_keenable_declaration(
            spec,
            KeenableSnippetDeclaration(
                schema_version="4",
                dataset_id=spec.dataset_id,
                search=KeenableSnippetSettings(seeds=()),
            ),
        )
        partition = DatasetRegistry(home).register(spec)
        instance = self.enterContext(dataset_instance(home))
        instance.add_dynamic_partitions("dataset_builds", [partition])
        definitions = build_definitions(home)
        job = definitions.resolve_job_def("prepare_dataset")
        mapping = mapping_key("merged_command_candidates")
        merge = AssetKey(["datasets", "merged_command_candidates"])
        manager = DataFrameIOManager.from_environment(spec.storage)

        prepared = job.execute_in_process(instance=instance, partition_key=partition)
        self.assertTrue(prepared.success)
        original = manager.reference_at(merge.path, partition)
        branch_references = {
            branch.namespace: manager.reference_at(("datasets", *branch.terminal.split("/")), partition)
            for branch in COLLECTION_BRANCHES
        }
        refreshed = job.execute_in_process(
            instance=instance, partition_key=partition, asset_selection=[mapping]
        )
        self.assertTrue(refreshed.success)
        selected = job.execute_in_process(instance=instance, partition_key=partition, asset_selection=[merge])
        self.assertTrue(selected.success)
        self.assertEqual(
            {event.step_key for event in selected.all_events if event.is_step_start},
            {merge.to_python_identifier()},
        )
        self.assertCountEqual(
            [
                data.input_name
                for event in selected.all_events
                if isinstance(data := event.event_specific_data, LoadedInputData)
            ],
            ["mapped_config", *(f"{branch}_candidates" for branch in branch_references)],
        )
        [before] = prepared.asset_materializations_for_node(mapping.to_python_identifier())
        [after] = refreshed.asset_materializations_for_node(mapping.to_python_identifier())
        assert before.tags is not None and after.tags is not None
        self.assertEqual(before.tags["dagster/data_version"], after.tags["dagster/data_version"])
        value = manager.read_version(manager.reference_at(merge.path, partition))
        previous = manager.read_version(original)
        assert isinstance(value, pd.DataFrame) and isinstance(previous, pd.DataFrame)
        self.assertEqual(
            value.attrs[FINALIZATION_BINDING],
            {
                branch: {"state": "configured", "digest": reference.digest}
                for branch, reference in branch_references.items()
            },
        )
        self.assertEqual(previous["candidate_id"].tolist(), value["candidate_id"].tolist())
