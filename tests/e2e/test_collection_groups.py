from pathlib import Path
from typing import Final
from unittest.mock import patch

import pandas as pd
import pytest
from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, Definitions, ExecuteInProcessResult
from test_declared_dataset_graph import collection_fixture, fixture_spec, with_keenable_declaration
from test_keenable import PEER, PLAN, SOURCE_BODY, permission, result

from codebench_pipeline.config_assets import CONFIG_DIRECTORY
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.keenable_policy import CASES, KeenableDeclaration
from codebench_pipeline.keenable_transport import DirectResponse
from codebench_pipeline.runtime_instance import dataset_instance

MERGE_KEY: Final = AssetKey(["datasets", "merged_command_candidates"])


def materialize_collection_outputs(
    definitions: Definitions, instance: DagsterInstance, partition: str
) -> ExecuteInProcessResult:
    ancestors = definitions.resolve_asset_graph().get_ancestor_asset_keys(MERGE_KEY, include_self=True)
    return definitions.resolve_job_def("prepare_dataset").execute_in_process(
        instance=instance,
        partition_key=partition,
        asset_selection=list(ancestors - {CONFIG_DIRECTORY}),
    )


def test_merge_consumes_all_three_collection_group_outputs(tmp_path: Path) -> None:
    with (
        collection_fixture(fixture_spec(tmp_path)) as spec,
        dataset_instance(tmp_path) as instance,
        patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
        patch(
            "codebench_pipeline.keenable.request_direct",
            side_effect=(result(PLAN), DirectResponse(SOURCE_BODY, PEER)),
        ),
    ):
        DatasetRegistry(tmp_path).register(spec)
        instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        definitions = build_definitions(tmp_path)
        run = materialize_collection_outputs(definitions, instance, spec.dataset_id)
        assert run.success

        data = DatasetDataService(tmp_path, instance)
        merged = data.read_current(spec.dataset_id, "merged_command_candidates")
        assert merged.publication_state is None
        assert isinstance(merged.value, pd.DataFrame)
        merged_rows = frame_records(merged.value)
        assert len(merged_rows) == 3
        rows_by_id = {str(row["candidate_id"]): row for row in merged_rows}
        for branch, stage, command, weight in (
            ("dataset", "command_candidates", "docker compose ps --all", 0.62),
            ("documentation", "documentation/command_candidates", "docker ps --all", 0.9),
            ("keenable", "keenable/command_candidates", "terraform plan", 0.24),
        ):
            output = data.read_current(spec.dataset_id, stage)
            assert isinstance(output.value, pd.DataFrame)
            (original,) = frame_records(output.value)
            assert original["command"] == command
            identifier = f"{branch}:{original['candidate_id']}"
            assert rows_by_id[identifier] == {
                **original,
                "candidate_id": identifier,
                "source_id": f"{branch}:{original['source_id']}",
                "source_weight": weight,
            }
            assert merged.value.attrs[FINALIZATION_BINDING][branch] == {
                "state": "configured",
                "digest": output.reference.digest,
            }


def test_configured_empty_keenable_merges_but_failed_replacement_cannot_reuse_it(tmp_path: Path) -> None:
    with collection_fixture(fixture_spec(tmp_path)) as spec, dataset_instance(tmp_path) as instance:
        registry = DatasetRegistry(tmp_path)
        registry.register(spec)
        instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        definitions = build_definitions(tmp_path)

        with (
            patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
            patch("codebench_pipeline.keenable.request_direct", side_effect=(result(),)) as search,
        ):
            healthy = materialize_collection_outputs(definitions, instance, spec.dataset_id)
        assert search.called
        assert healthy.success
        data = DatasetDataService(tmp_path, instance)
        output = data.read_current(spec.dataset_id, "keenable/command_candidates")
        assert output.publication_state is None
        assert isinstance(output.value, pd.DataFrame)
        assert output.value.empty
        merged = data.read_current(spec.dataset_id, "merged_command_candidates")
        assert isinstance(merged.value, pd.DataFrame)
        assert sorted(str(row["command"]) for row in frame_records(merged.value)) == [
            "docker compose ps --all",
            "docker ps --all",
        ]
        assert merged.publication_state is None
        assert merged.value.attrs[FINALIZATION_BINDING]["keenable"] == {
            "state": "configured",
            "digest": output.reference.digest,
        }
        listing = AssetRecordsFilter(asset_key=MERGE_KEY, asset_partitions=[spec.dataset_id])
        (previous_merge,) = instance.fetch_materializations(listing, limit=1).records

        broken = KeenableDeclaration(
            dataset=spec.dataset_id,
            generation="fixture-failed",
            generation_review="fixture-review",
            cases=CASES,
            permissions=(permission(),),
        )
        registry.register(with_keenable_declaration(spec, broken))
        with (
            patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=(result(PLAN), *(result() for _ in CASES[1:]), DirectResponse(SOURCE_BODY, PEER)),
            ),
        ):
            failed = definitions.resolve_job_def("build_keenable").execute_in_process(
                instance=instance, partition_key=spec.dataset_id, raise_on_error=False
            )
        assert not failed.success
        (failure,) = failed.get_step_failure_events()
        assert failure.step_key == "datasets__keenable__capture_origin_pages"
        assert failure.step_failure_data is not None
        error = failure.step_failure_data.error
        assert error is not None
        assert "origin_pin_mismatch (200)" in error.to_string()

        merge_job = definitions.get_implicit_global_asset_job_def()
        with pytest.raises(HumanReviewError, match="keenable acquisition receipt binding changed"):
            merge_job.execute_in_process(
                instance=instance, partition_key=spec.dataset_id, asset_selection=[MERGE_KEY]
            )
        (latest_merge,) = instance.fetch_materializations(listing, limit=1).records
        assert latest_merge.storage_id == previous_merge.storage_id
