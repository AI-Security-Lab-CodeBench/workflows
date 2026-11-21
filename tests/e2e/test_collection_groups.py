from pathlib import Path
from typing import Final

import pandas as pd
import pytest
from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, Definitions, ExecuteInProcessResult
from keenable_mcp_fixture import SNIPPET, keenable_mcp_fixture
from test_declared_dataset_graph import (
    collection_fixture,
    fixture_spec,
    with_keenable_declaration,
)
from test_keenable import PLAN

from codebench_pipeline.collection_config import KeenableSnippetDeclaration
from codebench_pipeline.config_assets import CONFIG_DIRECTORY
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.dataset_collection_binding import FINALIZATION_BINDING
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.definitions import build_definitions
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.keenable_inspection import INSPECTIONS
from codebench_pipeline.keenable_snippets import SnippetCollectionReceipt
from codebench_pipeline.keenable_tables import PROVIDER_RECEIPT
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.source_evidence import SOURCE_RECEIPT

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
        keenable_mcp_fixture() as queries,
    ):
        DatasetRegistry(tmp_path).register(spec)
        instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        definitions = build_definitions(tmp_path)
        run = materialize_collection_outputs(definitions, instance, spec.dataset_id)
        assert run.success
        assert queries == [INSPECTIONS["terraform_preview"].query]

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
            if branch == "keenable":
                assert original["source_url"] == PLAN
                assert original["context"] == "Preview the changes:"
                raw = output.value.attrs[PROVIDER_RECEIPT]
                receipt = SnippetCollectionReceipt.model_validate_json(raw)
                evidence = receipt.candidates[str(original["candidate_id"])]
                assert (evidence.source_type, evidence.url, evidence.license) == (
                    "web_page",
                    PLAN,
                    "keenable",
                )
                assert receipt.read_body(evidence.body_sha256) == SNIPPET.encode()
                branch_evidence = merged.value.attrs["branch_metadata"][branch]
                assert branch_evidence[PROVIDER_RECEIPT] == raw
                assert branch_evidence[SOURCE_RECEIPT] == output.value.attrs[SOURCE_RECEIPT]
                storage = DataFrameIOManager.from_environment(spec.storage)
                assert storage.read_evidence(branch_evidence[SOURCE_RECEIPT]["key"]) == raw.encode()
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

        with keenable_mcp_fixture(empty=True) as queries:
            healthy = materialize_collection_outputs(definitions, instance, spec.dataset_id)
        assert queries == [INSPECTIONS["terraform_preview"].query]
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

        assert spec.declared_collection is not None
        declaration = spec.declared_collection.keenable_declaration
        assert isinstance(declaration, KeenableSnippetDeclaration)
        broken = declaration.model_copy(
            update={"search": declaration.search.model_copy(update={"seeds": ("docker_inventory",)})}
        )
        registry.register(with_keenable_declaration(spec, broken))
        with keenable_mcp_fixture(failed=True):
            failed = definitions.resolve_job_def("build_keenable").execute_in_process(
                instance=instance, partition_key=spec.dataset_id, raise_on_error=False
            )
        assert not failed.success
        (failure,) = failed.get_step_failure_events()
        assert failure.step_key == "datasets__keenable__command_candidates"
        assert failure.step_failure_data is not None
        error = failure.step_failure_data.error
        assert error is not None
        assert "provider_search_failed" in error.to_string()

        merge_job = definitions.get_implicit_global_asset_job_def()
        with pytest.raises(HumanReviewError, match="keenable acquisition receipt binding changed"):
            merge_job.execute_in_process(
                instance=instance, partition_key=spec.dataset_id, asset_selection=[MERGE_KEY]
            )
        (latest_merge,) = instance.fetch_materializations(listing, limit=1).records
        assert latest_merge.storage_id == previous_merge.storage_id
