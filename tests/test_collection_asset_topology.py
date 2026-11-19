import tempfile
import unittest
from pathlib import Path

from dagster import AssetKey, Definitions, validate_run_config
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.artifact_schemas import declared_artifact_schemas
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.stage_configuration import collection_asset_settings


def key(stage: str) -> AssetKey:
    return AssetKey(["datasets", *stage.split("/")])


class CollectionAssetTopologyTests(unittest.TestCase):
    def test_declared_branch_jobs_follow_their_own_evidence_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            spec = fixture_spec(home)
            definitions = build_definitions(home, specification=spec)
            Definitions.validate_loadable(definitions)
            graph = definitions.resolve_asset_graph()
            schemas = declared_artifact_schemas(definitions)

        cases: tuple[tuple[Branch, str, str, tuple[str, ...]], ...] = (
            (
                "dataset",
                "build_existing_datasets",
                "existing_datasets",
                ("enumerate_corpus_files", "capture_corpus_files", "command_candidates"),
            ),
            (
                "documentation",
                "build_documentation",
                "documentation_crawl",
                ("documentation/capture_documentation", "documentation/command_candidates"),
            ),
            (
                "keenable",
                "build_keenable",
                "keenable_use_cases",
                (
                    "keenable/discover_use_cases",
                    "keenable/capture_origin_pages",
                    "keenable/command_candidates",
                ),
            ),
        )
        for branch, job_name, group, stages in cases:
            with self.subTest(job=job_name):
                job = definitions.resolve_job_def(job_name)
                validate_run_config(job, dataset_run_config(spec, home, branch).to_config_dict())
                mapping = key(f"map_{group}_config")
                self.assertEqual(
                    {node.name for node in job.nodes},
                    {
                        node.to_python_identifier()
                        for node in (key("global_config"), key("category_rules"), mapping, *map(key, stages))
                    },
                )
                for position, stage in enumerate(stages):
                    asset = graph.get(key(stage))
                    expected = {mapping}
                    if position:
                        expected.add(key(stages[position - 1]))
                    self.assertEqual(asset.parent_keys, expected)
                    self.assertEqual(asset.group_name, group)
                    schema = (
                        "CommandCandidatesTable"
                        if position == len(stages) - 1
                        else "SourceDocumentsTable"
                        if position == len(stages) - 2
                        else "SourceCatalogTable"
                    )
                    self.assertEqual(schemas[tuple(key(stage).path)]["title"], schema)
        self.assertEqual(
            graph.get(key("merged_command_candidates")).parent_keys,
            {
                key("command_candidates"),
                key("documentation/command_candidates"),
                key("keenable/command_candidates"),
                key("map_merged_command_candidates_config"),
            },
        )
        self.assertEqual(
            graph.get(key("filtered_commands")).parent_keys,
            {key("merged_command_candidates"), key("map_filtered_commands_config")},
        )
        self.assertFalse(
            {"datasets__labeling_attempts", "datasets__human_review_queue"}
            & {node.name for node in definitions.resolve_job_def("prepare_dataset").nodes}
        )
        for prefix in ("", "documentation/", "keenable/"):
            for stage in ("source_catalog", "source_documents", "document_adapters"):
                self.assertNotIn(key(f"{prefix}{stage}"), graph.get_all_asset_keys())

    def test_declared_mapping_contains_one_pathless_configuration_per_branch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spec = fixture_spec(Path(directory))
            for branch in ("dataset", "documentation", "keenable"):
                with self.subTest(branch=branch):
                    payload = collection_asset_settings(spec, branch)
                    self.assertEqual(set(payload), {"collection"})
                    config = payload["collection"]
                    assert isinstance(config, dict)
                    self.assertEqual(config["branch"], branch)
                    source = config["source"]
                    assert isinstance(source, dict)
                    self.assertEqual(set(source), {"declaration", "digest"})
