import tempfile
import unittest
from pathlib import Path

from dagster import AssetKey, Definitions, validate_run_config
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.artifact_schemas import declared_artifact_schemas
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig, collection_asset_settings


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
                self.assertEqual(graph.get(mapping).group_name, group)
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
        for asset_key in graph.get_all_asset_keys():
            if asset_key.path[-1].startswith("map_"):
                mapping_node = graph.get(asset_key)
                self.assertEqual(
                    {graph.get(child).group_name for child in mapping_node.child_keys},
                    {mapping_node.group_name},
                    asset_key,
                )
        self.assertEqual(graph.get(key("labeling_failures")).group_name, "dataset_audit")
        self.assertEqual(graph.get(key("filtered_commands")).group_name, "filtering")
        self.assertEqual(graph.get(key("labeling_attempts")).group_name, "labeling")
        self.assertEqual(
            {node for node in graph.get_all_asset_keys() if graph.get(node).group_name == "deduplication"},
            {key("deduplicated_annotations")},
        )
        for stage in ("human_review_queue", "human_review_decisions", "dataset_release"):
            self.assertEqual(graph.get(key(stage)).group_name, "dataset_release")
        for prefix in ("", "documentation/", "keenable/"):
            for stage in ("source_catalog", "source_documents", "document_adapters"):
                self.assertNotIn(key(f"{prefix}{stage}"), graph.get_all_asset_keys())

    def test_declared_mapping_contains_one_pathless_configuration_per_branch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spec = fixture_spec(Path(directory))
            for branch in ("dataset", "documentation", "keenable"):
                with self.subTest(branch=branch):
                    settings = collection_asset_settings(spec, branch)
                    self.assertEqual(settings.collection.branch, branch)
                    payload = settings.model_dump(mode="json")
                    self.assertEqual(set(payload), {"collection"})
                    self.assertEqual(
                        set(settings.collection.source.model_dump(mode="json")), {"declaration", "digest"}
                    )

    def test_declared_labeling_preview_exposes_request_bytes_without_historical_retry_setting(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spec = fixture_spec(Path(directory))
            preview = DeclaredGlobalConfig.from_spec(spec)

        settings = preview.scopes.labeling_attempts.labeling_attempts.model_dump(mode="json")
        self.assertEqual(settings["max_request_bytes"], 65536)
        self.assertNotIn("maximum_context_characters", settings)
        self.assertNotIn("retry_revision", settings["batch"])
        self.assertEqual(spec.annotation.model_dump(mode="json")["batch"]["retry_revision"], "1")
