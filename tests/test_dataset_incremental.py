import copy
import io
import json
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import write_fixture
from dagster import AssetKey, AssetRecordsFilter, build_sensor_context, partition_loading_context
from pydantic import BaseModel

from codebench_pipeline.dataset_cli import execute_build, execute_registered_build
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_records import LabelDecision
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_source_models import SourceSelection
from codebench_pipeline.definitions import build_definitions, dataset_run_config
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import AdapterSpec
from codebench_pipeline.runtime_instance import dataset_instance


def provider_response(
    schema: type[BaseModel], system: str, prompt: str, model: ModelConfig, out: Path
) -> BaseModel:
    payload = json.loads(prompt[prompt.index("{") :])
    if schema is SourceSelection:
        return SourceSelection.model_validate(
            {
                "sources": [
                    {
                        "source_id": row["source_id"],
                        "decision": "selected",
                        "rationale": "Inspected example.",
                        "analysis": "The inspected source contains in-scope command examples.",
                        "confidence": "high",
                    }
                    for row in payload["sources"]
                ]
            }
        )
    if schema is AdapterSpec:
        return AdapterSpec(format="markdown", rationale="Markdown example source.")
    if issubclass(schema, LabelDecision):
        candidate = payload["candidate"]
        risk = payload["allowed_risk_ids"][0]
        return LabelDecision.model_validate(
            {
                "candidate_id": candidate["candidate_id"],
                "decision": "accept",
                "category_id": risk.split(".")[0],
                "risk_id": risk,
                "severity": "low",
                "class": "allow",
                "description": "Inspects repository state.",
                "util": candidate["util"],
                "category_confidence": "medium",
                "risk_confidence": "high",
                "severity_confidence": "high",
                "class_confidence": "high",
                "rationale": "Documented inspection.",
                "documentation_urls": [payload["documentation"][0]["url"]],
            }
        )
    raise AssertionError(f"Unexpected provider schema: {schema}")


class IncrementalDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.home = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        original = write_fixture(self.home)
        self.configuration = json.loads(original.read_text(encoding="utf-8"))
        if self.configuration.get("documentation_sources") is not None:
            self.configuration["seeds"] = self.configuration["documentation_sources"]
            self.configuration["documentation_sources"] = None
        for seed in self.configuration["seeds"]["documents"]:
            seed["local_path"] = str((original.parent / seed["local_path"]).resolve())
        self.configuration["storage"] = {"local_root": str(self.home / "tables")}
        self.path = self.home / "category.json"
        for module in ("dataset_annotation",):
            self.stack.enter_context(
                patch(f"codebench_pipeline.{module}.infer_typed", side_effect=provider_response)
            )
        self.first = self.build()

    def build(self) -> dict[str, bool]:
        self.path.write_text(json.dumps(self.configuration), encoding="utf-8")
        with redirect_stdout(io.StringIO()):
            return execute_build(self.path, self.home)

    def materializations(self, dataset: str) -> dict[str, int]:
        with dataset_instance(self.home) as instance:
            return {
                stage: instance.fetch_materializations(
                    AssetRecordsFilter(asset_key=AssetKey(["datasets", stage]), asset_partitions=[dataset]),
                    limit=1,
                )
                .records[0]
                .storage_id
                for stage in self.first
            }

    def test_unchanged_build_creates_no_run_or_materialization(self) -> None:
        before = self.materializations(self.configuration["dataset_id"])
        with dataset_instance(self.home) as instance:
            runs_before = len(instance.get_runs())
        self.assertEqual(self.build(), {})
        self.assertEqual(self.materializations(self.configuration["dataset_id"]), before)
        with dataset_instance(self.home) as instance:
            self.assertEqual(len(instance.get_runs()), runs_before)

    def test_added_documentation_branch_preserves_current_dataset_outputs(self) -> None:
        before = self.materializations(self.configuration["dataset_id"])
        registry = DatasetRegistry(self.home)
        spec = registry.get(self.configuration["dataset_id"])
        registry.register(
            spec.model_copy(
                update={
                    "documentation_catalog": spec.catalog,
                    "documentation_snapshots": spec.snapshots,
                }
            )
        )
        with redirect_stdout(io.StringIO()):
            changed = execute_registered_build(spec.dataset_id, self.home)
        finalization = {
            "merged_command_candidates",
            "filtered_commands",
            "labeling_documentation",
            "proposed_annotations",
            "deduplicated_annotations",
            "human_review_queue",
        }
        self.assertEqual(
            set(changed),
            {
                f"documentation/{stage}"
                for stage in self.first
                if stage not in finalization | {"category_rules"}
            }
            | finalization,
        )
        after = self.materializations(spec.dataset_id)
        self.assertEqual({stage for stage in before if before[stage] != after[stage]}, finalization)
        with redirect_stdout(io.StringIO()):
            self.assertEqual(execute_registered_build(spec.dataset_id, self.home), {})

    def test_labeler_change_materializes_only_labels_and_finalization(self) -> None:
        before = self.materializations(self.configuration["dataset_id"])
        self.configuration["models"]["labeler"]["timeout_seconds"] += 1
        self.configuration["revision"] = "label-settings-update"
        changed = self.build()
        expected = {
            "proposed_annotations",
            "deduplicated_annotations",
            "human_review_queue",
        }
        self.assertEqual(set(changed), expected)
        after = self.materializations(self.configuration["dataset_id"])
        self.assertEqual({stage for stage in before if before[stage] != after[stage]}, expected)
        self.assertEqual(self.build(), {})

    def test_policy_change_does_not_rematerialize_source_or_filter_tables(self) -> None:
        self.configuration["rules"]["policy"] += " Check the supplied evidence carefully."
        self.configuration["rules"]["version"] = "policy-update"
        self.assertEqual(
            set(self.build()),
            {
                "category_rules",
                "labeling_documentation",
                "proposed_annotations",
                "human_review_queue",
                "deduplicated_annotations",
            },
        )

    def test_publication_cap_changes_only_prepared_review_queue(self) -> None:
        self.configuration["publication"]["limit_per_risk"] = 1
        self.assertEqual(set(self.build()), {"human_review_queue"})
        with dataset_instance(self.home) as instance:
            run = instance.get_runs(limit=1)[0]
        receipt = json.loads((self.home / f"build-{run.run_id}.json").read_text())
        self.assertEqual(receipt["status"], "awaiting_human_review")

    def test_failed_labeling_retries_only_unfinished_dependent_stages(self) -> None:
        before = self.materializations(self.configuration["dataset_id"])
        self.configuration["models"]["labeler"]["timeout_seconds"] += 1
        with patch(
            "codebench_pipeline.dataset_annotation.infer_typed",
            side_effect=RuntimeError("Provider rejected this request"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Provider rejected this request"):
                self.build()
        self.assertEqual(self.materializations(self.configuration["dataset_id"]), before)
        self.assertEqual(
            set(self.build()),
            {
                "proposed_annotations",
                "deduplicated_annotations",
                "human_review_queue",
            },
        )

    def test_sensor_skips_unchanged_categories_and_selects_only_changed_labels(self) -> None:
        definitions = build_definitions(self.home)
        sensor = definitions.get_sensor_def("dataset_build_requests")
        with (
            dataset_instance(self.home) as instance,
            partition_loading_context(dynamic_partitions_store=instance),
        ):
            with build_sensor_context(
                instance=instance, repository_def=definitions.get_repository_def()
            ) as ctx:
                self.assertEqual(list(sensor.evaluate_tick(ctx).run_requests or []), [])
        self.configuration["models"]["labeler"]["timeout_seconds"] += 1
        self.path.write_text(json.dumps(self.configuration), encoding="utf-8")
        DatasetRegistry(self.home).register(load_build_config(self.path, self.home))
        with (
            dataset_instance(self.home) as instance,
            partition_loading_context(dynamic_partitions_store=instance),
        ):
            with build_sensor_context(
                instance=instance, repository_def=definitions.get_repository_def()
            ) as ctx:
                requests = list(sensor.evaluate_tick(ctx).run_requests or [])
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].partition_key, self.configuration["dataset_id"])
        resolved = DatasetRegistry(self.home).get(self.configuration["dataset_id"])
        pinned = dataset_run_config(resolved, self.home).to_config_dict()
        self.assertEqual(requests[0].run_config, pinned)
        DatasetRegistry(self.home).register(
            resolved.model_copy(update={"rules": resolved.rules.model_copy(update={"policy": "Edited policy."})})
        )
        self.assertEqual(requests[0].run_config, pinned)
        self.assertEqual(
            set(requests[0].asset_selection or []),
            {
                AssetKey(["datasets", stage])
                for stage in (
                    "proposed_annotations",
                    "deduplicated_annotations",
                    "human_review_queue",
                )
            },
        )

    def test_seed_revision_preserves_rules_and_refreshes_source_bound_documentation(self) -> None:
        before = self.materializations(self.configuration["dataset_id"])
        self.configuration["seeds"]["revision"] = "source-refresh"
        changed = self.build()
        self.assertEqual(set(changed), set(self.first) - {"category_rules"})
        after = self.materializations(self.configuration["dataset_id"])
        self.assertEqual(after["category_rules"], before["category_rules"])
        self.assertNotEqual(after["labeling_documentation"], before["labeling_documentation"])

    def test_adding_and_changing_category_leaves_existing_category_events_untouched(self) -> None:
        original = copy.deepcopy(self.configuration)
        identity = original["dataset_id"]
        before = self.materializations(identity)
        self.configuration["dataset_id"] = "another_category"
        self.configuration["rules"]["category_id"] = "CAT-9"
        self.configuration["rules"]["risks"][0]["id"] = "CAT-9.1"
        self.assertEqual(set(self.build()), set(self.first))
        self.configuration["models"]["labeler"]["timeout_seconds"] += 1
        self.assertEqual(
            set(self.build()),
            {
                "proposed_annotations",
                "deduplicated_annotations",
                "human_review_queue",
            },
        )
        self.assertEqual(self.materializations(identity), before)
        self.configuration = original
        self.assertEqual(self.build(), {})
