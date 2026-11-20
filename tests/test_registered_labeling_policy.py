from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import example_config, seed_prepared_snapshot
from pydantic import JsonValue, TypeAdapter
from test_batch_labeling import batch_config, remote_model
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_rules
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.dataset_annotation import request
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.labeling_eval import CohortRecord, EvaluationCohort, run_evaluation


class RegisteredLabelingPolicyTests(unittest.TestCase):
    def test_annotation_uses_only_the_supplied_prompt_and_policy(self) -> None:
        supplied = make_rules().model_copy(update={"base_prompt": "", "policy": "Fixture policy."})
        system, _ = request(make_candidate(), documentation(), supplied)
        base, policy, _ = system.split("\n\n", 2)
        self.assertEqual(base, "")
        self.assertEqual(policy, "Fixture policy.")

    def test_evaluation_records_the_exact_registered_policy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rules = make_rules().model_copy(update={"base_prompt": "  Registered evaluation policy.\n"})
            spec = fixture_spec(root).model_copy(update={"rules": rules, "config_directory": None})
            archived, revision = seed_prepared_snapshot(root, spec, example_config().documentation)
            registry = DatasetRegistry(root)
            registry.register(spec.model_copy(update={"rules": make_rules()}))
            self.assertNotEqual(registry.get(spec.dataset_id).rules, archived.rules)
            (root / "spec.json").write_bytes(
                (registry.root / spec.dataset_id / f"{revision}.json").read_bytes()
            )
            model = remote_model()
            record = CohortRecord(
                row=make_candidate().model_dump(mode="json"),
                seed_documentation=documentation(),
                selection_reason="Synthetic evaluation record",
                split="heldout",
                documentation_gap_candidate=False,
                category_certain_other_uncertain=False,
            )
            cohort = root / "cohort.json"
            cohort.write_text(
                EvaluationCohort(
                    dataset=spec.dataset_id,
                    selection_version="fixture",
                    baseline_model=model,
                    snapshots={},
                    records=[record],
                ).model_dump_json(),
                encoding="utf-8",
            )
            annotation = root / "annotation.json"
            annotation.write_text(
                AnnotationConfig(model=model, batch=batch_config(root)).model_dump_json(),
                encoding="utf-8",
            )
            with patch("codebench_pipeline.labeling_eval.annotate_batches", return_value=[]) as batches:
                summary = run_evaluation(cohort, annotation, root / "evaluation", "heldout", None)

            self.assertEqual(summary["rows"], 1)
            batches.assert_called_once()
            self.assertEqual(batches.call_args.args[0], [record.candidate()])
            self.assertEqual(batches.call_args.args[2], archived.rules)
            receipt = TypeAdapter(dict[str, JsonValue]).validate_json(
                (root / "evaluation/run-input.json").read_bytes()
            )
            self.assertEqual(receipt["rules"], spec.rules.model_dump(mode="json"))
