from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from configuration_fixture import example_config
from test_batch_labeling import batch_config, remote_model
from test_dataset_annotation import documentation
from test_dataset_bundle import JSON_OBJECT, write_bundle
from test_dataset_rules import make_candidate, make_rules

from codebench_pipeline.dataset_annotation import request
from codebench_pipeline.dataset_config import AnnotationConfig, load_build_config
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
            path = write_bundle(root)
            raw = JSON_OBJECT.validate_json(path.read_bytes())
            finalization = JSON_OBJECT.validate_python(raw["finalization"])
            finalization["rules"] = (
                make_rules()
                .model_copy(update={"base_prompt": "  Registered evaluation policy.\n"})
                .model_dump(mode="json")
            )
            path.write_bytes(JSON_OBJECT.dump_json({**raw, "finalization": finalization}))
            spec = load_build_config(path, root)
            (root / "spec.json").write_text(spec.model_dump_json(), encoding="utf-8")
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
                    dataset=example_config().dataset_id,
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
            with patch("codebench_pipeline.labeling_eval.annotate_batches", return_value=[]):
                run_evaluation(cohort, annotation, root / "evaluation", "development", None)

            receipt = JSON_OBJECT.validate_json((root / "evaluation/run-input.json").read_bytes())
            self.assertEqual(receipt["rules"], spec.rules.model_dump(mode="json"))
