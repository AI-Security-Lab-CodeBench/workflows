from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import ANY, patch

from configuration_fixture import example_config, seed_prepared_snapshot
from pydantic import JsonValue, TypeAdapter
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules
from test_declared_dataset_graph import fixture_spec

from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.dataset_annotation import request
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.grounding_tools import LiveGroundingConfig
from codebench_pipeline.labeling_eval import CohortRecord, EvaluationCohort, run_evaluation
from codebench_pipeline.labeling_outcomes import failed_attempt, label_attempt
from codebench_pipeline.labeling_stage_settings import LabelingRules
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse


class RegisteredLabelingPolicyTests(unittest.TestCase):
    def test_annotation_uses_only_the_supplied_prompt_and_policy(self) -> None:
        supplied = make_rules().model_copy(update={"base_prompt": "", "policy": "Fixture policy."})
        system, _ = request(make_candidate(), documentation(), supplied)
        base, policy, _ = system.split("\n\n", 2)
        self.assertEqual(base, "")
        self.assertEqual(policy, "Fixture policy.")


class RegisteredEvaluationTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(directory)
        rules = make_rules().model_copy(update={"base_prompt": "  Registered evaluation policy.\n"})
        self.spec = fixture_spec(self.root).model_copy(update={"rules": rules, "config_directory": None})
        self.archived, revision = seed_prepared_snapshot(self.root, self.spec, example_config().documentation)
        registry = DatasetRegistry(self.root)
        self.registry = registry
        (self.root / "spec.json").write_bytes(
            (registry.root / self.spec.dataset_id / f"{revision}.json").read_bytes()
        )
        self.candidate = make_candidate()
        self.record = CohortRecord(
            row=self.candidate.model_dump(mode="json"),
            selection_reason="Synthetic evaluation record",
            split="heldout",
        )
        self.cohort_path = self.root / "cohort.json"
        self.annotation_path = self.root / "annotation.json"
        self.out = self.root / "evaluation"
        self.annotation_path.write_text(
            AnnotationConfig(
                model=ModelConfig(
                    provider="openai_compatible", model="fixture/model", base_url="http://fixture.invalid/v1"
                ),
                batch=BatchLabelingConfig(
                    backend_revision="fixture-v1",
                    inference_mode="plain",
                    grounding=LiveGroundingConfig(allowed_domains=["example.org"]),
                ),
            ).model_dump_json(),
            encoding="utf-8",
        )
        self.failure = failed_attempt(
            self.candidate, "unsupported", "No qualifying page.", [], "a" * 64, "label/receipt.json"
        )

    def write_cohort(self, *records: CohortRecord) -> None:
        self.cohort_path.write_text(
            EvaluationCohort(
                dataset=self.spec.dataset_id,
                selection_version="fixture",
                snapshots={},
                records=list(records),
            ).model_dump_json(),
            encoding="utf-8",
        )

    def test_evaluation_records_the_exact_registered_policy(self) -> None:
        self.registry.register(self.spec.model_copy(update={"rules": make_rules()}))
        self.assertNotEqual(self.registry.get(self.spec.dataset_id).rules, self.archived.rules)
        self.write_cohort(self.record)
        with patch(
            "codebench_pipeline.labeling_eval.annotate_batches", return_value=[self.failure]
        ) as batches:
            summary = run_evaluation(self.cohort_path, self.annotation_path, self.out, "heldout", None)

        self.assertEqual(summary["rows"], 1)
        self.assertEqual(summary["outcomes"], {"failure": 1})
        batches.assert_called_once()
        self.assertEqual(batches.call_args.args[0], [self.candidate])
        scoped = LabelingRules.from_rules(self.archived.rules)
        self.assertEqual(batches.call_args.args[1], scoped)
        receipt = TypeAdapter(dict[str, JsonValue]).validate_json((self.out / "run-input.json").read_bytes())
        self.assertEqual(receipt["rules"], scoped.model_dump(mode="json"))

    def test_incomplete_or_duplicate_outcomes_fail_with_attempt_evidence_retained(self) -> None:
        self.write_cohort(self.record)
        for case, attempts, message, missing, duplicates in (
            ("missing", [], "exact eligible candidate identities", [self.candidate.candidate_id], {}),
            (
                "duplicate",
                [self.failure, self.failure],
                "Duplicate candidate identities",
                [],
                {self.candidate.candidate_id: 2},
            ),
        ):
            with self.subTest(case=case):
                out = self.root / case
                with (
                    patch("codebench_pipeline.labeling_eval.annotate_batches", return_value=attempts),
                    self.assertRaisesRegex(ValueError, message),
                ):
                    run_evaluation(self.cohort_path, self.annotation_path, out, "heldout", None)

                stored = TypeAdapter(list[JsonValue]).validate_json((out / "attempts.json").read_bytes())
                self.assertEqual(stored, [attempt.model_dump(mode="json") for attempt in attempts])
                summary = TypeAdapter(dict[str, JsonValue]).validate_json((out / "summary.json").read_bytes())
                self.assertEqual(summary["status"], "failed")
                self.assertEqual(summary["quality_gate"], "not_evaluable")
                self.assertEqual(
                    summary["outcome_validation"],
                    {
                        "detail": ANY,
                        "missing_candidate_ids": missing,
                        "unexpected_candidate_ids": [],
                        "duplicate_candidate_ids": duplicates,
                    },
                )

    def test_evaluation_rejects_seed_documentation_before_labeling(self) -> None:
        self.write_cohort(self.record.model_copy(update={"seed_documentation": documentation()}))
        with (
            patch("codebench_pipeline.labeling_eval.annotate_batches") as batches,
            self.assertRaisesRegex(ValueError, "cannot use seed documentation"),
        ):
            run_evaluation(self.cohort_path, self.annotation_path, self.out, "heldout", None)
        batches.assert_not_called()
        self.assertFalse(self.out.exists())

    def test_successes_need_adjudication_with_or_without_baseline_agreement(self) -> None:
        matching_candidate = make_candidate("with-baseline")
        matching_label = make_label(matching_candidate, self.archived.rules)
        matching_row = matching_candidate.model_dump(mode="json") | matching_label.model_dump(mode="json")
        self.write_cohort(self.record, self.record.model_copy(update={"row": matching_row}))
        attempts = [
            label_attempt(candidate, label, documentation(), "a" * 64, "label/receipt.json")
            for candidate, label in (
                (self.candidate, make_label(self.candidate, self.archived.rules)),
                (matching_candidate, matching_label),
            )
        ]
        with patch("codebench_pipeline.labeling_eval.annotate_batches", return_value=attempts):
            summary = run_evaluation(self.cohort_path, self.annotation_path, self.out, "heldout", None)

        self.assertEqual(summary["rows_with_baseline"], 1)
        self.assertEqual(summary["quality_gate"], "pending_grounded_adjudication")
        self.assertEqual(
            summary["changes"],
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "status": "success",
                    "changes": {},
                    "baseline_non_allow_to_allow": baseline_delta,
                    "requires_grounded_adjudication": True,
                }
                for candidate, baseline_delta in ((self.candidate, None), (matching_candidate, False))
            ],
        )

    def test_evaluation_reports_actual_membership_and_admitted_grounding_calls(self) -> None:
        self.write_cohort(self.record)
        query = json.dumps({"candidate_ids": [self.candidate.candidate_id], "query": "probe inspect"})
        response = HTTPResponse(
            json.dumps(
                {
                    "id": "fixture-completion",
                    "model": "fixture/model",
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "tool_calls",
                            "message": {
                                "role": "assistant",
                                "tool_calls": [
                                    {
                                        "id": str(index),
                                        "type": "function",
                                        "function": {"name": "query", "arguments": query},
                                    }
                                    for index in range(2)
                                ],
                            },
                        }
                    ],
                }
            ).encode(),
            request_id=None,
        )

        with (
            patch.dict(os.environ, {"KEENABLE_API_KEY": "synthetic-key-for-eval-test"}),
            patch("codebench_pipeline.batch_labeling.plain_complete", return_value=response),
            patch("codebench_pipeline.live_grounding._retrieve", return_value=()) as retrieve,
        ):
            summary = run_evaluation(self.cohort_path, self.annotation_path, self.out, "heldout", None)

        retrieve.assert_awaited_once()
        self.assertEqual(summary["documentation_provider_calls"], 1)
        self.assertEqual(summary["untranscribed_candidate_ids"], [])
        self.assertEqual(
            summary["batches"],
            [
                {
                    "batch_id": ANY,
                    "transcript": ANY,
                    "candidate_ids": [self.candidate.candidate_id],
                    "rows": 1,
                    "grounding_call_budget": 1,
                    "documentation_provider_calls": 1,
                    "query_calls": 1,
                    "page_fetch_calls": 0,
                }
            ],
        )
