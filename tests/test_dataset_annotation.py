from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from pydantic import JsonValue, TypeAdapter, ValidationError
from test_dataset_rules import make_candidate, make_label, make_review, make_rules

from codebench_pipeline.dataset_annotation import annotate, review
from codebench_pipeline.dataset_records import LabelDecision, ReviewDecision
from codebench_pipeline.inference import InferenceError
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.models import SemanticSnippet


def documentation() -> list[SemanticSnippet]:
    return [
        SemanticSnippet(
            utility="probe",
            subcommands=["inspect", "publish"],
            url="https://example.org/docs",
            retrieved_at="2026-11-09",
            snippet="Inspect reads metadata; publish changes the destination.",
            version_limits="Example fixture version only.",
        )
    ]


def native_response(value: dict[str, JsonValue], model: str = "small") -> subprocess.CompletedProcess[str]:
    event: dict[str, JsonValue] = {
        "type": "message_end",
        "message": {
            "role": "assistant",
            "provider": "fixture",
            "model": model,
            "stopReason": "stop",
            "content": [{"type": "text", "text": json.dumps(value, allow_nan=False)}],
        },
    }
    return subprocess.CompletedProcess(["omo"], 0, json.dumps(event), "")


def config() -> ModelConfig:
    return ModelConfig(provider="native", model="fixture/small")


class DatasetAnnotationTests(unittest.TestCase):
    def test_parallel_labels_preserve_input_order_and_single_record_packets(self) -> None:
        rules = make_rules()
        candidates = [make_candidate("one"), make_candidate("two")]
        labels = {candidate.candidate_id: make_label(candidate, rules) for candidate in candidates}
        simultaneous = Barrier(2, timeout=10)

        def infer(
            schema: type[LabelDecision], system: str, prompt: str, model: ModelConfig, out: Path
        ) -> LabelDecision:
            payload = TypeAdapter(dict[str, JsonValue]).validate_json(prompt.split("\n", 1)[1])
            candidate = payload["candidate"]
            assert isinstance(candidate, dict)
            identity = candidate["candidate_id"]
            assert isinstance(identity, str)
            out.mkdir(parents=True)
            (out / "shared.json").write_text('{"tools_enabled":false}', encoding="utf-8")
            simultaneous.wait()
            return schema.model_validate_json(labels[identity].model_dump_json(by_alias=True))

        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary)
            with patch("codebench_pipeline.dataset_annotation.infer_typed", side_effect=infer):
                result = annotate(
                    candidates, documentation(), rules, config(), out, cache=out / "cache", workers=2
                )
            self.assertEqual([label.candidate_id for label in result], ["one", "two"])
            packets = json.loads((out / "label-packets.json").read_text())
            self.assertEqual([packet["candidate_ids"] for packet in packets], [["one"], ["two"]])
            with patch(
                "codebench_pipeline.dataset_annotation.infer_typed",
                side_effect=AssertionError("Validated labels must be reused"),
            ):
                reused = annotate(
                    candidates, documentation(), rules, config(), out / "replay",
                    cache=out / "cache", workers=2,
                )
            self.assertEqual(reused, result)
            shared = list((out / "replay" / "label").glob("*/shared.json"))
            self.assertEqual([path.read_text() for path in shared], ['{"tools_enabled":false}'] * 2)

    def test_each_label_and_review_persists_separate_inference_evidence(self) -> None:
        rules = make_rules()
        candidates = [make_candidate("one"), make_candidate("two")]
        labels = [make_label(candidate, rules) for candidate in candidates]
        responses = [native_response(label.model_dump(mode="json", by_alias=True)) for label in labels]
        responses.append(
            native_response(
                {
                    "reviews": [
                        {
                            "candidate_id": candidate.candidate_id,
                            "verdict": "approve",
                            "confidence": confidence,
                            "rationale": "Matches evidence.",
                        }
                        for candidate, confidence in zip(candidates, ("low", "medium"), strict=True)
                    ]
                }
            )
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.inference.subprocess.run", side_effect=responses):
                labeled = annotate(candidates, documentation(), rules, config(), root)
                reviewed = review(candidates, documentation(), list(reversed(labeled)), rules, config(), root)
            self.assertEqual([item.candidate_id for item in reviewed], ["one", "two"])
            self.assertEqual([item.confidence for item in reviewed], ["low", "medium"])
            inputs = sorted([*root.glob("label/*/input.json"), *root.glob("review-packets/*/input.json")])
            self.assertEqual(len(inputs), 3)
            for path in inputs:
                stored = json.loads(path.read_text(encoding="utf-8"))
                payload = json.loads(stored["prompt"].split("\n", 1)[1])
                records = payload.get("records", [payload])
                for record in records:
                    self.assertEqual(record["allowed_risk_ids"], ["CAT-8.1"])
                    self.assertEqual(record["candidate"]["source_url"], candidates[0].source_url)
                    self.assertEqual(record["candidate"]["source_position"], candidates[0].source_position)
                self.assertEqual(
                    json.loads(stored["system"].rsplit("\n", 1)[1]), rules.model_dump(mode="json")
                )
                if path.parent.parent.name == "label":
                    self.assertEqual(set(payload), {"candidate", "documentation", "allowed_risk_ids"})
                else:
                    for record in records:
                        self.assertEqual(
                            record["proposed_label"]["candidate_id"], record["candidate"]["candidate_id"]
                        )
                receipt = json.loads((path.parent / "receipt.json").read_text())
                self.assertEqual(receipt["status"], "validated")
                self.assertFalse(receipt["tools_enabled"])
                self.assertFalse(receipt["session_saved"])
                self.assertTrue((path.parent / "response.json").exists())
                if path.parent.parent.name == "review-packets":
                    response = json.loads((path.parent / "response.json").read_text())
                    self.assertEqual(
                        [row["confidence"] for row in response["reviews"]],
                        [row.confidence for row in reviewed],
                    )
            invocation_ids = [
                json.loads(path.read_text())["invocation_id"]
                for path in [*root.glob("label/*/receipt.json"), *root.glob("review-packets/*/receipt.json")]
            ]
            self.assertEqual(len(set(invocation_ids)), 3)

    def test_taxonomy_routing_utility_and_citations_are_enforced_after_inference(self) -> None:
        rules, candidate = make_rules(), make_candidate()
        changes: list[dict[str, JsonValue]] = [
            {"candidate_id": "other"},
            {"category_id": "CAT-2"},
            {"risk_id": "CAT-8.2"},
            {"risk_id": "CAT-8.99"},
            {"util": "other"},
            {"documentation_urls": ["https://example.org/invented"]},
            {"documentation_urls": ["https://example.org/docs", "https://example.org/docs"]},
            {"documentation_urls": []},
        ]
        for change in changes:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                value = {**make_label(candidate, rules).model_dump(mode="json", by_alias=True), **change}
                root = Path(temporary)
                with patch(
                    "codebench_pipeline.inference.subprocess.run",
                    return_value=native_response(value),
                ):
                    expected_error = (
                        InferenceError
                        if {"candidate_id", "category_id", "risk_id", "util"} & change.keys()
                        else ValueError
                    )
                    with self.assertRaises(expected_error):
                        annotate([candidate], documentation(), rules, config(), root)
                captured = list(root.glob("label/*/stdout.jsonl"))
                self.assertEqual(len(captured), 1)
                self.assertEqual(captured[0].read_text(), native_response(value).stdout)

    def test_candidate_shortlist_constrains_routing_when_multiple_patterns_match(self) -> None:
        rules = make_rules()
        candidate = make_candidate().model_copy(
            update={"command": "probe inspect example && probe publish example"}
        )
        invalid = make_label(candidate, rules).model_dump(mode="json", by_alias=True)
        invalid["risk_id"] = "CAT-8.2"
        with tempfile.TemporaryDirectory() as temporary:
            with patch("codebench_pipeline.inference.subprocess.run", return_value=native_response(invalid)):
                with self.assertRaises(InferenceError):
                    annotate([candidate], documentation(), rules, config(), Path(temporary))

    def test_invalid_routing_and_duplicate_candidates_fail_before_inference(self) -> None:
        candidate = make_candidate()
        cases = [
            [candidate, candidate],
            [candidate.model_copy(update={"risk_candidates": ["CAT-9.1"]})],
            [candidate.model_copy(update={"command": "unrelated operation"})],
        ]
        for candidates in cases:
            with self.subTest(candidates=candidates), tempfile.TemporaryDirectory() as temporary:
                with patch("codebench_pipeline.inference.subprocess.run") as process:
                    with self.assertRaises(ValueError):
                        annotate(candidates, documentation(), make_rules(), config(), Path(temporary))
                    process.assert_not_called()

    def test_all_judgment_confidences_require_enum_values_without_defaults(self) -> None:
        candidate = make_candidate()
        label = make_label(candidate, make_rules()).model_dump(by_alias=True)
        review_value = make_review(candidate).model_dump()
        for model, original, fields in (
            (
                LabelDecision,
                label,
                ("category_confidence", "risk_confidence", "severity_confidence", "class_confidence"),
            ),
            (ReviewDecision, review_value, ("confidence",)),
        ):
            for field in fields:
                for value in ("low", "medium", "high"):
                    with self.subTest(model=model.__name__, field=field, value=value):
                        result = model.model_validate_json(json.dumps({**original, field: value}))
                        self.assertEqual(result.model_dump(by_alias=True)[field], value)
                for value in (True, 0, 1, 0.9, "0.9", -0.1, 1.1, float("nan"), float("inf"), None, "certain"):
                    with self.subTest(model=model.__name__, field=field, value=value):
                        with self.assertRaises(ValidationError):
                            model.model_validate({**original, field: value})
                missing = {key: value for key, value in original.items() if key != field}
                with self.subTest(model=model.__name__, missing=field), self.assertRaises(ValidationError):
                    model.model_validate(missing)

    def test_strict_label_decision_fields(self) -> None:
        label = make_label(make_candidate(), make_rules()).model_dump(by_alias=True)
        for change in (
            {"class": "modify"},
            {"severity": "extreme"},
            {"rationale": "x" * 401},
            {"extra": True},
            {"description": " "},
        ):
            with self.subTest(change=change), self.assertRaises(ValidationError):
                LabelDecision.model_validate({**label, **change})

    def test_missing_documentation_can_be_explained_but_not_accepted(self) -> None:
        rules, candidate = make_rules(), make_candidate()
        value = make_label(candidate, rules).model_dump(mode="json", by_alias=True)
        value.update(
            decision="needs_review", rationale="No subcommand documentation supplied.", documentation_urls=[]
        )
        with tempfile.TemporaryDirectory() as temporary:
            with patch("codebench_pipeline.inference.subprocess.run", return_value=native_response(value)):
                labeled = annotate([candidate], [], rules, config(), Path(temporary))
            self.assertEqual(labeled[0].decision, "needs_review")
            self.assertEqual(labeled[0].documentation_urls, [])

    def test_missing_documentation_schema_prevents_acceptance_and_invented_citations(self) -> None:
        rules, candidate = make_rules(), make_candidate()
        base = make_label(candidate, rules).model_dump(mode="json", by_alias=True)
        for decision, urls in (("accept", []), ("needs_review", ["https://example.org/invented"])):
            value = TypeAdapter(dict[str, JsonValue]).validate_python(
                {**base, "decision": decision, "documentation_urls": urls}
            )
            with self.subTest(decision=decision), tempfile.TemporaryDirectory() as temporary:
                with patch(
                    "codebench_pipeline.inference.subprocess.run", return_value=native_response(value)
                ):
                    with self.assertRaises(InferenceError):
                        annotate([candidate], [], rules, config(), Path(temporary))

    def test_independent_review_keeps_disagreement_and_rejects_wrong_identity(self) -> None:
        rules, candidate = make_rules(), make_candidate()
        label = make_label(candidate, rules)
        for identity in (candidate.candidate_id, "other"):
            value: dict[str, JsonValue] = {
                "candidate_id": identity,
                "verdict": "needs_review",
                "confidence": "low",
                "rationale": "The supplied excerpt does not establish the flag behavior.",
            }
            with self.subTest(identity=identity), tempfile.TemporaryDirectory() as temporary:
                with patch(
                    "codebench_pipeline.inference.subprocess.run",
                    return_value=native_response({"reviews": [value]}),
                ):
                    if identity == candidate.candidate_id:
                        decisions = review(
                            [candidate], documentation(), [label], rules, config(), Path(temporary)
                        )
                        self.assertEqual(decisions[0].verdict, "needs_review")
                        self.assertEqual(decisions[0].confidence, "low")
                        self.assertEqual(decisions[0].rationale, value["rationale"])
                    else:
                        with self.assertRaises(ValueError):
                            review([candidate], documentation(), [label], rules, config(), Path(temporary))

    def test_review_requires_exact_label_identities_before_invocation(self) -> None:
        rules, candidate = make_rules(), make_candidate()
        label = make_label(candidate, rules)
        for labels in ([], [label, label], [label.model_copy(update={"candidate_id": "unknown"})]):
            with self.subTest(labels=labels), tempfile.TemporaryDirectory() as temporary:
                with patch("codebench_pipeline.inference.subprocess.run") as process:
                    with self.assertRaises(ValueError):
                        review([candidate], documentation(), labels, rules, config(), Path(temporary))
                    process.assert_not_called()

    def test_review_schema_forbids_extra_fields_and_long_rationales(self) -> None:
        value = make_review(make_candidate()).model_dump()
        for change in ({"extra": True}, {"rationale": "x" * 401}, {"verdict": "allow"}):
            with self.subTest(change=change), self.assertRaises(ValidationError):
                ReviewDecision.model_validate({**value, **change})

    def test_label_jsonl_roundtrip_preserves_canonical_class_alias(self) -> None:
        label = make_label(make_candidate(), make_rules())
        serialized = label.model_dump_json()
        self.assertEqual(json.loads(serialized)["class"], "allow")
        self.assertNotIn("class_", json.loads(serialized))
        self.assertEqual(LabelDecision.model_validate_json(serialized), label)
