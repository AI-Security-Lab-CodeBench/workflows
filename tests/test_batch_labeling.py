import json
import os
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.request import Request

from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.batch_labeling import annotate_batches
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_tools import GroundingConfig
from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse


def batch_config(root: Path, revision: str = "1") -> BatchLabelingConfig:
    return BatchLabelingConfig(
        batch_size=25,
        maximum_model_turns=2,
        maximum_grounding_rounds=1,
        backend_revision="fixture-v1",
        grounding=GroundingConfig(
            allowed_domains=["example.org"],
            pinned_library_ids=["/fixture/docs"],
            revision=revision,
        ),
        capacity=CapacityConfig(state_directory=str(root / "capacity"), expected_model_path="/fixture/model"),
    )


def remote_model() -> ModelConfig:
    return ModelConfig(provider="openai_compatible", model="fixture/model", base_url="http://fixture/v1")


def tool_reply(name: str, arguments: dict, finish_reason: str = "tool_calls") -> HTTPResponse:
    return HTTPResponse(
        json.dumps(
            {
                "id": "fixture-completion",
                "model": "fixture/model",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": finish_reason,
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "fixture-tool",
                                    "index": 0,
                                    "type": "function",
                                    "function": {"name": name, "arguments": json.dumps(arguments)},
                                }
                            ],
                        },
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 100},
            }
        ).encode(),
        "fixture-request",
    )


def submitted(record: dict, evidence_ids: list[str], url: str = "https://example.org/docs") -> dict:
    candidate = CandidateRecord.model_validate(record["candidate"])
    label = make_label(candidate, make_rules()).model_copy(update={"documentation_urls": [url]})
    return {
        "candidate_id": candidate.candidate_id,
        "row_input_fingerprint": record["row_input_fingerprint"],
        "label": label.model_dump(mode="json", by_alias=True),
        "evidence_ids": evidence_ids,
        "grounding_quotes": {identifier: "Inspect reads metadata" for identifier in evidence_ids},
        "grounding_rationale": "The supplied operation documentation establishes inspection behavior.",
        "failure_code": "",
        "failure_detail": "",
    }


class _DocResponse(BytesIO):
    def __init__(self, url: str, text: str) -> None:
        self.url = url
        super().__init__(
            json.dumps(
                {
                    "infoSnippets": [{"pageId": "https://example.org/docs", "content": text}],
                    "codeSnippets": [],
                }
            ).encode()
        )

    def geturl(self) -> str:
        return self.url


class _DocOpener:
    def __init__(self, text: str) -> None:
        self.text = text

    def open(self, request: Request, timeout: float) -> _DocResponse:
        return _DocResponse(request.full_url, self.text)


class BatchLabelingTests(unittest.TestCase):
    def test_reasoning_effort_reaches_template_and_invalidates_cached_judgment(self) -> None:
        efforts = []

        def provider(base_url, body, key, timeout, config, out):
            request = json.loads(body)
            efforts.append(request.get("chat_template_kwargs"))
            records = json.loads(request["messages"][1]["content"])["records"]
            return tool_reply(
                "submit_labels",
                {"outcomes": [submitted(row, row["evidence_ids"]) for row in records]},
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider):
                for index, effort in enumerate(("low", "low", "high")):
                    results = annotate_batches(
                        [make_candidate("one")],
                        documentation(),
                        make_rules(),
                        remote_model(),
                        batch_config(root).model_copy(update={"reasoning_effort": effort}),
                        root / str(index),
                        cache=root / "cache",
                    )
                    self.assertEqual(results[0].status, "success")
        self.assertEqual(efforts, [{"reasoning_effort": "low"}, {"reasoning_effort": "high"}])

    def test_one_submission_labels_batch_and_neighbor_changes_reuse_original_receipts(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            payload = json.loads(json.loads(body)["messages"][1]["content"])
            return tool_reply(
                "submit_labels",
                {"outcomes": [submitted(row, row["evidence_ids"]) for row in payload["records"]]},
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first, second = make_candidate("one"), make_candidate("two")
            with patch(
                "codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider
            ) as inference:
                initial = annotate_batches(
                    [first],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "initial",
                    cache=root / "cache",
                )
                receipt = next((root / "initial" / "label").glob("*/receipt.json")).read_bytes()
                expanded = annotate_batches(
                    [second, first],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root).model_copy(update={"batch_size": 50}),
                    root / "expanded",
                    cache=root / "cache",
                )
            self.assertEqual([row.status for row in expanded], ["success", "success"])
            self.assertEqual(expanded[1], initial[0])
            self.assertEqual(inference.call_count, 2)
            restored = [
                path
                for path in (root / "expanded" / "label").glob("*/receipt.json")
                if json.loads(path.read_text())["candidate_id"] == "one"
            ]
            self.assertEqual(restored[0].read_bytes(), receipt)

    def test_invalid_peer_evidence_or_quote_is_quarantined_without_losing_valid_peer(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            payload = json.loads(json.loads(body)["messages"][1]["content"])
            rows = payload["records"]
            outcomes = []
            for row in rows:
                identity = row["candidate"]["candidate_id"]
                outcome = submitted(row, ["invented"] if identity == "bad" else row["evidence_ids"])
                if identity == "bad_quote":
                    outcome["grounding_quotes"] = {
                        identifier: "Fabricated operation documentation" for identifier in row["evidence_ids"]
                    }
                outcomes.append(outcome)
            return tool_reply("submit_labels", {"outcomes": outcomes})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider):
                results = annotate_batches(
                    [make_candidate("good"), make_candidate("bad"), make_candidate("bad_quote")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                )
            self.assertEqual([row.status for row in results], ["success", "failure", "failure"])
            self.assertEqual(results[1].label_json, "")
            self.assertEqual(results[2].label_json, "")
            self.assertIn("Quote is absent", results[2].failure_detail)
            self.assertEqual(results[1].failure_code, "protocol_exhausted")

    def test_stale_fingerprint_requires_explicit_correct_resubmission(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            messages = json.loads(body)["messages"]
            row = json.loads(messages[1]["content"])["records"][0]
            outcome = submitted(row, row["evidence_ids"])
            feedback = [message for message in messages if message["role"] == "tool"]
            if feedback:
                self.assertIn(
                    row["row_input_fingerprint"], json.loads(feedback[-1]["content"])["errors"][0]["error"]
                )
            else:
                outcome["row_input_fingerprint"] = "0" * 64
            return tool_reply("submit_labels", {"outcomes": [outcome]})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider) as calls:
                result = annotate_batches(
                    [make_candidate("one")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                )
            self.assertEqual(result[0].status, "success")
            self.assertEqual(calls.call_count, 2)

    def test_quote_id_mismatch_feedback_uses_only_actual_candidate_evidence(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            messages = json.loads(body)["messages"]
            payload = json.loads(messages[1]["content"])
            row = payload["records"][0]
            evidence = payload["documentation"]
            correct = next(key for key, item in evidence.items() if item["snippet"]["url"].endswith("/docs"))
            wrong = next(key for key, item in evidence.items() if item["snippet"]["url"].endswith("/other"))
            feedback = [message for message in messages if message["role"] == "tool"]
            if feedback:
                self.assertIn(correct, json.loads(feedback[-1]["content"])["errors"][0]["error"])
            return tool_reply(
                "submit_labels",
                {"outcomes": [submitted(row, [correct if feedback else wrong])]},
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            docs = documentation()
            docs.append(
                docs[0].model_copy(
                    update={"url": "https://example.org/other", "snippet": "Different documented operation."}
                )
            )
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider) as calls:
                result = annotate_batches(
                    [make_candidate("one")],
                    docs,
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                )
            self.assertEqual(result[0].status, "success")
            self.assertEqual(calls.call_count, 2)

    def test_schema_feedback_allows_repair_without_accepting_the_malformed_label(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            messages = json.loads(body)["messages"]
            row = json.loads(messages[1]["content"])["records"][0]
            outcome = submitted(row, row["evidence_ids"])
            feedback = [message for message in messages if message["role"] == "tool"]
            if not feedback:
                outcome["decision"] = outcome["label"].pop("decision")
                outcome["label"].pop("candidate_id")
            else:
                errors = json.loads(feedback[-1]["content"])["errors"]
                locations = {tuple(issue["loc"]) for issue in errors[0]["issues"]}
                self.assertEqual(locations, {("label", "candidate_id"), ("label", "decision"), ("decision",)})
            return tool_reply("submit_labels", {"outcomes": [outcome]})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider) as calls:
                results = annotate_batches(
                    [make_candidate("one")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                )
            self.assertEqual(results[0].status, "success")
            self.assertEqual(calls.call_count, 2)

    def test_real_broker_grounding_and_unchanged_refresh_reuse_successful_label(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            messages = json.loads(body)["messages"]
            record = json.loads(messages[1]["content"])["records"][0]
            tool_messages = [message for message in messages if message["role"] == "tool"]
            if not tool_messages:
                return tool_reply(
                    "request_documentation",
                    {
                        "queries": [
                            {
                                "action": "context7_context",
                                "query": "probe inspect operation",
                                "candidate_ids": [record["candidate"]["candidate_id"]],
                                "library_id": "/fixture/docs",
                            }
                        ]
                    },
                )
            documents = json.loads(tool_messages[-1]["content"])["results"][0]["result"]["documents"]
            return tool_reply(
                "submit_labels", {"outcomes": [submitted(record, [documents[0]["evidence_id"]])]}
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = make_candidate("one")
            with (
                patch.dict(os.environ, {"CONTEXT7_API_KEY": "fixture-context-key"}),
                patch(
                    "codebench_pipeline.grounding_tools.build_opener",
                    return_value=_DocOpener("Inspect reads metadata."),
                ),
                patch(
                    "codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider
                ) as inference,
            ):
                first = annotate_batches(
                    [candidate],
                    [],
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "first",
                    cache=root / "cache",
                )
                refreshed = annotate_batches(
                    [candidate],
                    [],
                    make_rules(),
                    remote_model(),
                    batch_config(root, "2"),
                    root / "refresh",
                    cache=root / "cache",
                )
            self.assertEqual(first[0].status, "success")
            self.assertEqual(refreshed, first)
            self.assertEqual(inference.call_count, 2)
            self.assertIn("Inspect reads metadata.", first[0].documentation_json)

    def test_capacity_pause_does_not_manufacture_failed_label(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch(
                    "codebench_pipeline.batch_labeling.guarded_complete",
                    side_effect=CapacityUnavailable("side load"),
                ),
                self.assertRaises(CapacityUnavailable),
            ):
                annotate_batches(
                    [make_candidate("one")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                )
            self.assertEqual(list((root / "run").glob("label/*/response.json")), [])

    def test_oversized_batch_splits_without_sending_over_budget_requests(self) -> None:
        def provider(base_url, body, key, timeout, config, out):
            self.assertLessEqual(len(body), 50000)
            records = json.loads(json.loads(body)["messages"][1]["content"])["records"]
            return tool_reply(
                "submit_labels",
                {"outcomes": [submitted(record, record["evidence_ids"]) for record in records]},
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidates = [
                make_candidate(str(index)).model_copy(update={"context": "x" * 30000}) for index in range(3)
            ]
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider) as calls:
                results = annotate_batches(
                    candidates,
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                    maximum_context_characters=50000,
                )
            self.assertEqual([row.status for row in results], ["success", "success", "success"])
            self.assertEqual(calls.call_count, 3)

    def test_single_oversized_record_has_a_failure_without_model_submission(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "codebench_pipeline.batch_labeling.guarded_complete",
                side_effect=AssertionError("Oversized input must not be submitted"),
            ):
                results = annotate_batches(
                    [make_candidate("large")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                    maximum_context_characters=1024,
                )
            self.assertEqual(results[0].failure_code, "context_limit")
            self.assertEqual(results[0].label_json, "")

    def test_capacity_pause_after_partial_submission_preserves_completed_peer(self) -> None:
        calls = 0

        def provider(base_url, body, key, timeout, config, out):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise CapacityUnavailable("side load")
            records = json.loads(json.loads(body)["messages"][1]["content"])["records"]
            return tool_reply(
                "submit_labels",
                {"outcomes": [submitted(records[0], records[0]["evidence_ids"])]},
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidates = [make_candidate("one"), make_candidate("two")]
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider):
                with self.assertRaises(CapacityUnavailable):
                    annotate_batches(
                        candidates,
                        documentation(),
                        make_rules(),
                        remote_model(),
                        batch_config(root),
                        root / "paused",
                        cache=root / "cache",
                    )
                original = next((root / "paused" / "label").glob("*/receipt.json")).read_bytes()
                resumed = annotate_batches(
                    candidates,
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "resumed",
                    cache=root / "cache",
                )
            self.assertEqual([row.status for row in resumed], ["success", "success"])
            self.assertEqual(calls, 3)
            receipts = [path.read_bytes() for path in (root / "resumed" / "label").glob("*/receipt.json")]
            self.assertIn(original, receipts)
