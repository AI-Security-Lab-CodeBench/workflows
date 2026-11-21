import json
import os
import tempfile
import unittest
from collections import Counter
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from threading import Barrier
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

import numpy as np
from jsonschema import validate
from numpy.typing import NDArray
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.batch_labeling import annotate_batches
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_tools import GroundingConfig
from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable, ContextBudgetExceeded
from codebench_pipeline.labeling_outcomes import LabelingAttempt
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
    def test_plain_inference_submits_directly_without_reserving_capacity(self) -> None:
        def provider(url: str, body: bytes, _key: str | None, _timeout: float) -> HTTPResponse:
            self.assertEqual(url, "http://fixture/v1/chat/completions")
            request = json.loads(body)
            self.assertNotIn("chat_template_kwargs", request)
            records = json.loads(request["messages"][1]["content"])["records"]
            return tool_reply(
                "submit_labels", {"outcomes": [submitted(r, r["evidence_ids"]) for r in records]}
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.batch_labeling.plain_complete", side_effect=provider):
                outcomes = annotate_batches(
                    [make_candidate("one")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root).model_copy(
                        update={"inference_mode": "plain", "reasoning_effort": "high", "capacity": None}
                    ),
                    root / "run",
                )
            self.assertEqual([row.status for row in outcomes], ["success"])
            self.assertFalse((root / "capacity").exists())

    def test_disjoint_cached_rows_overlap_and_identical_rows_reuse_original_receipt(self) -> None:
        arrivals = Barrier(2)

        def provider(_url: str, body: bytes, _key: str | None, _timeout: float) -> HTTPResponse:
            (row,) = json.loads(json.loads(body)["messages"][1]["content"])["records"]
            arrivals.wait(timeout=10)
            return tool_reply("submit_labels", {"outcomes": [submitted(row, row["evidence_ids"])]})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = batch_config(root).model_copy(update={"inference_mode": "plain"})

            def annotate(candidate: CandidateRecord, name: str) -> LabelingAttempt:
                return annotate_batches(
                    [candidate],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    config,
                    root / name,
                    cache=root / "cache",
                )[0]

            one, two = make_candidate("one"), make_candidate("two")
            with (
                patch("codebench_pipeline.batch_labeling.plain_complete", side_effect=provider) as inference,
                ThreadPoolExecutor(3) as workers,
            ):
                first = workers.submit(annotate, one, "first")
                duplicate = workers.submit(annotate, one, "duplicate")
                disjoint = workers.submit(annotate, two, "disjoint")
                original, reused, other = [job.result(timeout=15) for job in (first, duplicate, disjoint)]

            self.assertEqual([original.status, reused.status, other.status], ["success"] * 3)
            self.assertEqual(inference.call_count, 2)
            receipts = [
                next((root / name).glob("label/*/receipt.json")).read_bytes()
                for name in ("first", "duplicate")
            ]
            self.assertEqual(receipts[0], receipts[1])

    def test_compact_submission_binds_identity_and_rejects_conflicting_explicit_identity(self) -> None:
        def provider(
            _base_url: str,
            body: bytes,
            _key: str | None,
            _timeout: float,
            _config: CapacityConfig,
            _out: Path,
        ) -> HTTPResponse:
            wire = json.loads(body)
            records = json.loads(wire["messages"][1]["content"])["records"]
            by_id = {row["candidate"]["candidate_id"]: row for row in records}
            compact = submitted(by_id["one"], by_id["one"]["evidence_ids"])
            del compact["label"]["candidate_id"]
            del compact["label"]["util"]
            parameters = wire["tools"][0]["function"]["parameters"]
            validate({"outcomes": [compact]}, parameters)
            invalid = submitted(by_id["two"], by_id["two"]["evidence_ids"])
            invalid["label"]["candidate_id"] = "one"
            return tool_reply("submit_labels", {"outcomes": [compact, invalid]})

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider):
                valid, invalid = annotate_batches(
                    [make_candidate("one"), make_candidate("two")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    root / "run",
                )
            self.assertEqual(valid.status, "success")
            assert valid.label is not None
            self.assertEqual(valid.label.candidate_id, "one")
            self.assertEqual(valid.label.util, valid.util)
            self.assertEqual(invalid.status, "failure")
            self.assertIn("Label identity differs from candidate", invalid.failure_detail)

    def setUp(self) -> None:
        def fixture_vectors(commands: Sequence[str]) -> NDArray[np.float32]:
            return np.ones((len(commands), 1), dtype=np.float32)

        encoder = patch("codebench_pipeline.command_batches.encode_commands", side_effect=fixture_vectors)
        encoder.start()
        self.addCleanup(encoder.stop)

    def test_changed_batch_instructions_reach_provider_and_invalidate_cache(self) -> None:
        prompts = []

        def provider(base_url, body, key, timeout, config, out):
            messages = json.loads(body)["messages"]
            prompts.append(messages[0]["content"])
            rows = json.loads(messages[1]["content"])["records"]
            return tool_reply(
                "submit_labels", {"outcomes": [submitted(row, row["evidence_ids"]) for row in rows]}
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider):
                for i, note in enumerate(("Explain units.", "Explain units.", "Describe timeouts.")):
                    results = annotate_batches(
                        [make_candidate("one")],
                        documentation(),
                        make_rules(),
                        remote_model(),
                        batch_config(root).model_copy(update={"instructions": note}),
                        root / str(i),
                        cache=root / "cache",
                    )
                    self.assertEqual(results[0].status, "success")
        self.assertEqual(len(prompts), 2)
        self.assertIn("Explain units.", prompts[0])
        self.assertIn("Describe timeouts.", prompts[1])

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
            self.assertIsNone(results[1].label)
            self.assertIsNone(results[2].label)
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
                outcome["label"]["candidate_id"] = 1
            else:
                errors = json.loads(feedback[-1]["content"])["errors"]
                issues = {(tuple(issue["loc"]), issue["type"]) for issue in errors[0]["issues"]}
                self.assertEqual(
                    issues,
                    {
                        (("label", "candidate_id"), "string_type"),
                        (("label", "decision"), "missing"),
                        (("decision",), "extra_forbidden"),
                    },
                )
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
            self.assertTrue(any("Inspect reads metadata." in doc.snippet for doc in first[0].documentation))

    def test_stale_refresh_shares_provider_budget_with_new_judgments(self) -> None:
        def complete(
            base_url: str, body: bytes, key: str | None, timeout: float, capacity: CapacityConfig, path: Path
        ) -> HTTPResponse:
            messages = json.loads(body)["messages"]
            records = json.loads(messages[1]["content"])["records"]
            if not any(message["role"] == "tool" for message in messages):
                identities = [record["candidate"]["candidate_id"] for record in records]
                return tool_reply(
                    "request_documentation",
                    {
                        "queries": [
                            {
                                "action": "context7_context",
                                "query": f"{identity}-{index}",
                                "candidate_ids": [identity],
                                "library_id": "/fixture/docs",
                            }
                            for identity in identities
                            for index in range(3 if identity == "new" else 1)
                        ]
                    },
                )
            return tool_reply(
                "submit_labels",
                {"outcomes": [submitted(record, record["evidence_ids"]) for record in records]},
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = batch_config(root)
            grounding = config.grounding.model_copy(update={"maximum_provider_calls": 3})
            candidates = [make_candidate("old-a"), make_candidate("old-b"), make_candidate("new")]
            runs = []
            queries: list[str] = []

            def wire(request: Request, timeout: float) -> _DocResponse:
                queries.append(parse_qs(urlsplit(request.full_url).query)["query"][0])
                return _DocResponse(request.full_url, "Inspect reads metadata.")

            with (
                patch.dict(os.environ, {"CONTEXT7_API_KEY": "fixture-context-key"}),
                patch("codebench_pipeline.grounding_tools.build_opener") as opener,
                patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=complete),
            ):
                opener.return_value.open.side_effect = wire
                for revision, rows in (("1", candidates[:2]), ("2", candidates)):
                    runs.append(
                        annotate_batches(
                            rows,
                            documentation(),
                            make_rules(),
                            remote_model(),
                            config.model_copy(
                                update={"grounding": grounding.model_copy(update={"revision": revision})}
                            ),
                            root / revision,
                            cache=root / "cache",
                        )
                    )
            self.assertEqual(queries, ["old-a-0", "old-b-0", "old-a-0", "old-b-0", "new-0"])
            self.assertEqual(runs[1][:2], runs[0])
            self.assertEqual([row.status for row in runs[1]], ["success"] * 3)

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

    def test_provider_budget_isolated_by_logical_batch_and_survives_context_splits(self) -> None:
        for configured, split, expected in ((22, False, 10), (22, True, 10), (3, False, 3)):
            with self.subTest(configured=configured, split=split), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                defaults = batch_config(root)
                config = defaults.model_copy(
                    update={
                        "batch_size": 2,
                        "maximum_model_turns": 3,
                        "maximum_grounding_rounds": 2,
                        "grounding": defaults.grounding.model_copy(
                            update={"maximum_provider_calls": configured}
                        ),
                    }
                )
                batches: dict[str, str] = {}
                outbound: Counter[str] = Counter()
                retried: set[str] = set()

                def wire(request: Request, timeout: float) -> _DocResponse:
                    query = parse_qs(urlsplit(request.full_url).query)["query"][0]
                    outbound[query.split(":")[0]] += 1
                    raise URLError("fixture transport failure")

                def complete(
                    base_url: str,
                    body: bytes,
                    key: str | None,
                    timeout: float,
                    capacity: CapacityConfig,
                    path: Path,
                ) -> HTTPResponse:
                    messages = json.loads(body)["messages"]
                    records = json.loads(messages[1]["content"])["records"]
                    identities = [record["candidate"]["candidate_id"] for record in records]
                    rounds = sum(message["role"] == "tool" for message in messages)
                    if len(identities) == 2:
                        for identity in identities:
                            batches[identity] = identities[0]
                        if split and rounds:
                            retried.add(identities[0])
                            raise ContextBudgetExceeded("fixture accumulated context")
                    if rounds < 2:
                        return tool_reply(
                            "request_documentation",
                            {
                                "queries": [
                                    {
                                        "action": "context7_context",
                                        "query": f"{batches[identity]}:{identity}:{len(identities)}:{rounds}:{index}",
                                        "candidate_ids": [identity],
                                        "library_id": "/fixture/docs",
                                    }
                                    for index in range(7)
                                    for identity in [identities[index % len(identities)]]
                                ]
                            },
                        )
                    return tool_reply(
                        "submit_labels",
                        {"outcomes": [submitted(record, record["evidence_ids"]) for record in records]},
                    )

                with (
                    patch.dict(os.environ, {"CONTEXT7_API_KEY": "fixture-context-key"}),
                    patch("codebench_pipeline.grounding_tools.build_opener") as opener,
                    patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=complete),
                ):
                    opener.return_value.open.side_effect = wire
                    results = annotate_batches(
                        [make_candidate(str(index)) for index in range(4)],
                        documentation(),
                        make_rules(),
                        remote_model(),
                        config,
                        root / "run",
                    )
                self.assertEqual(len(set(batches.values())), 2)
                self.assertEqual(outbound, {batch: expected for batch in set(batches.values())})
                self.assertEqual(retried, set(batches.values()) if split else set())
                self.assertEqual([row.status for row in results], ["success"] * 4)

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
            self.assertIsNone(results[0].label)

    def test_truncated_output_does_not_consume_the_repair_context_budget(self) -> None:
        truncated = tool_reply("submit_labels", {"outcomes": []}, finish_reason="length")
        envelope = json.loads(truncated.body)
        envelope["choices"][0]["message"]["reasoning"] = "reasoning " * 6000
        truncated = HTTPResponse(json.dumps(envelope).encode(), "fixture-truncated")

        def complete(
            _base_url: str, body: bytes, _key: str | None, _timeout: float, _config: CapacityConfig, out: Path
        ) -> HTTPResponse:
            request = json.loads(body)
            if out.parent.name == "turn-00":
                self.assertNotIn("chat_template_kwargs", request)
                return truncated
            self.assertEqual(request["chat_template_kwargs"], {"enable_thinking": False})
            self.assertEqual(request["max_completion_tokens"], remote_model().max_output_tokens)
            messages = request["messages"]
            (row,) = json.loads(messages[1]["content"])["records"]
            self.assertEqual([message["role"] for message in messages], ["system", "user", "user"])
            return tool_reply(
                "submit_labels", {"outcomes": [submitted(row, row["evidence_ids"])]}, finish_reason="stop"
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch(
                "codebench_pipeline.batch_labeling.guarded_complete", side_effect=complete
            ) as provider:
                (result,) = annotate_batches(
                    [make_candidate("one")],
                    documentation(),
                    make_rules(),
                    remote_model(),
                    batch_config(root).model_copy(update={"backend_revision": "qwen38-27b-fp8"}),
                    root / "run",
                    maximum_context_characters=50000,
                )
            self.assertEqual(result.status, "success", result.failure_detail)
            self.assertEqual(provider.call_count, 2)
            raw_response = next((root / "run").glob("batches/*/turn-00/raw_response.json"))
            self.assertEqual(json.loads(raw_response.read_text()), envelope)

    def test_capacity_pause_replays_only_the_failed_conversation(self) -> None:
        requests: list[tuple[str, ...]] = []

        def provider(
            _base_url: str,
            body: bytes,
            _key: str | None,
            _timeout: float,
            _config: CapacityConfig,
            _out: Path,
        ) -> HTTPResponse:
            records = json.loads(json.loads(body)["messages"][1]["content"])["records"]
            requests.append(tuple(row["candidate"]["candidate_id"] for row in records))
            if len(requests) == 3:
                raise CapacityUnavailable("side load")
            if len(requests) == 2:
                records = records[:1]
            return tool_reply(
                "submit_labels", {"outcomes": [submitted(row, row["evidence_ids"]) for row in records]}
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidates = [make_candidate(identity) for identity in ("a", "b", "c", "d")]
            config = batch_config(root).model_copy(update={"batch_size": 2})
            with patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=provider):
                with self.assertRaises(CapacityUnavailable):
                    annotate_batches(
                        candidates,
                        documentation(),
                        make_rules(),
                        remote_model(),
                        config,
                        root / "paused",
                        cache=root / "cache",
                    )
                original = {
                    path.parent.name: path.read_bytes()
                    for path in (root / "paused" / "label").glob("*/receipt.json")
                    if json.loads(path.read_text())["candidate_id"] in {"a", "b"}
                }
                resumed = annotate_batches(
                    candidates,
                    documentation(),
                    make_rules(),
                    remote_model(),
                    config,
                    root / "resumed",
                    cache=root / "cache",
                )
            self.assertEqual([row.status for row in resumed], ["success"] * 4)
            self.assertEqual(requests, [("a", "b"), ("c", "d"), ("c", "d"), ("c", "d")])
            self.assertEqual(len(original), 2)
            for row_dir, receipt in original.items():
                self.assertEqual(
                    (root / "resumed" / "label" / row_dir / "receipt.json").read_bytes(), receipt
                )
