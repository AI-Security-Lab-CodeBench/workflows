import json
import os
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
from unittest import TestCase
from unittest.mock import patch
from urllib.error import HTTPError

from jsonschema import Draft202012Validator
from mcp.types import CallToolResult, TextContent
from pydantic import JsonValue
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.batch_labeling import (
    BatchExecution,
    LabelingGeneration,
    annotate_batches,
    prepare_batch_rows,
    run_prepared_batches,
)
from codebench_pipeline.batch_labeling_config import BatchLabelingConfig
from codebench_pipeline.batch_labeling_protocol import LiveSupport
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_tools import GroundingCredentials, LiveGroundingConfig
from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable, ContextBudgetExceeded
from codebench_pipeline.keenable_provider import admit_tool_dispatch
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.storage import IntegrityError

URL = "https://example.org/docs"
QUOTE = "`probe inspect` reads metadata."


def batch_config(root: Path, revision: str = "1") -> BatchLabelingConfig:
    return BatchLabelingConfig(
        backend_revision=f"fixture-v{revision}",
        grounding=LiveGroundingConfig(allowed_domains=["example.org"]),
        capacity=CapacityConfig(state_directory=str(root / "capacity"), expected_model_path="/fixture/model"),
    )


def remote_model() -> ModelConfig:
    return ModelConfig(provider="openai_compatible", model="fixture/model", base_url="http://fixture/v1")


def tool_reply(name: str, arguments: Mapping[str, object], finish_reason: str = "tool_calls") -> HTTPResponse:
    return tool_replies((name, arguments), finish_reason=finish_reason)


def tool_replies(
    *calls: tuple[str, Mapping[str, object] | str], finish_reason: str = "tool_calls"
) -> HTTPResponse:
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
                                    "id": f"fixture-tool-{index}",
                                    "index": index,
                                    "type": "function",
                                    "function": {
                                        "name": name,
                                        "arguments": args
                                        if isinstance(args, str)
                                        else json.dumps(dict(args)),
                                    },
                                }
                                for index, (name, args) in enumerate(calls)
                            ],
                        },
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 100},
            }
        ).encode(),
        "fixture-request",
    )


def submitted(
    record: dict[str, JsonValue],
    evidence_ids: list[str],
    url: str = URL,
    *,
    quote: str = QUOTE,
    documentation_urls: list[str] | None = None,
) -> dict[str, JsonValue]:
    candidate_data = record["candidate"]
    assert isinstance(candidate_data, dict)
    candidate = CandidateRecord.model_validate({**candidate_data, "transformations": []})
    choices = record["allowed_risk_ids"]
    assert isinstance(choices, list) and choices and isinstance(choices[0], str)
    label = make_label(
        candidate, make_rules(), risk_id=next(iter(candidate.risk_candidates), choices[0])
    ).model_copy(
        update={"documentation_urls": documentation_urls if documentation_urls is not None else [url]}
    )
    return {
        "candidate_id": candidate.candidate_id,
        "row_input_fingerprint": record["row_input_fingerprint"],
        "label": label.model_dump(mode="json", by_alias=True, exclude={"candidate_id", "util"}),
        "evidence_ids": list[JsonValue](evidence_ids),
        "grounding_quotes": {identifier: quote for identifier in evidence_ids},
        "grounding_rationale": "The quoted documentation describes this operation.",
        "failure_code": "",
        "failure_detail": "",
    }


def unsupported(record: dict[str, JsonValue]) -> dict[str, JsonValue]:
    return {
        **submitted(record, []),
        "label": None,
        "grounding_rationale": "",
        "failure_code": "missing_operation_documentation",
        "failure_detail": "No fetched evidence supports the operation.",
    }


@contextmanager
def live_mcp_fixture(
    pages: dict[str, str], *, response_url: str | None = None, admit: bool = False
) -> Iterator[list[tuple[str, dict]]]:
    calls: list[tuple[str, dict]] = []

    class Session:
        async def call_tool(self, name: str, arguments: dict) -> CallToolResult:
            if admit:
                await admit_tool_dispatch()
            calls.append((name, arguments))
            if name == "search_web_pages":
                if arguments["query"] == "failed":
                    return CallToolResult(isError=True, content=[TextContent(type="text", text="error")])
                body = {
                    "results": [
                        {"url": url, "snippet": arguments["query"]}
                        for url in list(pages)[: arguments["max_results"]]
                    ]
                }
            else:
                assert name == "fetch_page_content" and arguments["live"] and "prompt" not in arguments
                url = arguments["url"]
                body = {"url": response_url or url, "content": pages[url]}
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(body))])

    @asynccontextmanager
    async def session(api_key: str) -> AsyncIterator[Session]:
        if not api_key.startswith(("fixture-", "synthetic-")):
            raise ValueError("Offline MCP fixture refused a non-synthetic credential")
        yield Session()

    with (
        patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
        patch("codebench_pipeline.live_grounding.search_session", side_effect=session),
    ):
        for name in tuple(os.environ):
            if name.startswith(("KEENABLE_API_KEY_", "KEENABLE_ORG_")) or name == "KEENABLE_ORG":
                del os.environ[name]
        yield calls


def request_parts(body: bytes) -> tuple[list[dict], list[dict]]:
    messages = json.loads(body)["messages"]
    record = json.loads(messages[1]["content"])
    assert "records" not in record
    return [record], [json.loads(message["content"]) for message in messages if message["role"] == "tool"]


def row_retrieval(body: bytes) -> HTTPResponse:
    (row,), results = request_parts(body)
    identity = row["candidate"]["candidate_id"]
    if not results:
        return tool_reply(
            "query", {"query": "Official probe inspect documentation", "candidate_ids": [identity]}
        )
    if not results[-1].get("page"):
        return tool_reply("page_fetch", {"url": URL, "candidate_ids": [identity]})
    return tool_reply(
        "finalize", {"outcome": submitted(row, [results[-1]["page"]["evidence_ids"][identity]])}
    )


class BatchLabelingTests(TestCase):
    def run_batch(self, provider, candidates, out: Path, **settings):
        config = batch_config(out).model_copy(update=settings)
        endpoint = "plain_complete" if config.inference_mode == "plain" else "guarded_complete"
        with patch(f"codebench_pipeline.batch_labeling.{endpoint}", side_effect=provider):
            return annotate_batches(candidates, make_rules(), remote_model(), config, out)

    def test_independent_parallel_requests_and_endpoint_failure_keep_order_and_evidence(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        first_wave = Barrier(2)
        candidates = [make_candidate("broken"), make_candidate("b"), make_candidate("c")]

        def reply(url: str, body: bytes, *_):
            (row,), results = request_parts(body)
            identity = row["candidate"]["candidate_id"]
            if not results and identity in {"broken", "b"}:
                first_wave.wait(timeout=5)
            if identity == "broken":
                raise HTTPError(url, 503, "private response text", Message(), None)
            return row_retrieval(body)

        with live_mcp_fixture({URL: QUOTE}) as calls:
            attempts = self.run_batch(
                reply, candidates, out, row_concurrency=2, inference_mode="plain", capacity=None
            )
        self.assertEqual([item.candidate_id for item in attempts], ["broken", "b", "c"])
        self.assertEqual([item.status for item in attempts], ["failure", "success", "success"])
        self.assertEqual(attempts[0].failure_code, "endpoint_error")
        self.assertIn("HTTP 503", attempts[0].failure_detail)
        self.assertTrue((out / attempts[0].attempt_evidence_reference).is_file())
        transcripts = {
            row["candidate_ids"][0]: row
            for file in out.glob("batches/*/transcript.json")
            for row in [json.loads(file.read_bytes())]
        }
        self.assertEqual(
            [transcripts[i]["candidate_ids"] for i in ("broken", "b", "c")], [["broken"], ["b"], ["c"]]
        )
        self.assertEqual(transcripts["broken"]["tool_wave_sizes"], [])
        self.assertEqual(transcripts["b"]["tool_wave_sizes"], [1, 1])
        self.assertLess(
            transcripts["b"]["started_monotonic_ns"], transcripts["broken"]["finished_monotonic_ns"]
        )
        self.assertEqual([call[0] for call in calls].count("fetch_page_content"), 2)
        proofs = [
            LiveSupport.model_validate_json(
                (out / attempt.attempt_evidence_reference).with_name("verified.json").read_bytes()
            )
            for attempt in attempts[1:]
        ]
        for candidate, attempt, proof in zip(candidates[1:], attempts[1:], proofs, strict=True):
            proof.verify(candidate, attempt, batch_config(out).grounding)
        with self.assertRaises(IntegrityError):
            proofs[0].verify(candidates[2], attempts[2], batch_config(out).grounding)

    def test_last_grounding_iteration_allows_parallel_queries_before_finalize_only(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        invocations = 0

        def reply(_url: str, body: bytes, *_):
            nonlocal invocations
            invocations += 1
            (row,), results = request_parts(body)
            wire = json.loads(body)
            names = {entry["function"]["name"] for entry in wire["tools"]}
            if invocations <= 5:
                self.assertTrue(wire["parallel_tool_calls"])
                self.assertEqual(names, {"query", "page_fetch", "finalize"})
                self.assertEqual(len(results), 2 * (invocations - 1))
                return tool_replies(
                    ("query", {"query": f"manual {invocations}", "candidate_ids": ["a"]}),
                    ("query", {"query": "failed", "candidate_ids": ["a"]}),
                )
            self.assertFalse(wire["parallel_tool_calls"])
            self.assertEqual(names, {"finalize"})
            self.assertEqual(wire["tool_choice"]["function"]["name"], "finalize")
            self.assertEqual(len(results), 10)
            self.assertEqual(
                [result.get("error") for result in results[1::2]], ["provider_search_failed"] * 5
            )
            return tool_reply("finalize", {"outcome": unsupported(row)})

        with live_mcp_fixture({URL: QUOTE}) as calls:
            (attempt,) = self.run_batch(reply, [make_candidate("a")], out)
        self.assertEqual(invocations, 6)
        self.assertEqual(attempt.failure_code, "missing_operation_documentation")
        self.assertEqual(len(calls), 10)
        (transcript,) = out.glob("batches/*/transcript.json")
        stats = json.loads(transcript.read_bytes())
        self.assertEqual((stats["grounding_iterations"], stats["tool_wave_sizes"]), (5, [2] * 5))
        self.assertEqual((stats["provider_calls"], stats["grounding_call_budget"]), (10, 25))

    def test_malformed_and_oversized_waves_are_bounded_and_mixed_finalize_waits(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        invocations = 0

        def reply(_url: str, body: bytes, *_):
            nonlocal invocations
            invocations += 1
            (row,), results = request_parts(body)
            query = ("query", {"query": "Official probe docs", "candidate_ids": ["a"]})
            match invocations:
                case 1:
                    return tool_replies(("finalize", "{"))
                case 2:
                    return tool_replies(*([query] * 6))
                case 3:
                    return tool_replies(("finalize", {"outcome": unsupported(row)}), query)
                case 4:
                    self.assertIn("error", results[-2])
                    self.assertIn("discovery", results[-1])
                    return tool_reply("page_fetch", {"url": URL, "candidate_ids": ["a"]})
                case _:
                    evidence = results[-1]["page"]["evidence_ids"]["a"]
                    return tool_reply("finalize", {"outcome": submitted(row, [evidence])})

        with live_mcp_fixture({URL: QUOTE}) as calls:
            (attempt,) = self.run_batch(reply, [make_candidate("a")], out, maximum_grounding_turns=4)
        self.assertEqual(invocations, 5)
        self.assertEqual(attempt.status, "success")
        self.assertEqual([name for name, _ in calls], ["search_web_pages", "fetch_page_content"])
        (transcript,) = out.glob("batches/*/transcript.json")
        self.assertEqual(json.loads(transcript.read_bytes())["tool_wave_sizes"], [0, 0, 1, 1])

    def test_finalize_only_rejects_retrieval_when_no_grounding_turns_are_configured(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))

        def reply(_url: str, body: bytes, *_):
            wire = json.loads(body)
            self.assertEqual([entry["function"]["name"] for entry in wire["tools"]], ["finalize"])
            return tool_reply("page_fetch", {"url": URL, "candidate_ids": ["a"]})

        with live_mcp_fixture({URL: QUOTE}) as calls:
            (attempt,) = self.run_batch(reply, [make_candidate("a")], out, maximum_grounding_turns=0)
        self.assertEqual((attempt.failure_code, calls), ("protocol_exhausted", []))
        (transcript,) = out.glob("batches/*/transcript.json")
        self.assertEqual(json.loads(transcript.read_bytes())["grounding_iterations"], 0)

    def test_immediate_valid_judgment_does_not_count_as_grounding(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))

        def reply(_url: str, body: bytes, *_):
            (row,), results = request_parts(body)
            self.assertEqual(results, [])
            return tool_reply("finalize", {"outcome": unsupported(row)})

        with live_mcp_fixture({}) as calls:
            (attempt,) = self.run_batch(reply, [make_candidate("a")], out)
        self.assertEqual((attempt.failure_code, calls), ("missing_operation_documentation", []))
        (transcript,) = out.glob("batches/*/transcript.json")
        stats = json.loads(transcript.read_bytes())
        self.assertEqual((stats["grounding_iterations"], stats["tool_wave_sizes"]), (0, []))
        self.assertEqual(len(stats["model_request_sha256"]), 1)

    def test_emitted_finalize_tool_and_native_batch_accept_off_hint_and_other(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        flag_passage = "Use --namespace to choose which namespace to inspect."
        version_passage = "`probe version` prints the local client version, not metadata or a publication."
        content = ("Navigation\n" * 200) + flag_passage + "\n" + version_passage + "\n" + ("Footer\n" * 240)
        cases: dict[str, tuple[str, list[str], str, str]] = {
            "wrong_hint": ("probe inspect --namespace fixture", ["CAT-8.2"], "CAT-8.1", "accept"),
            "multiple_hints": (
                "probe inspect --namespace fixture",
                ["CAT-8.2", "CAT-8.1"],
                "CAT-8.1",
                "accept",
            ),
            "empty_hints": ("probe version", [], "other", "reject"),
        }
        candidates = [
            make_candidate(identity).model_copy(update={"command": command, "risk_candidates": hints})
            for identity, (command, hints, _, _) in cases.items()
        ]

        def reply(_url: str, body: bytes, *_args: str) -> HTTPResponse:
            (row,), results = request_parts(body)
            identity = row["candidate"]["candidate_id"]
            _, hints, risk_id, decision = cases[identity]
            self.assertEqual(row["candidate"]["risk_candidates"], hints)
            parameters = next(
                entry["function"]["parameters"]
                for entry in json.loads(body)["tools"]
                if entry["function"]["name"] == "finalize"
            )
            validator = Draft202012Validator(parameters)
            label_properties = parameters["properties"]["outcome"]["properties"]["label"]["anyOf"][0][
                "properties"
            ]
            self.assertCountEqual(label_properties["risk_id"]["enum"], ["CAT-8.1", "CAT-8.2", "other"])
            if not results or not results[-1].get("page"):
                self.assertTrue(validator.is_valid({"outcome": unsupported(row)}))
                self.assertFalse(validator.is_valid({"outcome": submitted(row, ["search-hit"])}))
                return row_retrieval(body)

            page = results[-1]["page"]
            passages = page["verbatim_passages"]
            quote = version_passage if risk_id == "other" else flag_passage
            self.assertIn(quote, passages)
            self.assertTrue(all(passage in content and len(passage) <= 260 for passage in passages))
            evidence_id = page["evidence_ids"][identity]
            outcome = submitted(row, [evidence_id], quote=quote)
            label = outcome["label"]
            assert isinstance(label, dict)
            label.update(risk_id=risk_id, decision=decision)
            if risk_id == "other":
                label.update(description="Prints the local client version.", rationale=version_passage)
            self.assertTrue(validator.is_valid({"outcome": outcome}))
            for field in ("description", "rationale"):
                self.assertNotIn("pattern", label_properties[field])
            for changes in (
                {"category_id": "C"},
                {"risk_id": "CAT-8.99"},
                {"documentation_urls": ["https://example.org/not-fetched"]},
            ):
                self.assertFalse(validator.is_valid({"outcome": {**outcome, "label": {**label, **changes}}}))
            self.assertFalse(
                validator.is_valid(
                    {"outcome": {**outcome, "grounding_quotes": {evidence_id: "Choose a namespace."}}}
                )
            )
            return tool_reply("finalize", {"outcome": outcome})

        with live_mcp_fixture({URL: content}):
            attempts = self.run_batch(reply, candidates, out)
        self.assertCountEqual([attempt.candidate_id for attempt in attempts], cases)
        for attempt in attempts:
            _, hints, risk_id, decision = cases[attempt.candidate_id]
            self.assertEqual((attempt.status, attempt.risk_candidates), ("success", hints))
            assert attempt.label is not None
            self.assertEqual((attempt.label.risk_id, attempt.label.decision), (risk_id, decision))
            quote = version_passage if risk_id == "other" else flag_passage
            self.assertEqual([(doc.url, doc.snippet) for doc in attempt.documentation], [(URL, quote)])

    def test_unknown_and_reserved_hint_ids_fail_batch_preflight(self) -> None:
        root = Path(self.enterContext(TemporaryDirectory()))
        for bad_id in ("CAT-8.99", "CAT-9.1", "other"):
            with self.subTest(bad_id=bad_id), self.assertRaisesRegex(ValueError, "Unknown risk hint"):
                prepare_batch_rows(
                    [make_candidate(risk=bad_id)],
                    make_rules(),
                    remote_model(),
                    batch_config(root),
                    implementation="fixture",
                )

    def test_plain_reasoning_setting_can_produce_tools_without_exhausting_output(self) -> None:
        for effort, template in (
            ("low", {"reasoning_effort": "low"}),
            ("none", {"enable_thinking": False}),
        ):
            with self.subTest(effort=effort), TemporaryDirectory() as directory:
                out = Path(directory)

                def reply(_url: str, body: bytes, *_):
                    if json.loads(body).get("chat_template_kwargs") != template:
                        return tool_replies(finish_reason="length")
                    return row_retrieval(body)

                with live_mcp_fixture({URL: QUOTE}):
                    (attempt,) = self.run_batch(
                        reply,
                        [make_candidate("a")],
                        out,
                        inference_mode="plain",
                        capacity=None,
                        reasoning_effort=effort,
                    )
                self.assertEqual(attempt.status, "success")

    def test_grounded_judgment_requires_matching_fingerprint_fetched_quote_and_trusted_url(self) -> None:
        for failure in (
            "stale_input",
            "discovery_only",
            "forged_quote",
            "foreign_evidence",
            "untrusted_url",
            "wrong_cited_url",
            "wrong_risk",
            "wrong_category",
            "wrong_candidate",
            "other_accept",
            "unsupported_other",
            "blank_description",
        ):
            with self.subTest(failure=failure), TemporaryDirectory() as directory:
                out = Path(directory)

                def reply(_url: str, body: bytes, *_):
                    (row,), results = request_parts(body)
                    if not results or (len(results) == 1 and failure != "discovery_only"):
                        return row_retrieval(body)
                    grounding_result = results[0] if failure == "discovery_only" else results[1]
                    page = grounding_result.get("page", {})
                    evidence = page.get("evidence_ids", {}).get("a", "fabricated")
                    if failure == "untrusted_url":
                        self.assertEqual(grounding_result["error"], "untrusted_fetch_response_url")
                    if failure == "foreign_evidence":
                        evidence = "foreign-proof"
                    outcome = submitted(
                        row, [evidence], quote="Forged quote." if failure == "forged_quote" else QUOTE
                    )
                    if failure == "stale_input":
                        outcome["row_input_fingerprint"] = "wrong"
                    label = outcome["label"]
                    assert isinstance(label, dict)
                    if failure == "wrong_cited_url":
                        label["documentation_urls"] = ["https://example.org/not-fetched"]
                    if failure == "wrong_risk":
                        label["risk_id"] = "CAT-8.99"
                    if failure == "wrong_category":
                        label["category_id"] = "CAT-9"
                    if failure == "wrong_candidate":
                        outcome["candidate_id"] = "foreign"
                    if failure == "other_accept":
                        label["risk_id"] = "other"
                    if failure == "unsupported_other":
                        label.update(risk_id="other", decision="reject", documentation_urls=[])
                        outcome.update(evidence_ids=[], grounding_quotes={})
                    if failure == "blank_description":
                        label["description"] = "  "
                    return tool_reply("finalize", {"outcome": outcome})

                alternate = "https://untrusted.example/docs" if failure == "untrusted_url" else None
                with live_mcp_fixture({URL: QUOTE}, response_url=alternate):
                    (attempt,) = self.run_batch(reply, [make_candidate("a")], out)
                self.assertEqual((attempt.status, attempt.documentation), ("failure", []))
                self.assertFalse(
                    (out / attempt.attempt_evidence_reference).with_name("verified.json").exists()
                )

    def test_numbered_key_in_decoded_fetch_url_never_enters_pages_or_model(self) -> None:
        key = 'fixture-extra"provider-key'
        self._assert_fetch_url_does_not_reach_model(key, key)

    def test_numbered_key_in_serialized_fetch_url_never_enters_pages_or_model(self) -> None:
        self._assert_fetch_url_does_not_reach_model(
            r"fixture-extra\"provider-key", 'fixture-extra"provider-key'
        )

    def _assert_fetch_url_does_not_reach_model(self, key: str, decoded_url_suffix: str) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        returned_url = f"{URL}/{decoded_url_suffix}"
        observations: list[dict[str, JsonValue]] = []

        def reply(_url: str, body: bytes, *_):
            (row,), results = request_parts(body)
            if len(results) < 2:
                return row_retrieval(body)
            observations.append(results[1])
            return tool_reply("finalize", {"outcome": unsupported(row)})

        with (
            live_mcp_fixture({URL: QUOTE}, response_url=returned_url),
            patch.dict("os.environ", {"KEENABLE_API_KEY_1": key}),
        ):
            (attempt,) = self.run_batch(reply, [make_candidate("a")], out)

        self.assertEqual((attempt.status, attempt.documentation), ("failure", []))
        (observation,) = observations
        self.assertEqual(observation.get("error"), "sensitive_provider_response")
        self.assertNotIn("page", observation)
        self.assertNotIn("provider_error", observation)
        retained = (out / attempt.attempt_evidence_reference).with_name("grounding.json")
        receipt = json.loads(retained.read_bytes())["provider_receipts"][-1]
        self.assertEqual(
            (receipt["error"], receipt["page_id"], receipt["response_sha256"]),
            ("sensitive_provider_response", None, None),
        )
        self.assertEqual(
            receipt["provider_error"],
            {"code": "sensitive_provider_response", "status": None, "retry_after_seconds": None},
        )
        self.assertNotIn(key.encode(), retained.read_bytes())
        self.assertEqual(list(out.glob("**/pages/*.json")), [])

    def test_guarded_capacity_pause_propagates_without_becoming_a_label_failure(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        with self.assertRaises(CapacityUnavailable):
            self.run_batch(CapacityUnavailable("Fixture pause"), [make_candidate("a")], out)
        self.assertEqual(list(out.glob("label/*/response.json")), [])

    def test_context_failure_is_row_local_and_has_a_receipt(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))

        def reply(_url: str, body: bytes, *_):
            (row,), _ = request_parts(body)
            if row["candidate"]["candidate_id"] == "a":
                raise ContextBudgetExceeded("fixture")
            return row_retrieval(body)

        with live_mcp_fixture({URL: QUOTE}):
            failed, succeeded = self.run_batch(reply, [make_candidate("a"), make_candidate("b")], out)
        self.assertEqual((failed.failure_code, succeeded.status), ("context_limit", "success"))
        self.assertEqual(
            json.loads((out / failed.attempt_evidence_reference).read_bytes())["status"], "failure"
        )

    def test_oversized_singleton_creates_an_inspectable_failure_without_grounding(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        with patch("codebench_pipeline.batch_labeling.guarded_complete") as provider:
            (attempt,) = annotate_batches(
                [make_candidate("a")],
                make_rules(),
                remote_model(),
                batch_config(out),
                out,
                maximum_context_characters=100,
            )
        provider.assert_not_called()
        self.assertEqual(attempt.failure_code, "context_limit")
        self.assertEqual(
            json.loads((out / attempt.attempt_evidence_reference).read_bytes())["status"], "failure"
        )

    def test_same_generation_resumes_a_row_even_with_a_different_neighbor(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        model, rules, cfg = remote_model(), make_rules(), batch_config(out)
        execution = BatchExecution(None, GroundingCredentials("fixture-key"))
        generation = LabelingGeneration(LabelingCheckpointCache(out / "store", out / "operations"), "a" * 64)
        candidate = make_candidate("a")

        def run(name: str, candidates: list[CandidateRecord], config: BatchLabelingConfig):
            rows = prepare_batch_rows(candidates, rules, model, config, implementation="implementation-id")
            return run_prepared_batches(
                candidates, rows, rules, model, config, out / name, execution, 1_048_576, generation
            )

        with (
            live_mcp_fixture({URL: QUOTE}) as calls,
            patch(
                "codebench_pipeline.batch_labeling.guarded_complete",
                side_effect=lambda _, body, *a: row_retrieval(body),
            ) as infer,
        ):
            (first,) = run("first", [candidate], cfg)
            peer, resumed = run("resumed", [make_candidate("other"), candidate], cfg)
            self.assertEqual(infer.call_count, 6)
            (revised,) = run("revised", [candidate], cfg.model_copy(update={"instructions": "Revised rules"}))
        self.assertEqual((len(calls), peer.status), (6, "success"))
        self.assertEqual(first, resumed)
        self.assertTrue(
            (out / "resumed" / resumed.attempt_evidence_reference).with_name("reuse.json").is_file()
        )
        self.assertNotEqual(first.row_input_fingerprint, revised.row_input_fingerprint)
        self.assertEqual(revised.status, "success")
