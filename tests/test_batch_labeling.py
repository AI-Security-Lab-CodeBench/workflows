import json
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

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
from codebench_pipeline.batch_labeling_protocol import LiveSupport, page_observation
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_tools import GroundingCredentials, LiveGroundingConfig
from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable, ContextBudgetExceeded
from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.openai_transport import HTTPResponse
from codebench_pipeline.storage import IntegrityError

URL = "https://example.org/docs"
QUOTE = "`probe inspect` reads metadata."


def batch_config(root: Path, revision: str = "1") -> BatchLabelingConfig:
    return BatchLabelingConfig(
        batch_size=25,
        maximum_model_turns=6,
        grounding_calls_per_command=2,
        backend_revision=f"fixture-v{revision}",
        grounding=LiveGroundingConfig(allowed_domains=["example.org"]),
        capacity=CapacityConfig(state_directory=str(root / "capacity"), expected_model_path="/fixture/model"),
    )


def remote_model() -> ModelConfig:
    return ModelConfig(provider="openai_compatible", model="fixture/model", base_url="http://fixture/v1")


def tool_reply(name: str, arguments: dict, finish_reason: str = "tool_calls") -> HTTPResponse:
    return tool_replies((name, arguments), finish_reason=finish_reason)


def tool_replies(*calls: tuple[str, dict | str], finish_reason: str = "tool_calls") -> HTTPResponse:
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
                                        "arguments": args if isinstance(args, str) else json.dumps(args),
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
    label = make_label(candidate, make_rules()).model_copy(
        update={
            "documentation_urls": documentation_urls if documentation_urls is not None else [url],
        }
    )
    return {
        "candidate_id": candidate.candidate_id,
        "row_input_fingerprint": record["row_input_fingerprint"],
        "label": label.model_dump(mode="json", by_alias=True),
        "evidence_ids": [identifier for identifier in evidence_ids],
        "grounding_quotes": {identifier: quote for identifier in evidence_ids},
        "grounding_rationale": "The quoted documentation describes this operation.",
        "failure_code": "",
        "failure_detail": "",
    }


@contextmanager
def live_mcp_fixture(
    pages: dict[str, str], *, response_url: str | None = None
) -> Iterator[list[tuple[str, dict]]]:
    calls: list[tuple[str, dict]] = []

    class Session:
        async def call_tool(self, name: str, arguments: dict) -> CallToolResult:
            calls.append((name, arguments))
            if name == "search_web_pages":
                if arguments["query"] == "failed":
                    return CallToolResult(isError=True, content=[TextContent(type="text", text="error")])
                body = {
                    "results": [
                        {"url": url, "snippet": QUOTE} for url in list(pages)[: arguments["max_results"]]
                    ]
                }
            else:
                assert name == "fetch_page_content" and arguments["live"] and "prompt" not in arguments
                url = arguments["url"]
                body = {"url": response_url or url, "content": pages[url]}
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(body))])

    @asynccontextmanager
    async def session(api_key: str) -> AsyncIterator[Session]:
        assert api_key.startswith("fixture-") or api_key.startswith("synthetic-")
        yield Session()

    with (
        patch.dict("os.environ", {"KEENABLE_API_KEY": "fixture-key"}),
        patch("codebench_pipeline.live_grounding.search_session", side_effect=session),
    ):
        yield calls


def request_parts(body: bytes) -> tuple[list[dict], list[dict]]:
    payload = json.loads(body)
    messages = payload["messages"]
    return json.loads(messages[1]["content"])["records"], [
        json.loads(message["content"]) for message in messages if message["role"] == "tool"
    ]


def shared_retrieval(body: bytes) -> HTTPResponse:
    records, results = request_parts(body)
    ids = [row["candidate"]["candidate_id"] for row in records]
    if not results:
        return tool_reply("query", {"query": "Official probe inspect documentation", "candidate_ids": ids})
    if not results[-1].get("page"):
        return tool_reply("page_fetch", {"url": URL, "candidate_ids": ids})
    page = results[-1]["page"]
    return tool_reply(
        "finalize",
        {
            "outcomes": [
                submitted(row, [page["evidence_ids"][identity]])
                for row, identity in zip(records, ids, strict=True)
            ]
        },
    )


class BatchLabelingTests(TestCase):
    def test_large_followups_bound_wire_size_and_reach_every_row(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        candidates = [
            make_candidate(f"row-{index:02}").model_copy(
                update={"context": f"Independent example {index}. " + "Maintainer reference detail. " * 58}
            )
            for index in range(25)
        ]
        urls = {row.candidate_id: f"{URL}/{row.candidate_id}" for row in candidates[:22]}
        multi_page_row, second_page_row = list(urls)[:2]
        deferred = {row.candidate_id for row in candidates[22:]}
        pages = {url: (f"{QUOTE} Read-only inspection. " * 76) for url in [URL, *urls.values()]}
        wire_sizes: list[int] = []
        seen: set[str] = set()
        completed: set[str] = set()
        rejected: str | None = None

        def reply(_url: str, body: bytes, _key: str | None, _timeout: float) -> HTTPResponse:
            nonlocal rejected
            wire_sizes.append(len(body))
            request = json.loads(body)
            self.assertEqual(request["max_completion_tokens"], 8192)
            messages = request["messages"]
            prompt = json.loads(messages[1]["content"])
            records = {record["candidate"]["candidate_id"]: record for record in prompt["records"]}
            if len(wire_sizes) == 1:
                self.assertEqual(set(records), {row.candidate_id for row in candidates})
                return tool_replies(
                    *(
                        (
                            "page_fetch",
                            {
                                "url": url,
                                "candidate_ids": [identity, multi_page_row]
                                if identity == second_page_row
                                else [identity],
                            },
                        )
                        for identity, url in urls.items()
                    ),
                    ("query", {"query": "failed", "candidate_ids": sorted(deferred)}),
                    ("query", {"query": "Probe inspect documentation", "candidate_ids": sorted(deferred)}),
                )
            self.assertLess(len(body), min(wire_sizes[0], 45_000), "follow-up grew beyond its wire budget")
            self.assertEqual([message["role"] for message in messages], ["system", "user"])
            self.assertLessEqual(len(records), 8)
            self.assertFalse(completed.intersection(records))
            self.assertEqual(prompt["grounding_call_budget"], 25)
            self.assertLessEqual(len(prompt["evidence"]), 8)
            seen.update(records)
            evidence: dict[str, list[tuple[str, str]]] = {}
            for page in prompt["evidence"]:
                if QUOTE in page["content"]:
                    for identity, evidence_id in page["evidence_ids"].items():
                        evidence.setdefault(identity, []).append((page["url"], evidence_id))
            if len(wire_sizes) == 2:
                self.assertEqual(set(evidence), records.keys())
            calls: list[tuple[str, dict[str, JsonValue]]] = []
            if missing := deferred.intersection(records) - evidence.keys():
                self.assertEqual(prompt["remaining_grounding_calls"], 1)
                for identity in missing:
                    self.assertIn(URL, {hit["url"] for hit in prompt["discoveries"][identity]})
                    self.assertIn(identity, prompt["retrieval_errors"])
                pending_ids: list[JsonValue] = [identity for identity in sorted(deferred - completed)]
                calls.append(("page_fetch", {"url": URL, "candidate_ids": pending_ids}))
            outcomes: list[JsonValue] = []
            for identity, record in records.items():
                if identity not in evidence:
                    continue
                row_evidence = evidence[identity]
                outcome = submitted(
                    record,
                    [evidence_id for _, evidence_id in row_evidence],
                    documentation_urls=[url for url, _ in row_evidence],
                )
                if rejected is None:
                    rejected = identity
                    outcome["row_input_fingerprint"] = "incorrect"
                else:
                    if identity == rejected:
                        self.assertIn(identity, prompt["submission_errors"])
                    completed.add(identity)
                outcomes.append(outcome)
            if outcomes:
                calls.append(("finalize", {"outcomes": outcomes}))
            return tool_replies(*calls)

        config = batch_config(out).model_copy(
            update={
                "inference_mode": "plain",
                "capacity": None,
                "grounding_calls_per_command": 1,
                "maximum_model_turns": 8,
            }
        )
        model = remote_model().model_copy(update={"max_output_tokens": 8192})
        with (
            live_mcp_fixture(pages) as provider_calls,
            patch("codebench_pipeline.batch_labeling.plain_complete", side_effect=reply),
        ):
            attempts = annotate_batches(candidates, make_rules(), model, config, out)
        self.assertEqual(seen, {row.candidate_id for row in candidates})
        self.assertEqual([attempt.status for attempt in attempts], ["success"] * 25)
        self.assertEqual(len(provider_calls), 25)
        (transcript_path,) = (out / "batches").glob("*/transcript.json")
        transcript = json.loads(transcript_path.read_bytes())
        self.assertEqual(transcript["provider_calls"], 25)
        self.assertEqual(
            sum(
                "page" in json.loads(message["content"])
                for message in transcript["messages"]
                if message["role"] == "tool"
            ),
            23,
        )
        for candidate, attempt in zip(candidates, attempts, strict=True):
            proof = LiveSupport.model_validate_json(
                (out / attempt.attempt_evidence_reference).with_name("verified.json").read_bytes()
            )
            proof.verify(candidate, attempt, batch_config(out).grounding)
            expected_urls = {urls.get(candidate.candidate_id, URL)}
            if candidate.candidate_id == multi_page_row:
                expected_urls.add(urls[second_page_row])
            self.assertEqual(
                {page.url: page.content for page in proof.pages.values()},
                {url: pages[url] for url in expected_urls},
            )

    def test_long_page_shows_a_verbatim_window_containing_the_relevant_flag(self) -> None:
        body = (
            ("Navigation\n" * 200)
            + "Use --namespace to choose which namespace to inspect."
            + ("Footer\n" * 240)
        )
        window = page_observation(body, ["probe inspect --namespace fixture"])
        self.assertIn("--namespace to choose which namespace", window)
        self.assertIn(window, body)
        self.assertLess(len(window), len(body))

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
                    return shared_retrieval(body)

                with live_mcp_fixture({URL: QUOTE}):
                    attempts = self.run_batch(
                        reply,
                        [make_candidate("a")],
                        out,
                        inference_mode="plain",
                        capacity=None,
                        reasoning_effort=effort,
                    )
                self.assertEqual([row.status for row in attempts], ["success"])

    def test_large_provenance_does_not_split_a_sixteen_row_model_batch(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        candidates = [
            make_candidate(f"row-{index:02}").model_copy(update={"transformations": ["f" * 10000]})
            for index in range(16)
        ]
        config = batch_config(out).model_copy(update={"inference_mode": "plain", "capacity": None})
        with (
            live_mcp_fixture({URL: QUOTE}),
            patch(
                "codebench_pipeline.batch_labeling.plain_complete",
                side_effect=lambda url, body, *_: shared_retrieval(body),
            ),
        ):
            attempts = annotate_batches(
                candidates, make_rules(), remote_model(), config, out, maximum_context_characters=32768
            )
        self.assertEqual([row.status for row in attempts], ["success"] * 16)
        self.assertEqual(len(list((out / "batches").glob("*/transcript.json"))), 1)
        self.assertEqual(
            [row.transformations for row in attempts], [row.transformations for row in candidates]
        )

    def run_batch(self, provider, candidates, out: Path, **settings):
        config = batch_config(out).model_copy(update=settings)
        endpoint = "plain_complete" if config.inference_mode == "plain" else "guarded_complete"
        with patch(f"codebench_pipeline.batch_labeling.{endpoint}", side_effect=provider):
            return annotate_batches(candidates, make_rules(), remote_model(), config, out)

    def test_shared_fetch_supports_independent_rows_and_verifies_without_network(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        candidates = [make_candidate("b"), make_candidate("a")]
        with (
            live_mcp_fixture({URL: QUOTE}) as calls,
            patch(
                "codebench_pipeline.batch_labeling.guarded_complete",
                side_effect=AssertionError("guarded called"),
            ),
        ):
            attempts = self.run_batch(
                lambda url, body, *_: shared_retrieval(body),
                candidates,
                out,
                inference_mode="plain",
                capacity=None,
            )
        self.assertEqual([attempt.candidate_id for attempt in attempts], ["b", "a"])
        self.assertEqual([attempt.status for attempt in attempts], ["success", "success"])
        self.assertEqual([call[0] for call in calls], ["search_web_pages", "fetch_page_content"])
        proofs = [
            LiveSupport.model_validate_json(
                (out / attempt.attempt_evidence_reference).with_name("verified.json").read_bytes()
            )
            for attempt in attempts
        ]
        for candidate, attempt, proof in zip(candidates, attempts, proofs, strict=True):
            proof.verify(candidate, attempt, batch_config(out).grounding)
        with self.assertRaises(IntegrityError):
            proofs[0].verify(candidates[1], attempts[1], batch_config(out).grounding)

    def test_bad_fingerprint_and_foreign_evidence_are_correctable_without_dropping_peer(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        candidates = [make_candidate(identity) for identity in ("a", "b", "c")]

        def reply(_url: str, body: bytes, *_):
            records, results = request_parts(body)
            if len(results) < 2:
                return shared_retrieval(body)
            page = results[1]["page"]
            a, b, c = records
            ids = page["evidence_ids"]
            if len(results) == 2:
                return tool_reply(
                    "finalize",
                    {
                        "outcomes": [
                            submitted(a, [ids["a"]]),
                            {**submitted(b, [ids["b"]]), "row_input_fingerprint": "wrong"},
                            submitted(c, [ids["a"]]),
                        ],
                    },
                )
            self.assertEqual(results[-1]["recorded"], ["a"])
            self.assertEqual(set(results[-1]["errors"]), {"b", "c"})
            return tool_reply(
                "finalize",
                {
                    "outcomes": [submitted(b, [ids["b"]]), submitted(c, [ids["c"]])],
                },
            )

        with live_mcp_fixture({URL: QUOTE}):
            attempts = self.run_batch(reply, candidates, out)
        self.assertEqual([attempt.status for attempt in attempts], ["success"] * 3)

    def test_malformed_duplicate_and_truncated_finalizations_remain_unsubmitted(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        candidates = [make_candidate("a"), make_candidate("b")]
        turn = 0

        def reply(_url: str, body: bytes, *_):
            nonlocal turn
            turn += 1
            records, results = request_parts(body)
            if turn <= 2:
                return shared_retrieval(body)
            a, b = records
            page = results[1]["page"]["evidence_ids"]
            first, second = submitted(a, [page["a"]]), submitted(b, [page["b"]])
            match turn:
                case 3:
                    return tool_replies(("finalize", "{"))
                case 4:
                    return tool_reply("finalize", {"outcomes": [first, first, second]})
                case 5:
                    self.assertEqual(results[-1]["recorded"], ["b"])
                    return tool_reply("finalize", {"outcomes": [first]}, finish_reason="length")
                case _:
                    return tool_reply("finalize", {"outcomes": [first]})

        with live_mcp_fixture({URL: QUOTE}):
            attempts = self.run_batch(reply, candidates, out)
        self.assertEqual(turn, 6)
        self.assertEqual([attempt.status for attempt in attempts], ["success", "success"])

    def test_search_only_and_fabricated_quotes_cannot_support_success(self) -> None:
        for page_allowed in (False, True):
            with self.subTest(page_allowed=page_allowed), TemporaryDirectory() as directory:
                out = Path(directory)
                sibling = "`probe publish` updates metadata."

                def reply(_url: str, body: bytes, *_):
                    records, results = request_parts(body)
                    if not results or page_allowed and len(results) == 1:
                        return shared_retrieval(body)
                    (row,) = records
                    evidence = results[1]["page"]["evidence_ids"]["a"] if page_allowed else "discovery"
                    return tool_reply("finalize", {"outcomes": [submitted(row, [evidence], quote=sibling)]})

                with live_mcp_fixture({URL: QUOTE}):
                    (attempt,) = self.run_batch(reply, [make_candidate("a")], out)
                self.assertEqual(attempt.status, "failure")
                self.assertEqual(attempt.documentation, [])
                self.assertFalse(
                    (out / attempt.attempt_evidence_reference).with_name("verified.json").exists()
                )

    def test_actual_batch_budget_counts_failed_queries_and_does_not_count_finalize(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        candidates = [make_candidate(identity) for identity in ("a", "b")]

        def reply(_url: str, body: bytes, *_):
            rows, results = request_parts(body)
            ids = [row["candidate"]["candidate_id"] for row in rows]
            if not results:
                return tool_replies(
                    *[
                        ("query", {"query": question, "candidate_ids": ids})
                        for question in ("failed", "failed", "probe docs", "over budget")
                    ]
                )
            self.assertEqual(
                [result.get("error") for result in results],
                ["provider_search_failed", "provider_search_failed", None, "grounding_budget_exhausted"],
            )
            return tool_reply(
                "finalize",
                {
                    "outcomes": [
                        {
                            **submitted(row, []),
                            "label": None,
                            "failure_code": "budget",
                            "failure_detail": "No fetch",
                        }
                        for row in rows
                    ]
                },
            )

        with live_mcp_fixture({URL: QUOTE}) as calls:
            attempts = self.run_batch(reply, candidates, out, grounding_calls_per_command=1.5)
        self.assertEqual([attempt.failure_code for attempt in attempts], ["budget", "budget"])
        self.assertEqual(len(calls), 3)
        (transcript,) = out.glob("batches/*/transcript.json")
        self.assertEqual(json.loads(transcript.read_bytes())["grounding_call_budget"], 3)

    def test_guarded_capacity_pause_propagates_without_becoming_a_label_failure(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        with self.assertRaises(CapacityUnavailable):
            self.run_batch(CapacityUnavailable("Fixture pause"), [make_candidate("a")], out)
        self.assertEqual(list(out.glob("label/*/response.json")), [])

    def test_default_singleton_budget_allows_one_direct_trusted_fetch(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))

        def reply(_url: str, body: bytes, *_):
            (row,), results = request_parts(body)
            if not results:
                return tool_reply("page_fetch", {"url": URL, "candidate_ids": ["a"]})
            return tool_reply(
                "finalize",
                {
                    "outcomes": [submitted(row, [results[-1]["page"]["evidence_ids"]["a"]])],
                },
            )

        with live_mcp_fixture({URL: QUOTE}) as calls:
            (attempt,) = self.run_batch(reply, [make_candidate("a")], out, grounding_calls_per_command=1.5)
        self.assertEqual(attempt.status, "success")
        self.assertEqual([name for name, _ in calls], ["fetch_page_content"])

    def test_untrusted_fetch_destination_fails_even_if_mcp_returns_documentation(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))

        def reply(_url: str, body: bytes, *_):
            (row,), results = request_parts(body)
            if not results:
                return tool_reply("page_fetch", {"url": URL, "candidate_ids": ["a"]})
            self.assertEqual(results[0]["error"], "untrusted_fetch_response_url")
            self.assertNotIn("page", results[0])
            return tool_reply("finalize", {"outcomes": [submitted(row, ["fabricated"])]})

        with live_mcp_fixture({URL: QUOTE}, response_url="https://untrusted.example/docs") as calls:
            (attempt,) = self.run_batch(reply, [make_candidate("a")], out, grounding_calls_per_command=1.5)
        self.assertEqual(len(calls), 1)
        self.assertEqual(attempt.status, "failure")

    def test_oversized_initial_request_splits_and_recalculates_each_batch_budget(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        budgets: list[tuple[int, int]] = []

        def reply(_url: str, body: bytes, *_):
            messages = json.loads(body)["messages"]
            if len(messages) == 2:
                payload = json.loads(messages[1]["content"])
                size = len(payload["records"])
                budgets.append((size, payload["grounding_call_budget"]))
                if size > 1:
                    raise ContextBudgetExceeded("fixture")
            return shared_retrieval(body)

        with live_mcp_fixture({URL: QUOTE}) as calls:
            attempts = self.run_batch(reply, [make_candidate("a"), make_candidate("b")], out)
        self.assertEqual([attempt.status for attempt in attempts], ["success", "success"])
        self.assertEqual(budgets, [(2, 4), (1, 2), (1, 2)])
        self.assertEqual(len(calls), 4)

    def test_oversized_singleton_creates_an_inspectable_failure_without_grounding(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        config = batch_config(out)
        with (
            patch("codebench_pipeline.batch_labeling.guarded_complete") as provider,
            live_mcp_fixture({URL: QUOTE}) as calls,
        ):
            (attempt,) = annotate_batches(
                [make_candidate("a")],
                make_rules(),
                remote_model(),
                config,
                out,
                maximum_context_characters=100,
            )
        provider.assert_not_called()
        self.assertEqual(calls, [])
        self.assertEqual(attempt.failure_code, "context_limit")
        self.assertEqual(
            json.loads((out / attempt.attempt_evidence_reference).read_bytes())["status"], "failure"
        )

    def test_same_generation_resumes_exact_input_but_prompt_changes_invalidate(self) -> None:
        out = Path(self.enterContext(TemporaryDirectory()))
        candidates = [make_candidate("a")]
        model = remote_model()
        rules = make_rules()
        cfg = batch_config(out)
        execution = BatchExecution(None, GroundingCredentials("fixture-key"))
        generation = LabelingGeneration(LabelingCheckpointCache(out / "store", out / "operations"), "a" * 64)

        def run(name: str, config: BatchLabelingConfig):
            rows = prepare_batch_rows(candidates, rules, model, config, implementation="implementation-id")
            return run_prepared_batches(
                candidates, rows, rules, model, config, out / name, execution, 1_048_576, generation
            )[0]

        def reply(_url: str, body: bytes, *_):
            if cfg.instructions == "" and "Revised classification instructions" in body.decode():
                self.assertIn(
                    "Revised classification instructions", json.loads(body)["messages"][0]["content"]
                )
            return shared_retrieval(body)

        with (
            live_mcp_fixture({URL: QUOTE}) as calls,
            patch("codebench_pipeline.batch_labeling.guarded_complete", side_effect=reply) as infer,
        ):
            first = run("first", cfg)
            resumed = run("resumed", cfg)
            self.assertEqual(infer.call_count, 3)
            revised = run(
                "revised", cfg.model_copy(update={"instructions": "Revised classification instructions"})
            )
        self.assertEqual(len(calls), 4)
        self.assertEqual(first, resumed)
        self.assertNotEqual(first.row_input_fingerprint, revised.row_input_fingerprint)
        self.assertEqual(revised.status, "success")
