import json
import os
import tempfile
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch

from pydantic import ValidationError
from test_keenable import DECLARATION, result

from codebench_pipeline.inference_capacity import CapacityConfig, CapacityUnavailable
from codebench_pipeline.keenable import KeenableCollection, KeenableError, load_declaration
from codebench_pipeline.keenable_planner import SearchPlanner
from codebench_pipeline.keenable_policy import CurrentAdministrativeSearch, digest, parse_declaration
from codebench_pipeline.keenable_transport import DirectResponse, Request
from codebench_pipeline.openai_transport import HTTPResponse


def completion(decision: dict[str, str | None]) -> HTTPResponse:
    body = {
        "id": "fixture-response",
        "model": "test-model",
        "choices": [
            {
                "index": 0,
                "finish_reason": "stop",
                "message": {"role": "assistant", "content": json.dumps(decision)},
            }
        ],
    }
    return HTTPResponse(json.dumps(body).encode(), None)


class AdministrativeSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        current = load_declaration(DECLARATION)
        workload, docker = (source.url_prefix for source in current.sources if source.allow_discovery)
        assert workload is not None and docker is not None
        values = current.model_dump(mode="json")
        values["generation"] = "scoped-fixture"
        values["search_plan"] = {
            "mode": "fixed",
            "max_queries": 2,
            "scopes": [
                {
                    "case": "workloads",
                    "origin_id": "kubernetes-website-kubectl",
                    "reviewed_urls": [workload + "quick-reference.md", workload + "jsonpath.md"],
                },
                {
                    "case": "containers",
                    "origin_id": "docker-engine-resource-docs",
                    "reviewed_urls": [
                        docker + name
                        for name in (
                            "manage-resources/labels.md",
                            "daemon/live-restore.md",
                            "containers/start-containers-automatically.md",
                            "logging/drivers/local.md",
                        )
                    ],
                },
            ],
        }
        self.fixed = parse_declaration(json.dumps(values))
        assert isinstance(self.fixed.search_plan, CurrentAdministrativeSearch)
        values["generation"] = "adaptive-fixture"
        values["search_plan"]["mode"] = "adaptive"
        self.adaptive = parse_declaration(json.dumps(values))
        self.planner = SearchPlanner(
            endpoint="https://model.example:443/v1",
            model="test-model",
            capacity=CapacityConfig(
                state_directory=str(self.root / "capacity"), expected_model_path="test-path"
            ),
            receipt_directory=self.root / "receipts",
        )
        self.urls = tuple(scope.reviewed_urls for scope in self.fixed.search_plan.scopes)
        self.enterContext(patch.dict(os.environ, {"KEENABLE_API_KEY": "fixture-key"}))

    def test_fixed_comparison_uses_reviewed_urls_and_never_calls_model(self) -> None:
        workload, docker = self.urls
        with (
            patch("codebench_pipeline.keenable_planner.guarded_complete") as model,
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=(result(workload[1], docker[0]), result(docker[0])),
            ),
        ):
            catalog = KeenableCollection(self.root, self.fixed, planner=self.planner).discover()
        assert catalog.plan is not None
        self.assertEqual(catalog.plan.stop, "budget")
        self.assertEqual(
            [item.option for item in catalog.plan.feedback], ["workloads-reference", "containers-reference"]
        )
        self.assertEqual([item.new_urls for item in catalog.plan.feedback], [1, 1])
        self.assertEqual(len([lead for lead in catalog.leads if lead.excluded == "outside_case"]), 1)
        model.assert_not_called()

    def test_shipped_discovery_ignores_reference_hits_and_switches_symptom(self) -> None:
        community = "https://operator.example/stale-workload-status"
        with (
            patch(
                "codebench_pipeline.keenable_planner.guarded_complete",
                side_effect=[
                    completion({"option": "workloads-unexpected-output"}),
                    completion({"option": "workloads-quoting"}),
                    completion({"option": None}),
                ],
            ),
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=[
                    result("https://kubernetes.io/docs/reference/kubectl/"),
                    result(community),
                ],
            ),
        ):
            catalog = KeenableCollection(
                self.root, load_declaration(DECLARATION), planner=self.planner
            ).discover()
        assert catalog.plan is not None
        self.assertEqual([page.lead.url for page in catalog.selected], [community])
        self.assertEqual([turn.new_urls for turn in catalog.plan.feedback], [0, 1])
        self.assertEqual(
            [lead.excluded for lead in catalog.leads if lead.url is None], ["official_documentation"]
        )

    def test_adaptive_uses_counts_only_and_reuses_completed_generation(self) -> None:
        workload, docker = self.urls
        prompts: list[str] = []

        def model(
            _endpoint: str, payload: bytes, *_args: str | float | CapacityConfig | Path | None
        ) -> HTTPResponse:
            content = json.loads(payload)["messages"][1]["content"]
            prompts.append(content)
            prompt = json.loads(content)
            previous = prompt["previous"]
            if not previous:
                return completion({"option": "workloads-inventory"})
            if previous == [{"option": "workloads-inventory", "returned": 4, "eligible": 2, "new_urls": 1}]:
                return completion({"option": "containers-retention"})
            return completion({"option": None})

        def search(request: Request) -> DirectResponse:
            assert request.payload is not None
            query = json.loads(request.payload)["query"]
            if "read-only inventory examples" in query:
                return result(workload[0], workload[1], workload[1], "https://discard.example/private-marker")
            if "data retention limits and defaults" in query:
                return result(docker[3])
            raise AssertionError("Unexpected search")

        with (
            patch("codebench_pipeline.keenable_planner.guarded_complete", side_effect=model),
            patch("codebench_pipeline.keenable.request_direct", side_effect=search) as wire,
        ):
            branch = KeenableCollection(self.root, self.adaptive, planner=self.planner)
            with patch("codebench_pipeline.keenable.freeze_catalog", side_effect=InterruptedError):
                with self.assertRaises(InterruptedError):
                    branch.discover()
            catalog = branch.discover()
            replay = KeenableCollection(self.root, self.adaptive).discover()
            with self.assertRaisesRegex(KeenableError, "planner_binding_mismatch"):
                KeenableCollection(
                    self.root, self.adaptive, planner=replace(self.planner, backend_revision="other-backend")
                ).discover()
        self.assertEqual(replay.model_dump_json(), catalog.model_dump_json())
        assert catalog.plan is not None
        self.assertEqual(catalog.plan.stop, "budget")
        self.assertEqual(len(catalog.plan.decisions), 2)
        self.assertEqual(
            [item.option for item in catalog.plan.feedback], ["workloads-inventory", "containers-retention"]
        )
        self.assertEqual(wire.call_count, 2)
        self.assertEqual(len(prompts), 2)
        self.assertNotIn("https://", json.dumps(prompts))
        self.assertNotIn("private-marker", catalog.model_dump_json())

    def test_invalid_model_selection_or_guard_failure_blocks_generation(self) -> None:
        cases = (
            (completion({"option": "nonexistent-option"}), "planner_option_not_available"),
            (completion({"option": None, "query": "unreviewed"}), "planner_response_invalid"),
            (HTTPResponse(b"x" * 16_385, None), "planner_response_budget"),
            (CapacityUnavailable("stale_snapshot"), "planner_call_failed"),
        )
        for index, (response, reason) in enumerate(cases):
            with (
                self.subTest(reason=reason),
                patch(
                    "codebench_pipeline.keenable_planner.guarded_complete", side_effect=[response]
                ) as model,
                patch("codebench_pipeline.keenable.request_direct") as wire,
            ):
                declaration = self.adaptive.model_copy(update={"generation": f"blocked-{index}"})
                branch = KeenableCollection(self.root, declaration, planner=self.planner)
                for _ in range(2):
                    with self.assertRaisesRegex(KeenableError, reason):
                        branch.discover()
                model.assert_called_once()
                wire.assert_not_called()

    def test_adaptive_requires_planner_and_can_end_without_searching(self) -> None:
        with patch("codebench_pipeline.keenable.request_direct") as wire:
            with self.assertRaisesRegex(KeenableError, "adaptive_planner_required"):
                KeenableCollection(self.root, self.adaptive).discover()
            with patch(
                "codebench_pipeline.keenable_planner.guarded_complete",
                return_value=completion({"option": None}),
            ):
                catalog = KeenableCollection(self.root, self.adaptive, planner=self.planner).discover()
        assert catalog.plan is not None
        self.assertEqual(catalog.plan.stop, "planner_stop")
        self.assertEqual(catalog.searches, ())
        wire.assert_not_called()

    def test_plain_planner_completes_against_chat_only_endpoint(self) -> None:
        requests: list[tuple[str, str]] = []

        class ChatOnly(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append((self.path, request["model"]))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(completion({"option": None}).body)

        with HTTPServer(("127.0.0.1", 0), ChatOnly) as server:
            thread = Thread(target=server.serve_forever)
            thread.start()
            try:
                planner = replace(
                    self.planner, endpoint=f"http://127.0.0.1:{server.server_port}/v1", capacity=None
                )
                catalog = KeenableCollection(self.root, self.adaptive, planner=planner).discover()
            finally:
                server.shutdown()
                thread.join()
        assert catalog.plan is not None
        self.assertEqual(catalog.plan.stop, "planner_stop")
        self.assertEqual(requests, [("/v1/chat/completions", self.planner.model)])

    def test_quarantined_guard_cannot_dispatch_search_or_retry(self) -> None:
        assert self.planner.capacity is not None
        state = Path(self.planner.capacity.state_directory)
        state.mkdir()
        (state / (digest(self.planner.endpoint.encode()) + ".json")).write_text("{}")
        branch = KeenableCollection(self.root, self.adaptive, planner=self.planner)
        with (
            patch("codebench_pipeline.inference_capacity._snapshot") as metrics,
            patch("codebench_pipeline.inference_capacity.complete") as inference,
            patch("codebench_pipeline.keenable.request_direct") as search,
        ):
            for _ in range(2):
                with self.assertRaisesRegex(KeenableError, "planner_guard_quarantined"):
                    branch.discover()
        metrics.assert_not_called()
        inference.assert_not_called()
        search.assert_not_called()
        self.assertEqual(len(list(self.planner.receipt_directory.glob("capacity-*"))), 1)

    def test_scope_validation_rejects_urls_from_a_different_case(self) -> None:
        values = self.fixed.model_dump(mode="json")
        values["search_plan"]["scopes"][0]["reviewed_urls"] = list(self.urls[1])
        with self.assertRaisesRegex(ValidationError, "within the discovery scope"):
            parse_declaration(json.dumps(values))
