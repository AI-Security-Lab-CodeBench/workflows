from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPRedirectHandler, Request

from dagster import RunConfig
from pydantic import JsonValue, ValidationError

from codebench_pipeline.grounding_tools import (
    DocumentationQuery,
    GroundingBroker,
    GroundingConfig,
)
from codebench_pipeline.typed_config import TypedConfig


class BatchConfig(TypedConfig):
    grounding: GroundingConfig


class AnnotationConfig(TypedConfig):
    batch: BatchConfig


class Response(io.BytesIO):
    def __init__(self, body: bytes, url: str, bounds: list[int | None]) -> None:
        super().__init__(body)
        self.url = url
        self.bounds = bounds

    def geturl(self) -> str:
        return self.url

    def read(self, size: int | None = -1) -> bytes:
        self.bounds.append(size)
        return super().read(size)


class Wire:
    def __init__(self, *responses: JsonValue | bytes | HTTPError | URLError) -> None:
        self.responses = list(responses)
        self.requests: list[Request] = []
        self.read_bounds: list[int | None] = []
        self.final_url: str | None = None

    def open(self, request: Request, timeout: float) -> Response:
        self.requests.append(request)
        value = self.responses.pop(0)
        if isinstance(value, (HTTPError, URLError)):
            raise value
        body = value if isinstance(value, bytes) else json.dumps(value).encode()
        return Response(body, self.final_url or request.full_url, self.read_bounds)


class GroundingToolsTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = GroundingConfig(
            allowed_domains=["docs.example.org"],
            trusted_url_prefixes=["https://github.com/example/tool"],
        )
        environment = patch.dict(
            "os.environ",
            {"KEENABLE_API_KEY": "fixture-keenable-key", "CONTEXT7_API_KEY": "fixture-context-key"},
        )
        environment.start()
        self.addCleanup(environment.stop)

    def query(self, action: str = "keenable_search", **fields: JsonValue) -> DocumentationQuery:
        return DocumentationQuery.model_validate(
            {
                "action": action,
                "query": "volume removal behavior",
                "candidate_ids": ["candidate-1"],
                **fields,
            }
        )

    def broker(self, config: GroundingConfig | None = None, name: str = "out") -> GroundingBroker:
        return GroundingBroker(config or self.config, self.root / name, self.root / "cache")

    def wire(self, *responses: JsonValue | bytes | HTTPError | URLError) -> Wire:
        wire = Wire(*responses)
        boundary = patch("codebench_pipeline.grounding_tools.build_opener", return_value=wire)
        boundary.start()
        self.addCleanup(boundary.stop)
        return wire

    def search_page(self, url: str = "https://docs.example.org/volumes") -> JsonValue:
        return {
            "query": "volume removal behavior",
            "mode": "pro",
            "results": [{"url": url, "title": "Volumes", "snippet": "Discovery only"}],
            "usage": {"credits": 1},
        }

    def context(self, text: str = "Removing a volume deletes its contents.") -> JsonValue:
        return {
            "codeSnippets": [
                {
                    "codeTitle": "Remove volume",
                    "codeDescription": "Generated provider explanation",
                    "codeId": "https://github.com/example/tool/blob/main/volumes.md",
                    "codeList": [{"language": "shell", "code": "tool volume rm sample"}],
                    "generationDate": "2026-11-01",
                    "rank": 1,
                }
            ],
            "infoSnippets": [
                {
                    "pageId": "https://docs.example.org/volumes",
                    "breadcrumb": "Volumes",
                    "content": text,
                    "rank": 1,
                },
                {"pageId": "https://untrusted.example/volumes", "content": "Not authoritative", "rank": 2},
            ],
            "version": "2.1",
        }

    def test_configuration_and_action_fields_reject_unsafe_contracts(self) -> None:
        with self.assertRaises(ValidationError):
            GroundingConfig()
        for fields in (
            {"query": "x" * 501},
            {"candidate_ids": []},
            {"action": "context7_context"},
            {"action": "keenable_fetch"},
            {"library_id": "/example/tool"},
        ):
            with self.subTest(fields=fields), self.assertRaises(ValidationError):
                self.query(**fields)

    def test_grounding_configuration_supports_nested_dagster_schema(self) -> None:
        annotation = AnnotationConfig(batch=BatchConfig(grounding=self.config))
        AnnotationConfig.to_config_schema()
        payload = RunConfig({"label": annotation}).to_config_dict()
        grounding = payload["ops"]["label"]["config"]["batch"]["grounding"]
        self.assertEqual(grounding["allowed_domains"], ["docs.example.org"])
        self.assertEqual(grounding["revision"], "1")

    def test_search_is_discovery_and_cache_coalesces_candidate_ids(self) -> None:
        wire = self.wire(self.search_page())
        broker = self.broker()
        first = broker.execute(self.query())
        second = broker.execute(self.query(candidate_ids=["candidate-2"], query=" volume removal behavior "))
        self.assertEqual(first.documents, [])
        self.assertEqual([hit.url for hit in first.discovery_results], ["https://docs.example.org/volumes"])
        self.assertEqual((first.provider_calls, second.provider_calls, broker.provider_calls), (1, 0, 1))
        self.assertTrue(second.cache_hit)
        self.assertEqual(len(wire.requests), 1)
        receipts = [json.loads(path.read_text()) for path in (self.root / "out/receipts").glob("*.json")]
        self.assertEqual(
            {tuple(receipt["query"]["candidate_ids"]) for receipt in receipts},
            {("candidate-1",), ("candidate-2",)},
        )
        self.assertEqual(len({receipt["request_sha256"] for receipt in receipts}), 1)

    def test_keenable_fetch_uses_search_authorization_and_original_content(self) -> None:
        url = "https://docs.example.org/volumes"
        wire = self.wire(
            self.search_page(),
            {"url": url, "content": "Original page", "truncated": True, "total_chars": 50000},
        )
        broker = self.broker()
        broker.execute(self.query())
        result = broker.execute(self.query("keenable_fetch", url=url))
        self.assertIsNone(result.error)
        self.assertEqual(result.documents[0].text, "Original page")
        self.assertTrue(result.documents[0].metadata.truncated)
        self.assertEqual(result.documents[0].url, url)
        search, fetch = wire.requests
        self.assertEqual(search.get_method(), "POST")
        self.assertEqual(
            json.loads(search.data or b"{}"),
            {"query": "volume removal behavior", "max_results": 3, "snippet_max_length": 1500},
        )
        self.assertEqual(search.get_header("X-api-key"), "fixture-keenable-key")
        self.assertEqual(
            parse_qs(urlsplit(fetch.full_url).query), {"url": [url], "max_chars": ["12000"], "live": ["true"]}
        )
        self.assertEqual(fetch.get_method(), "GET")
        self.assertNotIn("prompt", fetch.full_url)

    def test_unseen_unsafe_and_prefix_sibling_fetches_make_no_requests(self) -> None:
        broker = self.broker()
        wire = self.wire(self.search_page("https://github.com/example/tool-evil/page"))
        broker.execute(self.query())
        for url in (
            "https://docs.example.org/unseen",
            "https://github.com/example/tool-evil/page",
            "https://docs.example.org.evil/page",
            "http://docs.example.org/page",
            "file:///etc/passwd",
            "https://user:password@docs.example.org/page",
            "https://docs.example.org:8443/page",
            "https://github.com/example/tool/../elsewhere",
            "https://github.com/example/tool/%2e%2e/elsewhere",
        ):
            with self.subTest(url=url):
                result = broker.execute(self.query("keenable_fetch", url=url))
                self.assertEqual(result.documents, [])
                self.assertIsNotNone(result.error)
                self.assertEqual(result.provider_calls, 0)
        self.assertEqual(len(wire.requests), 1)

    def test_context_requires_selected_returned_id_and_attributes_each_source(self) -> None:
        wire = self.wire(
            {
                "results": [
                    {"id": "/unrelated/tool", "title": "Other"},
                    {"id": "/example/tool", "title": "Tool", "versions": ["2.1"]},
                ]
            },
            self.context(),
        )
        broker = self.broker()
        denied = broker.execute(self.query("context7_context", library_id="/example/tool"))
        self.assertEqual(denied.error.code if denied.error else None, "unresolved_library")
        search = broker.execute(self.query("context7_search", library_name="tool"))
        self.assertEqual(
            [hit.library_id for hit in search.discovery_results], ["/unrelated/tool", "/example/tool"]
        )
        self.assertEqual(search.documents, [])
        result = broker.execute(
            self.query("context7_context", library_id="/example/tool", requested_version="2.1")
        )
        self.assertIsNone(result.error)
        self.assertEqual(
            [doc.text for doc in result.documents],
            ["Removing a volume deletes its contents.", "tool volume rm sample"],
        )
        self.assertEqual(
            [doc.url for doc in result.documents],
            ["https://docs.example.org/volumes", "https://github.com/example/tool/blob/main/volumes.md"],
        )
        self.assertEqual(result.documents[1].metadata.provider_summary, "Generated provider explanation")
        self.assertEqual(result.documents[0].metadata.requested_version, "2.1")
        self.assertEqual(result.documents[0].metadata.returned_version, "2.1")
        self.assertEqual(
            parse_qs(urlsplit(wire.requests[0].full_url).query),
            {"libraryName": ["tool"], "query": ["volume removal behavior"], "fast": ["true"]},
        )
        self.assertEqual(
            parse_qs(urlsplit(wire.requests[1].full_url).query),
            {
                "libraryId": ["/example/tool"],
                "query": ["volume removal behavior"],
                "type": ["json"],
                "fast": ["true"],
            },
        )
        self.assertEqual(wire.requests[1].get_header("Authorization"), "Bearer fixture-context-key")
        with self.assertRaises(ValidationError):
            result.documents[0].metadata.kind = "code"

    def test_pinned_library_filters_each_returned_source_by_generic_trust(self) -> None:
        wire = self.wire(self.context(), self.context(), self.context())
        for domains, prefixes, expected in (
            (["docs.example.org"], [], ["https://docs.example.org/volumes"]),
            (
                [],
                ["https://github.com/example/tool"],
                ["https://github.com/example/tool/blob/main/volumes.md"],
            ),
            (["other.example.org"], [], []),
        ):
            with self.subTest(domains=domains, prefixes=prefixes):
                config = GroundingConfig(
                    allowed_domains=domains,
                    trusted_url_prefixes=prefixes,
                    pinned_library_ids=["/example/tool"],
                )
                result = self.broker(config).execute(
                    self.query("context7_context", library_id="/example/tool")
                )
                self.assertIsNone(result.error)
                self.assertEqual([doc.url for doc in result.documents], expected)
        self.assertEqual(len(wire.requests), 3)

    def test_http_failures_consume_budget_without_retaining_error_secrets(self) -> None:
        headers = Message()
        headers["Retry-After"] = "7"
        error = HTTPError(
            "https://provider.example", 429, "fixture-keenable-key", headers, io.BytesIO(b"private body")
        )
        wire = self.wire(error, URLError("fixture-context-key"))
        broker = self.broker(self.config.model_copy(update={"maximum_provider_calls": 2}))
        first = broker.execute(self.query())
        duplicate = broker.execute(self.query())
        second = broker.execute(self.query(query="different operation"))
        exhausted = broker.execute(self.query(query="third operation"))
        self.assertEqual(first.error.status if first.error else None, 429)
        self.assertEqual(first.error.retry_after if first.error else None, "7")
        self.assertEqual(second.error.code if second.error else None, "transport_error")
        self.assertEqual(exhausted.error.code if exhausted.error else None, "provider_budget_exhausted")
        self.assertEqual((broker.provider_calls, duplicate.provider_calls, len(wire.requests)), (2, 0, 2))
        self.assertEqual(list((self.root / "cache").glob("*.json")), [])
        for path in self.root.rglob("*.json"):
            text = path.read_text()
            self.assertNotIn("fixture-keenable-key", text)
            self.assertNotIn("fixture-context-key", text)
            self.assertNotIn("private body", text)

    def test_raw_response_and_query_receipts_are_hashed_without_auth(self) -> None:
        self.wire(self.search_page())
        self.broker().execute(self.query())
        for path in (self.root / "out/responses").glob("*.json"):
            self.assertEqual(path.stem, hashlib.sha256(path.read_bytes()).hexdigest())
        receipts = list((self.root / "out/receipts").glob("*.json"))
        self.assertEqual(len(receipts), 1)
        receipt = json.loads(receipts[0].read_text())
        self.assertTrue((self.root / "out/responses" / (receipt["response_sha256"] + ".json")).is_file())
        for path in self.root.rglob("*.json"):
            text = path.read_text()
            self.assertNotIn("fixture-keenable-key", text)
            self.assertNotIn("fixture-context-key", text)
            self.assertNotIn("Authorization", text)
            self.assertNotIn("X-API-Key", text)

    def test_sensitive_and_malformed_responses_never_enter_evidence_or_cache(self) -> None:
        for index, body in enumerate(
            (
                b'{"results":[{"url":"https://docs.example.org/volumes","snippet":"AKIAABCDEFGHIJKLMNOP"}]}',
                b'{"results":[{"snippet":"fixture-keenable-key"}]}',
                b'{"results":[{"snippet":"fixture-keenable-\\u006bey"}]}',
                b"not JSON",
                b"\xff",
                b'{"results":"wrong shape"}',
            )
        ):
            with self.subTest(body=body):
                self.wire(body)
                result = self.broker(name=str(index)).execute(self.query())
                self.assertIsNotNone(result.error)
                self.assertEqual(result.provider_calls, 1)
                self.assertEqual(list((self.root / str(index) / "responses").glob("*")), [])
                self.assertEqual(list((self.root / "cache").glob("*")), [])

    def test_response_byte_bounds_apply_before_json_parsing(self) -> None:
        wire = self.wire(b"x" * 51)
        broker = self.broker(self.config.model_copy(update={"maximum_response_bytes": 50}))
        result = broker.execute(self.query())
        self.assertEqual(result.error.code if result.error else None, "response_too_large")
        self.assertEqual(wire.read_bounds, [51])
        self.assertEqual(list((self.root / "out/responses").glob("*")), [])

    def test_total_response_budget_counts_failed_response_bytes(self) -> None:
        wire = self.wire(b"x" * 11)
        broker = self.broker(self.config.model_copy(update={"maximum_total_response_bytes": 10}))
        first = broker.execute(self.query())
        second = broker.execute(self.query(query="other operation"))
        self.assertEqual(first.error.code if first.error else None, "response_too_large")
        self.assertEqual(second.error.code if second.error else None, "response_budget_exhausted")
        self.assertEqual(broker.provider_calls, 1)
        self.assertEqual(wire.read_bounds, [11])

    def test_evidence_budget_includes_metadata_and_multibyte_content(self) -> None:
        self.wire(self.context("\u00e9" * 20))
        config = self.config.model_copy(
            update={"pinned_library_ids": ["/example/tool"], "maximum_evidence_bytes": 30}
        )
        result = self.broker(config).execute(self.query("context7_context", library_id="/example/tool"))
        self.assertEqual(result.error.code if result.error else None, "evidence_budget_exhausted")
        self.assertEqual(result.documents, [])
        self.assertEqual(list((self.root / "out/documents").glob("*")), [])

    def test_persistent_cache_restores_discovery_permissions_without_http(self) -> None:
        url = "https://docs.example.org/volumes"
        wire = self.wire(self.search_page(), {"url": url, "content": "Original"})
        first = self.broker(name="first")
        first.execute(self.query())
        original = first.execute(self.query("keenable_fetch", url=url))
        second = self.broker(name="second")
        discovery = second.execute(self.query())
        cached = second.execute(self.query("keenable_fetch", url=url))
        self.assertEqual((second.provider_calls, cached.provider_calls), (0, 0))
        self.assertTrue(discovery.cache_hit)
        self.assertTrue(cached.cache_hit)
        self.assertEqual(cached.documents, original.documents)
        self.assertEqual(len(wire.requests), 2)
        self.assertEqual(len(list((self.root / "second/responses").glob("*"))), 2)

    def test_revision_refresh_keeps_identity_for_same_text_not_new_timestamps(self) -> None:
        self.wire(self.context(), self.context(), self.context("Changed original content."))
        query = self.query("context7_context", library_id="/example/tool")
        documents = []
        with patch("codebench_pipeline.grounding_tools.datetime") as clock:
            for index, revision in enumerate(("1", "2", "3")):
                clock.now.return_value = datetime(2026, 11, 17 + index, tzinfo=UTC)
                config = self.config.model_copy(
                    update={"revision": revision, "pinned_library_ids": ["/example/tool"]}
                )
                result = self.broker(config).execute(query)
                self.assertEqual(result.provider_calls, 1)
                documents.append(result.documents[0])
        self.assertEqual(documents[0].evidence_id, documents[1].evidence_id)
        self.assertNotEqual(documents[0].retrieved_at, documents[1].retrieved_at)
        self.assertNotEqual(documents[1].evidence_id, documents[2].evidence_id)
        self.assertEqual(
            documents[0].content_sha256,
            hashlib.sha256(b"Removing a volume deletes its contents.").hexdigest(),
        )
        self.assertEqual(len(list((self.root / "out/documents").glob("*"))), 6)

    def test_untrusted_provider_fetch_redirect_is_not_evidence(self) -> None:
        self.wire(self.search_page(), {"url": "https://evil.example/page", "content": "Redirected"})
        broker = self.broker()
        broker.execute(self.query())
        result = broker.execute(self.query("keenable_fetch", url="https://docs.example.org/volumes"))
        self.assertEqual(result.error.code if result.error else None, "untrusted_final_url")
        self.assertEqual(result.documents, [])
        self.assertEqual(len(list((self.root / "out/responses").glob("*"))), 1)

    def test_changed_served_version_invalidates_evidence_identity_with_unchanged_text(self) -> None:
        first_payload = self.context()
        second_payload = self.context()
        assert isinstance(second_payload, dict)
        second_payload["version"] = "2.2"
        self.wire(first_payload, second_payload)
        query = self.query("context7_context", library_id="/example/tool")
        first = self.broker(
            self.config.model_copy(update={"pinned_library_ids": ["/example/tool"]}), name="v1"
        ).execute(query)
        second = self.broker(
            self.config.model_copy(update={"pinned_library_ids": ["/example/tool"], "revision": "2"}),
            name="v2",
        ).execute(query)
        self.assertEqual(first.documents[0].text, second.documents[0].text)
        self.assertNotEqual(first.documents[0].evidence_id, second.documents[0].evidence_id)

    def test_http_redirect_handler_denies_forwarding_auth(self) -> None:
        broker = self.broker()

        def build(handler: HTTPRedirectHandler) -> Wire:
            request = Request(
                "https://api.keenable.ai/v1/search", headers={"X-API-Key": "fixture-keenable-key"}
            )
            self.assertIsNone(
                handler.redirect_request(
                    request, io.BytesIO(), 302, "redirect", Message(), "https://evil.example"
                )
            )
            return Wire(HTTPError(request.full_url, 302, "redirect", Message(), io.BytesIO()))

        with patch("codebench_pipeline.grounding_tools.build_opener", side_effect=build):
            result = broker.execute(self.query())
        self.assertEqual(result.error.status if result.error else None, 302)
        self.assertEqual(result.provider_calls, 1)

    def test_missing_key_and_sensitive_query_do_not_consume_budget(self) -> None:
        broker = self.broker()
        wire = self.wire()
        with patch.dict("os.environ", {"KEENABLE_API_KEY": ""}):
            missing = broker.execute(self.query())
        sensitive = broker.execute(self.query(query="api_key=abcdefghijklmnop012345"))
        self.assertEqual(missing.error.code if missing.error else None, "missing_api_key")
        self.assertEqual(sensitive.error.code if sensitive.error else None, "sensitive_query")
        self.assertEqual(broker.provider_calls, 0)
        self.assertEqual(wire.requests, [])
        for path in self.root.rglob("*.json"):
            self.assertNotIn("abcdefghijklmnop012345", path.read_text())

    def test_query_history_preserves_candidate_attribution_on_cached_evidence(self) -> None:
        self.wire(self.context())
        config = self.config.model_copy(update={"pinned_library_ids": ["/example/tool"]})
        broker = self.broker(config)
        first = broker.execute(self.query("context7_context", library_id="/example/tool"))
        second_query = self.query(
            "context7_context", library_id="/example/tool", candidate_ids=["candidate-2"]
        )
        second = broker.execute(second_query)
        history = broker.query_history
        self.assertEqual([item.query.candidate_ids for item in history], [["candidate-1"], ["candidate-2"]])
        self.assertEqual([item.provider_calls for item in history], [1, 0])
        self.assertEqual(history[0].evidence_ids, tuple(doc.evidence_id for doc in first.documents))
        self.assertEqual(history[1].evidence_ids, history[0].evidence_ids)
        self.assertEqual(history[1].response_sha256, history[0].response_sha256)
        self.assertTrue(second.cache_hit)
        history[0].query.candidate_ids.clear()
        second_query.candidate_ids.clear()
        self.assertEqual(broker.query_history[0].query.candidate_ids, ["candidate-1"])
        self.assertEqual(broker.query_history[1].query.candidate_ids, ["candidate-2"])

    def test_cache_integrity_failure_returns_error_without_overwriting(self) -> None:
        wire = self.wire(self.search_page())
        self.broker(name="first").execute(self.query())
        path = next((self.root / "cache").glob("*.json"))
        body = json.loads(path.read_text())
        body["response_sha256"] = "0" * 64
        changed = json.dumps(body)
        path.write_text(changed)
        result = self.broker(name="second").execute(self.query())
        self.assertEqual(result.error.code if result.error else None, "invalid_cache")
        self.assertEqual(result.provider_calls, 0)
        self.assertEqual(path.read_text(), changed)
        self.assertEqual(len(wire.requests), 1)

    def test_failed_provider_result_is_not_reused_by_new_batch(self) -> None:
        wire = self.wire(URLError("unavailable"), self.search_page())
        first = self.broker(name="first").execute(self.query())
        second = self.broker(name="second").execute(self.query())
        self.assertEqual(first.error.code if first.error else None, "transport_error")
        self.assertIsNone(second.error)
        self.assertFalse(second.cache_hit)
        self.assertEqual(second.provider_calls, 1)
        self.assertEqual(len(wire.requests), 2)
