import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from codebench_pipeline.keenable import KeenableCollection, KeenableError, load_declaration
from codebench_pipeline.keenable_policy import (
    CASES,
    PAGE_CASES,
    QUERIES,
    AcquisitionBudget,
    KeenableDeclaration,
    PagePermission,
    PublicLicense,
    ReviewedSpan,
    SourcePermission,
)
from codebench_pipeline.keenable_receipts import CallReceipt
from codebench_pipeline.keenable_transport import DirectResponse, Request

DECLARATION = Path("business-configs/categories/cat3/keenable/declaration.json")
PLAN = "https://developer.hashicorp.com/terraform/cli/commands/plan"
VALIDATE = "https://developer.hashicorp.com/terraform/cli/commands/validate"
DOCKER = "https://docs.docker.com/reference/cli/docker/container/ls/"
BODY = b"<pre>terraform plan</pre>"
PEER = "1.1.1.1"
SOURCE_BODY = b"<p>Terraform plan previews proposed changes.</p><pre>terraform plan</pre>"


def result(*urls: str) -> DirectResponse:
    return DirectResponse(
        json.dumps({"results": [{"url": url, "snippet": "not evidence"} for url in urls]}).encode(), PEER
    )


def permission(url: str = PLAN, body: bytes = BODY, literal: bytes = b"terraform plan") -> PagePermission:
    start = body.index(literal)
    return PagePermission(
        url=url,
        origin_id="terraform",
        content_sha256=hashlib.sha256(body).hexdigest(),
        media_type="text/html",
        rights_evidence_sha256="1" * 64,
        steward_decision="fixture-review",
        valid_until=datetime(2099, 1, 1, tzinfo=UTC),
        attribution="Synthetic docs fixture",
        permitted_use="fetch_retain_extract",
        spans=(ReviewedSpan(start=start, end=start + len(literal)),),
    )


def source_declaration(*, extract: bool = True) -> KeenableDeclaration:
    active = load_declaration(DECLARATION)
    return KeenableDeclaration(
        dataset=active.dataset,
        generation="source-scope-fixture",
        generation_review="fixture-review",
        cases=CASES,
        revision="2",
        sources=(
            SourcePermission(
                origin_id="terraform",
                url_prefix="https://developer.hashicorp.com/terraform/cli/commands/",
                cases=("plan",),
                media_type="text/html",
                rights_evidence_sha256="1" * 64,
                steward_decision="fixture-rights",
                valid_until=datetime(2099, 1, 1, tzinfo=UTC),
                attribution="Synthetic Terraform documentation",
                permitted_use="fetch_retain_extract_quote" if extract else "fetch_retain_quote",
            ),
        ),
    )


class KeenableCollectionTests(unittest.TestCase):
    def setUp(self) -> None:
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.root = Path(root.name)
        current = load_declaration(DECLARATION)
        self.declaration = KeenableDeclaration(
            dataset=current.dataset,
            generation="legacy-fixture",
            generation_review="fixture-review",
            cases=CASES,
        )
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {"KEENABLE_API_KEY": "fixture-key"}).start()

    def collection(self, *permissions: PagePermission) -> KeenableCollection:
        return KeenableCollection(self.root, self.declaration.model_copy(update={"permissions": permissions}))

    def wire(self, *outcomes: DirectResponse | BaseException) -> MagicMock:
        return patch("codebench_pipeline.keenable.request_direct", side_effect=outcomes).start()

    def persisted(self) -> tuple[list[CallReceipt], list[bytes], int]:
        path = next(self.root.rglob("generation.sqlite3"))
        with closing(sqlite3.connect(path)) as db:
            rows = db.execute("SELECT receipt, payload FROM call").fetchall()
            (lock,) = db.execute("SELECT COUNT(*) FROM artifact WHERE name = 'lock'").fetchone()
        return (
            [CallReceipt.model_validate_json(raw) for raw, _ in rows],
            [payload for _, payload in rows if payload is not None],
            lock,
        )

    def test_catalog_bounds_round_robin_and_discards_unsafe_leads(self) -> None:
        unsafe = PLAN + "?token=fixture-key"
        provider = self.wire(
            result(
                PLAN + "#usage",
                PLAN.replace("hashicorp", "HASHICORP"),
                unsafe,
                VALIDATE,
                "https://developer.hashicorp.com/terraform/cli/commands/other",
                VALIDATE,
            ),
            result(),
            result(DOCKER),
            result(),
            result(),
        )
        branch = self.collection(permission())
        catalog = branch.discover()
        self.assertEqual([item.lead.url for item in catalog.selected], [PLAN, DOCKER, VALIDATE])
        self.assertEqual(
            [lead.excluded for lead in catalog.leads if lead.excluded],
            ["duplicate", "invalid_url", "outside_case"],
        )
        self.assertEqual([item.state for item in catalog.selected], ["pending", "withheld", "withheld"])
        self.assertNotIn("fixture-key", catalog.model_dump_json())
        self.assertNotIn("not evidence", catalog.model_dump_json())
        self.assertEqual(provider.call_count, 5)
        for call, case in zip(provider.call_args_list, CASES, strict=True):
            request = call.args[0]
            self.assertEqual(json.loads(request.payload)["query"], QUERIES[case])
            self.assertEqual(json.loads(request.payload)["max_results"], 5)

    def test_capture_replays_pinned_origin_bytes_and_literal_offsets_without_network(self) -> None:
        lead = result(PLAN)
        provider = self.wire(lead, result(), result(), result(), result(), DirectResponse(BODY, PEER))
        branch = self.collection(permission())
        catalog = branch.discover()
        lock = branch.capture(catalog)
        reopened = self.collection(permission())
        candidates = reopened.candidates(lock, reopened.adapters(lock))
        self.assertEqual(reopened.capture(reopened.discover()), lock)
        self.assertEqual(reopened.read_body(lock, PLAN), BODY)
        self.assertEqual(provider.call_count, 6)
        self.assertEqual(provider.call_args.args, (Request(PLAN, "text/html"),))
        self.assertEqual(catalog.searches[0].response_sha256, hashlib.sha256(lead.body).hexdigest())
        (candidate,) = candidates
        self.assertEqual((candidate.command, candidate.start, candidate.end), ("terraform plan", 5, 19))
        self.assertEqual(candidate.body_sha256, hashlib.sha256(BODY).hexdigest())
        self.assertEqual(candidate.span_sha256, hashlib.sha256(b"terraform plan").hexdigest())

    def test_source_scope_admits_new_pages_without_a_page_permission_or_predeclared_hash(self) -> None:
        new_page = PLAN + "-reference"
        outside = PLAN.replace("/commands/", "/commands-extra/")
        wire = self.wire(
            result(outside, new_page, new_page + "?token=fixture-key"),
            DirectResponse(SOURCE_BODY, PEER),
        )
        collection = KeenableCollection(self.root, source_declaration())
        catalog = collection.discover()
        lock = collection.capture(catalog)
        (page,) = lock.pages
        self.assertEqual((page.url, page.body_sha256), (new_page, hashlib.sha256(SOURCE_BODY).hexdigest()))
        (candidate,) = collection.candidates(lock, collection.adapters(lock))
        self.assertEqual(SOURCE_BODY[candidate.start : candidate.end], b"terraform plan")
        self.assertEqual(collection.read_bodies(lock), {new_page: SOURCE_BODY})
        self.assertEqual(wire.call_count, 2)
        self.assertEqual(wire.call_args.args, (Request(new_page, "text/html"),))

    def test_quotable_source_cannot_emit_commands(self) -> None:
        self.wire(result(PLAN), DirectResponse(SOURCE_BODY, PEER))
        collection = KeenableCollection(self.root, source_declaration(extract=False))
        lock = collection.capture(collection.discover())
        self.assertEqual(collection.read_body(lock, PLAN), SOURCE_BODY)
        self.assertEqual(collection.candidates(lock, collection.adapters(lock)), ())

    def test_exact_sources_share_one_pinned_license_reservation_and_reject_corruption(self) -> None:
        commit = "6" * 40
        origin = f"https://raw.githubusercontent.com/kubernetes/website/{commit}/"
        license_url = origin + "LICENSE"
        rights = b"Copyright and permission notice fixture"
        rights_hash = hashlib.sha256(rights).hexdigest()
        document_urls = (origin + "docs/plan.md", origin + "docs/notes/validate.md")
        sources = tuple(
            SourcePermission(
                origin_id=f"scope-{i}",
                url_prefix=prefix,
                exact_urls=(url,),
                cases=("plan",),
                media_type="text/plain",
                rights_evidence_sha256=rights_hash,
                public_license=PublicLicense(
                    identifier="CC-BY-4.0",
                    evidence_url=license_url,
                    license_url="https://creativecommons.org/licenses/by/4.0/",
                ),
                valid_until=datetime(2099, 1, 1, tzinfo=UTC),
                attribution="Synthetic fixture",
                permitted_use="fetch_retain_quote",
            )
            for i, (prefix, url) in enumerate(zip((origin + "docs/", origin + "docs/notes/"), document_urls))
        )
        active = source_declaration()
        values = active.model_dump()
        values.update(sources=sources, budget=AcquisitionBudget(origin_calls=2, search_calls=0))
        with self.assertRaisesRegex(ValidationError, "deduplicated rights files"):
            KeenableDeclaration.model_validate(values)
        values["budget"] = AcquisitionBudget(origin_calls=3, search_calls=0)
        collection = KeenableCollection(self.root, KeenableDeclaration.model_validate(values))
        wire = self.wire(DirectResponse(rights, PEER), DirectResponse(BODY, PEER), DirectResponse(BODY, PEER))
        catalog = collection.discover()
        lock = collection.capture(catalog)
        self.assertEqual(catalog.searches, ())
        self.assertEqual(set(collection.read_bodies(lock)), set(document_urls))
        self.assertEqual([call.args[0].url for call in wire.call_args_list], [license_url, *document_urls])
        (rights_receipt,) = lock.rights_calls
        path = next(self.root.rglob("generation.sqlite3"))
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("UPDATE call SET payload = ? WHERE slot = ?", (b"tampered", rights_receipt.slot))
        with self.assertRaisesRegex(KeenableError, "retained_payload_integrity"):
            collection.read_bodies(lock)
        self.assertEqual(wire.call_count, 3)

    def test_public_html_without_current_notice_is_withheld(self) -> None:
        declaration = load_declaration(DECLARATION)
        source = declaration.sources[0].model_copy(
            update={"exact_urls": (declaration.sources[0].exact_urls[0],)}
        )
        collection = KeenableCollection(
            self.root, declaration.model_copy(update={"sources": (source,), "search_plan": None})
        )
        self.wire(DirectResponse(b"<p>Original notice no longer present.</p>", PEER))
        lock = collection.capture(collection.discover())
        self.assertEqual(lock.pages, ())
        self.assertEqual(lock.withheld, source.exact_urls)
        self.assertEqual(lock.origin_calls[0].reason, "license_notice_missing")

    def test_a_lead_without_permission_cannot_be_captured_or_read(self) -> None:
        provider = self.wire(result(PLAN), result(), result(), result(), result())
        branch = self.collection()
        lock = branch.capture(branch.discover())
        self.assertEqual(lock.withheld, (PLAN,))
        self.assertEqual(lock.origin_calls, ())
        with self.assertRaisesRegex(KeenableError, "origin_not_captured"):
            branch.read_body(lock, PLAN)
        self.assertEqual(provider.call_count, 5)

    def test_selected_pin_failure_preserves_prior_receipts_and_forbids_partial_lock(self) -> None:
        other = b"<pre>terraform validate</pre>"
        provider = self.wire(
            result(PLAN, VALIDATE),
            result(),
            result(),
            result(),
            result(),
            DirectResponse(BODY, PEER),
            DirectResponse(b"drift", PEER),
        )
        branch = self.collection(permission(), permission(VALIDATE, other, b"terraform validate"))
        catalog = branch.discover()
        for _ in range(2):
            with self.assertRaisesRegex(KeenableError, "origin_pin_mismatch"):
                branch.capture(catalog)
        receipts, payloads, locks = self.persisted()
        self.assertEqual(provider.call_count, 7)
        self.assertEqual(locks, 0)
        self.assertIn(BODY, payloads)
        self.assertNotIn(b"drift", payloads)
        (failure,) = [receipt for receipt in receipts if receipt.state == "failed"]
        self.assertEqual(
            (failure.reason, failure.status, failure.reserved_bytes), ("origin_pin_mismatch", 200, 524_288)
        )

    def test_ambiguous_dispatch_is_charged_and_never_retried(self) -> None:
        provider = self.wire(KeyboardInterrupt())
        branch = self.collection()
        with self.assertRaises(KeyboardInterrupt):
            branch.discover()
        with self.assertRaisesRegex(KeenableError, "ambiguous_dispatch"):
            self.collection().discover()
        receipts, _, locks = self.persisted()
        (receipt,) = receipts
        self.assertEqual(
            (receipt.state, receipt.reason, receipt.reserved_bytes, locks),
            ("failed", "ambiguous_dispatch", 524_288, 0),
        )
        self.assertEqual(provider.call_count, 1)

    def test_reservations_refuse_combined_budget_before_origin_dispatch(self) -> None:
        provider = self.wire(result(PLAN), result(), result(), result(), result())
        branch = self.collection(permission())
        with patch("codebench_pipeline.keenable_ledger.MAX_BYTES", 5 * 524_288):
            catalog = branch.discover()
            with self.assertRaisesRegex(KeenableError, "generation_budget_exhausted"):
                branch.capture(catalog)
        self.assertEqual(provider.call_count, 5)
        self.assertEqual(self.persisted()[2], 0)

    def test_concurrent_healthy_empty_generation_publishes_one_lock(self) -> None:
        provider = self.wire(*(result() for _ in CASES))
        start = Barrier(2, timeout=5)

        def run() -> bytes:
            start.wait()
            branch = self.collection()
            return branch.capture(branch.discover()).model_dump_json().encode()

        with ThreadPoolExecutor(max_workers=2) as executor:
            first, second = executor.submit(run), executor.submit(run)
            self.assertEqual(first.result(timeout=10), second.result(timeout=10))
        self.assertEqual(provider.call_count, 5)
        self.assertEqual(self.persisted()[2], 1)

    def test_changed_authorization_requires_a_new_generation_even_on_replay(self) -> None:
        self.wire(result(PLAN), result(), result(), result(), result(), DirectResponse(BODY, PEER))
        branch = self.collection(permission())
        lock = branch.capture(branch.discover())
        with self.assertRaisesRegex(KeenableError, "generation_declaration_changed"):
            self.collection().read_body(lock, PLAN)

    def test_replay_refuses_a_corrupted_local_capture_without_refetching(self) -> None:
        provider = self.wire(result(PLAN), result(), result(), result(), result(), DirectResponse(BODY, PEER))
        branch = self.collection(permission())
        lock = branch.capture(branch.discover())
        path = next(self.root.rglob("generation.sqlite3"))
        tampered = BODY.replace(b"plan", b"copy")
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("UPDATE call SET payload = ? WHERE kind = 'origin'", (tampered,))
        with self.assertRaisesRegex(KeenableError, "retained_payload_integrity"):
            branch.read_body(lock, PLAN)
        self.assertEqual(provider.call_count, 6)

    def test_provider_record_failure_is_distinct_from_an_empty_result(self) -> None:
        provider = self.wire(DirectResponse(b'{"unexpected":[]}', PEER))
        branch = self.collection()
        with self.assertRaisesRegex(KeenableError, "provider_response_invalid"):
            branch.discover()
        with self.assertRaisesRegex(KeenableError, "provider_response_invalid"):
            branch.discover()
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(self.persisted()[0][0].status, 200)

    def test_reviewed_but_unallowlisted_literal_withholds_body(self) -> None:
        body = b"<pre>terraform apply</pre>"
        self.wire(result(PLAN), result(), result(), result(), result(), DirectResponse(body, PEER))
        branch = self.collection(permission(body=body, literal=b"terraform apply"))
        lock = branch.capture(branch.discover())
        self.assertEqual(
            (lock.pages, lock.withheld, lock.origin_calls[0].reason), ((), (PLAN,), "content_policy")
        )
        self.assertNotIn(body, self.persisted()[1])

    def test_many_safe_hits_cannot_exceed_the_distinct_page_selection_budget(self) -> None:
        by_case = {case: [url for url in PAGE_CASES if PAGE_CASES[url] == case] for case in CASES}
        self.wire(*(result(*by_case[case]) for case in CASES))
        catalog = self.collection().discover()
        self.assertEqual(len({page.lead.url for page in catalog.selected}), 15)
        self.assertEqual(
            [lead.url for lead in catalog.leads if lead.excluded == "selection_budget"],
            ["https://www.postgresql.org/docs/current/infoschema-columns.html"],
        )
