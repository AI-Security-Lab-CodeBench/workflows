import json
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from test_keenable import PEER, result

from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_policy import (
    CallBudget,
    CurrentKeenableDeclaration,
    KeenableError,
    KeenableSourceAccess,
    UnrestrictedSearch,
    digest,
)
from codebench_pipeline.keenable_receipts import CallReceipt, CaptureLock
from codebench_pipeline.keenable_tables import (
    CANDIDATE_RECEIPTS,
    PROVIDER_RECEIPT,
    provider_adapters,
    provider_candidates,
    provider_catalog,
    provider_documents,
)
from codebench_pipeline.keenable_transport import DirectResponse, Request

URL = "https://unlisted.example/reference"
DENIED = "https://unlicensed.example/reference"
NONPUBLIC = "https://127.0.0.1/docs"
LICENSE = b'<link rel="license" href="https://creativecommons.org/licenses/by/4.0/">'
COMMAND = b"kubectl get deployments -n platform"
MAIN = (
    "<p>Récapitulatif</p>".encode()
    + b'<!-- <pre>$ kubectl get secrets</pre> --><pre><code class="language-bash">\n'
    + COMMAND
    + b"\n</code></pre>"
)
HEAD = b'<head><meta name="author" content="Example Authors">' + LICENSE + b"</head>"
BODY = HEAD + b"<main>" + MAIN + b"</main><footer>Unretained footer</footer>"


class UnrestrictedCollectionTests(TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.declaration = CurrentKeenableDeclaration(
            schema_version="3",
            dataset_id="dataset",
            generation="unrestricted-fixture",
            budget=CallBudget(search_calls=1, origin_calls=3),
            search_plan=UnrestrictedSearch(mode="fixed", max_queries=1, unrestricted_cases=("workloads",)),
            sources=(),
        )
        self.discover = KeenableCollection(self.root, self.declaration)
        self.collect = KeenableCollection(self.root, KeenableSourceAccess.project(self.declaration))

    def test_discovered_rows_require_acquired_bytes_with_separate_rights_evidence(self) -> None:
        def origin(request: Request) -> DirectResponse:
            if request.payload is not None:
                query = json.loads(request.payload)["query"]
                if "site:" in query:
                    return result()
                return result(DENIED, NONPUBLIC, URL, "https://over-budget.example/ignored")
            if request.url == NONPUBLIC:
                raise KeenableError("dns_nonpublic_address")
            if request.url == DENIED:
                return DirectResponse(b"<main><!--" + LICENSE + b"-->" + MAIN + b"</main>", PEER)
            if request.url == URL:
                return DirectResponse(BODY, PEER)
            raise AssertionError(f"Unexpected outbound request: {request.url}")

        with patch("codebench_pipeline.keenable.request_direct", side_effect=origin):
            catalog = provider_catalog(self.discover, "fixture")
            documents = provider_documents(self.collect, catalog, "fixture")
            candidates = provider_candidates(
                self.collect, documents, provider_adapters(self.collect, documents)
            )
        self.assertEqual(catalog.decision.tolist(), ["selected", "selected", "selected", "exclude"])
        self.assertEqual(catalog.rationale.tolist()[-1], "selection_budget")
        self.assertEqual(documents.content.tolist(), [MAIN.decode()])
        self.assertEqual(documents.provenance_type.tolist(), ["web_search"])
        self.assertEqual(candidates.command.tolist(), [COMMAND.decode()])
        lock = CaptureLock.model_validate_json(documents.attrs[PROVIDER_RECEIPT])
        self.assertEqual(lock.withheld, (DENIED, NONPUBLIC))
        self.assertEqual(
            [(call.target, call.reason) for call in lock.origin_calls],
            [(DENIED, "unverified_page_license"), (NONPUBLIC, "dns_nonpublic_address"), (URL, None)],
        )
        (candidate,) = CANDIDATE_RECEIPTS.validate_json(candidates.attrs[PROVIDER_RECEIPT])
        evidence = candidate.observed_license
        assert evidence is not None
        self.assertIsNone(self.declaration.authority(candidate.url, candidate.case))
        self.assertEqual(candidate.url, URL)
        self.assertEqual(evidence.notice.encode(), LICENSE)
        self.assertEqual(evidence.notice_sha256, digest(LICENSE))
        self.assertEqual(evidence.attribution, "Example Authors; unchanged excerpt")
        self.assertEqual(evidence.retained_start + candidate.start, BODY.index(COMMAND))
        self.assertEqual(MAIN[candidate.start : candidate.end], COMMAND)
        self.assertEqual(lock.pages[0].call.response_sha256, digest(BODY))
        self.assertEqual(candidate.body_sha256, digest(MAIN))
        self.assertEqual(candidate.span_sha256, digest(COMMAND))
        self.assertEqual(candidates.transformations.tolist(), [["licensed-html-main-v1"]])

    def test_partial_and_published_replay_detect_stored_content_or_license_tampering(self) -> None:
        with patch(
            "codebench_pipeline.keenable.request_direct",
            side_effect=(result(URL), DirectResponse(BODY, PEER)),
        ) as wire:
            catalog = self.discover.discover_with_credential("fixture")
            with (
                patch("codebench_pipeline.keenable.CaptureLock", side_effect=InterruptedError),
                self.assertRaises(InterruptedError),
            ):
                self.collect.capture(catalog)
            self.assertEqual(wire.call_count, 2)
        path = self.root / "dataset" / "keenable" / self.declaration.generation / "generation.sqlite3"
        with closing(sqlite3.connect(path)) as db:
            receipt_json, original = db.execute(
                "SELECT receipt, payload FROM call WHERE kind = 'origin'"
            ).fetchone()
            receipt = CallReceipt.model_validate_json(receipt_json)
            invalid_notice = LICENSE.replace(b'rel="license"', b'rel="invalid"')
            wrong_license = original.replace(b'rel=\\"license\\"', b'rel=\\"invalid\\"').replace(
                digest(LICENSE).encode(), digest(invalid_notice).encode()
            )
            for tampered, receipt_wire, reason in (
                (
                    original.replace(COMMAND, b"kubectl get services"),
                    receipt_json,
                    "retained_payload_integrity",
                ),
                (b'{"unrecognized":true}', None, "origin_body_integrity"),
                (wrong_license, None, "origin_body_integrity"),
            ):
                if receipt_wire is None:
                    receipt_wire = receipt.model_copy(
                        update={"retained_sha256": digest(tampered)}
                    ).model_dump_json()
                with db:
                    db.execute(
                        "UPDATE call SET payload=?, receipt=? WHERE kind='origin'", (tampered, receipt_wire)
                    )
                with self.assertRaisesRegex(KeenableError, reason):
                    self.collect.capture(catalog)
            with db:
                db.execute(
                    "UPDATE call SET payload=?, receipt=? WHERE kind='origin'", (original, receipt_json)
                )
            with patch("codebench_pipeline.keenable.request_direct") as replay_wire:
                lock = self.collect.capture(catalog)
                self.assertEqual(self.collect.read_body(lock, URL), MAIN)
            replay_wire.assert_not_called()
            modified = original.replace(b"Example Authors", b"Altered Authors")
            replaced = receipt.model_copy(update={"retained_sha256": digest(modified)}).model_dump_json()
            with db:
                db.execute("UPDATE call SET payload=?, receipt=? WHERE kind='origin'", (modified, replaced))
            with self.assertRaisesRegex(KeenableError, "origin_body_integrity"):
                self.collect.read_body(lock, URL)
