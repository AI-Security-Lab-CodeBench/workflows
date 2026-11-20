import sqlite3
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_ledger import SlotIdentity, open_ledger
from codebench_pipeline.keenable_policy import (
    CASES,
    RESPONSE_BYTES,
    AcquisitionBudget,
    KeenableDeclaration,
    KeenableError,
    KeenableSourceAccess,
    PublicLicense,
    SourcePermission,
    digest,
)
from codebench_pipeline.keenable_transport import DirectResponse, HttpStatusError

_PREFIX = "https://raw.githubusercontent.com/kubernetes/website/" + "6" * 40 + "/"
_RIGHTS = _PREFIX + "LICENSE"
_FIRST = _PREFIX + "docs/first.md"
_SECOND = _PREFIX + "docs/second.md"
_PEER = "1.1.1.1"
_LICENSE = b"Synthetic copyright and permission notice."
_BODY = b"Synthetic documentation fixture."


def identity(url: str) -> SlotIdentity:
    kind = "rights" if url == _RIGHTS else "origin"
    return SlotIdentity(
        slot=f"{kind}:{digest(url.encode())}",
        target=url,
        request_sha256=digest(f"GET\n{url}\ntext/plain".encode()),
    )


class KeenableCaptureRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.declaration = KeenableDeclaration(
            dataset="synthetic",
            generation="capture-retry",
            revision="2",
            generation_review="fixture-review",
            cases=CASES,
            sources=(
                SourcePermission(
                    origin_id="fixture",
                    url_prefix=_PREFIX + "docs/",
                    exact_urls=(_FIRST, _SECOND),
                    cases=("plan",),
                    media_type="text/plain",
                    rights_evidence_sha256=digest(_LICENSE),
                    public_license=PublicLicense(
                        identifier="CC-BY-4.0",
                        evidence_url=_RIGHTS,
                        license_url="https://creativecommons.org/licenses/by/4.0/",
                    ),
                    valid_until=datetime(2099, 1, 1, tzinfo=UTC),
                    attribution="Synthetic fixture",
                    permitted_use="fetch_retain_quote",
                ),
            ),
            budget=AcquisitionBudget(search_calls=0, origin_calls=7),
        )

    def test_saved_catalog_and_prior_captures_survive_historical_origin_timeout(self) -> None:
        declaration = KeenableDeclaration.model_validate(
            {
                **self.declaration.model_dump(),
                "sources": (self.declaration.sources[0].model_copy(update={"discover": True}),),
                "budget": AcquisitionBudget(search_calls=1, origin_calls=7),
            }
        )
        discovery = KeenableCollection(self.root, declaration)
        with patch(
            "codebench_pipeline.keenable.request_direct",
            return_value=DirectResponse(b'{"results":[]}', _PEER),
        ):
            catalog = discovery.discover_with_credential("fixture-key")
        self.assertEqual(len(catalog.searches), 1)
        access = KeenableSourceAccess.project(declaration)
        with open_ledger(self.root, access) as ledger:
            rights = ledger.finish(
                ledger.reserve("origin", identity(_RIGHTS)),
                state="succeeded",
                status=200,
                response=_LICENSE,
                payload=_LICENSE,
            )
            first = ledger.finish(
                ledger.reserve("origin", identity(_FIRST)),
                state="succeeded",
                status=200,
                response=_BODY,
                payload=_BODY,
            )
            failed = ledger.finish(
                ledger.reserve("origin", identity(_SECOND)), state="failed", reason="request_timeout"
            )
        db_path = next(self.root.rglob("generation.sqlite3"))
        with closing(sqlite3.connect(db_path)) as db:
            original_rows = db.execute("SELECT slot, receipt, payload FROM call ORDER BY rowid").fetchall()

        capture = KeenableCollection(self.root, access)
        with (
            patch(
                "codebench_pipeline.keenable.request_direct", return_value=DirectResponse(_BODY, _PEER)
            ) as wire,
            patch("codebench_pipeline.keenable_client.sleep"),
        ):
            self.assertEqual(discovery.discover_with_credential(""), catalog)
            lock = capture.capture_with_credential(catalog, "")
            self.assertEqual(capture.read_bodies(lock), {_FIRST: _BODY, _SECOND: _BODY})

        self.assertEqual([request.args[0].url for request in wire.call_args_list], [_SECOND])
        self.assertEqual(lock.rights_calls, (rights,))
        self.assertEqual(lock.origin_calls[0], first)
        self.assertEqual(lock.origin_calls[1].request_sha256, failed.request_sha256)
        with closing(sqlite3.connect(db_path)) as db:
            rows = db.execute("SELECT slot, receipt, payload FROM call ORDER BY rowid").fetchall()
        self.assertEqual(rows[:-1], original_rows)
        self.assertEqual(len(rows), 5)

    def test_rights_rate_limit_blocks_early_replay_and_then_resumes_pinned_capture(self) -> None:
        collection = KeenableCollection(self.root, self.declaration)
        catalog = collection.discover_with_credential("")
        with patch(
            "codebench_pipeline.keenable.request_direct",
            side_effect=[
                HttpStatusError(429, 120, _PEER),
                DirectResponse(_LICENSE, _PEER),
                DirectResponse(_BODY, _PEER),
                DirectResponse(_BODY, _PEER),
            ],
        ) as wire:
            for _ in range(2):
                with self.assertRaisesRegex(KeenableError, r"http_status \(429\)"):
                    collection.capture_with_credential(catalog, "")
            self.assertEqual(wire.call_count, 1)
            with open_ledger(self.root, self.declaration) as ledger:
                prior = ledger.call(identity(_RIGHTS).slot)
            assert prior is not None and prior[0].observed_at is not None
            self.assertEqual((prior[0].retry_after, prior[0].peer), (120, _PEER))
            with patch("codebench_pipeline.keenable_client.datetime", wraps=datetime) as clock:
                clock.now.return_value = datetime.fromisoformat(prior[0].observed_at) + timedelta(seconds=121)
                with patch("codebench_pipeline.keenable_client.sleep"):
                    lock = collection.capture_with_credential(catalog, "")

        self.assertEqual(lock.rights_calls[0].retained_sha256, digest(_LICENSE))
        self.assertEqual(
            [request.args[0].url for request in wire.call_args_list], [_RIGHTS, _RIGHTS, _FIRST, _SECOND]
        )

    def test_rights_retry_does_not_dispatch_after_authorization_expires_during_backoff(self) -> None:
        collection = KeenableCollection(self.root, self.declaration)
        catalog = collection.discover_with_credential("")
        deadline = self.declaration.sources[0].valid_until
        with (
            patch(
                "codebench_pipeline.keenable.request_direct", side_effect=KeenableError("request_timeout")
            ) as wire,
            patch("codebench_pipeline.keenable_client.datetime", wraps=datetime) as clock,
        ):
            clock.now.return_value = deadline - timedelta(seconds=1)

            def expire(delay: float) -> None:
                clock.now.return_value += timedelta(seconds=delay + 1)

            with (
                patch("codebench_pipeline.keenable_client.sleep", side_effect=expire),
                self.assertRaisesRegex(KeenableError, "authorization_expired"),
            ):
                collection.capture_with_credential(catalog, "")

        self.assertEqual(wire.call_count, 1)
        with open_ledger(self.root, self.declaration) as ledger:
            self.assertIsNone(ledger.artifact("lock"))

    def test_transient_receipt_cannot_retry_a_different_request(self) -> None:
        collection = KeenableCollection(self.root, self.declaration)
        catalog = collection.discover_with_credential("")
        inconsistent = identity(_RIGHTS).model_copy(update={"request_sha256": "1" * 64})
        with open_ledger(self.root, self.declaration) as ledger:
            ledger.finish(ledger.reserve("origin", inconsistent), state="failed", reason="request_timeout")

        with (
            patch(
                "codebench_pipeline.keenable.request_direct",
                return_value=DirectResponse(_LICENSE, _PEER),
            ) as wire,
            self.assertRaisesRegex(KeenableError, "origin_receipt_integrity"),
        ):
            collection.capture_with_credential(catalog, "")
        wire.assert_not_called()

    def test_capture_retries_charge_both_limits_before_dispatching(self) -> None:
        for call_budget, byte_budget in ((3, 7), (7, 3)):
            with self.subTest(call_budget=call_budget, byte_budget=byte_budget):
                declaration = self.declaration.model_copy(
                    update={"generation": f"budget-{call_budget}-{byte_budget}"}
                )
                discovery = KeenableCollection(self.root, declaration)
                catalog = discovery.discover_with_credential("")
                access = KeenableSourceAccess.project(declaration).model_copy(
                    update={
                        "budget": KeenableSourceAccess.project(declaration).budget.model_copy(
                            update={"origin_calls": call_budget, "total_bytes": byte_budget * RESPONSE_BYTES}
                        )
                    }
                )
                capture = KeenableCollection(self.root, access)
                with (
                    patch(
                        "codebench_pipeline.keenable.request_direct",
                        side_effect=[
                            DirectResponse(_LICENSE, _PEER),
                            KeenableError("request_timeout"),
                            DirectResponse(_BODY, _PEER),
                            DirectResponse(_BODY, _PEER),
                        ],
                    ) as wire,
                    patch("codebench_pipeline.keenable_client.sleep"),
                ):
                    with self.assertRaisesRegex(KeenableError, "generation_budget_exhausted"):
                        capture.capture_with_credential(catalog, "")
                self.assertEqual(
                    [request.args[0].url for request in wire.call_args_list], [_RIGHTS, _FIRST, _FIRST]
                )
                with open_ledger(self.root, access) as ledger:
                    self.assertIsNone(ledger.artifact("lock"))

    def test_permanent_or_ambiguous_capture_failure_leaves_generation_closed(self) -> None:
        for outcome, reason in (
            (HttpStatusError(404, None, _PEER), "http_status"),
            (KeyboardInterrupt(), "ambiguous_dispatch"),
        ):
            with self.subTest(reason=reason):
                declaration = self.declaration.model_copy(update={"generation": reason})
                collection = KeenableCollection(self.root, declaration)
                catalog = collection.discover_with_credential("")
                with patch("codebench_pipeline.keenable.request_direct", side_effect=outcome) as wire:
                    with self.assertRaises(type(outcome)):
                        collection.capture_with_credential(catalog, "")
                    with self.assertRaisesRegex(KeenableError, reason):
                        collection.capture_with_credential(catalog, "")
                self.assertEqual(wire.call_count, 1)
                with open_ledger(self.root, declaration) as ledger:
                    self.assertIsNone(ledger.artifact("lock"))
