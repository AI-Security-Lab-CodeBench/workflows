import sqlite3
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import call, patch

from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_ledger import SlotIdentity, open_ledger
from codebench_pipeline.keenable_policy import (
    CASES,
    PROVIDER,
    QUERIES,
    RESPONSE_BYTES,
    AcquisitionBudget,
    KeenableDeclaration,
    KeenableError,
    SourcePermission,
    digest,
)
from codebench_pipeline.keenable_receipts import CallReceipt
from codebench_pipeline.keenable_transport import DirectResponse, HttpStatusError, search_payload

_EMPTY = DirectResponse(b'{"results":[]}', "1.1.1.1")
_SCOPE = "https://developer.hashicorp.com/terraform/cli/commands/"


class KeenableRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = self.enterContext(TemporaryDirectory())
        self.root = Path(temporary)
        self.declaration = KeenableDeclaration(
            dataset="synthetic",
            generation="retry-fixture",
            generation_review="fixture-review",
            cases=CASES,
            revision="2",
            sources=(
                SourcePermission(
                    origin_id="fixture",
                    url_prefix=_SCOPE,
                    cases=("plan", "workloads"),
                    media_type="text/html",
                    rights_evidence_sha256="1" * 64,
                    steward_decision="fixture-review",
                    valid_until=datetime(2099, 1, 1, tzinfo=UTC),
                    attribution="Synthetic fixture",
                    permitted_use="fetch_retain_quote",
                ),
            ),
            budget=AcquisitionBudget(search_calls=5, origin_calls=1),
        )
        self.collection = KeenableCollection(self.root, self.declaration)

    def receipts(self) -> list[CallReceipt]:
        with closing(sqlite3.connect(next(self.root.rglob("generation.sqlite3")))) as db:
            return [
                CallReceipt.model_validate_json(raw)
                for (raw,) in db.execute("SELECT receipt FROM call ORDER BY rowid")
            ]

    def test_recovers_legacy_transient_failure_and_keeps_prior_success_and_failure(self) -> None:
        plan = SlotIdentity(
            slot="search:plan",
            target=PROVIDER,
            request_sha256=digest(search_payload(QUERIES["plan"], (_SCOPE,))),
        )
        workloads = SlotIdentity(
            slot="search:workloads",
            target=PROVIDER,
            request_sha256=digest(search_payload(QUERIES["workloads"], (_SCOPE,))),
        )
        with open_ledger(self.root, self.declaration) as ledger:
            success = ledger.finish(
                ledger.reserve("search", plan), state="succeeded", status=200, payload=b"[]"
            )
            failure = ledger.finish(
                ledger.reserve("search", workloads), state="failed", reason="http_status", status=503
            )

        with (
            patch("codebench_pipeline.keenable.request_direct", return_value=_EMPTY) as wire,
            patch("codebench_pipeline.keenable_search.sleep"),
        ):
            catalog = self.collection.discover_with_credential("fixture-key")
            self.assertEqual(self.collection.discover_with_credential(""), catalog)

        self.assertEqual(wire.call_count, 1)
        self.assertEqual(catalog.searches[0], success)
        self.assertEqual(self.receipts(), [success, failure, catalog.searches[1]])
        self.assertEqual(catalog.searches[1].request_sha256, workloads.request_sha256)

    def test_retry_reservations_exhaust_the_call_budget_before_another_dispatch(self) -> None:
        declaration = self.declaration.model_copy(
            update={"budget": AcquisitionBudget(search_calls=2, origin_calls=1)}
        )
        collection = KeenableCollection(self.root, declaration)
        with (
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=[KeenableError("request_timeout"), _EMPTY, _EMPTY],
            ) as wire,
            patch("codebench_pipeline.keenable_search.sleep"),
        ):
            with self.assertRaisesRegex(KeenableError, "generation_budget_exhausted"):
                collection.discover_with_credential("fixture-key")

        self.assertEqual(wire.call_count, 2)
        self.assertEqual(
            [(r.slot, r.state, r.reserved_bytes) for r in self.receipts()],
            [("search:plan", "failed", RESPONSE_BYTES), ("search:plan", "succeeded", RESPONSE_BYTES)],
        )

    def test_backoff_grows_and_caps_dispatches_across_reopens(self) -> None:
        def midpoint(low: float, high: float) -> float:
            return (low + high) / 2

        with (
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=KeenableError("http_status", 503),
            ) as wire,
            patch("codebench_pipeline.keenable_search.uniform", side_effect=midpoint),
            patch("codebench_pipeline.keenable_search.sleep") as sleep,
        ):
            for _ in range(2):
                with self.assertRaisesRegex(KeenableError, r"http_status \(503\)"):
                    self.collection.discover_with_credential("fixture-key")

        self.assertEqual(wire.call_count, 4)
        self.assertEqual(sleep.call_args_list, [call(0.75), call(1.5), call(3.0)])
        self.assertEqual([receipt.status for receipt in self.receipts()], [503] * 4)

    def test_long_retry_after_survives_restart_without_dispatching_early(self) -> None:
        with (
            patch(
                "codebench_pipeline.keenable.request_direct",
                side_effect=[HttpStatusError(429, 120, "1.1.1.1"), _EMPTY, _EMPTY],
            ) as wire,
            patch("codebench_pipeline.keenable_search.sleep") as sleep,
            patch("codebench_pipeline.keenable_search.datetime", wraps=datetime) as clock,
        ):
            for _ in range(2):
                with self.assertRaisesRegex(KeenableError, r"http_status \(429\)"):
                    self.collection.discover_with_credential("fixture-key")
            (failure,) = self.receipts()
            self.assertEqual((failure.retry_after, failure.peer), (120, "1.1.1.1"))
            self.assertEqual(wire.call_count, 1)
            sleep.assert_not_called()
            assert failure.observed_at is not None
            clock.now.return_value = datetime.fromisoformat(failure.observed_at) + timedelta(seconds=121)
            catalog = self.collection.discover_with_credential("fixture-key")

        self.assertEqual(self.receipts(), [failure, *catalog.searches])

    def test_billing_failure_remains_terminal(self) -> None:
        with patch(
            "codebench_pipeline.keenable.request_direct", side_effect=KeenableError("http_status", 402)
        ) as wire:
            for _ in range(2):
                with self.assertRaisesRegex(KeenableError, r"http_status \(402\)"):
                    self.collection.discover_with_credential("fixture-key")

        self.assertEqual(wire.call_count, 1)
        self.assertEqual([receipt.status for receipt in self.receipts()], [402])
