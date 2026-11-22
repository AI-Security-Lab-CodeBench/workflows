import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from select import select
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch
from uuid import uuid4

import anyio
import httpx

from codebench_pipeline import keenable_admission, keenable_provider
from codebench_pipeline.keenable_admission import KeenableAdmissionLedger
from codebench_pipeline.keenable_mcp import SearchResult, search_session, search_snippets
from codebench_pipeline.keenable_provider import (
    KeenableAdmission,
    KeenableCredentials,
    KeenableIncident,
    KeenableTool,
    KeenableTransport,
    KeenableTransportError,
    admit_tool_dispatch,
)


def _http_failure(status: int, retry_after: str) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.keenable.ai/mcp")
    response = httpx.Response(
        status, request=request, headers={"Retry-After": retry_after}, text="private provider failure"
    )
    return httpx.HTTPStatusError("private error detail", request=request, response=response)


class KeenableTransportTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.clock = 100.0
        self.incidents: list[KeenableIncident] = []
        self.enterContext(patch.object(keenable_provider, "_ADMISSION", KeenableAdmission()))
        self.enterContext(patch.object(keenable_provider, "monotonic", lambda: self.clock))
        self.enterContext(patch.object(keenable_provider, "sleep", self.advance))

    async def advance(self, delay: float) -> None:
        self.clock += delay

    async def test_invalid_ledger_never_reaches_mcp_wire_or_replaces_evidence(self) -> None:
        from keenable_mcp_fixture import keenable_mcp_fixture

        with TemporaryDirectory() as path, keenable_mcp_fixture(empty=True) as queries:
            directory = Path(path)
            credentials = KeenableCredentials.bind("fixture-key", lambda _: None)
            transport = KeenableTransport(credentials, ledger=KeenableAdmissionLedger(directory))

            async def search(credential: str) -> SearchResult:
                async with search_session(credential) as session:
                    return await search_snippets(session, "synthetic ledger check", (8, 500))

            await transport.call("search", search, audit=self.incidents.append)
            self.assertEqual(queries, ["synthetic ledger check"])
            (state_path,) = directory.glob("*.json")
            original = state_path.read_bytes()
            for case, invalid in (
                ("padded-prefix", original.ljust(4097, b" ") + b"!"),
                ("malformed-json", original + b"!"),
                ("invalid-slot", original.replace(b'"pending":[]', b'"pending":[5]')),
                ("ambiguous-field", b'{"resume_at":9223372036854775807,' + original[1:]),
                ("overflowing-timer", original.replace(b'"resume_at":0', b'"resume_at":' + b"9" * 350)),
            ):
                with self.subTest(case=case):
                    self.assertNotEqual(invalid, original)
                    state_path.write_bytes(invalid)
                    with self.assertRaises(KeenableTransportError) as caught:
                        await transport.call("search", search, audit=self.incidents.append)
                    self.assertEqual(caught.exception.detail.code, "provider_admission_unavailable")
                    self.assertEqual(queries, ["synthetic ledger check"])
                    self.assertEqual(state_path.read_bytes(), invalid)
            self.assertEqual(self.incidents, [])

    async def test_failed_atomic_replace_keeps_previous_ledger_and_prevents_dispatch(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            credentials = KeenableCredentials.bind("fixture-key", lambda _: None)
            transport = KeenableTransport(credentials, ledger=KeenableAdmissionLedger(directory))
            starts: list[str] = []

            async def send(credential: str) -> None:
                await admit_tool_dispatch()
                starts.append(credential)

            await transport.call("search", send, audit=self.incidents.append)
            (state_path,) = directory.glob("*.json")
            original = state_path.read_bytes()
            with (
                patch.object(
                    keenable_admission.os, "replace", side_effect=OSError("synthetic replace failure")
                ),
                self.assertRaises(KeenableTransportError) as caught,
            ):
                await transport.call("fetch", send, audit=self.incidents.append)
            self.assertEqual(caught.exception.detail.code, "provider_admission_unavailable")
            self.assertEqual(starts, ["fixture-key"])
            self.assertEqual(state_path.read_bytes(), original)
            self.assertEqual(list(directory.glob(".state-*")), [])

            await transport.call("fetch", send, audit=self.incidents.append)
            self.assertEqual(starts, ["fixture-key"] * 2)

    async def test_shared_search_and_fetch_rate_for_grouped_keys_and_independent_org(self) -> None:
        env = {
            "KEENABLE_API_KEY_1": "primary",
            "KEENABLE_API_KEY_2": "same-org",
            "KEENABLE_API_KEY_4": "other-org",
            "KEENABLE_ORG": "shared",
            "KEENABLE_ORG_2": "shared",
        }
        starts: list[tuple[float, str]] = []

        async def send(credential: str) -> None:
            await admit_tool_dispatch()
            starts.append((self.clock, credential))

        credentials = KeenableCredentials.bind("primary", env.get)
        for index in range(21):
            tool: KeenableTool = "search" if index % 2 else "fetch"
            await KeenableTransport(credentials).call(tool, send, audit=self.incidents.append)

        self.assertEqual(
            Counter(key for started, key in starts if started == 100),
            {"primary": 5, "same-org": 5, "other-org": 10},
        )
        self.assertEqual(starts[-1][0], 101)

    async def test_retry_after_prevents_early_retries_and_recovered_500_is_retained(self) -> None:
        starts: list[float] = []

        async def send(_credential: str) -> str:
            await admit_tool_dispatch()
            starts.append(self.clock)
            if len(starts) == 1:
                raise _http_failure(429, "3")
            if len(starts) == 2:
                raise _http_failure(500, "2")
            return "success"

        transport = KeenableTransport(KeenableCredentials.bind("primary", lambda _: None))
        result = await transport.call("fetch", send, audit=self.incidents.append)

        self.assertEqual((result, starts), ("success", [100, 103, 105]))
        self.assertEqual([(event.attempt, event.error.status) for event in self.incidents], [(2, 500)])

    async def test_session_failure_without_a_tool_dispatch_does_not_consume_rate(self) -> None:
        transport = KeenableTransport(KeenableCredentials.bind("primary", lambda _: None))

        async def fail_initialization(_credential: str) -> None:
            raise httpx.LocalProtocolError("synthetic failed initialization")

        async def send(_credential: str) -> None:
            await admit_tool_dispatch()

        for _ in range(10):
            with self.assertRaises(KeenableTransportError):
                await transport.call("search", fail_initialization, audit=self.incidents.append)
        for _ in range(10):
            await transport.call("fetch", send, audit=self.incidents.append)
        self.assertEqual(self.clock, 100)

    async def test_missing_host_boot_identity_fails_closed_before_provider_send(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            ledger = KeenableAdmissionLedger(directory / "admission")
            credentials = KeenableCredentials.bind("primary", lambda _: None)

            async def send(_credential: str) -> None:
                self.fail("Provider must not receive a call without a valid boot identity")

            with (
                patch.object(keenable_admission, "_BOOT_ID_PATH", directory / "missing"),
                self.assertRaises(KeenableTransportError) as caught,
            ):
                await KeenableTransport(credentials, ledger=ledger).call(
                    "search", send, audit=self.incidents.append
                )
            self.assertEqual(caught.exception.detail.code, "provider_admission_unavailable")
            self.assertEqual(list(ledger.directory.glob("*.json")), [])

    async def test_excess_retry_after_is_terminal_and_pauses_only_its_organization(self) -> None:
        org = KeenableCredentials.bind("primary", {"KEENABLE_ORG": "same"}.get)
        peer = KeenableCredentials.bind("peer", {"KEENABLE_ORG": "same"}.get)
        independent = KeenableCredentials.bind("independent", lambda _: None)

        async def limited(_credential: str) -> None:
            await admit_tool_dispatch()
            raise _http_failure(429, "40")

        async def stamp(_credential: str) -> float:
            await admit_tool_dispatch()
            return self.clock

        with self.assertRaises(KeenableTransportError) as caught:
            await KeenableTransport(org).call("search", limited, audit=self.incidents.append)
        self.assertEqual(caught.exception.detail.retry_after_seconds, 40)
        self.assertEqual(
            await KeenableTransport(independent).call("fetch", stamp, audit=self.incidents.append), 100
        )
        self.assertEqual(await KeenableTransport(peer).call("fetch", stamp, audit=self.incidents.append), 140)

    async def test_exhaustion_preserves_every_500_but_does_not_retry_402(self) -> None:
        for status, attempts in ((500, 4), (402, 1)):
            with self.subTest(status=status):
                starts: list[float] = []

                async def fail(_credential: str) -> None:
                    await admit_tool_dispatch()
                    starts.append(self.clock)
                    raise _http_failure(status, "0")

                transport = KeenableTransport(KeenableCredentials.bind("primary", lambda _: None))
                with self.assertRaises(KeenableTransportError) as caught:
                    await transport.call("search", fail, audit=self.incidents.append)
                self.assertEqual((len(starts), caught.exception.detail.status), (attempts, status))
        self.assertEqual([event.attempt for event in self.incidents], [1, 2, 3, 4])
        self.assertTrue(all(event.error.status == 500 for event in self.incidents))

    async def test_only_known_transient_network_failures_retry(self) -> None:
        errors = (
            (httpx.ConnectError, True),
            (httpx.ReadError, True),
            (httpx.WriteError, True),
            (httpx.RemoteProtocolError, True),
            (httpx.LocalProtocolError, False),
        )
        for error_type, retry in errors:
            with self.subTest(error=error_type.__name__):
                count = 0

                async def send(_credential: str) -> str:
                    nonlocal count
                    await admit_tool_dispatch()
                    count += 1
                    if count < 2:
                        raise ExceptionGroup("private MCP group", [error_type("private network error")])
                    return "recovered"

                credentials = KeenableCredentials.bind(error_type.__name__, lambda _: None)
                transport = KeenableTransport(credentials)
                if retry:
                    self.assertEqual(
                        await transport.call("search", send, audit=self.incidents.append), "recovered"
                    )
                else:
                    with self.assertRaises(KeenableTransportError) as caught:
                        await transport.call("search", send, audit=self.incidents.append)
                    self.assertEqual(caught.exception.detail.code, "provider_transport_failed")
                self.assertEqual(count, 2 if retry else 1)
        self.assertEqual(self.incidents, [])


def _worker(directory: Path, key: str, mode: str, wall_offset: float) -> None:
    now = 100.0
    credentials = KeenableCredentials.bind(key, {"KEENABLE_ORG": "shared-test-organization"}.get)
    ledger = KeenableAdmissionLedger(directory)
    transport = KeenableTransport(credentials, ledger=ledger)

    async def wait(delay: float) -> None:
        nonlocal now
        now += delay

    async def send(_key: str) -> None:
        if mode == "hold-handshake":
            print("session initialized", flush=True)
            sys.stdin.read(1)
        await admit_tool_dispatch()
        if mode == "throttle":
            raise _http_failure(429, "40")

    async def run() -> None:
        try:
            count = 10 if mode == "spend" else 5 if mode == "spend-five" else 1
            for _ in range(count):
                await transport.call("search", send, audit=lambda _: None)
        except KeenableTransportError as error:
            print(error.detail.code, error.detail.status)
        else:
            print(now)

    with (
        patch.object(keenable_admission, "monotonic_ns", lambda: int(now * 1_000_000_000)),
        patch.object(
            keenable_admission, "time_ns", lambda: int((now + wall_offset) * 1_000_000_000), create=True
        ),
        patch.object(keenable_provider, "sleep", wait),
    ):
        if mode in {"hold-slots", "hold-dispatched"}:
            org = credentials.organizations[0].identity
            slots = [ledger.claim_slot(org)[0] for _ in range(5)]
            for slot in slots:
                assert slot is not None
                if mode == "hold-dispatched":
                    assert ledger.dispatch_delay(org, slot) == 0
            print("holding five slots", flush=True)
            sys.stdin.read(1)
        elif mode == "probe-slot":
            org = credentials.organizations[0].identity
            descriptor, _ = ledger.claim_slot(org)
            if descriptor is None:
                print("blocked")
            else:
                ledger.release(org, descriptor, dispatched=False, retry_after=0)
                print("admitted")
        else:
            anyio.run(run)


class DurableKeenableAdmissionTests(TestCase):
    def _command(self, directory: Path, key: str, mode: str, wall_offset: float = 0) -> list[str]:
        return [sys.executable, str(Path(__file__).resolve()), str(directory), key, mode, str(wall_offset)]

    def _environment(self) -> dict[str, str]:
        return {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}

    def _process(self, directory: Path, key: str, mode: str, wall_offset: float = 0) -> str:
        return subprocess.check_output(
            self._command(directory, key, mode, wall_offset),
            env=self._environment(),
            text=True,
            timeout=15,
        ).strip()

    def test_independent_processes_share_rate_after_restart(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            self.assertEqual(self._process(directory, "first-process-secret", "spend"), "100.0")
            self.assertEqual(self._process(directory, "second-process-secret", "use"), "101.0")

    def test_independent_processes_honor_429_cooldown_after_restart(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            self.assertEqual(
                self._process(directory, "first-process-secret", "throttle"), "provider_http_status 429"
            )
            self.assertEqual(self._process(directory, "second-process-secret", "use"), "140.0")
            (state,) = directory.glob("*.json")
            self.assertNotIn("secret", state.name + state.read_text())
            self.assertNotIn("shared-test-organization", state.name + state.read_text())

    def test_corrupt_admission_state_refuses_dispatch(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            self.assertEqual(self._process(directory, "first-process-secret", "use"), "100.0")
            (state,) = directory.glob("*.json")
            state.write_text("{}")
            self.assertEqual(
                self._process(directory, "second-process-secret", "use"),
                "provider_admission_unavailable None",
            )

    def test_forward_and_backward_wall_steps_do_not_change_persisted_rate_or_cooldown(self) -> None:
        for offset in (3600, -3600):
            with self.subTest(wall_step=offset), TemporaryDirectory() as path:
                for initial, expected in (("spend", "101.0"), ("throttle", "140.0")):
                    directory = Path(path) / initial
                    self.assertEqual(
                        self._process(directory, "initial", initial),
                        "100.0" if initial == "spend" else "provider_http_status 429",
                    )
                    self.assertEqual(self._process(directory, "after-clock-step", "use", offset), expected)

    def test_previous_boot_and_legacy_state_refuse_dispatch_without_overwriting_state(self) -> None:
        current_boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        for prior in ("other-boot", "legacy-schema"):
            with self.subTest(prior=prior), TemporaryDirectory() as path:
                directory = Path(path)
                self.assertEqual(self._process(directory, "initial", "throttle"), "provider_http_status 429")
                (state,) = directory.glob("*.json")
                current_state = state.read_text()
                self.assertIn(f'"boot_id":"{current_boot}"', current_state)
                if prior == "other-boot":
                    body = current_state.replace(current_boot, str(uuid4()))
                else:
                    body = '{"version":1,"starts":[100000000000],"resume_at":140000000000,"pending":[]}'
                state.write_text(body)
                self.assertEqual(
                    self._process(directory, "new-worker", "use"), "provider_admission_unavailable None"
                )
                self.assertEqual(state.read_text(), body)

    def test_parallel_org_slots_are_shared_and_released_when_owner_exits(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            with subprocess.Popen(
                self._command(directory, "slot-owner", "hold-slots"),
                env=self._environment(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            ) as owner:
                assert owner.stdout is not None
                try:
                    ready, _, _ = select([owner.stdout], [], [], 8)
                    self.assertTrue(ready, "slot owner did not reach admission")
                    self.assertEqual(owner.stdout.readline().strip(), "holding five slots")
                    self.assertEqual(self._process(directory, "slot-peer", "probe-slot"), "blocked")
                finally:
                    owner.kill()
            self.assertEqual(self._process(directory, "slot-peer", "probe-slot"), "admitted")

    def test_cooldown_rechecked_at_dispatch_even_with_an_existing_slot_in_another_process(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            with subprocess.Popen(
                self._command(directory, "pending-worker", "hold-handshake"),
                env=self._environment(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            ) as worker:
                assert worker.stdout is not None
                try:
                    ready, _, _ = select([worker.stdout], [], [], 8)
                    self.assertTrue(ready, "worker never acquired its handshake slot")
                    self.assertEqual(worker.stdout.readline().strip(), "session initialized")
                    self.assertEqual(
                        self._process(directory, "throttled-peer", "throttle"), "provider_http_status 429"
                    )
                    stdout, _ = worker.communicate("x", timeout=8)
                    self.assertEqual(stdout.strip(), "140.0")
                    self.assertEqual(worker.returncode, 0)
                finally:
                    if worker.poll() is None:
                        worker.kill()

    def test_abandoned_dispatches_still_count_toward_rate_after_worker_exit(self) -> None:
        with TemporaryDirectory() as path:
            directory = Path(path)
            self.assertEqual(self._process(directory, "slot-peer", "spend-five"), "100.0")
            with subprocess.Popen(
                self._command(directory, "slot-owner", "hold-dispatched"),
                env=self._environment(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            ) as owner:
                assert owner.stdout is not None
                try:
                    ready, _, _ = select([owner.stdout], [], [], 8)
                    self.assertTrue(ready, "dispatch owner did not reach admission")
                    self.assertEqual(owner.stdout.readline().strip(), "holding five slots")
                finally:
                    owner.kill()
            self.assertEqual(self._process(directory, "slot-peer", "use"), "101.0")


if __name__ == "__main__":
    _worker(Path(sys.argv[1]), sys.argv[2], sys.argv[3], float(sys.argv[4]))
