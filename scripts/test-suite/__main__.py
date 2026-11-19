"""Own the deadline, isolated PostgreSQL, test workers, and suite reconciliation."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from types import FrameType
from typing import Final

from planning import plan_work, reconcile, worker_limit

WORK_SECONDS: Final = 592


class SuiteError(Exception):
    """A setup, worker, or reconciliation failure."""


class Supervisor:
    """Track task-owned process groups until they exit or are killed."""

    def __init__(self, root: Path, deadline: float) -> None:
        self.root = root
        self.deadline = deadline
        self.children: dict[str, subprocess.Popen[bytes]] = {}
        self.interrupted = 0
        self.environment = os.environ.copy()

    def interrupt(self, signum: int, _frame: FrameType | None) -> None:
        self.interrupted = signum

    def check(self) -> None:
        if self.interrupted:
            raise InterruptedError(f"Interrupted by signal {self.interrupted}")
        if monotonic() >= self.deadline:
            raise TimeoutError("600-second suite budget reached its cleanup reserve")

    def launch(
        self, name: str, command: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None
    ) -> None:
        self.check()
        with (self.root / f"{name}.log").open("wb") as log:
            self.children[name] = subprocess.Popen(
                command,
                cwd=cwd,
                env=self.environment if env is None else env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

    def reap(self, name: str) -> int | None:
        process = self.children[name]
        rc = process.poll()
        if rc is not None:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            del self.children[name]
        return rc

    def wait(self, name: str) -> int:
        while (rc := self.reap(name)) is None:
            self.check()
            sleep(0.05)
        return rc

    def require(self, name: str) -> None:
        if rc := self.wait(name):
            self.diagnostics(Path(f"{name}.log"))
            raise SuiteError(f"{name} exited {rc}")

    def diagnostics(self, log_path: Path, *, bytes_limit: int = 32768) -> None:
        with (self.root / log_path).open("rb") as log:
            log.seek(max(0, os.fstat(log.fileno()).st_size - bytes_limit))
            print(f"--- {log_path} (log tail) ---\n{log.read().decode(errors='replace')}", file=sys.stderr)

    def stop(self) -> None:
        for name, process in self.children.items():
            with suppress(ProcessLookupError):
                if name == "postgres":
                    process.send_signal(signal.SIGQUIT)
                else:
                    os.killpg(process.pid, signal.SIGKILL)
        limit = monotonic() + 2
        while self.children and monotonic() < limit:
            for name in tuple(self.children):
                self.reap(name)
            if self.children:
                sleep(0.05)
        for name, process in tuple(self.children.items()):
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            try:
                process.wait(timeout=max(0, limit + 1 - monotonic()))
            except subprocess.TimeoutExpired:
                continue
            del self.children[name]
        if self.children:
            raise SuiteError(f"Cleanup did not reap: {', '.join(self.children)}")

    def start_case_process(self, name: str) -> None:
        connection = ["-h", str(self.root), "-U", "suite"]
        self.launch("createdb", ["createdb", *connection, name])
        self.require("createdb")
        home = self.root / name
        for directory in ("home", "cache", "config", "data", "state", "dagster", "tmp"):
            (home / directory).mkdir(parents=True)
        env = self.environment.copy()
        env.update(
            HOME=str(home / "home"),
            XDG_CACHE_HOME=str(home / "cache"),
            XDG_CONFIG_HOME=str(home / "config"),
            XDG_DATA_HOME=str(home / "data"),
            XDG_STATE_HOME=str(home / "state"),
            TMPDIR=str(home / "tmp"),
            DAGSTER_HOME=str(home / "dagster"),
            CODEBENCH_HOME=str(home / "dagster"),
            CODEBENCH_REVIEW_DATABASE_URL=f"postgresql://suite@/{name}?host={self.root}",
            DAGSTER_DISABLE_TELEMETRY="1",
            OPENBLAS_NUM_THREADS="1",
            OMP_NUM_THREADS="1",
            MKL_NUM_THREADS="1",
            NUMEXPR_NUM_THREADS="1",
            POLARS_MAX_THREADS="1",
        )
        self.launch(
            name, [sys.executable, "-uB", "scripts/test-suite/worker.py", str(self.root), name], env=env
        )


def prepare(supervisor: Supervisor) -> None:
    root = supervisor.root
    configured = os.environ.get("CODEBENCH_BASHGUARD_AUDIT_BINARY")
    audit = Path(configured).resolve() if configured else root / "bashguard-audit"
    if not audit.is_file():
        audit = root / "bashguard-audit"
    if audit == root / "bashguard-audit":
        print("Building BashGuard audit executable from the checkout", flush=True)
        supervisor.launch(
            "audit-build",
            ["go", "build", "-mod=readonly", "-trimpath", "-o", str(audit), "./cmd/bashguard-audit"],
            cwd=Path("bashguard"),
        )
        supervisor.require("audit-build")
    if not audit.is_file() or not os.access(audit, os.X_OK):
        raise SuiteError(f"BashGuard audit executable is unavailable: {audit}")
    supervisor.environment["CODEBENCH_BASHGUARD_AUDIT_BINARY"] = str(audit)
    for key in tuple(supervisor.environment):
        if key.startswith("PG"):
            del supervisor.environment[key]
    data = str(root / "pg")
    supervisor.launch(
        "initdb", ["initdb", "-D", data, "-A", "trust", "-U", "suite", "-E", "UTF8", "--locale=C"]
    )
    supervisor.require("initdb")
    supervisor.launch("postgres", ["postgres", "-D", data, "-k", str(root), "-h", ""])
    while True:
        supervisor.launch(
            "readiness", ["pg_isready", "-q", "-t", "1", "-h", str(root), "-U", "suite", "-d", "postgres"]
        )
        if supervisor.wait("readiness") == 0:
            break
        if supervisor.reap("postgres") is not None:
            supervisor.diagnostics(Path("postgres.log"))
            raise SuiteError("Isolated PostgreSQL exited before readiness")
        sleep(0.1)
    supervisor.start_case_process("discover")
    supervisor.require("discover")


def main() -> int:
    started = monotonic()
    status = 0
    batches: list[list[str]] = []
    with TemporaryDirectory(prefix="cb-suite-", dir="/tmp") as temporary:
        root = Path(temporary)
        supervisor = Supervisor(root, started + WORK_SECONDS)
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(signum, supervisor.interrupt)
        try:
            limit = worker_limit()
            prepare(supervisor)
            batches = plan_work(root)
            count = min(limit, len(batches))
            print(
                f"Running {sum(map(len, batches))} unittest cases in {len(batches)} batches "
                f"on {count} workers; setup took {monotonic() - started:.1f}s",
                flush=True,
            )
            for number in range(count):
                supervisor.start_case_process(f"worker_{number}")
            while supervisor.children.keys() - {"postgres"}:
                supervisor.check()
                for name in sorted(supervisor.children.keys() - {"postgres"}):
                    if rc := supervisor.reap(name):
                        print(f"{name} exited {rc}", file=sys.stderr)
                        supervisor.diagnostics(Path(f"{name}.log"))
                        status = 1
                if supervisor.reap("postgres") is not None:
                    supervisor.diagnostics(Path("postgres.log"))
                    raise SuiteError("Isolated PostgreSQL exited during execution")
                sleep(0.05)
        except (OSError, SuiteError, ValueError) as error:
            status = 1
            if isinstance(error, TimeoutError):
                status = 124
            if supervisor.interrupted:
                status = 128 + supervisor.interrupted
            print(f"Suite stopped: {error}", file=sys.stderr)
        finally:
            active = sorted(supervisor.children.keys() - {"postgres"})
            try:
                supervisor.stop()
            except SuiteError as error:
                print(error, file=sys.stderr)
                status = status or 1
            for name in active:
                supervisor.diagnostics(Path(f"{name}.log"), bytes_limit=4096)
            for report in sorted(root.glob("worker_*/failures.log")):
                if report.stat().st_size:
                    supervisor.diagnostics(report.relative_to(root))
            if batches:
                if not reconcile(root, batches):
                    status = status or 1
            else:
                print("Suite counts unavailable: discovery did not produce a plan", file=sys.stderr)
    print(f"Suite finished in {monotonic() - started:.1f}s (exit {status})", flush=True)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
