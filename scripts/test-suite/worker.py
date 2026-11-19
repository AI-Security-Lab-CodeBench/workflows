"""Discover unittest cases and execute claimed work with native fixtures."""

from __future__ import annotations

import json
import sys
import unittest
from collections.abc import Iterable, Iterator
from inspect import getattr_static
from io import StringIO
from pathlib import Path
from typing import TextIO


class DiscoveryError(Exception):
    """Discovery could not produce a unique, runnable case inventory."""


def leaves(suite: Iterable[unittest.TestSuite | unittest.TestCase]) -> Iterator[unittest.TestCase]:
    for entry in suite:
        if isinstance(entry, unittest.TestSuite):
            yield from leaves(entry)
        else:
            yield entry


def discover() -> dict[str, unittest.TestCase]:
    loader = unittest.TestLoader()
    sys.path.insert(0, str(Path.cwd()))
    cases: dict[str, unittest.TestCase] = {}
    for case in leaves(loader.discover("tests")):
        if case.id() in cases:
            raise DiscoveryError(f"Duplicate unittest ID: {case.id()}")
        cases[case.id()] = case
    if loader.errors or not cases:
        raise DiscoveryError("\n".join(map(str, loader.errors)) or "No unittest cases discovered")
    return cases


class CaseResult(unittest.TextTestResult):
    """Record real unittest lifecycle events independently of console output."""

    ledger: TextIO

    def startTest(self, test: unittest.TestCase) -> None:
        super().startTest(test)
        self.ledger.write(json.dumps(["started", test.id()]) + "\n")

    def stopTest(self, test: unittest.TestCase) -> None:
        super().stopTest(test)
        self.ledger.write(json.dumps(["finished", test.id()]) + "\n")

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.ledger.write(json.dumps(["skipped", test.id()]) + "\n")


class CaseRunner(unittest.TextTestRunner):
    """Write unittest lifecycle receipts to the worker ledger."""

    def __init__(self, stream: StringIO, ledger: TextIO) -> None:
        super().__init__(stream, verbosity=2)
        self.ledger = ledger

    def _makeResult(self) -> CaseResult:
        result = CaseResult(self.stream, self.descriptions, self.verbosity)
        result.ledger = self.ledger
        return result


def run_worker(root: Path, name: str) -> int:
    cases = discover()
    if name == "discover":
        inventory: list[tuple[str, str, bool]] = []
        for identity, case in cases.items():
            cls = type(case)
            module = sys.modules[cls.__module__]
            has_shared_fixture = (
                hasattr(module, "setUpModule")
                or hasattr(module, "tearDownModule")
                or getattr_static(cls, "setUpClass") is not getattr_static(unittest.TestCase, "setUpClass")
                or getattr_static(cls, "tearDownClass")
                is not getattr_static(unittest.TestCase, "tearDownClass")
            )
            inventory.append((identity, cls.__module__, has_shared_fixture))
        (root / "inventory.json").write_text(json.dumps(inventory))
        return 0

    batches: list[list[str]] = json.loads((root / "plan.json").read_text())
    if set(cases) != {identity for batch in batches for identity in batch}:
        raise DiscoveryError("Worker discovery differs from the controller inventory")

    clean = True
    with (
        (root / name / "cases.jsonl").open("w", buffering=1) as ledger,
        (root / name / "failures.log").open("w", buffering=1) as failures,
    ):
        for index, batch in enumerate(batches):
            try:
                (root / "claims" / str(index)).mkdir()
            except FileExistsError:
                continue
            suite = unittest.TestSuite(cases[identity] for identity in batch)
            with StringIO() as report:
                if not CaseRunner(report, ledger).run(suite).wasSuccessful():
                    failures.write(report.getvalue())
                    clean = False
    return 0 if clean else 1


if __name__ == "__main__":
    raise SystemExit(run_worker(Path(sys.argv[1]), sys.argv[2]))
