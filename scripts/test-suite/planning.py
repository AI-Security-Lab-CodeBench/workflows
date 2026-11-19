"""Prioritize measured slow cases and account for every discovered test ID."""

from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Final

MEMORY_PER_WORKER: Final = 768 * 1024**2
MEMORY_RESERVE: Final = 1024**3
SLOW_MODULE_SECONDS: Final = {
    "test_confidence_publication": 70,
    "test_dataset_incremental": 100,
    "test_declared_dataset_graph": 35,
    "test_guarded_publication": 185,
    "test_guarded_release_views": 142,
    "test_independent_branches": 75,
    "test_labeling_tables": 67,
    "test_native_config_consumption": 207,
    "test_review_release": 70,
    "test_source_review": 70,
}


def worker_limit() -> int:
    cpus = len(os.sched_getaffinity(0))
    memory = (
        int(
            next(
                line.split()[1]
                for line in Path("/proc/meminfo").read_text().splitlines()
                if line.startswith("MemAvailable:")
            )
        )
        * 1024
    )
    mount = Path("/sys/fs/cgroup")
    group = next(
        (line[3:] for line in Path("/proc/self/cgroup").read_text().splitlines() if line.startswith("0::")),
        "/",
    )
    directory = mount / group.lstrip("/")
    for current in (directory, *directory.parents):
        if not current.is_relative_to(mount):
            break
        quota = current / "cpu.max"
        if quota.exists():
            capacity, period = quota.read_text().split()
            if capacity != "max":
                cpus = min(cpus, max(1, int(capacity) // int(period)))
        for name in ("memory.max", "memory.high"):
            limit = current / name
            if limit.exists() and (value := limit.read_text().strip()) != "max":
                usage = int((current / "memory.current").read_text())
                memory = min(memory, int(value) - usage)
    requested = int(os.environ.get("CODEBENCH_TEST_WORKERS", str(cpus)))
    if requested < 1:
        raise WorkerLimitError("CODEBENCH_TEST_WORKERS must be a positive integer")
    return max(1, min(requested, cpus, (memory - MEMORY_RESERVE) // MEMORY_PER_WORKER))


class WorkerLimitError(ValueError):
    """The requested worker limit is outside the supported range."""


def plan_work(root: Path) -> list[list[str]]:
    inventory: list[tuple[str, str, bool]] = json.loads((root / "inventory.json").read_text())
    grouped: dict[str, list[str]] = defaultdict(list)
    weighted: list[tuple[int, list[str]]] = []
    pinned = {module for _, module, has_fixture in inventory if has_fixture}
    for identity, module, _ in inventory:
        if module in SLOW_MODULE_SECONDS and module not in pinned:
            weighted.append((SLOW_MODULE_SECONDS[module], [identity]))
        else:
            grouped[module].append(identity)
    weighted.extend(
        (SLOW_MODULE_SECONDS.get(module, 0) * len(group), group) for module, group in grouped.items()
    )
    batches = [cases for _, cases in sorted(weighted, key=lambda batch: -batch[0])]
    (root / "claims").mkdir()
    (root / "plan.json").write_text(json.dumps(batches))
    return batches


def reconcile(root: Path, batches: list[list[str]]) -> bool:
    expected = Counter(identity for batch in batches for identity in batch)
    events: dict[str, Counter[str]] = {kind: Counter() for kind in ("started", "finished", "skipped")}
    corrupt = 0
    for ledger in sorted(root.glob("worker_*/cases.jsonl")):
        with ledger.open() as stream:
            for line in stream:
                try:
                    match json.loads(line):
                        case [str(kind), str(identity)] if kind in events:
                            events[kind][identity] += 1
                        case _:
                            corrupt += 1
                except ValueError:
                    corrupt += 1
    if corrupt:
        print(f"Invalid lifecycle receipts: {corrupt}", file=sys.stderr)
    started, finished = events["started"], events["finished"]
    print(
        f"Suite counts: discovered={expected.total()} started={started.total()} "
        f"finished={finished.total()} unittest_skip_reports={events['skipped'].total()}",
        flush=True,
    )
    for label, receipts in (("started", started), ("finished", finished)):
        for name, difference in (("missing", expected - receipts), ("extra", receipts - expected)):
            if difference:
                print(
                    f"{label}: {difference.total()} {name}; first IDs: {', '.join(list(difference)[:8])}",
                    file=sys.stderr,
                )
    return not corrupt and bool(expected) and expected == started == finished
