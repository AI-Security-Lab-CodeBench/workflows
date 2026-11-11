import fcntl
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from dagster import DagsterInstance


@contextmanager
def dataset_instance(home: Path) -> Generator[DagsterInstance]:
    """Serialize first-time local metadata initialization, not dataset execution."""
    directory = home / "dagster"
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".initialization.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            instance = DagsterInstance.local_temp(
                str(directory),
                overrides={
                    "run_coordinator": {
                        "module": "dagster._core.run_coordinator.sync_in_memory_run_coordinator",
                        "class": "SyncInMemoryRunCoordinator",
                    }
                },
            )
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    try:
        yield instance
    finally:
        instance.dispose()
