import os
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from fcntl import LOCK_EX, LOCK_NB, flock
from json import loads
from pathlib import Path
from re import fullmatch
from stat import S_ISREG
from time import monotonic_ns
from typing import Final, Literal
from uuid import uuid4

from pydantic import Field, ValidationError, model_validator

from .keenable_policy import KeenableError, Record
from .keenable_transport import MAX_RETRY_AFTER_SECONDS
from .storage import IntegrityError, open_directory, open_regular_file, write_new_file

ORG_RATE: Final = 10
ORG_PARALLEL: Final = 5
SLOT_WAIT: Final = 0.02
_SECOND: Final = 1_000_000_000
_MAX_STATE_BYTES: Final = 4096
_MAX_RETRY_AFTER_NS: Final = int(float(MAX_RETRY_AFTER_SECONDS) * _SECOND)
_BOOT_ID_PATH: Final = Path("/proc/sys/kernel/random/boot_id")
_BOOT_ID_PATTERN: Final = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


class _State(Record):
    version: Literal[2]
    boot_id: str = Field(pattern=f"^{_BOOT_ID_PATTERN}$")
    starts: tuple[int, ...] = Field(max_length=ORG_RATE)
    resume_at: int = Field(ge=0)
    pending: tuple[int, ...]

    @model_validator(mode="after")
    def ordered_starts(self) -> "_State":
        if (
            self.starts != tuple(sorted(self.starts))
            or any(start < 0 for start in self.starts)
            or len(self.starts) + len(self.pending) > ORG_RATE
            or len(set(self.pending)) != len(self.pending)
            or any(not 0 <= slot < ORG_PARALLEL for slot in self.pending)
        ):
            raise ValueError("Invalid Keenable admission state")
        return self

    def dispatch_window(self, now: int) -> tuple[tuple[int, ...], float]:
        starts = tuple(start for start in self.starts if now - start < _SECOND)
        rate_end = starts[0] + _SECOND if len(starts) + len(self.pending) >= ORG_RATE else 0
        return starts, max(0, max(rate_end, self.resume_at) - now) / _SECOND


def _lock(directory: int, name: str) -> int:
    fd = os.open(
        name,
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
        0o600,
        dir_fd=directory,
    )
    if not S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise IntegrityError("Invalid Keenable admission lock")
    return fd


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    fields: dict[str, object] = {}
    for key, value in pairs:
        if key in fields:
            raise IntegrityError("Duplicate Keenable admission state field")
        fields[key] = value
    return fields


def _read(directory: int, name: str, now: int) -> _State:
    boot_id = _BOOT_ID_PATH.read_text(encoding="ascii").strip()
    if not fullmatch(_BOOT_ID_PATTERN, boot_id):
        raise IntegrityError("Invalid Linux boot identity")
    try:
        with open_regular_file(directory, name) as stream:
            size = os.fstat(stream.fileno()).st_size
            if not 0 < size <= _MAX_STATE_BYTES:
                raise IntegrityError("Keenable admission state has an invalid size")
            body = stream.read(size)
            if len(body) != size or stream.read(1):
                raise IntegrityError("Keenable admission state size changed during read")
    except FileNotFoundError:
        return _State(version=2, boot_id=boot_id, starts=(), resume_at=0, pending=())
    state = _State.model_validate_json(body)
    try:
        loads(body, object_pairs_hook=_unique_fields)
    except (ValueError, RecursionError):
        raise IntegrityError("Invalid Keenable admission state JSON") from None
    if (
        state.boot_id != boot_id
        or (state.starts and state.starts[-1] > now)
        or state.resume_at > now + _MAX_RETRY_AFTER_NS
    ):
        raise IntegrityError("Keenable admission state belongs to a different clock epoch")
    return state


def _write(directory: int, name: str, state: _State) -> None:
    temporary = f".state-{uuid4().hex}"
    write_new_file(directory, temporary, state.model_dump_json().encode())
    try:
        os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
    except OSError:
        os.unlink(temporary, dir_fd=directory)
        raise
    os.fsync(directory)


def _finish(state: _State, now: int, slots: tuple[int, ...]) -> _State:
    starts, _ = state.dispatch_window(now)
    finished_at = max(now, starts[-1]) if starts else now
    return state.model_copy(
        update={
            "starts": starts + (finished_at,) * len(slots),
            "pending": tuple(slot for slot in state.pending if slot not in slots),
        }
    )


def _reap(directory: int, org: str, name: str, state: _State, now: int) -> _State:
    abandoned: list[int] = []
    for slot in state.pending:
        fd = _lock(directory, f"{org}.slot-{slot}")
        try:
            try:
                flock(fd, LOCK_EX | LOCK_NB)
            except BlockingIOError:
                continue
            abandoned.append(slot)
        finally:
            os.close(fd)
    if abandoned:
        state = _finish(state, now, tuple(abandoned))
        _write(directory, name, state)
    return state


@dataclass(frozen=True, slots=True)
class KeenableSlot:
    number: int
    descriptor: int


@dataclass(frozen=True, slots=True)
class KeenableAdmissionLedger:
    """Charge wire dispatches until completion plus one second, including abandoned dispatches."""

    directory: Path

    @contextmanager
    def _guard(self, org: str) -> Generator[tuple[int, int, str]]:
        if not fullmatch(r"[0-9a-f]{64}", org):
            raise KeenableError("provider_admission_unavailable")
        try:
            with open_directory(self.directory, create=True) as directory:
                guard = _lock(directory, f"{org}.lock")
                try:
                    yield directory, guard, f"{org}.json"
                finally:
                    os.close(guard)
        except (OSError, UnicodeError, ValidationError, IntegrityError):
            raise KeenableError("provider_admission_unavailable") from None

    def claim_slot(self, org: str) -> tuple[KeenableSlot | None, float]:
        with self._guard(org) as (directory, guard, name):
            try:
                flock(guard, LOCK_EX | LOCK_NB)
            except BlockingIOError:
                return None, SLOT_WAIT
            now = monotonic_ns()
            state = _reap(directory, org, name, _read(directory, name, now), now)
            _, wait = state.dispatch_window(now)
            if wait:
                return None, wait
            for slot in range(ORG_PARALLEL):
                fd = _lock(directory, f"{org}.slot-{slot}")
                retained = False
                try:
                    try:
                        flock(fd, LOCK_EX | LOCK_NB)
                    except BlockingIOError:
                        continue
                    retained = True
                    return KeenableSlot(slot, fd), 0
                finally:
                    if not retained:
                        os.close(fd)
            return None, SLOT_WAIT

    def dispatch_delay(self, org: str, slot: KeenableSlot) -> float:
        with self._guard(org) as (directory, guard, name):
            try:
                flock(guard, LOCK_EX | LOCK_NB)
            except BlockingIOError:
                return SLOT_WAIT
            now = monotonic_ns()
            state = _reap(directory, org, name, _read(directory, name, now), now)
            if slot.number in state.pending:
                raise IntegrityError("Keenable slot already dispatched")
            starts, wait = state.dispatch_window(now)
            if wait:
                return wait
            _write(
                directory,
                name,
                state.model_copy(update={"starts": starts, "pending": (*state.pending, slot.number)}),
            )
            return 0

    def release(self, org: str, slot: KeenableSlot, *, dispatched: bool, retry_after: float) -> None:
        try:
            if dispatched or retry_after > 0:
                with self._guard(org) as (directory, guard, name):
                    flock(guard, LOCK_EX)
                    now = monotonic_ns()
                    state = _read(directory, name, now)
                    if dispatched:
                        if slot.number not in state.pending:
                            raise IntegrityError("Keenable dispatch record missing")
                        state = _finish(state, now, (slot.number,))
                    if retry_after > 0:
                        resume_at = max(state.resume_at, now + int(retry_after * _SECOND))
                        state = state.model_copy(update={"resume_at": resume_at})
                    _write(directory, name, state)
        finally:
            os.close(slot.descriptor)
