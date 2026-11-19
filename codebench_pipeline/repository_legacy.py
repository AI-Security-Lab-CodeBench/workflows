from __future__ import annotations

import os
import re
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from stat import S_ISREG
from typing import Final

from codebench_pipeline.repository_config import Commit
from codebench_pipeline.repository_receipts import sha256
from codebench_pipeline.storage import IntegrityError

_DIRECTORY: Final = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
_REGULAR: Final = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
_COMPLETION: Final = re.compile(rb"[a-f0-9]{64}")


@contextmanager
def _directory(name: str | Path, parent: int | None = None) -> Generator[int, None, None]:
    flags = _DIRECTORY | os.O_NOFOLLOW if parent is not None else _DIRECTORY
    fd = os.open(name, flags, dir_fd=parent)
    try:
        yield fd
    finally:
        os.close(fd)


def _read_regular(parent: int, name: str, size: int) -> bytes:
    fd = os.open(name, _REGULAR, dir_fd=parent)
    try:
        with os.fdopen(fd, "rb", closefd=False) as stream:
            stat = os.fstat(fd)
            if not S_ISREG(stat.st_mode) or stat.st_size != size:
                raise IntegrityError(f"Legacy cache member has incorrect type or size: {name}")
            content = stream.read(size + 1)
            if len(content) != size:
                raise IntegrityError(f"Legacy cache member changed while being read: {name}")
            return content
    finally:
        os.close(fd)


def read_legacy_capture(home: Path, git_blob: Commit, expected_size: int) -> bytes | None:
    """Read a completed prepared-source object without following links inside the operator's cache root."""
    with ExitStack() as directories:
        try:
            root = directories.enter_context(_directory(home / "source-downloads"))
            sources = directories.enter_context(_directory("sources", root))
            key = directories.enter_context(_directory(git_blob, sources))
        except FileNotFoundError:
            return None
        try:
            body = directories.enter_context(_directory("body", key))
            pointer = _read_regular(body, "current", 64)
            if _COMPLETION.fullmatch(pointer) is None:
                raise IntegrityError(f"Invalid legacy cache SHA-256 pointer: {git_blob}")
            expected_sha256 = pointer.decode("ascii")
            revision = directories.enter_context(_directory(expected_sha256, body))
            content = _read_regular(revision, "data", expected_size)
        except FileNotFoundError as error:
            raise IntegrityError(f"Incomplete legacy cache object: {git_blob}") from error
        if sha256(content) != expected_sha256:
            raise IntegrityError(f"Legacy cache object SHA-256 differs: {git_blob}")
        return content
