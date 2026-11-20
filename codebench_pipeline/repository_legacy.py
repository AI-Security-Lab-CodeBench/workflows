from __future__ import annotations

import os
import re
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from stat import S_ISREG
from typing import Final

from codebench_pipeline.repository_config import Commit
from codebench_pipeline.repository_receipts import sha256
from codebench_pipeline.storage import IntegrityError, open_child_directory

_DIRECTORY: Final = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
_REGULAR: Final = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
_COMPLETION: Final = re.compile(rb"[a-f0-9]{64}")


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


@dataclass(frozen=True, slots=True)
class PreparedCaptureCache:
    """Read-only access to the operator's prepared-source cache."""

    root: Path

    def read(self, git_blob: Commit, expected_size: int) -> bytes | None:
        """Read a completed object without following links inside the cache root."""
        with ExitStack() as directories:
            try:
                root = os.open(self.root, _DIRECTORY)
                directories.callback(os.close, root)
                sources = directories.enter_context(open_child_directory(root, "sources"))
                key = directories.enter_context(open_child_directory(sources, git_blob))
            except FileNotFoundError:
                return None
            try:
                body = directories.enter_context(open_child_directory(key, "body"))
                pointer = _read_regular(body, "current", 64)
                if _COMPLETION.fullmatch(pointer) is None:
                    raise IntegrityError(f"Invalid legacy cache SHA-256 pointer: {git_blob}")
                expected_sha256 = pointer.decode("ascii")
                revision = directories.enter_context(open_child_directory(body, expected_sha256))
                content = _read_regular(revision, "data", expected_size)
            except FileNotFoundError as error:
                raise IntegrityError(f"Incomplete legacy cache object: {git_blob}") from error
            if sha256(content) != expected_sha256:
                raise IntegrityError(f"Legacy cache object SHA-256 differs: {git_blob}")
            return content
