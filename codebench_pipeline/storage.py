from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from errno import ELOOP, ENOTDIR
from pathlib import Path
from stat import S_ISREG
from typing import BinaryIO

from pydantic import JsonValue


class IntegrityError(RuntimeError):
    """A persisted artifact no longer matches its recorded content."""


_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ_FILE = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


def _require_component(name: str) -> None:
    if name in {"", ".", ".."} or "/" in name or "\0" in name:
        raise IntegrityError(f"Invalid local evidence component: {name!r}")


def _directory_descriptor(parent: int, name: str, *, create: bool = False) -> int:
    _require_component(name)
    try:
        try:
            return os.open(name, _DIRECTORY, dir_fd=parent)
        except FileNotFoundError:
            if not create:
                raise
            try:
                os.mkdir(name, dir_fd=parent)
            except FileExistsError:
                pass
            return os.open(name, _DIRECTORY, dir_fd=parent)
    except OSError as error:
        if error.errno in (ELOOP, ENOTDIR):
            raise IntegrityError(f"Linked or non-directory local evidence component: {name}") from error
        raise


@contextmanager
def open_directory(path: Path, *, create: bool = False) -> Generator[int]:
    absolute = path.absolute()
    if ".." in absolute.parts:
        raise IntegrityError(f"Invalid local evidence path: {path}")
    anchor, *parts = absolute.parts
    with ExitStack() as stack:
        directory = os.open(anchor, _DIRECTORY)
        stack.callback(os.close, directory)
        for part in parts:
            directory = _directory_descriptor(directory, part, create=create)
            stack.callback(os.close, directory)
        yield directory


@contextmanager
def open_child_directory(directory: int, name: str) -> Generator[int]:
    descriptor = _directory_descriptor(directory, name)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


@contextmanager
def open_regular_file(directory: int, name: str) -> Generator[BinaryIO]:
    _require_component(name)
    try:
        descriptor = os.open(name, _READ_FILE, dir_fd=directory)
    except OSError as error:
        if error.errno == ELOOP:
            raise IntegrityError(f"Linked local evidence: {name}") from error
        raise
    try:
        if not S_ISREG(os.fstat(descriptor).st_mode):
            raise IntegrityError(f"Local evidence is not a regular file: {name}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            yield stream
    finally:
        os.close(descriptor)


def write_new_file(directory: int, name: str, body: bytes, *, mode: int = 0o600) -> None:
    _require_component(name)
    descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, mode, dir_fd=directory)
    try:
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            os.close(descriptor)
    except OSError:
        os.unlink(name, dir_fd=directory)
        raise


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fingerprint(value: JsonValue) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


@contextmanager
def exclusive(path: Path) -> Generator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise IntegrityError(f"Another run owns {path}; inspect it before retrying") from error
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(str(os.getpid()))
        yield
    finally:
        path.unlink()
