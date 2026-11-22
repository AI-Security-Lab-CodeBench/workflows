from __future__ import annotations

import errno
import hashlib
import random
import re
import socket
import ssl
import time
from atexit import register
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from functools import cache
from http.client import HTTPException
from http.cookiejar import CookieJar, DefaultCookiePolicy
from pathlib import PurePosixPath
from threading import Lock
from typing import Final, Literal, Protocol, assert_never
from urllib.parse import quote

import httpx2
from pydantic import BaseModel, ConfigDict, Field

from codebench_pipeline.repository_config import Commit, RepositoryName, RepositoryOriginScope, check_path
from codebench_pipeline.storage import IntegrityError

TREE_LIMIT: Final = 32 * 1024 * 1024
COMPOSE_NAMES: Final = frozenset({"compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml"})
TLDR_PLATFORMS: Final = frozenset(
    {"android", "common", "freebsd", "linux", "netbsd", "openbsd", "osx", "sunos"}
)
_ATTEMPTS: Final = 4
_RETRYABLE_STATUS: Final = frozenset({408, 429, 500, 502, 503, 504})
_MAX_RETRY_DELAY: Final = 10
_TRANSIENT_ERRNOS: Final = frozenset({errno.ENETDOWN, errno.ENETUNREACH, errno.EHOSTUNREACH})
_CLIENT_LOCK: Final = Lock()


@cache
def _repository_client() -> httpx2.Client:
    """Share keep-alive connections across all repository capture workers in this process."""
    client = httpx2.Client(
        verify=True,
        follow_redirects=False,
        timeout=httpx2.Timeout(connect=5.0, read=30.0, write=10.0, pool=10.0),
        limits=httpx2.Limits(max_connections=200, max_keepalive_connections=40, keepalive_expiry=30.0),
        headers={"User-Agent": "CodeBench/2.0", "Accept-Encoding": "identity"},
        cookies=CookieJar(DefaultCookiePolicy(allowed_domains=())),
    )
    register(client.close)
    return client


def _retry_after(value: str) -> float:
    header = value.strip()
    if not header:
        return 0.0
    if re.fullmatch(r"[0-9]+", header):
        digits = header.lstrip("0")
        if len(digits) > len(str(_MAX_RETRY_DELAY)):
            return _MAX_RETRY_DELAY
        return min(_MAX_RETRY_DELAY, int(digits or "0"))
    try:
        until = parsedate_to_datetime(header)
    except (ValueError, OverflowError):
        return 0.0
    until = until.replace(tzinfo=UTC) if until.tzinfo is None else until
    return min(_MAX_RETRY_DELAY, max(0.0, (until - datetime.now(UTC)).total_seconds()))


def _transient_transport(error: httpx2.TransportError) -> bool:
    """Distinguish broken connections from malformed protocol, DNS, or TLS failures."""
    if isinstance(error, (httpx2.ConnectTimeout, httpx2.ReadTimeout, httpx2.WriteTimeout)):
        return True
    if isinstance(error, httpx2.RemoteProtocolError):
        return str(error).startswith(
            (
                "Server disconnected without sending a response.",
                "peer closed connection without sending complete message body",
            )
        )
    cause = error.__cause__
    while cause is not None:
        if isinstance(cause, ssl.SSLError):
            return isinstance(cause, ssl.SSLEOFError)
        if isinstance(cause, socket.gaierror):
            return cause.errno == socket.EAI_AGAIN
        if isinstance(cause, (ConnectionError, TimeoutError)):
            return True
        if isinstance(cause, OSError):
            return cause.errno in _TRANSIENT_ERRNOS
        cause = cause.__cause__
    return False


def fetch_repository_bytes(url: str, limit: int) -> bytes:
    """Fetch a complete, size-bounded pinned resource, retrying only transient failures."""
    with _CLIENT_LOCK:
        client = _repository_client()
    for attempt in range(1, _ATTEMPTS + 1):
        retry_after = 0.0
        try:
            with client.stream("GET", url) as response:
                status = response.status_code
                if status == 200:
                    return _read_repository_body(response, url, limit)
                if status not in _RETRYABLE_STATUS or attempt == _ATTEMPTS:
                    raise HTTPException(f"Repository HTTP {status} {response.reason_phrase}: {url}")
                retry_after = _retry_after(response.headers.get("Retry-After", ""))
        except httpx2.TransportError as error:
            if attempt == _ATTEMPTS or not _transient_transport(error):
                raise HTTPException(f"Repository transport failed for {url}: {error}") from error
        base = 0.5 * 2 ** (attempt - 1)
        time.sleep(min(_MAX_RETRY_DELAY, max(base, retry_after) + random.uniform(0, base / 2)))
    raise AssertionError("The final repository attempt must return or raise")


def _read_repository_body(response: httpx2.Response, url: str, limit: int) -> bytes:
    if str(response.url) != url:
        raise IntegrityError(f"Repository response did not return the exact requested resource: {url}")
    length = response.headers.get("Content-Length")
    if length is not None and int(length) > limit:
        raise IntegrityError(f"Repository resource exceeded its capture limit: {url}")
    body = bytearray()
    for chunk in response.iter_raw():
        if len(chunk) > limit - len(body):
            raise IntegrityError(f"Repository resource exceeded its capture limit: {url}")
        body.extend(chunk)
    return bytes(body)


class RepositoryLocation(Protocol):
    @property
    def repository(self) -> RepositoryName: ...

    @property
    def commit(self) -> Commit: ...


def raw_url(origin: RepositoryLocation, path: str) -> str:
    return f"https://raw.githubusercontent.com/{origin.repository}/{origin.commit}/{quote(path, safe='/')}"


def tree_url(origin: RepositoryLocation) -> str:
    return f"https://api.github.com/repos/{origin.repository}/git/trees/{origin.commit}?recursive=1"


class TreeEntry(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    path: str
    mode: Literal["100644", "100755", "120000", "040000", "160000"]
    type: Literal["blob", "tree", "commit"]
    sha: Commit
    size: int | None = Field(default=None, ge=0)

    @property
    def regular(self) -> bool:
        return self.type == "blob" and self.mode in {"100644", "100755"} and self.size is not None


class RepositoryTree(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sha: Commit
    truncated: bool
    tree: tuple[TreeEntry, ...]


def git_blob_sha1(body: bytes) -> str:
    return hashlib.sha1(f"blob {len(body)}\0".encode() + body, usedforsecurity=False).hexdigest()


def validate_tree(raw: bytes, origin: RepositoryOriginScope) -> tuple[TreeEntry, ...]:
    response = RepositoryTree.model_validate_json(raw)
    if response.truncated or response.sha != origin.commit or not response.tree:
        raise IntegrityError(f"Truncated, empty or unexpected tree for {origin.origin_id}")
    entries = tuple(sorted(response.tree, key=lambda entry: entry.path))
    paths = {entry.path: entry for entry in entries}
    if len(entries) != len(paths):
        raise IntegrityError(f"Duplicate paths in tree for {origin.origin_id}")
    normalized = hashlib.sha256()
    for entry in entries:
        check_path(entry.path)
        parent = entry.path.rpartition("/")[0]
        if parent and (parent not in paths or paths[parent].type != "tree" or paths[parent].mode != "040000"):
            raise IntegrityError(f"Incomplete or non-directory tree parent: {entry.path}")
        size = "" if entry.size is None else str(entry.size)
        normalized.update(f"{entry.path}\t{entry.mode}\t{entry.type}\t{entry.sha}\t{size}\n".encode())
    if normalized.hexdigest() != origin.tree_digest:
        raise IntegrityError(f"Tree contents differ from approved inventory for {origin.origin_id}")
    return entries


def scope_matches(origin: RepositoryOriginScope, entry: TreeEntry, regular_paths: set[str]) -> bool:
    path = PurePosixPath(entry.path)
    match origin.scope:
        case "cheatsheet":
            return len(path.parts) == 1 and not path.name.startswith(".") and path.name != "README.md"
        case "tldr":
            return (
                len(path.parts) == 3
                and path.parts[0] == "pages"
                and path.parts[1] in TLDR_PLATFORMS
                and path.suffix == ".md"
            )
        case "aws_examples":
            return path.parts[:2] == ("awscli", "examples") and path.suffix == ".rst"
        case "markdown_files":
            return entry.path in origin.included_paths
        case "readme" | "compose":
            if (
                len(path.parts) < 2
                or path.name != "README.md"
                or {".github", "test", "tests", "vendor"}.intersection(path.parts)
            ):
                return False
            return (
                path.parts[0] != "_archived"
                if origin.scope == "readme"
                else any((path.parent / name).as_posix() in regular_paths for name in COMPOSE_NAMES)
            )
    assert_never(origin.scope)


def undeclared_notices(origin: RepositoryOriginScope, entries: tuple[TreeEntry, ...]) -> set[str]:
    if origin.scope not in {"readme", "compose", "markdown_files"}:
        return set()
    pinned = {item.path for item in (origin.license, *origin.notices)}
    return {
        entry.path
        for entry in entries
        if entry.regular
        and re.fullmatch(
            r"(?:COPYING|NOTICE|AUTHORS|COPYRIGHT|LICENSE)(?:\.[^/]+)?",
            PurePosixPath(entry.path).name,
            re.I,
        )
        and entry.path not in pinned
    }
