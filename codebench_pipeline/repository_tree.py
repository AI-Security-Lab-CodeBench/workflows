from __future__ import annotations

import hashlib
import re
from pathlib import PurePosixPath
from typing import Final, Literal, Protocol, assert_never
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pydantic import BaseModel, ConfigDict, Field

from codebench_pipeline.repository_config import Commit, RepositoryName, RepositoryOriginScope, check_path
from codebench_pipeline.storage import IntegrityError

TREE_LIMIT: Final = 32 * 1024 * 1024
COMPOSE_NAMES: Final = frozenset({"compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml"})
TLDR_PLATFORMS: Final = frozenset(
    {"android", "common", "freebsd", "linux", "netbsd", "openbsd", "osx", "sunos"}
)


class _NoRedirect(HTTPRedirectHandler):
    max_redirections = 0


def fetch_repository_bytes(url: str, limit: int) -> bytes:
    request = Request(url, headers={"User-Agent": "CodeBench/2.0", "Accept-Encoding": "identity"})
    with build_opener(_NoRedirect()).open(request, timeout=45) as response:
        if response.status != 200 or response.geturl() != url:
            raise IntegrityError(f"Repository response did not return the exact requested resource: {url}")
        body = response.read(limit + 1)
    if len(body) > limit:
        raise IntegrityError(f"Repository resource exceeded its capture limit: {url}")
    return body


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
