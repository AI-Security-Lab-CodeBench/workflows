from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from http.client import HTTPException
from pathlib import PurePosixPath
from typing import Literal

from pydantic import JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.repository_config import Commit, RepositoryModel, RepositoryOrigin
from codebench_pipeline.repository_legacy import read_legacy_capture
from codebench_pipeline.repository_receipts import RepositoryRun, canonical, publish_bytes, sha256
from codebench_pipeline.repository_tree import (
    TREE_LIMIT,
    TreeEntry,
    fetch_repository_bytes,
    git_blob_sha1,
    raw_url,
    scope_matches,
    tree_url,
    undeclared_notices,
    validate_tree,
)
from codebench_pipeline.storage import IntegrityError
from scripts.dataset_schema import SECRET_PATTERNS


class CatalogSource(RepositoryModel):
    origin_id: str
    source_id: str
    path: str
    git_blob_sha1: Commit
    size: int
    exclusion: str = ""


class CapturedSource(RepositoryModel):
    source_id: str
    origin_id: str
    git_blob_sha1: str
    sha256: str
    capture_key: str
    size: int
    response_status: int | None
    cache_hit: bool
    acquired_via: Literal[
        "https", "repository_cache", "verified_prepared_cache", "v2_cache", "verified_v1_cache"
    ] = "https"
    retrieved_at: str


def _fetch_blob(origin: RepositoryOrigin, entry: TreeEntry, limit: int) -> bytes:
    body = fetch_repository_bytes(raw_url(origin, entry.path), limit)
    if git_blob_sha1(body) != entry.sha or len(body) != entry.size:
        raise IntegrityError(
            f"Repository Git blob does not match the pinned tree: {origin.origin_id}/{entry.path}"
        )
    return body


def catalog_origin(
    origin: RepositoryOrigin, run: RepositoryRun, limit: int
) -> tuple[list[CatalogSource], dict[str, JsonValue]]:
    tree_path = run.root / "trees" / f"{origin.tree_digest}.json"
    raw = (
        tree_path.read_bytes() if tree_path.exists() else fetch_repository_bytes(tree_url(origin), TREE_LIMIT)
    )
    entries = validate_tree(raw, origin)
    publish_bytes(tree_path, raw)
    files = {entry.path: entry for entry in entries if entry.regular}
    missing_inclusions = set(origin.included_paths) - files.keys()
    if missing_inclusions:
        raise IntegrityError(
            f"Declared Markdown files are absent or nonregular for {origin.origin_id}: "
            f"{sorted(missing_inclusions)}"
        )
    rights: list[JsonValue] = []
    for item in (origin.license, *origin.notices):
        if item.path not in files:
            raise IntegrityError(f"Pinned rights file is absent: {origin.origin_id}/{item.path}")
        entry = files[item.path]
        proof_path = run.root / "rights" / f"{item.sha256}.json"
        proof = canonical({"sha256": item.sha256, "git_blob_sha1": entry.sha})
        if not proof_path.exists():
            body = _fetch_blob(origin, entry, limit)
            if sha256(body) != item.sha256:
                raise IntegrityError(f"Rights evidence SHA-256 differs: {origin.origin_id}/{item.path}")
        publish_bytes(proof_path, proof)
        rights.append(
            {
                "url": raw_url(origin, item.path),
                "path": item.path,
                "sha256": item.sha256,
                "git_blob_sha1": entry.sha,
                "license_id": origin.license_id,
                "use": origin.use,
                "attribution": origin.attribution,
            }
        )

    regular_paths = set(files)
    rights_paths = {item.path for item in (origin.license, *origin.notices)}
    independent_notices = undeclared_notices(origin, entries)
    sources: list[CatalogSource] = []
    for entry in entries:
        if (
            entry.type == "tree"
            or entry.path in rights_paths
            or not scope_matches(origin, entry, regular_paths)
        ):
            continue
        path = PurePosixPath(entry.path)
        exclusion = ""
        if not entry.regular:
            exclusion = "not_regular_file"
        elif entry.path in origin.excluded_paths:
            exclusion = "withheld_by_reviewed_scope"
        elif any(
            path.parent.is_relative_to(PurePosixPath(notice).parent)
            or PurePosixPath(notice).parent.is_relative_to(path.parent)
            for notice in independent_notices
        ):
            exclusion = "unresolved_local_notice"
        sources.append(
            CatalogSource(
                origin_id=origin.origin_id,
                source_id=f"{origin.origin_id}-{sha256(entry.path.encode())[:24]}",
                path=entry.path,
                git_blob_sha1=entry.sha,
                size=entry.size or 0,
                exclusion=exclusion,
            )
        )
    missing_exclusions = set(origin.excluded_paths) - {source.path for source in sources}
    if not sources or missing_exclusions:
        raise IntegrityError(
            f"Missing declared scope or exclusions for {origin.origin_id}: {sorted(missing_exclusions)}"
        )
    return sources, {
        "repository": origin.repository,
        "commit": origin.commit,
        "tree_digest": origin.tree_digest,
        "tree_url": tree_url(origin),
        "tree_entries": len(entries),
        "scope_entries": len(sources),
        "rights": rights,
        "unresolved_notice_paths": [path for path in sorted(independent_notices)],
    }


def capture_sources(
    sources: tuple[CatalogSource, ...], origin: RepositoryOrigin, runtime: CaptureRuntime
) -> list[tuple[CapturedSource, str]]:
    def capture(source: CatalogSource) -> tuple[CapturedSource, str]:
        capture_key = f"blobs/{source.git_blob_sha1}"
        path = runtime.run.root / capture_key
        cached = path.is_file()
        digest_path = path.with_suffix(".sha256")
        try:
            if source.size > runtime.limit:
                raise IntegrityError(f"Repository capture exceeds the configured size: {source.path}")
            method: Literal["https", "repository_cache", "verified_prepared_cache"]
            if cached:
                body, method = path.read_bytes(), "repository_cache"
            else:
                imported = read_legacy_capture(runtime.run.dataset_home, source.git_blob_sha1, source.size)
                if imported is None:
                    body, method = (
                        fetch_repository_bytes(raw_url(origin, source.path), runtime.limit),
                        "https",
                    )
                else:
                    body, method = imported, "verified_prepared_cache"
            checksum = sha256(body)
            if cached and digest_path.read_text(encoding="ascii") != checksum:
                raise IntegrityError(f"Stored capture SHA-256 differs: {source.path}")
            if len(body) != source.size or git_blob_sha1(body) != source.git_blob_sha1:
                raise IntegrityError(f"Repository capture Git blob differs: {source.path}")
            text = body.decode("utf-8")
            if any(pattern.search(text) for pattern in SECRET_PATTERNS):
                raise IntegrityError(f"Sensitive repository capture withheld: {source.path}")
            publish_bytes(digest_path, checksum.encode("ascii"))
            publish_bytes(path, body)
        except (OSError, ValueError, IntegrityError, HTTPException) as error:
            failure = canonical(
                {
                    "dataset_id": runtime.run.dataset_id,
                    "run_id": runtime.run.run_id,
                    "source_id": source.source_id,
                    "origin_id": source.origin_id,
                    "path": source.path,
                    "stage": "source_documents",
                    "error_type": type(error).__name__,
                    "complete": False,
                }
            )
            publish_bytes(
                runtime.run.root
                / "failures"
                / runtime.run.run_id
                / f"{source.source_id}-{sha256(failure)}.json",
                failure,
            )
            raise
        return CapturedSource(
            source_id=source.source_id,
            origin_id=source.origin_id,
            git_blob_sha1=source.git_blob_sha1,
            sha256=checksum,
            capture_key=capture_key,
            size=len(body),
            response_status=200 if method == "https" else None,
            cache_hit=cached,
            acquired_via=method,
            retrieved_at=datetime.now(UTC).isoformat(),
        ), text

    with ThreadPoolExecutor(max_workers=runtime.workers) as executor:
        return list(executor.map(capture, sources))


class CaptureRuntime(RepositoryModel):
    limit: int
    workers: int
    run: RepositoryRun


CATALOG_SOURCES = TypeAdapter(tuple[CatalogSource, ...])
CAPTURED_SOURCES = TypeAdapter(tuple[CapturedSource, ...])


def origin_receipt_failure(
    run: RepositoryRun,
    origin: RepositoryOrigin,
    error: ValidationError | IntegrityError | OSError | HTTPException,
) -> None:
    failure = canonical(
        {
            "dataset_id": run.dataset_id,
            "run_id": run.run_id,
            "origin_id": origin.origin_id,
            "stage": "source_catalog",
            "commit": origin.commit,
            "error_type": type(error).__name__,
            "complete": False,
        }
    )
    publish_bytes(run.root / "failures" / run.run_id / f"{origin.origin_id}-{sha256(failure)}.json", failure)
