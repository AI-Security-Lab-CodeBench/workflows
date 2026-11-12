import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from codebench_pipeline.dataset_seed_config import SourceSeed
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.storage import IntegrityError
from scripts.dataset_schema import SECRET_PATTERNS


@dataclass(frozen=True, slots=True)
class RetrievedSource:
    content: str
    sha256: str
    bytes: int
    cache_hit: bool


def retrieve_source(
    source: SourceSeed, maximum_bytes: int, cache: Path | None, evidence: Path | None
) -> RetrievedSource:
    expected = source.sha256 or source.git_blob_sha1
    store = ObjectStore(StorageConfig(local_root=str(cache), prefix="sources")) if cache is not None else None
    key = f"{expected}/body" if expected is not None else ""
    hit = False
    attempts = 0
    if source.local_path is not None:
        with Path(source.local_path).open("rb") as stream:
            body = stream.read(maximum_bytes + 1)
    else:
        cached_body: bytes | None = None
        if store is not None:
            try:
                cached_body = store.read(key)
                hit = True
            except FileNotFoundError:
                cached_body = None
        if cached_body is None:
            while True:
                attempts += 1
                try:
                    with urlopen(
                        Request(source.url, headers={"User-Agent": "CodeBench/1.0", "Accept-Encoding": "identity"}),
                        timeout=60,
                    ) as response:
                        if response.geturl() != source.url:
                            raise IntegrityError(f"Source redirected outside its manifest URL: {source.source_id}")
                        body = bytes(response.read(maximum_bytes + 1))
                    break
                except HTTPError:
                    raise
                except (URLError, TimeoutError, ConnectionError, HTTPException) as error:
                    if evidence is not None:
                        evidence.mkdir(parents=True, exist_ok=True)
                        identity = hashlib.sha256(source.source_id.encode()).hexdigest()
                        (evidence / f"{identity}.attempt-{attempts}.json").write_text(
                            json.dumps({
                                "source_id": source.source_id,
                                "url": source.url,
                                "attempt": attempts,
                                "error_type": type(error).__name__,
                                "error": str(error),
                            }),
                            encoding="utf-8",
                        )
                    if attempts == 3:
                        raise
        else:
            body = cached_body
    if len(body) > maximum_bytes:
        raise IntegrityError(f"Source exceeds configured acquisition size: {source.source_id}")
    actual = hashlib.sha256(body).hexdigest()
    if source.sha256 is not None and actual != source.sha256:
        raise IntegrityError(f"Source SHA-256 differs from manifest: {source.source_id}")
    if source.git_blob_sha1 is not None:
        git_hash = hashlib.sha1(
            f"blob {len(body)}\0".encode() + body, usedforsecurity=False
        ).hexdigest()
        if git_hash != source.git_blob_sha1:
            raise IntegrityError(f"Source Git blob differs from manifest: {source.source_id}")
    text = body.decode("utf-8")
    if not text.strip():
        raise IntegrityError(f"Source body is empty: {source.source_id}")
    if any(pattern.search(text) for pattern in SECRET_PATTERNS):
        raise IntegrityError(f"Sensitive source content withheld: {source.source_id}")
    if store is not None and source.local_path is None and not hit:
        store.write(key, body)
    result = RetrievedSource(text, actual, len(body), hit)
    if evidence is not None:
        evidence.mkdir(parents=True, exist_ok=True)
        identity = hashlib.sha256(source.source_id.encode()).hexdigest()
        (evidence / f"{identity}.json").write_text(
            json.dumps(
                {
                    "source_id": source.source_id,
                    "url": source.url,
                    "sha256": actual,
                    "expected_sha256": source.sha256,
                    "expected_git_blob_sha1": source.git_blob_sha1,
                    "bytes": len(body),
                    "cache_hit": hit,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "download_performed": source.local_path is None and not hit,
                    "download_attempts": attempts,
                    "manifest_observed_at": source.retrieved_at,
                    "complete": True,
                    "transformations": source.transformations,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    return result
