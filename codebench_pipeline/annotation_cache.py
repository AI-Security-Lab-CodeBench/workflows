import fcntl
import hashlib
import json
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Final

from pydantic import BaseModel, ConfigDict

from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.storage import IntegrityError, digest, exclusive, fingerprint

ARTIFACT_WRITES: Final = Lock()


@dataclass(slots=True)
class AnnotationContextError(ValueError):
    """Exception tracebacks remain mutable for context-manager propagation."""

    candidate_id: str
    characters: int
    maximum: int

    def __str__(self) -> str:
        return f"Annotation context for {self.candidate_id} has {self.characters} characters; maximum is {self.maximum}"


@dataclass(frozen=True, slots=True)
class RowRequest:
    candidate_id: str
    system: str
    prompt: str
    characters: int


class CachedInvocation(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
    response: str
    artifacts: dict[str, str]


@contextmanager
def reviewer_cache_lock(cache: Path | None) -> Generator[None]:
    if cache is None:
        yield
        return
    cache.mkdir(parents=True, exist_ok=True)
    with (cache / "reviewer.lock").open("ab") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def bounded_packets(
    requests: Iterable[RowRequest],
    maximum_records: int,
    maximum_characters: int,
) -> Iterator[tuple[RowRequest, ...]]:
    if not 1 <= maximum_records <= 64 or maximum_characters < 1:
        raise ValueError("Annotation packet bounds must be positive and at most 64 records")
    packet: list[RowRequest] = []
    characters = 0
    for request in requests:
        if request.characters > maximum_characters:
            raise AnnotationContextError(request.candidate_id, request.characters, maximum_characters)
        if packet and (
            len(packet) == maximum_records or characters + request.characters > maximum_characters
        ):
            yield tuple(packet)
            packet = []
            characters = 0
        packet.append(request)
        characters += request.characters
    if packet:
        yield tuple(packet)


def invocation_version(
    schema: type[BaseModel],
    request: RowRequest,
    model: ModelConfig,
) -> str:
    package = Path(__file__).parent
    return fingerprint(
        {
            "system": request.system,
            "prompt": request.prompt,
            "schema": schema.model_json_schema(),
            "model": model.model_dump_json(),
            "implementation": {
                name: digest(package / name)
                for name in (
                    "annotation_cache.py",
                    "dataset_annotation.py",
                    "dataset_records.py",
                    "business_config.py",
                    "category_rules.py",
                    "model_config.py",
                    "inference.py",
                    "agents.py",
                    "openai_transport.py",
                    "mcp_transport.py",
                )
            },
        }
    )


def read_cached_invocation[Model: BaseModel](
    schema: type[Model],
    request: RowRequest,
    model: ModelConfig,
    out: Path,
    cache: Path | None,
) -> Model | None:
    if cache is None:
        return None
    version = invocation_version(schema, request, model)
    store = ObjectStore(StorageConfig(local_root=str(cache), prefix="invocations"))
    key = f"requests/{version}.json"
    try:
        stored = CachedInvocation.model_validate_json(store.read(key))
    except FileNotFoundError:
        return None
    result = schema.model_validate_json(stored.response)
    out.mkdir(parents=True, exist_ok=False)
    for name, content in stored.artifacts.items():
        path = out / name
        if not path.resolve().is_relative_to(out.resolve()):
            raise IntegrityError("Cached inference artifact escaped its invocation directory")
        payload = store.read(f"artifacts/{content}")
        if hashlib.sha256(payload).hexdigest() != content:
            raise IntegrityError("Cached inference evidence digest mismatch")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    (out / "reuse.json").write_text(
        json.dumps(
            {
                "input_fingerprint": version,
                "cache_hit": True,
                "original_invocation": store.uri(key),
                "artifacts": stored.artifacts,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return result


def save_cached_invocation(
    schema: type[BaseModel],
    request: RowRequest,
    model: ModelConfig,
    out: Path,
    cache: Path | None,
    result: BaseModel,
) -> None:
    if cache is None:
        return
    version = invocation_version(schema, request, model)
    store = ObjectStore(StorageConfig(local_root=str(cache), prefix="invocations"))
    key = f"requests/{version}.json"
    out.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, str] = {}
    for path in sorted(out.rglob("*")):
        if path.is_file():
            payload = path.read_bytes()
            content = sha256(payload)
            with ARTIFACT_WRITES:
                store.write(f"artifacts/{content}", payload)
            artifacts[path.relative_to(out).as_posix()] = content
    stored = CachedInvocation(response=result.model_dump_json(), artifacts=artifacts)
    store.write(key, stored.model_dump_json().encode())
    (out / "reuse.json").write_text(
        json.dumps(
            {
                "input_fingerprint": version,
                "cache_hit": False,
                "original_invocation": store.uri(key),
                "artifacts": artifacts,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def cached_invocation[Model: BaseModel](
    schema: type[Model],
    request: RowRequest,
    model: ModelConfig,
    out: Path,
    cache: Path | None,
    compute: Callable[[], Model],
) -> Model:
    if cache is None:
        return compute()
    version = invocation_version(schema, request, model)
    with exclusive(cache / "locks" / f"{version}.lock"):
        previous = read_cached_invocation(schema, request, model, out, cache)
        if previous is not None:
            return previous
        result = compute()
        save_cached_invocation(schema, request, model, out, cache, result)
        return result
