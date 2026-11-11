from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.models import Artifact, DatasetSpec, Identifier, Stage


class IntegrityError(RuntimeError):
    """A persisted artifact no longer matches its recorded content."""


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fingerprint(value: JsonValue) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def inventory(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in sorted(path.rglob("*")):
        if item.is_symlink():
            raise IntegrityError(f"Symlinks are not artifact files: {item}")
        if item.is_file() and item.name != "_artifact.json":
            result[item.relative_to(path).as_posix()] = digest(item)
    return result


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


def load_artifact(path: Path) -> Artifact:
    artifact = Artifact.model_validate_json(path.read_text(encoding="utf-8"))
    if artifact.path.resolve() != path.parent.resolve() or inventory(artifact.path) != artifact.files:
        raise IntegrityError(f"Artifact content mismatch: {path}")
    return artifact


class ArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def materialize(
        self,
        request: ArtifactRequest,
        build: Callable[[Path], dict[str, JsonValue]],
    ) -> tuple[Artifact, bool]:
        destination = self.root / "artifacts" / request.dataset_id / request.stage / request.fingerprint
        manifest = destination / "_artifact.json"
        if manifest.exists():
            return load_artifact(manifest), True
        with exclusive(destination.with_suffix(".lock")):
            if manifest.exists():
                return load_artifact(manifest), True
            with TemporaryDirectory(prefix=".build-", dir=destination.parent) as temporary:
                staging = Path(temporary) / "payload"
                staging.mkdir()
                try:
                    metadata = build(staging)
                except Exception:
                    failure = self.root / "failures" / request.dataset_id / request.stage / str(uuid.uuid4())
                    failure.parent.mkdir(parents=True, exist_ok=True)
                    os.rename(staging, failure)
                    raise
                artifact = Artifact(
                    dataset_id=request.dataset_id,
                    stage=request.stage,
                    fingerprint=request.fingerprint,
                    path=destination,
                    files=inventory(staging),
                    inputs=request.inputs,
                    created_at=datetime.now(UTC).isoformat(),
                    metadata=metadata,
                )
                (staging / "_artifact.json").write_text(artifact.model_dump_json(indent=2), encoding="utf-8")
                os.rename(staging, destination)
        return artifact, False


class ArtifactRequest:
    def __init__(self, spec: DatasetSpec, stage: Stage, inputs: dict[str, str]) -> None:
        self.dataset_id = spec.dataset_id
        self.stage: Stage = stage
        self.inputs = inputs
        self.fingerprint = fingerprint({key: value for key, value in sorted(inputs.items())})


class Registry:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve() / "registry"

    def register(self, spec: DatasetSpec) -> str:
        payload = spec.model_dump_json(indent=2)
        version = hashlib.sha256(payload.encode()).hexdigest()
        directory = self.root / spec.dataset_id
        directory.mkdir(parents=True, exist_ok=True)
        with exclusive(directory / "registration.lock"):
            revision = directory / f"{version}.json"
            if not revision.exists():
                revision.write_text(payload, encoding="utf-8")
            pointer = directory / "current"
            temporary = directory / ".current"
            temporary.write_text(version, encoding="ascii")
            os.replace(temporary, pointer)
        return spec.dataset_id

    def get(self, key: str) -> DatasetSpec:
        directory = self.root / TypeAdapter(Identifier).validate_python(key)
        version = (directory / "current").read_text(encoding="ascii")
        spec = DatasetSpec.model_validate_json((directory / f"{version}.json").read_text(encoding="utf-8"))
        if spec.dataset_id != key:
            raise IntegrityError(f"Registry identity mismatch: {key}")
        return spec

    def keys(self) -> list[str]:
        return sorted(path.parent.name for path in self.root.glob("*/current"))
