from pathlib import Path
from typing import Literal

from pydantic import TypeAdapter

from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.storage import IntegrityError

type ReviewArtifactNamespace = Literal["snapshots", "table-snapshots"]


def review_artifact_key(namespace: ReviewArtifactNamespace, snapshot_id: str) -> str:
    digest = TypeAdapter(Digest).validate_python(snapshot_id, strict=True)
    return f"review/{namespace}/{digest}.json"


def write_review_artifact(namespace: ReviewArtifactNamespace, payload: bytes, store: ObjectStore) -> str:
    snapshot_id = sha256(payload)
    store.write(review_artifact_key(namespace, snapshot_id), payload)
    return snapshot_id


def read_review_artifact(
    home: Path, namespace: ReviewArtifactNamespace, snapshot_id: str, store: ObjectStore | None
) -> bytes:
    if store is None:
        digest = TypeAdapter(Digest).validate_python(snapshot_id, strict=True)
        legacy = ObjectStore(StorageConfig(local_root=str(home / "human-review"), prefix=namespace))
        payload = legacy.read(f"{digest}.json")
    else:
        payload = store.read(review_artifact_key(namespace, snapshot_id))
    if sha256(payload) != snapshot_id:
        raise IntegrityError("Review artifact digest mismatch")
    return payload
