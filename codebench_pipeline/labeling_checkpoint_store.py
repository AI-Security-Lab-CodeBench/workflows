"""Persist complete batch conversations at their generation and batch addresses."""

import hashlib
import re
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from pydantic import ConfigDict

from codebench_pipeline.annotation_cache import reviewer_cache_lock
from codebench_pipeline.batch_labeling_rows import BatchRow
from codebench_pipeline.models import FrozenModel
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.repository_config import repository_component
from codebench_pipeline.repository_receipts import publish_bytes
from codebench_pipeline.storage import IntegrityError


class _ConversationCheckpoint(FrozenModel):
    model_config = ConfigDict(strict=True, ser_json_bytes="base64", val_json_bytes="base64")

    generation: str
    batch: str
    rows: list[BatchRow]


def _checkpoint_digest(value: str) -> str:
    if re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise IntegrityError("Labeling checkpoint requires a SHA-256 address")
    return value


@dataclass(frozen=True, slots=True)
class LabelingCheckpointCache:
    """Storage scoped to one dataset partition and complete labeling conversations."""

    root: Path
    operation_root: Path

    @contextmanager
    def claim(self, generation: str, batch: str) -> Generator[None]:
        address = f"{_checkpoint_digest(generation)}/{_checkpoint_digest(batch)}"
        with reviewer_cache_lock(self.root / "labeling-checkpoints-v1" / "locks" / address):
            yield

    def resume(self, generation: str, batch: str) -> list[BatchRow] | None:
        generation, batch = _checkpoint_digest(generation), _checkpoint_digest(batch)
        store = ObjectStore(StorageConfig(local_root=str(self.root), prefix="labeling-checkpoints-v1"))
        try:
            payload = store.read(f"generations/{generation}/{batch}.json")
        except FileNotFoundError:
            return None
        checkpoint = _ConversationCheckpoint.model_validate_json(payload)
        if (
            checkpoint.generation != generation
            or checkpoint.batch != batch
            or not checkpoint.rows
            or any(row.attempt.status != "success" for row in checkpoint.rows)
        ):
            raise IntegrityError("Labeling checkpoint differs from its complete conversation address")
        return checkpoint.rows

    def complete(
        self, generation: str, batch: str, rows: list[BatchRow], operations: dict[str, bytes]
    ) -> None:
        generation, batch = _checkpoint_digest(generation), _checkpoint_digest(batch)
        for key, body in operations.items():
            digest_value = hashlib.sha256(body).hexdigest()
            if key != f"shared-operations/{digest_value}.json":
                raise IntegrityError("Shared operation receipt differs from its content address")
            path = self.operation_root.joinpath(*(repository_component(part) for part in key.split("/")))
            publish_bytes(path, body)
        if not rows or any(row.attempt.status != "success" for row in rows):
            return
        store = ObjectStore(StorageConfig(local_root=str(self.root), prefix="labeling-checkpoints-v1"))
        checkpoint = _ConversationCheckpoint(generation=generation, batch=batch, rows=rows)
        store.write(f"generations/{generation}/{batch}.json", checkpoint.model_dump_json().encode())
