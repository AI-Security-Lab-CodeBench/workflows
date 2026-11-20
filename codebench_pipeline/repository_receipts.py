from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeVar
from uuid import uuid4

from pandas import DataFrame
from pydantic import JsonValue

from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import (
    RepositoryBranch,
    RepositoryModel,
    repository_component,
)
from codebench_pipeline.storage import IntegrityError, open_directory, open_regular_file, write_new_file

ReceiptStage = Literal["source_catalog", "source_documents", "document_adapters", "command_candidates"]
RECEIPT_ATTR = "repository_receipt_json"
Frame = TypeVar("Frame", bound=DataFrame)


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical(value: JsonValue | list[dict[str, JsonValue]]) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


@dataclass(frozen=True, slots=True)
class RepositoryRun:
    dataset_home: Path
    dataset_id: str
    run_id: str

    def __post_init__(self) -> None:
        repository_component(self.dataset_id)
        repository_component(self.run_id)

    @property
    def root(self) -> Path:
        return self.dataset_home / "repository-v2" / self.dataset_id

    def evidence_exists(self, key: str) -> bool:
        path = _evidence_address(self.root, key)
        try:
            with open_directory(path.parent) as directory, open_regular_file(directory, path.name):
                return True
        except FileNotFoundError:
            return False

    def read_evidence(self, key: str) -> bytes:
        return read_evidence_bytes(self.root, key)

    def read_capture(self, key: str, limit: int) -> bytes:
        return read_evidence_bytes(self.root, key, limit=limit + 1)

    def write_evidence(self, key: str, payload: bytes) -> None:
        publish_bytes(_evidence_address(self.root, key), payload)


class RepositoryReceipt(RepositoryModel):
    stage: ReceiptStage
    branch: RepositoryBranch
    dataset_id: str
    run_id: str
    config_digest: Digest
    table_digest: Digest
    upstream: tuple[Digest, ...]
    observations: dict[str, JsonValue]

    @property
    def body(self) -> bytes:
        return canonical(self.model_dump(mode="json"))

    @property
    def digest(self) -> str:
        return sha256(self.body)

    def follow(
        self, run: RepositoryRun, stage: ReceiptStage, observations: dict[str, JsonValue]
    ) -> RepositoryReceipt:
        return self.model_copy(
            update={
                "run_id": run.run_id,
                "stage": stage,
                "upstream": (self.digest,),
                "observations": {"complete": True, **observations},
            }
        )


def _evidence_address(root: Path, key: str) -> Path:
    return root.joinpath(*(repository_component(part) for part in key.split("/")))


def read_evidence_bytes(root: Path, key: str, *, limit: int = -1) -> bytes:
    """Read validated relative evidence beneath a bound root without following links."""
    path = _evidence_address(root, key)
    with open_directory(path.parent) as directory, open_regular_file(directory, path.name) as stream:
        return stream.read(limit)


def publish_bytes(path: Path, body: bytes) -> None:
    """Atomically publish immutable bytes without following links or replacing a winner."""
    with open_directory(path.parent, create=True) as directory:
        temporary = f".repository-{uuid4().hex}"
        write_new_file(directory, temporary, body)
        try:
            try:
                os.link(
                    temporary, path.name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False
                )
            except FileExistsError:
                with open_regular_file(directory, path.name) as current:
                    if current.read(len(body) + 1) != body:
                        raise IntegrityError(f"Conflicting immutable repository evidence: {path}") from None
        finally:
            os.unlink(temporary, dir_fd=directory)


def record_receipt(frame: Frame, receipt: RepositoryReceipt, run: RepositoryRun) -> Frame:
    completed = receipt.model_copy(update={"table_digest": sha256(canonical(frame_records(frame)))})
    run.write_evidence(f"receipts/{completed.digest}.json", completed.body)
    frame.attrs[RECEIPT_ATTR] = completed.body.decode()
    return frame


def read_receipt(frame: DataFrame, stage: ReceiptStage, run: RepositoryRun) -> RepositoryReceipt:
    serialized = frame.attrs.get(RECEIPT_ATTR)
    if not isinstance(serialized, str):
        raise IntegrityError(f"Missing repository receipt for {stage}")
    receipt = RepositoryReceipt.model_validate_json(serialized)
    key = f"receipts/{receipt.digest}.json"
    if (
        receipt.stage != stage
        or receipt.dataset_id != run.dataset_id
        or receipt.table_digest != sha256(canonical(frame_records(frame)))
        or not run.evidence_exists(key)
        or run.read_evidence(key) != receipt.body
    ):
        raise IntegrityError(f"Repository receipt differs from the persisted dataset evidence: {stage}")
    return receipt
