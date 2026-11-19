from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal, TypeVar

from pandas import DataFrame
from pydantic import JsonValue

from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import RepositoryBranch, RepositoryModel, repository_component
from codebench_pipeline.storage import IntegrityError

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


def publish_bytes(path: Path, body: bytes) -> None:
    """Atomically publish immutable bytes without overwriting concurrent evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent) as temporary:
        temporary.write(body)
        temporary.flush()
        os.fsync(temporary.fileno())
        try:
            os.link(temporary.name, path)
        except FileExistsError:
            if path.read_bytes() != body:
                raise IntegrityError(f"Conflicting immutable repository evidence: {path}") from None


def record_receipt(frame: Frame, receipt: RepositoryReceipt, run: RepositoryRun) -> Frame:
    completed = receipt.model_copy(update={"table_digest": sha256(canonical(frame_records(frame)))})
    publish_bytes(run.root / "receipts" / f"{completed.digest}.json", completed.body)
    frame.attrs[RECEIPT_ATTR] = completed.body.decode()
    return frame


def read_receipt(frame: DataFrame, stage: ReceiptStage, run: RepositoryRun) -> RepositoryReceipt:
    serialized = frame.attrs.get(RECEIPT_ATTR)
    if not isinstance(serialized, str):
        raise IntegrityError(f"Missing repository receipt for {stage}")
    receipt = RepositoryReceipt.model_validate_json(serialized)
    path = run.root / "receipts" / f"{receipt.digest}.json"
    if (
        receipt.stage != stage
        or receipt.dataset_id != run.dataset_id
        or receipt.table_digest != sha256(canonical(frame_records(frame)))
        or not path.is_file()
        or path.read_bytes() != receipt.body
    ):
        raise IntegrityError(f"Repository receipt differs from the persisted dataset evidence: {stage}")
    return receipt
