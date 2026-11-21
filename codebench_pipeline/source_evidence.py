from datetime import UTC, datetime
from hashlib import sha256
from typing import Final

import pandas as pd

from codebench_pipeline.collection_config import DeclaredSourceStageConfig, KeenableSnippetDeclaration
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.keenable_policy import KeenableError
from codebench_pipeline.storage import IntegrityError

SOURCE_RECEIPT: Final = "codebench_source_receipt"
_HISTORICAL_SOURCE_RECEIPT: Final = "codebench_v2_source_receipt"


def require_source_rights(config: DeclaredSourceStageConfig) -> None:
    if config.branch == "keenable":
        declaration = config.source.keenable_config()
        if isinstance(declaration, KeenableSnippetDeclaration):
            return
        try:
            declaration.check_current(datetime.now(UTC))
        except KeenableError as error:
            raise HumanReviewError("stale", "Keenable source authorization has expired") from error


def receipt_payload(frame: pd.DataFrame, config: DeclaredSourceStageConfig) -> bytes:
    field = "keenable_receipt_json" if config.branch == "keenable" else "repository_receipt_json"
    receipt = frame.attrs.get(field)
    if not isinstance(receipt, str):
        raise IntegrityError(f"{config.branch} output lacks its acquisition receipt")
    return receipt.encode()


def retain_source_receipt(
    frame: pd.DataFrame, config: DeclaredSourceStageConfig, storage: DataFrameIOManager
) -> None:
    payload = receipt_payload(frame, config)
    digest = sha256(payload).hexdigest()
    key = f"source-receipts/{config.branch}/{digest}"
    storage.write_evidence(key, payload)
    frame.attrs[SOURCE_RECEIPT] = {"key": key, "digest": digest, "declaration": config.declaration_digest}


def validate_source_receipt(
    frame: pd.DataFrame, config: DeclaredSourceStageConfig, storage: DataFrameIOManager
) -> None:
    payload = receipt_payload(frame, config)
    digest = sha256(payload).hexdigest()
    if SOURCE_RECEIPT in frame.attrs:
        binding = frame.attrs[SOURCE_RECEIPT]
        key = f"source-receipts/{config.branch}/{digest}"
    else:
        binding = frame.attrs.get(_HISTORICAL_SOURCE_RECEIPT)
        key = f"v2-source-receipts/{config.branch}/{digest}"
    if binding != {
        "key": key,
        "digest": digest,
        "declaration": config.declaration_digest,
    }:
        raise HumanReviewError("stale", f"{config.branch} acquisition receipt binding changed")
    if storage.read_evidence(key) != payload:
        raise IntegrityError(f"{config.branch} acquisition receipt bytes changed")
