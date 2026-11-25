import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

from dagster import AssetMaterialization, IntMetadataValue, TextMetadataValue

from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID

type Outcome = Literal["ok", "error", "canceled", "interrupted", "retry", "skipped"]
type Product = Mapping[str, str | int]
type ScalarFields = tuple[tuple[str, str], ...]

_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_CAPTURE_PARTITION: Final = re.compile(r"CAT-[1-9][0-9]*")
_RULES: Final = ("rules", "rule_count")
_TRAIN_ROWS: Final = ("Train rows", "train_rows")
_TRAIN_DIGEST: Final = ("Train digest", "train_digest")
_CONFIG_DIGEST: Final = ("codebench/config_digest", "config_digest")
_EVIDENCE_DIGEST: Final = ("codebench/private_evidence_digest", "evidence_digest")
_CONTENT_DIGEST: Final = ("codebench/content_digest", "content_digest")
_REGISTRATION_DIGEST: Final = ("codebench/registration_digest", "registration_digest")
_TABLE_ROWS: Final = ("dagster/row_count", "table_rows")


@dataclass(frozen=True, slots=True)
class _Asset:
    name: str
    counts: ScalarFields = ()
    digests: ScalarFields = ()


_ASSETS: Final = {
    f"codebench__{asset.name}": asset
    for asset in (
        _Asset(
            "rules_capture",
            (_RULES, ("batches", "batch_count")),
            (("category digest", "category_digest"), _CONFIG_DIGEST),
        ),
        _Asset(
            "rules_dedup",
            (_RULES, ("judge batches", "judge_batch_count"), _TRAIN_ROWS),
            (_TRAIN_DIGEST, _CONFIG_DIGEST),
        ),
        _Asset(
            "rules_finalize",
            (_RULES, _TRAIN_ROWS),
            (("bundle digest", "bundle_digest"), _TRAIN_DIGEST, _CONFIG_DIGEST, _EVIDENCE_DIGEST),
        ),
        _Asset(
            "rules_eval",
            (
                ("Held-out support", "heldout_support"),
                ("Exact matches", "heldout_exact_matches"),
                ("Wrong matches", "heldout_wrong_matches"),
                ("Abstentions", "heldout_abstentions"),
                ("Evaluator errors", "heldout_evaluator_errors"),
                ("Test rows", "test_rows"),
                _TRAIN_ROWS,
                _TABLE_ROWS,
            ),
            (
                ("Bundle digest", "bundle_digest"),
                ("Test digest", "test_digest"),
                _TRAIN_DIGEST,
                _EVIDENCE_DIGEST,
                _CONTENT_DIGEST,
                _REGISTRATION_DIGEST,
            ),
        ),
        _Asset("rules_eval_report", (_TABLE_ROWS,), (_CONTENT_DIGEST, _REGISTRATION_DIGEST)),
    )
}


class RulesTraceOutputs:
    """Accumulate one safe materialization per rules step attempt in event-log order."""

    def __init__(self) -> None:
        self._pending: dict[str, Product | None] = {}
        self._products: dict[str, Product] = {}

    def record(self, step_key: str, materialization: AssetMaterialization) -> None:
        asset = _ASSETS.get(step_key)
        if asset is None or materialization.asset_key.path != ["codebench", asset.name]:
            return
        if step_key in self._pending:
            self._pending[step_key] = None
            return
        safe: dict[str, str | int] = {}
        partition = materialization.partition
        if partition is not None and (
            _CAPTURE_PARTITION.fullmatch(partition) and len(partition) <= 32
            if asset.name == "rules_capture"
            else partition == CODEBENCH_DATASET_ID
        ):
            safe["partition"] = partition
        for source, field in asset.counts:
            value = materialization.metadata.get(source)
            if (
                isinstance(value, IntMetadataValue)
                and value.value is not None
                and type(value.value) is int
                and 0 <= value.value <= 2**53 - 1
            ):
                safe[field] = value.value
        for source, field in asset.digests:
            value = materialization.metadata.get(source)
            if isinstance(value, TextMetadataValue) and _SHA256.fullmatch(value.value):
                safe[field] = value.value
        self._pending[step_key] = safe

    def finish(self, step_key: str, outcome: Outcome) -> Product | None:
        asset = _ASSETS.get(step_key)
        if asset is None:
            return None
        status = "ambiguous" if step_key in self._pending else "missing"
        metadata = self._pending.pop(step_key, None)
        if metadata is not None:
            status = "materialized"
        product = {
            "asset_key": f"codebench/{asset.name}",
            "status": status,
            "execution_status": outcome,
            **(metadata or {}),
        }
        if metadata is not None:
            self._products[step_key] = product
        return product

    def summarize(self, outcome: Outcome) -> str | dict[str, Outcome | list[Product]]:
        if not self._products:
            return outcome
        return {"execution_status": outcome, "products": list(self._products.values())}
