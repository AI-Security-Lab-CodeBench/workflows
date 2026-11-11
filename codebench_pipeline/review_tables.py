import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pydantic import Field, JsonValue, TypeAdapter

from codebench_pipeline.dataframe_io import frame_bytes
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.human_review import HumanReviewError, ReviewModel, ReviewSnapshot
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.review_api_models import EvidenceResponse
from codebench_pipeline.review_stages import ReviewInput, ReviewStage
from codebench_pipeline.storage import IntegrityError, fingerprint


@dataclass(frozen=True, slots=True)
class ReviewTable:
    frame: pd.DataFrame
    id_column: str
    revision: str
    evidence: dict[str, JsonValue]
    eligible_ids: tuple[str, ...]
    record_policy: JsonValue = None


class TableReviewManifest(ReviewModel):
    dataset_id: str
    review_stage: str
    id_column: str
    revision: str
    table_digest: str
    rows: list[dict[str, JsonValue]]
    evidence: dict[str, JsonValue]
    eligible_ids: tuple[str, ...]
    min_approvals: int
    max_approvals: int | None
    record_fingerprints: dict[str, str] = Field(default_factory=dict)


class TableReviewSource:
    """Adapt an authoritative table reader without imposing dataset-label columns."""

    def __init__(self, home: Path, stage: ReviewStage, resolve: Callable[[str], ReviewTable]) -> None:
        self.stage = stage
        self.resolve = resolve
        self.store = ObjectStore(
            StorageConfig(local_root=str(home.resolve() / "human-review"), prefix="table-snapshots")
        )

    def current(self, dataset_id: str) -> ReviewInput:
        table = self.resolve(dataset_id)
        ids = TypeAdapter(list[str]).validate_python(table.frame[table.id_column].tolist(), strict=True)
        manifest = TableReviewManifest(
            dataset_id=dataset_id,
            review_stage=self.stage.stage_id,
            id_column=table.id_column,
            revision=table.revision,
            table_digest=sha256(frame_bytes(table.frame)),
            rows=frame_records(table.frame),
            evidence=table.evidence,
            eligible_ids=table.eligible_ids,
            min_approvals=self.stage.min_approvals,
            max_approvals=self.stage.max_approvals,
            record_fingerprints={
                str(row[table.id_column]): fingerprint(
                    {
                        "record": row,
                        "policy": table.record_policy,
                        "stage": self.stage.model_dump_json(),
                    }
                )
                for row in frame_records(table.frame)
            }
            if table.record_policy is not None
            else {},
        )
        payload = json.dumps(
            manifest.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        snapshot = ReviewSnapshot(
            dataset_id=dataset_id,
            review_stage=self.stage.stage_id,
            snapshot_id=sha256(payload),
            candidate_ids=tuple(ids),
            eligible_candidate_ids=table.eligible_ids,
            min_approvals=self.stage.min_approvals,
            max_approvals=self.stage.max_approvals,
            record_fingerprints=manifest.record_fingerprints,
        )
        self.store.write(f"{snapshot.snapshot_id}.json", payload)
        return ReviewInput(snapshot, table.frame, table.id_column)

    def evidence(self, snapshot: ReviewSnapshot, candidate_id: str) -> EvidenceResponse:
        payload = self.store.read(f"{snapshot.snapshot_id}.json")
        if sha256(payload) != snapshot.snapshot_id:
            raise IntegrityError("Review table manifest digest mismatch")
        manifest = TableReviewManifest.model_validate_json(payload)
        if manifest.dataset_id != snapshot.dataset_id or manifest.review_stage != snapshot.review_stage:
            raise HumanReviewError("conflict", "Review table evidence has a different scope")
        for row in manifest.rows:
            if row[manifest.id_column] == candidate_id:
                return EvidenceResponse(fields={"record": row, "evidence": manifest.evidence})
        raise HumanReviewError("not_found", "Record is not in the evidence snapshot")
