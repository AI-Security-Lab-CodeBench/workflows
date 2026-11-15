import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from pydantic import Field, JsonValue, TypeAdapter

from codebench_pipeline.dataframe_io import frame_bytes
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.human_review import HumanReviewError, ReviewModel, ReviewSnapshot
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.review_api_models import EvidenceResponse
from codebench_pipeline.review_identity import review_identity
from codebench_pipeline.review_stages import ReviewInput, ReviewStage
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.table_query import TableQuery, query_table


@dataclass(frozen=True, slots=True)
class ReviewTable:
    frame: pd.DataFrame
    id_column: str
    revision: str
    evidence: dict[str, JsonValue]
    eligible_ids: tuple[str, ...]
    record_policy: JsonValue = None
    artifact_id: str = ""
    scope_id: str = ""
    membership: TableQuery = field(default_factory=TableQuery)
    column_schema: dict[str, dict[str, JsonValue]] = field(default_factory=dict)
    validation: dict[str, JsonValue] = field(default_factory=dict)
    partition_key: str | None = None


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
    review_columns: tuple[str, ...] = ()
    artifact_id: str = ""
    scope_id: str = ""
    membership: TableQuery = Field(default_factory=TableQuery)
    column_schema: dict[str, dict[str, JsonValue]] = Field(default_factory=dict)
    validation: dict[str, JsonValue] = Field(default_factory=dict)
    partition_key: str | None = None


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
        if table.membership.offset or table.membership.sort:
            raise HumanReviewError("conflict", "Preparation membership accepts search and filters, not paging or sort")
        selected: list[int] = []
        query = table.membership.model_copy(update={"offset": 0, "limit": 200})
        query_table(table.frame.iloc[:0], query)
        for start in range(0, len(table.frame), 200):
            page = query_table(table.frame.iloc[start:start + 200], query)
            selected.extend(start + row.row_index for row in page.rows)
        frame = table.frame.iloc[selected].copy()
        ids = TypeAdapter(list[str]).validate_python(frame[table.id_column].tolist(), strict=True)
        eligible = set(table.eligible_ids)
        eligible_ids = tuple(identifier for identifier in ids if identifier in eligible)
        manifest = TableReviewManifest(
            dataset_id=dataset_id,
            artifact_id=table.artifact_id,
            scope_id=table.scope_id,
            partition_key=table.partition_key,
            membership=table.membership.model_copy(update={"limit": 25}),
            column_schema=table.column_schema,
            validation=table.validation,
            review_stage=self.stage.stage_id,
            id_column=table.id_column,
            revision=table.revision,
            table_digest=sha256(frame_bytes(frame)),
            rows=frame_records(frame),
            evidence=table.evidence,
            eligible_ids=eligible_ids,
            min_approvals=self.stage.min_approvals,
            max_approvals=self.stage.max_approvals,
            record_fingerprints={
                str(row[table.id_column]): review_identity(row, self.stage.review_columns)
                for row in frame_records(frame)
            },
            review_columns=self.stage.review_columns,
        )
        payload = json.dumps(
            manifest.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        snapshot = ReviewSnapshot(
            dataset_id=dataset_id,
            artifact_id=table.artifact_id,
            scope_id=table.scope_id,
            partition_key=table.partition_key,
            review_stage=self.stage.stage_id,
            snapshot_id=sha256(payload),
            candidate_ids=tuple(ids),
            eligible_candidate_ids=eligible_ids,
            min_approvals=self.stage.min_approvals,
            max_approvals=self.stage.max_approvals,
            record_fingerprints=manifest.record_fingerprints,
            review_columns=self.stage.review_columns,
        )
        self.store.write(f"{snapshot.snapshot_id}.json", payload)
        return ReviewInput(
            snapshot, frame, table.id_column, validation=table.validation,
            evidence=table.evidence,
            column_schema=table.column_schema,
        )

    def evidence(self, snapshot: ReviewSnapshot, candidate_id: str) -> EvidenceResponse:
        payload = self.store.read(f"{snapshot.snapshot_id}.json")
        if sha256(payload) != snapshot.snapshot_id:
            raise IntegrityError("Review table manifest digest mismatch")
        manifest = TableReviewManifest.model_validate_json(payload)
        if (
            manifest.dataset_id, manifest.review_stage, manifest.artifact_id, manifest.scope_id
        ) != (
            snapshot.dataset_id, snapshot.review_stage, snapshot.artifact_id, snapshot.scope_id
        ):
            raise HumanReviewError("conflict", "Review table evidence has a different scope")
        for row in manifest.rows:
            if row[manifest.id_column] == candidate_id:
                return EvidenceResponse(fields={"record": row, "evidence": manifest.evidence})
        raise HumanReviewError("not_found", "Record is not in the evidence snapshot")
