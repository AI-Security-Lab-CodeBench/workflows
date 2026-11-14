import hashlib
import json
from pathlib import Path

import pandas as pd
from pydantic import Field, JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import frame_records
from codebench_pipeline.human_review import ReviewBatch, ReviewModel
from codebench_pipeline.object_store import ObjectStore, StorageConfig
from codebench_pipeline.review_snapshots import ReviewManifest
from codebench_pipeline.storage import IntegrityError


class LegacyManifest(ReviewModel):
    model_config = {"extra": "ignore", "frozen": True}
    dataset_id: str
    review_stage: str
    id_column: str
    review_columns: tuple[str, ...]
    rows: tuple[dict[str, JsonValue], ...]
    evidence: dict[str, JsonValue] = Field(default_factory=dict)


def load_legacy_manifest(home: Path, directory: Path | None, batch: ReviewBatch) -> LegacyManifest | None:
    key = batch.snapshot.snapshot_id + ".json"
    if directory is not None:
        path = directory / key
        if not path.is_file():
            return None
        payload = path.read_bytes()
    else:
        payload = None
        for prefix in ("table-snapshots", "snapshots"):
            store = ObjectStore(StorageConfig(local_root=str(home / "human-review"), prefix=prefix))
            try:
                payload = store.read(key)
            except (FileNotFoundError, IntegrityError):
                continue
            break
        if payload is None:
            return None
    if hashlib.sha256(payload).hexdigest() != batch.snapshot.snapshot_id:
        return None
    try:
        raw = TypeAdapter(dict[str, JsonValue]).validate_json(payload)
    except ValidationError:
        return None
    if "references" in raw and batch.snapshot.review_stage == "annotations":
        try:
            annotation = ReviewManifest.model_validate_json(payload)
            stage = next(
                (name for name in ("category_eligible_annotations", "deduplicated_annotations", "reviewed_annotations")
                 if name in annotation.references),
                None,
            )
            if stage is None:
                return None
            value = DataFrameIOManager(storage=annotation.storage).read_version(annotation.references[stage])
            if not isinstance(value, pd.DataFrame):
                return None
            records = {str(row["candidate_id"]): row for row in frame_records(value)}
            if any(identifier not in records for identifier in batch.snapshot.candidate_ids):
                return None
            raw = LegacyManifest(
                dataset_id=annotation.dataset_id,
                review_stage="annotations",
                id_column="candidate_id",
                review_columns=tuple(annotation.publication.review_columns),
                rows=tuple(records[identifier] for identifier in batch.snapshot.candidate_ids),
                evidence=annotation.evidence,
            ).model_dump(mode="json")
            payload = json.dumps(raw).encode()
        except (ValidationError, FileNotFoundError, IntegrityError):
            return None
    if not {"review_columns", "rows", "id_column", "dataset_id", "review_stage"}.issubset(raw):
        return None
    try:
        manifest = LegacyManifest.model_validate_json(payload)
    except ValidationError:
        return None
    if manifest.dataset_id != batch.snapshot.dataset_id or manifest.review_stage != batch.snapshot.review_stage:
        return None
    identifiers = tuple(row.get(manifest.id_column) for row in manifest.rows)
    if identifiers != batch.snapshot.candidate_ids or not manifest.review_columns:
        return None
    if batch.snapshot.review_columns and set(batch.snapshot.review_columns) != set(manifest.review_columns):
        return None
    try:
        json.dumps(manifest.rows, allow_nan=False)
    except ValueError:
        return None
    return manifest
