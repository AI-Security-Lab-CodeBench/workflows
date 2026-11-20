from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.artifact_schemas import ArtifactSchema, rules_artifact_schema
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import sha256

MAX_CONFIG_FILE_BYTES: Final = 32 * 1024 * 1024
MAX_CONFIG_VALUE_BYTES: Final = 3 * 1024 * 1024
CONFIG_OBJECT: Final = TypeAdapter(dict[str, JsonValue])


@dataclass(frozen=True, slots=True)
class ConfigPreview:
    digest: Digest
    value: dict[str, JsonValue]
    schema: ArtifactSchema


def read_config_preview(
    path: Path, snapshot_id: Digest | None, declared: dict[str, JsonValue] | None = None
) -> ConfigPreview:
    """Return complete top-level fields within the inspector budget."""
    try:
        with path.open("rb") as source:
            payload = source.read(MAX_CONFIG_FILE_BYTES + 1)
    except FileNotFoundError as error:
        raise DatasetViewError("not_materialized", "This config has no persisted output") from error
    if len(payload) > MAX_CONFIG_FILE_BYTES:
        raise DatasetViewError("not_ready", "This persisted config exceeds the inspector read limit.")
    digest = sha256(payload)
    if snapshot_id is not None and snapshot_id != digest:
        raise HumanReviewError("stale", "The artifact changed; refresh before continuing")

    value = CONFIG_OBJECT.validate_json(payload)
    preview: dict[str, JsonValue] = {}
    remaining = MAX_CONFIG_VALUE_BYTES - 2
    for name, field in value.items():
        size = len(CONFIG_OBJECT.dump_json({name: field})) - 2 + bool(preview)
        if size <= remaining:
            preview[name] = field
            remaining -= size

    schema = rules_artifact_schema(value, declared)
    if len(preview) < len(value):
        schema = ArtifactSchema(
            source=schema.source,
            schema={**schema.schema_, "x-codebench-truncated": True},
        )
    return ConfigPreview(digest, preview, schema)
