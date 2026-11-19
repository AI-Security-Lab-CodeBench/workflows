from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, Protocol, assert_never

from dagster import Definitions
from pydantic import Field, JsonValue, TypeAdapter

from codebench_pipeline.artifact_schemas import (
    ArtifactSchema,
    declared_artifact_schemas,
    rules_artifact_schema,
    table_artifact_schema,
)
from codebench_pipeline.config_artifact_preview import read_config_preview
from codebench_pipeline.dataframe_io import AssetReference, read_frame
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import ObjectStore, StorageConfig, sha256
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.table_query import QueryModel, TablePage, TableQuery, TableQueryError, query_table

AssetComponent = Annotated[str, Field(min_length=1, max_length=512)]
PublicationState = Literal["published", "legacy_unreviewed", "stale"]


class ArtifactKey(QueryModel):
    asset_key: tuple[AssetComponent, ...] = Field(min_length=1, max_length=32)


class ArtifactTarget(ArtifactKey):
    partition_key: str | None = Field(default=None, max_length=1024)


class ArtifactQuery(ArtifactTarget):
    snapshot_id: Digest | None = None
    query: TableQuery = Field(default_factory=TableQuery)


class ArtifactPartitions(ArtifactKey):
    partitions: tuple[str | None, ...]


class ArtifactTable(ArtifactTarget):
    kind: Literal["table"] = "table"
    snapshot_id: Digest
    page: TablePage
    data_schema: ArtifactSchema
    publication_state: PublicationState | None = None


class ArtifactJSON(ArtifactTarget):
    kind: Literal["json"] = "json"
    snapshot_id: Digest
    value: JsonValue
    data_schema: ArtifactSchema
    publication_state: PublicationState | None = None


@dataclass(frozen=True, slots=True)
class ArtifactBinding:
    storage: StorageConfig
    reference: AssetReference
    publication_state: PublicationState | None = None


@dataclass(frozen=True, slots=True)
class RunConfigBinding:
    path: Path
    publication_state: PublicationState | None = None


class ArtifactCatalog(Protocol):
    def partitions(self, key: ArtifactKey) -> ArtifactPartitions: ...

    def resolve(self, target: ArtifactTarget) -> ArtifactBinding | RunConfigBinding: ...


class ArtifactDataService:
    """Browse persisted artifact bindings without evaluating pipeline business rules."""

    def __init__(self, catalog: ArtifactCatalog, definitions: Definitions | None = None) -> None:
        self.catalog = catalog
        self.schemas = declared_artifact_schemas(definitions) if definitions is not None else {}

    def partitions(self, key: ArtifactKey) -> ArtifactPartitions:
        return self.catalog.partitions(key)

    def query(self, request: ArtifactQuery) -> ArtifactTable | ArtifactJSON:
        binding = self.catalog.resolve(request)
        if not isinstance(binding, ArtifactBinding):
            query = request.query
            if query.search or query.filters or query.sort or query.offset:
                raise TableQueryError("query", "JSON artifacts are not queryable tables")
            preview = read_config_preview(binding.path, request.snapshot_id)
            return ArtifactJSON(
                asset_key=request.asset_key,
                partition_key=request.partition_key,
                snapshot_id=preview.digest,
                value=preview.value,
                data_schema=ArtifactSchema(source="storage", schema=preview.schema),
                publication_state=binding.publication_state,
            )
        if request.snapshot_id is not None and request.snapshot_id != binding.reference.digest:
            raise HumanReviewError("stale", "The artifact changed; refresh before continuing")
        suffix = "parquet" if binding.reference.format == "parquet" else "json"
        payload = ObjectStore(binding.storage).read(f"objects/{binding.reference.digest}.{suffix}")
        if sha256(payload) != binding.reference.digest:
            raise IntegrityError("Artifact reference content mismatch")
        match binding.reference.format:
            case "parquet":
                value = read_frame(payload)
                return ArtifactTable(
                    asset_key=request.asset_key,
                    partition_key=request.partition_key,
                    snapshot_id=binding.reference.digest,
                    page=query_table(value, request.query),
                    data_schema=table_artifact_schema(
                        payload, tuple(value.columns), self.schemas.get(request.asset_key)
                    ),
                    publication_state=binding.publication_state,
                )
            case "rules":
                query = request.query
                if query.search or query.filters or query.sort or query.offset:
                    raise TableQueryError("query", "JSON artifacts are not queryable tables")
                value = TypeAdapter(JsonValue).validate_json(payload)
                return ArtifactJSON(
                    asset_key=request.asset_key,
                    partition_key=request.partition_key,
                    snapshot_id=binding.reference.digest,
                    value=value,
                    data_schema=rules_artifact_schema(value, self.schemas.get(request.asset_key)),
                )
            case _:
                assert_never(binding.reference.format)
