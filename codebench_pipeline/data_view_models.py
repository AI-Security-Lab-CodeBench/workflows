from typing import Literal

from pydantic import Field

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.table_query import QueryModel, TablePage, TableQuery


class DatasetQueryRequest(QueryModel):
    dataset_id: Identifier
    snapshot_id: Digest | None = None
    query: TableQuery = Field(default_factory=TableQuery)


class DatasetDescriptor(QueryModel):
    dataset_id: Identifier
    category_id: str
    category: str


class DatasetList(QueryModel):
    datasets: tuple[DatasetDescriptor, ...]


class TableResponse(QueryModel):
    kind: Literal["table"] = "table"
    dataset_id: Identifier
    stage: str
    snapshot_id: Digest
    page: TablePage
    publication_state: Literal["published", "legacy_unreviewed", "stale"] | None = None


class RulesResponse(QueryModel):
    kind: Literal["rules"] = "rules"
    dataset_id: Identifier
    stage: Literal["category_rules"] = "category_rules"
    snapshot_id: Digest
    rules: CategoryRules


class DatasetViewError(RuntimeError):
    code: Literal["not_found", "not_materialized", "not_ready"]
    detail: str

    def __init__(self, code: Literal["not_found", "not_materialized", "not_ready"], detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)
