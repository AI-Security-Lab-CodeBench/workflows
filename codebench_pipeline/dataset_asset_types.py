import pandera.pandas as pa
from dagster import AssetIn, AssetKey, DagsterType, DynamicPartitionsDefinition, TypeCheckContext
from dagster_pandera import pandera_schema_to_dagster_type

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    CommandsTable,
    DocumentAdaptersTable,
    DocumentationTable,
    FilteredCommandsTable,
    HumanReviewDecisionsTable,
    ProposedAnnotationsTable,
    ReviewQueueTable,
    SourceCatalogTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_branches import Branch, asset_path

DATASET_PARTITIONS = DynamicPartitionsDefinition(name="dataset_builds")
TABLE_TYPES: dict[type[pa.DataFrameModel], DagsterType] = {
    schema: pandera_schema_to_dagster_type(schema)
    for schema in (
        SourceCatalogTable,
        SourceDocumentsTable,
        DocumentAdaptersTable,
        CommandCandidatesTable,
        FilteredCommandsTable,
        DocumentationTable,
        ProposedAnnotationsTable,
        ReviewQueueTable,
        HumanReviewDecisionsTable,
        CommandsTable,
    )
}


def rules_type_check(context: TypeCheckContext, value: CategoryRules | None) -> bool:
    return isinstance(value, CategoryRules)


RULES_TYPE = DagsterType(name="CategoryRuleSet", type_check_fn=rules_type_check, typing_type=CategoryRules)


def table_input(name: str, schema: type[pa.DataFrameModel], branch: Branch = "dataset") -> AssetIn:
    return AssetIn(key=AssetKey(asset_path(name, branch)), dagster_type=TABLE_TYPES[schema])


def rules_input() -> AssetIn:
    return AssetIn(key=AssetKey(["datasets", "category_rules"]), dagster_type=RULES_TYPE)
