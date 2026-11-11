from pathlib import Path

from dagster import AssetExecutionContext, AssetsDefinition, Output, ResourceParam, asset
from pandera.typing import DataFrame

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    DocumentationTable,
    FilteredCommandsTable,
    ProposedAnnotationsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, rules_input, table_input
from codebench_pipeline.dataset_branches import Branch, asset_path
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.dataset_tables import proposed_annotations as label_table


def annotation_assets(branch: Branch = "dataset") -> list[AssetsDefinition]:
    key_prefix = asset_path("proposed_annotations", branch)[:-1]

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        ins={
            "filtered_commands": table_input("filtered_commands", FilteredCommandsTable, branch),
            "labeling_documentation": table_input("labeling_documentation", DocumentationTable, branch),
            "category_rules": rules_input(),
        },
    )
    def proposed_annotations(
        context: AssetExecutionContext,
        config: AnnotationConfig,
        filtered_commands: DataFrame[FilteredCommandsTable],
        labeling_documentation: DataFrame[DocumentationTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {
                "commands": filtered_commands,
                "documentation": labeling_documentation,
                "category_rules": category_rules,
            },
            ProposedAnnotationsTable,
            lambda evidence: label_table(
                filtered_commands,
                labeling_documentation,
                category_rules,
                config,
                evidence,
                cache=Path(dataset_io_manager.review_home) / "row-inference" / context.partition_key
                if dataset_io_manager.review_home is not None
                else None,
            ),
        )

    return [proposed_annotations]
