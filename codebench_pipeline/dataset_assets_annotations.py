from pathlib import Path

from dagster import AssetExecutionContext, AssetsDefinition, Output, ResourceParam, asset
from pandera.typing import DataFrame

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    DocumentationTable,
    FilteredCommandsTable,
    LabelingAttemptsTable,
    LabelingFailuresTable,
    ProposedAnnotationsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, rules_input, table_input
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.labeling_tables import (
    failures_from_attempts,
    labeling_attempts_table,
    proposed_from_attempts,
)
from codebench_pipeline.typed_config import TypedConfig


def annotation_assets() -> list[AssetsDefinition]:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[LabelingAttemptsTable],
        ins={
            "filtered_commands": table_input("filtered_commands", FilteredCommandsTable),
            "labeling_documentation": table_input("labeling_documentation", DocumentationTable),
            "category_rules": rules_input(),
        },
    )
    def labeling_attempts(
        context: AssetExecutionContext,
        config: AnnotationConfig,
        filtered_commands: DataFrame[FilteredCommandsTable],
        labeling_documentation: DataFrame[DocumentationTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[LabelingAttemptsTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {
                "commands": filtered_commands,
                "documentation": labeling_documentation,
                "category_rules": category_rules,
            },
            LabelingAttemptsTable,
            lambda evidence: labeling_attempts_table(
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

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        ins={
            "labeling_attempts": table_input("labeling_attempts", LabelingAttemptsTable),
            "category_rules": rules_input(),
        },
    )
    def proposed_annotations(
        context: AssetExecutionContext,
        labeling_attempts: DataFrame[LabelingAttemptsTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            TypedConfig(),
            {"attempts": labeling_attempts, "category_rules": category_rules},
            ProposedAnnotationsTable,
            lambda evidence: proposed_from_attempts(labeling_attempts, category_rules),
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[LabelingFailuresTable],
        ins={"labeling_attempts": table_input("labeling_attempts", LabelingAttemptsTable)},
    )
    def labeling_failures(
        context: AssetExecutionContext,
        labeling_attempts: DataFrame[LabelingAttemptsTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[LabelingFailuresTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            TypedConfig(),
            {"attempts": labeling_attempts},
            LabelingFailuresTable,
            lambda evidence: failures_from_attempts(labeling_attempts),
        )

    return [labeling_attempts, proposed_annotations, labeling_failures]
