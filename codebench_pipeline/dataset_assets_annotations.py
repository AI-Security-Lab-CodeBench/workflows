from pathlib import Path

from dagster import AssetExecutionContext, AssetsDefinition, MetadataValue, Output, ResourceParam, asset
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.config_assets import (
    CONFIG_JSON,
    labeling_settings,
    mapped_rules,
    mapped_settings,
    policy_input,
)
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    DocumentationTable,
    FilteredCommandsTable,
    LabelingAttemptsTable,
    LabelingFailuresTable,
    ProposedAnnotationsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.dataset_config import AnnotationConfig
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.grounding_tables import GuardedLabeling
from codebench_pipeline.labeling_tables import (
    failures_from_attempts,
    labeling_attempts_table,
    proposed_from_attempts,
)
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import TypedConfig


def annotation_assets(*, declared: bool = False) -> list[AssetsDefinition]:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[LabelingAttemptsTable],
        ins={
            "filtered_commands": table_input("filtered_commands", FilteredCommandsTable),
            "labeling_documentation": table_input("labeling_documentation", DocumentationTable),
            "mapped_config": policy_input("labeling_attempts"),
        },
    )
    def labeling_attempts(
        context: AssetExecutionContext,
        config: AnnotationConfig,
        filtered_commands: DataFrame[FilteredCommandsTable],
        labeling_documentation: DataFrame[DocumentationTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[LabelingAttemptsTable]]:
        category_rules = mapped_rules(mapped_config)
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
        name="labeling_attempts",
        key_prefix=["datasets"],
        group_name="dataset_finalization",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[LabelingAttemptsTable],
        ins={
            "filtered_commands": table_input("filtered_commands", FilteredCommandsTable),
            "mapped_config": policy_input("labeling_attempts"),
        },
    )
    def guarded_labeling_attempts(
        context: AssetExecutionContext,
        filtered_commands: DataFrame[FilteredCommandsTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[LabelingAttemptsTable]]:
        if dataset_io_manager.review_home is None:
            raise IntegrityError("Guarded labeling requires a registered dataset home")
        home = Path(dataset_io_manager.review_home)
        spec = DatasetRegistry(home).get_snapshot(context.partition_key)
        rules = mapped_rules(mapped_config)
        if (
            not spec.has_declared_collection
            or CONFIG_JSON.validate_json(mapped_config)["labeling_attempts"]
            != labeling_settings(spec.annotation)
            or spec.rules != rules
            or spec.storage != dataset_io_manager.storage
        ):
            raise IntegrityError("Guarded labeling inputs differ from the registered guarded configuration")
        labeling = GuardedLabeling(home, spec)
        output = cached_table(
            context,
            dataset_io_manager,
            spec.annotation,
            {"commands": filtered_commands, "category_rules": rules},
            LabelingAttemptsTable,
            lambda evidence: labeling.attempts(filtered_commands, evidence),
        )
        labeling.verify(output.value)
        metadata = TypeAdapter(dict[str, JsonValue])
        context.add_output_metadata(
            {
                key: MetadataValue.json(metadata.validate_python(output.value.attrs[key]))
                for key in ("labeling_coverage", "labeling_quota")
                if key in output.value.attrs
            }
        )
        return output

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        ins={
            "labeling_attempts": table_input("labeling_attempts", LabelingAttemptsTable),
            "mapped_config": policy_input("proposed_annotations"),
        },
    )
    def proposed_annotations(
        context: AssetExecutionContext,
        labeling_attempts: DataFrame[LabelingAttemptsTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[ProposedAnnotationsTable]]:
        category_rules = mapped_rules(mapped_config)
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
        ins={
            "labeling_attempts": table_input("labeling_attempts", LabelingAttemptsTable),
            "mapped_config": policy_input("labeling_failures"),
        },
    )
    def labeling_failures(
        context: AssetExecutionContext,
        labeling_attempts: DataFrame[LabelingAttemptsTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[LabelingFailuresTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            mapped_settings(mapped_config, "labeling_failures", TypedConfig),
            {"attempts": labeling_attempts},
            LabelingFailuresTable,
            lambda evidence: failures_from_attempts(labeling_attempts),
        )

    return [
        guarded_labeling_attempts if declared else labeling_attempts,
        proposed_annotations,
        labeling_failures,
    ]
