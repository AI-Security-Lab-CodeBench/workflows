from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    DataVersion,
    MetadataValue,
    Output,
    ResourceParam,
    asset,
)
from pandera.typing import DataFrame

from codebench_pipeline.category_rules import CategoryRules, CategoryRulesConfig
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    DocumentAdaptersTable,
    DocumentationTable,
    FilteredCommandsTable,
    SourceCatalogTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_asset_types import (
    DATASET_PARTITIONS,
    RULES_TYPE,
    TABLE_TYPES,
    rules_input,
    table_input,
)
from codebench_pipeline.dataset_branches import Branch, asset_path
from codebench_pipeline.dataset_catalog import build_catalog
from codebench_pipeline.dataset_config import (
    AdapterConfig,
    CatalogConfig,
    DocumentationConfig,
    ExtractionConfig,
    FilteringConfig,
    SnapshotConfig,
)
from codebench_pipeline.dataset_documentation import collect_documentation
from codebench_pipeline.dataset_documents import document_adapters as infer_document_adapters
from codebench_pipeline.dataset_documents import snapshot_documents
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.dataset_extract import extract_candidates, filter_candidates
from codebench_pipeline.storage import fingerprint


def source_assets(branch: Branch = "dataset") -> list[AssetsDefinition]:
    key_prefix = asset_path("source_catalog", branch)[:-1]

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=RULES_TYPE,
    )
    def category_rules(
        config: CategoryRulesConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[CategoryRules]:
        version = fingerprint(config.model_dump_json())
        key = f"category-rules/{version}"
        hit = dataset_io_manager.exists(key)
        dataset_io_manager.write_evidence(key, config.model_dump_json(indent=2).encode())
        return Output(
            CategoryRules.model_validate_json(config.model_dump_json()),
            data_version=DataVersion(version),
            metadata={
                "cache_hit": hit,
                "category": config.canonical,
                "version": config.version,
                "rules": MetadataValue.json(config.model_dump(mode="json")),
            },
        )

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[SourceCatalogTable],
        ins={"category_rules": rules_input()},
    )
    def source_catalog(
        context: AssetExecutionContext,
        config: CatalogConfig,
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceCatalogTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"category_rules": category_rules},
            SourceCatalogTable,
            lambda evidence: build_catalog(config, category_rules, evidence, branch=branch),
        )

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[SourceDocumentsTable],
        ins={
            "source_catalog": table_input("source_catalog", SourceCatalogTable, branch),
        },
    )
    def source_documents(
        context: AssetExecutionContext,
        config: SnapshotConfig,
        source_catalog: DataFrame[SourceCatalogTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"source_catalog": source_catalog},
            SourceDocumentsTable,
            lambda evidence: snapshot_documents(
                source_catalog,
                config,
                cache=Path(dataset_io_manager.review_home) / "source-downloads"
                if dataset_io_manager.review_home is not None else None,
                evidence=evidence / "retrieval",
            ),
        )

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[DocumentAdaptersTable],
        ins={"source_documents": table_input("source_documents", SourceDocumentsTable, branch)},
    )
    def document_adapters(
        context: AssetExecutionContext,
        config: AdapterConfig,
        source_documents: DataFrame[SourceDocumentsTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[DocumentAdaptersTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"source_documents": source_documents},
            DocumentAdaptersTable,
            lambda evidence: infer_document_adapters(source_documents, config, evidence),
        )

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[CommandCandidatesTable],
        ins={
            "source_documents": table_input("source_documents", SourceDocumentsTable, branch),
            "document_adapters": table_input("document_adapters", DocumentAdaptersTable, branch),
        },
    )
    def command_candidates(
        context: AssetExecutionContext,
        config: ExtractionConfig,
        source_documents: DataFrame[SourceDocumentsTable],
        document_adapters: DataFrame[DocumentAdaptersTable],
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"source_documents": source_documents, "adapters": document_adapters},
            CommandCandidatesTable,
            lambda evidence: extract_candidates(source_documents, document_adapters, config),
        )

    return [
        *([category_rules] if branch == "dataset" else []),
        source_catalog,
        source_documents,
        document_adapters,
        command_candidates,
    ]


def processing_assets() -> list[AssetsDefinition]:
    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[FilteredCommandsTable],
        ins={
            "command_candidates": table_input("merged_command_candidates", CommandCandidatesTable),
            "category_rules": rules_input(),
        },
    )
    def filtered_commands(
        context: AssetExecutionContext,
        config: FilteringConfig,
        command_candidates: DataFrame[CommandCandidatesTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[FilteredCommandsTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"command_candidates": command_candidates, "category_rules": category_rules},
            FilteredCommandsTable,
            lambda evidence: filter_candidates(command_candidates, category_rules, config),
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[DocumentationTable],
        ins={
            "filtered_commands": table_input("filtered_commands", FilteredCommandsTable),
            "category_rules": rules_input(),
        },
    )
    def labeling_documentation(
        context: AssetExecutionContext,
        config: DocumentationConfig,
        filtered_commands: DataFrame[FilteredCommandsTable],
        category_rules: CategoryRules,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[DocumentationTable]]:
        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"commands": filtered_commands, "category_rules": category_rules},
            DocumentationTable,
            lambda evidence: collect_documentation(filtered_commands, category_rules, config, evidence),
        )

    return [filtered_commands, labeling_documentation]
