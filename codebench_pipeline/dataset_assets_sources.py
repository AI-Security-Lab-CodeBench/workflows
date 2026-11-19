from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetsDefinition,
    DataVersion,
    MetadataValue,
    Output,
    ResourceParam,
    asset,
)
from pandera.typing import DataFrame

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.config_assets import (
    COLLECTION_SCOPES,
    GLOBAL_CONFIG,
    REGISTERED_CONFIG_TYPE,
    RegisteredConfig,
    mapped_input,
)
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
from codebench_pipeline.stage_configuration import (
    DocumentationCrawlConfig,
    ExistingDatasetsConfig,
    FilterConfig,
    LabelingDocumentationConfig,
    require_mapped_config,
)
from codebench_pipeline.storage import fingerprint


def source_assets(branch: Branch = "dataset") -> list[AssetsDefinition]:
    key_prefix = asset_path("source_catalog", branch)[:-1]
    mapping = mapped_input(
        COLLECTION_SCOPES[branch],
        ExistingDatasetsConfig if branch == "dataset" else DocumentationCrawlConfig,
    )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=RULES_TYPE,
        ins={"registered": AssetIn(key=GLOBAL_CONFIG, dagster_type=REGISTERED_CONFIG_TYPE)},
    )
    def category_rules(
        registered: RegisteredConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[CategoryRules]:
        rules = registered.rules
        version = fingerprint(rules.model_dump_json())
        key = f"category-rules/{version}"
        hit = dataset_io_manager.exists(key)
        dataset_io_manager.write_evidence(key, rules.model_dump_json(indent=2).encode())
        return Output(
            rules,
            data_version=DataVersion(version),
            metadata={
                "cache_hit": hit,
                "category": rules.canonical,
                "version": rules.version,
                "rules": MetadataValue.json(rules.model_dump(mode="json")),
            },
        )

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[SourceCatalogTable],
        ins={"mapped_config": mapping},
    )
    def source_catalog(
        context: AssetExecutionContext,
        config: CatalogConfig,
        mapped_config: ExistingDatasetsConfig | DocumentationCrawlConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceCatalogTable]]:
        settings = require_mapped_config(context.asset_key, config, mapped_config.source_catalog)
        category_rules = mapped_config.category_rules
        return cached_table(
            context,
            dataset_io_manager,
            settings,
            {"category_rules": category_rules},
            SourceCatalogTable,
            lambda evidence: build_catalog(settings, category_rules, evidence, branch=branch),
        )

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[SourceDocumentsTable],
        ins={
            "source_catalog": table_input("source_catalog", SourceCatalogTable, branch),
            "mapped_config": mapping,
        },
    )
    def source_documents(
        context: AssetExecutionContext,
        config: SnapshotConfig,
        source_catalog: DataFrame[SourceCatalogTable],
        mapped_config: ExistingDatasetsConfig | DocumentationCrawlConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        settings = require_mapped_config(context.asset_key, config, mapped_config.source_documents)
        return cached_table(
            context,
            dataset_io_manager,
            settings,
            {"source_catalog": source_catalog},
            SourceDocumentsTable,
            lambda evidence: snapshot_documents(
                source_catalog,
                settings,
                cache=Path(dataset_io_manager.review_home) / "source-downloads"
                if dataset_io_manager.review_home is not None
                else None,
                evidence=evidence / "retrieval",
            ),
        )

    @asset(
        key_prefix=key_prefix,
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[DocumentAdaptersTable],
        ins={
            "source_documents": table_input("source_documents", SourceDocumentsTable, branch),
            "mapped_config": mapping,
        },
    )
    def document_adapters(
        context: AssetExecutionContext,
        config: AdapterConfig,
        source_documents: DataFrame[SourceDocumentsTable],
        mapped_config: ExistingDatasetsConfig | DocumentationCrawlConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[DocumentAdaptersTable]]:
        settings = require_mapped_config(context.asset_key, config, mapped_config.document_adapters)
        return cached_table(
            context,
            dataset_io_manager,
            settings,
            {"source_documents": source_documents},
            DocumentAdaptersTable,
            lambda evidence: infer_document_adapters(source_documents, settings, evidence),
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
            "mapped_config": mapping,
        },
    )
    def command_candidates(
        context: AssetExecutionContext,
        config: ExtractionConfig,
        source_documents: DataFrame[SourceDocumentsTable],
        document_adapters: DataFrame[DocumentAdaptersTable],
        mapped_config: ExistingDatasetsConfig | DocumentationCrawlConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        settings = require_mapped_config(context.asset_key, config, mapped_config.command_candidates)
        return cached_table(
            context,
            dataset_io_manager,
            settings,
            {"source_documents": source_documents, "adapters": document_adapters},
            CommandCandidatesTable,
            lambda evidence: extract_candidates(source_documents, document_adapters, settings),
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
            "mapped_config": mapped_input("filtered_commands", FilterConfig),
        },
    )
    def filtered_commands(
        context: AssetExecutionContext,
        config: FilteringConfig,
        command_candidates: DataFrame[CommandCandidatesTable],
        mapped_config: FilterConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[FilteredCommandsTable]]:
        settings = require_mapped_config(context.asset_key, config, mapped_config.filtered_commands)
        category_rules = mapped_config.category_rules
        return cached_table(
            context,
            dataset_io_manager,
            settings,
            {"command_candidates": command_candidates, "category_rules": category_rules},
            FilteredCommandsTable,
            lambda evidence: filter_candidates(command_candidates, category_rules, settings),
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_building",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[DocumentationTable],
        ins={
            "filtered_commands": table_input("filtered_commands", FilteredCommandsTable),
            "mapped_config": mapped_input("labeling_documentation", LabelingDocumentationConfig),
        },
    )
    def labeling_documentation(
        context: AssetExecutionContext,
        config: DocumentationConfig,
        filtered_commands: DataFrame[FilteredCommandsTable],
        mapped_config: LabelingDocumentationConfig,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[DocumentationTable]]:
        settings = require_mapped_config(context.asset_key, config, mapped_config.labeling_documentation)
        category_rules = mapped_config.category_rules
        return cached_table(
            context,
            dataset_io_manager,
            settings,
            {"commands": filtered_commands, "category_rules": category_rules},
            DocumentationTable,
            lambda evidence: collect_documentation(filtered_commands, category_rules, settings, evidence),
        )

    return [filtered_commands, labeling_documentation]
