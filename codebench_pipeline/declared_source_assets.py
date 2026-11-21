import os
from dataclasses import dataclass, field
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetsDefinition,
    Output,
    ResourceParam,
    asset,
)
from pandera.typing import DataFrame

from codebench_pipeline.collection_stage_settings import (
    DocumentationCaptureSettings,
    KeenableCandidateSettings,
    RepositoryCaptureSettings,
    RepositoryCatalogSettings,
    RepositoryExtractionSettings,
)
from codebench_pipeline.config_assets import collection_mapping_key
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    SourceCatalogTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.dataset_branches import CollectionBranch, asset_path
from codebench_pipeline.finalization_table_asset import (
    COLLECTION_INPUT,
    FINALIZATION_TABLE_IO,
    CollectionInput,
)
from codebench_pipeline.finalization_table_io import retain_failed_finalization
from codebench_pipeline.keenable_snippets import provider_snippet_candidates
from codebench_pipeline.repository import (
    RepositoryRun,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_legacy import PreparedCaptureCache
from codebench_pipeline.repository_receipts import RECEIPT_ATTR


def source_run(context: AssetExecutionContext, storage: DataFrameIOManager) -> RepositoryRun:
    return storage.repository_run(context.partition_key, context.run_id)


@dataclass(frozen=True, slots=True)
class KeenableDiscovery:
    api_key: str = field(repr=False)

    @classmethod
    def from_environment(cls) -> "KeenableDiscovery":
        return cls(os.getenv("KEENABLE_API_KEY", ""))


def declared_source_assets(branch: CollectionBranch) -> list[AssetsDefinition]:
    prefix = asset_path("command_candidates", branch.namespace)[:-1]
    assets: list[AssetsDefinition] = []

    def mapped_edge(stage: str) -> AssetIn:
        return AssetIn(
            key=collection_mapping_key(branch.namespace, stage),
            dagster_type=COLLECTION_INPUT,
            input_manager_key=FINALIZATION_TABLE_IO,
        )

    if branch.capture is None:

        @asset(
            name="command_candidates",
            key_prefix=prefix,
            group_name=branch.group,
            partitions_def=DATASET_PARTITIONS,
            io_manager_key=FINALIZATION_TABLE_IO,
            hooks={retain_failed_finalization},
            dagster_type=TABLE_TYPES[CommandCandidatesTable],
            ins={"mapped_config": mapped_edge("command_candidates")},
        )
        def keenable_command_candidates(
            mapped_config: CollectionInput,
            keenable_credential: ResourceParam[KeenableDiscovery],
        ) -> Output[DataFrame[CommandCandidatesTable]]:
            settings = mapped_config.for_operation(KeenableCandidateSettings)
            return mapped_config.output(
                CommandCandidatesTable,
                lambda _evidence: provider_snippet_candidates(settings.search, keenable_credential.api_key),
            )

        return [keenable_command_candidates]

    def index_sources(
        context: AssetExecutionContext,
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceCatalogTable]]:
        run = source_run(context, dataset_io_manager)
        settings = mapped_config.for_operation(RepositoryCatalogSettings)

        def compute(evidence: Path) -> DataFrame[SourceCatalogTable]:
            return repository_catalog(settings, "dataset", run)

        return mapped_config.output(SourceCatalogTable, compute)

    def capture_indexed_sources(
        context: AssetExecutionContext,
        source_catalog: DataFrame[SourceCatalogTable],
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        prepared_capture_cache: ResourceParam[PreparedCaptureCache],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        run = source_run(context, dataset_io_manager)
        settings = mapped_config.for_operation(RepositoryCaptureSettings)

        def compute(evidence: Path) -> DataFrame[SourceDocumentsTable]:
            return repository_documents(source_catalog, settings, run, prepared_cache=prepared_capture_cache)

        return mapped_config.output(SourceDocumentsTable, compute)

    def capture_documentation(
        context: AssetExecutionContext,
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        prepared_capture_cache: ResourceParam[PreparedCaptureCache],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        run = source_run(context, dataset_io_manager)
        settings = mapped_config.for_operation(DocumentationCaptureSettings)

        def compute(evidence: Path) -> DataFrame[SourceDocumentsTable]:
            catalog = repository_catalog(settings, "documentation", run)
            scope_receipt = catalog.attrs[RECEIPT_ATTR]
            (evidence / "documentation-scope.json").write_text(scope_receipt, encoding="utf-8")
            captured = repository_documents(
                catalog, settings.capture(), run, prepared_cache=prepared_capture_cache
            )
            captured.attrs["documentation_scope_receipt_json"] = scope_receipt
            return captured

        return mapped_config.output(SourceDocumentsTable, compute)

    if branch.index is not None:
        assets.append(
            asset(
                name=branch.index,
                key_prefix=prefix,
                group_name=branch.group,
                partitions_def=DATASET_PARTITIONS,
                io_manager_key=FINALIZATION_TABLE_IO,
                hooks={retain_failed_finalization},
                dagster_type=TABLE_TYPES[SourceCatalogTable],
                ins={"mapped_config": mapped_edge(branch.index)},
            )(index_sources)
        )
    assets.append(
        asset(
            name=branch.capture,
            key_prefix=prefix,
            group_name=branch.group,
            partitions_def=DATASET_PARTITIONS,
            io_manager_key=FINALIZATION_TABLE_IO,
            hooks={retain_failed_finalization},
            dagster_type=TABLE_TYPES[SourceDocumentsTable],
            ins={
                "mapped_config": mapped_edge(branch.capture),
                **(
                    {"source_catalog": table_input(branch.index, SourceCatalogTable, branch.namespace)}
                    if branch.index is not None
                    else {}
                ),
            },
        )(capture_indexed_sources if branch.index is not None else capture_documentation)
    )

    def command_candidates(
        context: AssetExecutionContext,
        source_documents: DataFrame[SourceDocumentsTable],
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        run = source_run(context, dataset_io_manager)
        repository = mapped_config.for_operation(RepositoryExtractionSettings)

        def compute(evidence: Path) -> DataFrame[CommandCandidatesTable]:
            source = dataset_io_manager.implementation_source
            return repository_candidates(
                source_documents,
                repository_adapters(source_documents, repository, run, implementation_source=source),
                repository,
                run,
                implementation_source=source,
            )

        return mapped_config.output(CommandCandidatesTable, compute)

    assets.append(
        asset(
            name="command_candidates",
            key_prefix=prefix,
            group_name=branch.group,
            partitions_def=DATASET_PARTITIONS,
            io_manager_key=FINALIZATION_TABLE_IO,
            hooks={retain_failed_finalization},
            dagster_type=TABLE_TYPES[CommandCandidatesTable],
            ins={
                "source_documents": table_input(branch.capture, SourceDocumentsTable, branch.namespace),
                "mapped_config": mapped_edge("command_candidates"),
            },
        )(command_candidates)
    )
    return assets
