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

from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    SourceCatalogTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.dataset_branches import CollectionBranch, asset_branch, asset_path
from codebench_pipeline.finalization_table_asset import FINALIZATION_TABLE_IO, CollectionInput
from codebench_pipeline.finalization_table_io import retain_failed_finalization
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_planner import GuardedSearchPlanner
from codebench_pipeline.keenable_tables import (
    provider_adapters,
    provider_candidates,
    provider_catalog,
    provider_documents,
)
from codebench_pipeline.labeling_runtime import LabelingRuntime
from codebench_pipeline.repository import (
    RepositoryRun,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_legacy import PreparedCaptureCache
from codebench_pipeline.repository_receipts import RECEIPT_ATTR
from codebench_pipeline.source_evidence import require_source_rights
from codebench_pipeline.stage_configuration import DeclaredBranchConfig
from codebench_pipeline.storage import IntegrityError


def source_run(
    context: AssetExecutionContext, storage: DataFrameIOManager, mapped_config: DeclaredBranchConfig
) -> tuple[DeclaredSourceStageConfig, RepositoryRun]:
    config = mapped_config.collection
    branch = asset_branch(context.asset_key.path)
    if config.branch != branch:
        raise IntegrityError("Collection mapping belongs to another branch")
    require_source_rights(config)
    return config, storage.repository_run(context.partition_key, context.run_id)


def provider_collection(
    config: DeclaredSourceStageConfig,
    run: RepositoryRun,
    *,
    plan_receipts: Path | None = None,
    model_key: str | None = None,
    capacity: CapacityConfig | None = None,
) -> KeenableCollection:
    declaration = config.source.keenable_config()
    if declaration.dataset != run.dataset_id:
        raise IntegrityError("Keenable declaration belongs to another partition")
    if (
        plan_receipts is not None
        and declaration.search_plan is not None
        and declaration.search_plan.mode == "adaptive"
    ):
        planner = config.planner
        if planner is None or capacity is None:
            raise IntegrityError("Adaptive discovery requires mapped planner settings and bound capacity")
        return KeenableCollection(
            run.dataset_home,
            declaration,
            planner=GuardedSearchPlanner(
                planner.base_url,
                planner.model,
                capacity,
                plan_receipts,
                key=model_key,
                backend_revision=planner.backend_revision,
            ),
        )
    return KeenableCollection(run.dataset_home, declaration)


@dataclass(frozen=True, slots=True)
class KeenableDiscovery:
    api_key: str = field(repr=False)

    @classmethod
    def from_environment(cls) -> "KeenableDiscovery":
        return cls(os.getenv("KEENABLE_API_KEY", ""))

    def catalog(
        self, config: DeclaredSourceStageConfig, run: RepositoryRun, evidence: Path, runtime: LabelingRuntime
    ) -> DataFrame[SourceCatalogTable]:
        planner = config.planner
        key = os.environ.get(planner.api_key_env) if planner and planner.api_key_env else None
        capacity = None
        if planner is not None:
            paths = runtime.capacity_paths(run.dataset_id)
            capacity = planner.capacity.bind(paths.state_directory, paths.expected_model_path)
        return provider_catalog(
            provider_collection(config, run, plan_receipts=evidence, model_key=key, capacity=capacity),
            self.api_key,
        )


def declared_source_assets(branch: CollectionBranch) -> list[AssetsDefinition]:
    prefix = asset_path(branch.capture, branch.namespace)[:-1]
    mapping = mapped_input(branch.group, DeclaredBranchConfig)
    mapped_edge = AssetIn(
        key=mapping.key, dagster_type=mapping.dagster_type, input_manager_key=FINALIZATION_TABLE_IO
    )
    assets: list[AssetsDefinition] = []

    def index_sources(
        context: AssetExecutionContext,
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceCatalogTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[SourceCatalogTable]:
            return repository_catalog(config.repository_config, "dataset", run)

        return mapped_config.output(SourceCatalogTable, compute)

    def capture_indexed_sources(
        context: AssetExecutionContext,
        source_catalog: DataFrame[SourceCatalogTable],
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        prepared_capture_cache: ResourceParam[PreparedCaptureCache],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[SourceDocumentsTable]:
            return repository_documents(
                source_catalog, config.repository_config, run, prepared_cache=prepared_capture_cache
            )

        return mapped_config.output(SourceDocumentsTable, compute)

    def capture_use_case_pages(
        context: AssetExecutionContext,
        source_catalog: DataFrame[SourceCatalogTable],
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        keenable_discovery: ResourceParam[KeenableDiscovery],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)
        return mapped_config.output(
            SourceDocumentsTable,
            lambda evidence: provider_documents(
                provider_collection(config, run), source_catalog, keenable_discovery.api_key
            ),
        )

    def capture_documentation(
        context: AssetExecutionContext,
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        prepared_capture_cache: ResourceParam[PreparedCaptureCache],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[SourceDocumentsTable]:
            repository = config.repository_config
            catalog = repository_catalog(repository, "documentation", run)
            scope_receipt = catalog.attrs[RECEIPT_ATTR]
            (evidence / "documentation-scope.json").write_text(scope_receipt, encoding="utf-8")
            captured = repository_documents(catalog, repository, run, prepared_cache=prepared_capture_cache)
            captured.attrs["documentation_scope_receipt_json"] = scope_receipt
            return captured

        return mapped_config.output(SourceDocumentsTable, compute)

    def discover_use_cases(
        context: AssetExecutionContext,
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
        keenable_discovery: ResourceParam[KeenableDiscovery],
        labeling_runtime: ResourceParam[LabelingRuntime],
    ) -> Output[DataFrame[SourceCatalogTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)
        return mapped_config.output(
            SourceCatalogTable,
            lambda evidence: keenable_discovery.catalog(config, run, evidence, labeling_runtime),
        )

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
                ins={"mapped_config": mapped_edge},
            )(discover_use_cases if branch.namespace == "keenable" else index_sources)
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
                "mapped_config": mapped_edge,
                **(
                    {"source_catalog": table_input(branch.index, SourceCatalogTable, branch.namespace)}
                    if branch.index is not None
                    else {}
                ),
            },
        )(
            capture_use_case_pages
            if branch.namespace == "keenable"
            else capture_indexed_sources
            if branch.index is not None
            else capture_documentation
        )
    )

    def keenable_command_candidates(
        context: AssetExecutionContext,
        source_documents: DataFrame[SourceDocumentsTable],
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[CommandCandidatesTable]:
            collection = provider_collection(config, run)
            return provider_candidates(
                collection, source_documents, provider_adapters(collection, source_documents)
            )

        return mapped_config.output(CommandCandidatesTable, compute)

    def command_candidates(
        context: AssetExecutionContext,
        source_documents: DataFrame[SourceDocumentsTable],
        mapped_config: CollectionInput,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[CommandCandidatesTable]:
            repository = config.repository_config
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
                "mapped_config": mapped_edge,
            },
        )(keenable_command_candidates if branch.namespace == "keenable" else command_candidates)
    )
    return assets
