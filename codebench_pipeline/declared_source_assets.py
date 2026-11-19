import json
import os
from pathlib import Path

from dagster import AssetExecutionContext, AssetsDefinition, Output, ResourceParam, asset
from pandera.typing import DataFrame

from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.config_assets import CONFIG_JSON, mapped_rules, policy_input
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    SourceCatalogTable,
    SourceDocumentsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES, table_input
from codebench_pipeline.dataset_branches import CollectionBranch, asset_branch, asset_path
from codebench_pipeline.dataset_engine import cached_table
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.keenable import KeenableCollection, KeenableDeclaration
from codebench_pipeline.keenable_planner import GuardedSearchPlanner
from codebench_pipeline.keenable_tables import (
    provider_adapters,
    provider_candidates,
    provider_catalog,
    provider_documents,
)
from codebench_pipeline.repository import (
    RepositoryRun,
    repository_adapters,
    repository_candidates,
    repository_catalog,
    repository_documents,
)
from codebench_pipeline.repository_receipts import RECEIPT_ATTR
from codebench_pipeline.source_evidence import require_source_rights
from codebench_pipeline.stage_configuration import declared_source_config
from codebench_pipeline.storage import IntegrityError


def source_run(
    context: AssetExecutionContext, storage: DataFrameIOManager, mapped_config: str
) -> tuple[DeclaredSourceStageConfig, RepositoryRun]:
    if storage.review_home is None:
        raise IntegrityError("Declared source acquisition requires a registered dataset home")
    config = DeclaredSourceStageConfig.model_validate(CONFIG_JSON.validate_json(mapped_config)["collection"])
    home = Path(storage.review_home)
    spec = DatasetRegistry(home).get_snapshot(context.partition_key)
    branch = asset_branch(context.asset_key.path)
    current = declared_source_config(spec, branch)
    if (
        config.branch != branch
        or config.declaration_digest != current.declaration_digest
        or storage.storage != spec.storage
    ):
        raise IntegrityError("Source acquisition settings no longer match the registered dataset")
    require_source_rights(config)
    return config, RepositoryRun(home, context.partition_key, context.run_id)


def provider_collection(
    config: DeclaredSourceStageConfig, run: RepositoryRun, *, plan_receipts: Path | None = None
) -> KeenableCollection:
    declaration = KeenableDeclaration.model_validate_json(json.dumps(config.source.declaration))
    if declaration.dataset != run.dataset_id:
        raise IntegrityError("Keenable declaration belongs to another partition")
    if (
        plan_receipts is not None
        and declaration.search_plan is not None
        and declaration.search_plan.mode == "adaptive"
    ):
        snapshot = DatasetRegistry(run.dataset_home).get_snapshot(run.dataset_id)
        if config.declaration_digest != declared_source_config(snapshot, "keenable").declaration_digest:
            raise IntegrityError("Adaptive discovery settings changed since the catalog was planned")
        annotation = snapshot.annotation
        model, batch = annotation.model, annotation.batch
        if model.provider != "openai_compatible" or model.base_url is None or batch is None:
            raise IntegrityError(
                "Adaptive discovery requires a registered remote model and shared capacity configuration"
            )
        return KeenableCollection(
            run.dataset_home,
            declaration,
            planner=GuardedSearchPlanner(
                model.base_url,
                model.model,
                batch.capacity,
                plan_receipts,
                key=os.environ.get(model.api_key_env) if model.api_key_env else None,
                backend_revision=batch.backend_revision,
            ),
        )
    return KeenableCollection(run.dataset_home, declaration)


def declared_source_assets(branch: CollectionBranch) -> list[AssetsDefinition]:
    prefix = asset_path(branch.capture, branch.namespace)[:-1]
    mapping = policy_input(branch.group)
    assets: list[AssetsDefinition] = []

    def index_sources(
        context: AssetExecutionContext,
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceCatalogTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[SourceCatalogTable]:
            if config.branch == "keenable":
                return provider_catalog(provider_collection(config, run, plan_receipts=evidence))
            return repository_catalog(config.repository_config, config.branch, run)

        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"category_rules": mapped_rules(mapped_config)},
            SourceCatalogTable,
            compute,
        )

    def capture_indexed_sources(
        context: AssetExecutionContext,
        source_catalog: DataFrame[SourceCatalogTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[SourceDocumentsTable]:
            if config.branch == "keenable":
                return provider_documents(provider_collection(config, run), source_catalog)
            return repository_documents(source_catalog, config.repository_config, run)

        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"source_catalog": source_catalog},
            SourceDocumentsTable,
            compute,
        )

    def capture_documentation(
        context: AssetExecutionContext,
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[SourceDocumentsTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[SourceDocumentsTable]:
            repository = config.repository_config
            catalog = repository_catalog(repository, "documentation", run)
            scope_receipt = catalog.attrs[RECEIPT_ATTR]
            (evidence / "documentation-scope.json").write_text(scope_receipt, encoding="utf-8")
            captured = repository_documents(catalog, repository, run)
            captured.attrs["documentation_scope_receipt_json"] = scope_receipt
            return captured

        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"category_rules": mapped_rules(mapped_config)},
            SourceDocumentsTable,
            compute,
        )

    if branch.index is not None:
        assets.append(
            asset(
                name=branch.index,
                key_prefix=prefix,
                group_name=branch.group,
                partitions_def=DATASET_PARTITIONS,
                io_manager_key="dataset_io_manager",
                dagster_type=TABLE_TYPES[SourceCatalogTable],
                ins={"mapped_config": mapping},
            )(index_sources)
        )
    assets.append(
        asset(
            name=branch.capture,
            key_prefix=prefix,
            group_name=branch.group,
            partitions_def=DATASET_PARTITIONS,
            io_manager_key="dataset_io_manager",
            dagster_type=TABLE_TYPES[SourceDocumentsTable],
            ins={
                "mapped_config": mapping,
                **(
                    {"source_catalog": table_input(branch.index, SourceCatalogTable, branch.namespace)}
                    if branch.index is not None
                    else {}
                ),
            },
        )(capture_indexed_sources if branch.index is not None else capture_documentation)
    )

    @asset(
        key_prefix=prefix,
        group_name=branch.group,
        partitions_def=DATASET_PARTITIONS,
        io_manager_key="dataset_io_manager",
        dagster_type=TABLE_TYPES[CommandCandidatesTable],
        ins={
            "source_documents": table_input(branch.capture, SourceDocumentsTable, branch.namespace),
            "mapped_config": mapping,
        },
    )
    def command_candidates(
        context: AssetExecutionContext,
        source_documents: DataFrame[SourceDocumentsTable],
        mapped_config: str,
        dataset_io_manager: ResourceParam[DataFrameIOManager],
    ) -> Output[DataFrame[CommandCandidatesTable]]:
        config, run = source_run(context, dataset_io_manager, mapped_config)

        def compute(evidence: Path) -> DataFrame[CommandCandidatesTable]:
            if config.branch == "keenable":
                collection = provider_collection(config, run)
                return provider_candidates(
                    collection, source_documents, provider_adapters(collection, source_documents)
                )
            repository = config.repository_config
            return repository_candidates(
                source_documents,
                repository_adapters(source_documents, repository, run),
                repository,
                run,
            )

        return cached_table(
            context,
            dataset_io_manager,
            config,
            {"source_documents": source_documents},
            CommandCandidatesTable,
            compute,
        )

    return [*assets, command_candidates]
