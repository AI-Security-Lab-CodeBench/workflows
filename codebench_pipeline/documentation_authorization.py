from pathlib import Path
from typing import Final

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, build_input_context
from pandera.typing import DataFrame
from pydantic import BaseModel, ConfigDict, ValidationError

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import SourceCatalogTable, SourceDocumentsTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, Branch, asset_path
from codebench_pipeline.dataset_config import catalog_for_branch
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.stage_cache_compatibility import legacy_signature_matches
from codebench_pipeline.stage_configuration import declared_source_config

DOCUMENTATION_BINDING: Final = "codebench_documentation_sources"
INVENTORY_BINDING: Final = "codebench_dataset_sources"


class SourceInventoryAuthorization(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    dataset_id: Identifier
    branch: Branch
    catalog_digest: Digest
    inventory_signature: Digest


def inventory_authorization(
    home: Path, instance: DagsterInstance, dataset_id: str, branch: Branch
) -> tuple[DataFrame[SourceCatalogTable] | DataFrame[SourceDocumentsTable], SourceInventoryAuthorization]:
    spec = DatasetRegistry(home).get(dataset_id)
    stage = "source_catalog"
    schema = SourceCatalogTable
    if spec.has_declared_collection:
        descriptor = BRANCH_BY_NAMESPACE[branch]
        stage = descriptor.declared_stages[0]
        if descriptor.index is None:
            schema = SourceDocumentsTable
        config = declared_source_config(spec, branch)
        require_source_rights(config)
    else:
        config = catalog_for_branch(spec, branch)
        if not config.configured:
            raise HumanReviewError("pending", "No source inventory is configured")
    expected = stage_signature(stage, config, {"category_rules": spec.rules})
    key = AssetKey(asset_path(stage, branch))
    records = instance.fetch_materializations(
        AssetRecordsFilter(asset_key=key, asset_partitions=[dataset_id]), limit=1
    ).records
    if not records or records[0].asset_materialization is None:
        raise HumanReviewError("pending", "Materialize the source manifest catalog before processing it")
    manager = DataFrameIOManager(storage=spec.storage)
    recorded = str(records[0].asset_materialization.metadata["input_fingerprint"].value)
    if recorded != expected and (
        spec.has_declared_collection
        or not legacy_signature_matches(
            manager, "source_catalog", config, {"category_rules": spec.rules}, recorded
        )
    ):
        raise HumanReviewError("stale", "Source inventory changed")
    with build_input_context(
        asset_key=key,
        partition_key=dataset_id,
        asset_partitions_def=DATASET_PARTITIONS,
        instance=instance,
    ) as context:
        reference = manager.current_reference(context)
    if records[0].asset_materialization.metadata["codebench/content_digest"].value != reference.digest:
        raise HumanReviewError("stale", "Source inventory reference differs from its materialization")
    value = manager.read_version(reference)
    if not isinstance(value, pd.DataFrame):
        raise HumanReviewError("conflict", "Source catalog is not a table")
    if spec.has_declared_collection:
        validate_source_receipt(value, declared_source_config(spec, branch), manager)
    return schema.validate(value), SourceInventoryAuthorization(
        dataset_id=dataset_id, branch=branch, catalog_digest=reference.digest, inventory_signature=recorded
    )


def inventory_binding(branch: Branch) -> str:
    return {
        "dataset": INVENTORY_BINDING,
        "documentation": DOCUMENTATION_BINDING,
        "keenable": "codebench_keenable_sources",
    }[branch]


def frame_inventory_authorization(frame: pd.DataFrame, branch: Branch) -> SourceInventoryAuthorization:
    try:
        binding = SourceInventoryAuthorization.model_validate(frame.attrs.get(inventory_binding(branch)))
    except ValidationError as error:
        raise HumanReviewError("stale", "Output lacks its source inventory binding") from error
    if binding.branch != branch:
        raise HumanReviewError("stale", "Output belongs to another source branch")
    return binding
