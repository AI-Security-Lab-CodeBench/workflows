from typing import Final

import pandas as pd
from dagster import AssetKey, DagsterInstance, TextMetadataValue
from pandera.typing import DataFrame
from pydantic import BaseModel, ConfigDict, ValidationError

from codebench_pipeline.collection_stage_settings import collection_operation
from codebench_pipeline.dataframe_io import DataFrameIOManager, materialized_reference
from codebench_pipeline.dataframe_schemas import SourceCatalogTable, SourceDocumentsTable
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, Branch, asset_path
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.input_provenance import current_materialization
from codebench_pipeline.models import Digest, Identifier
from codebench_pipeline.source_evidence import require_source_rights, validate_source_receipt
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig

DOCUMENTATION_BINDING: Final = "codebench_documentation_sources"
INVENTORY_BINDING: Final = "codebench_dataset_sources"


class SourceInventoryAuthorization(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    dataset_id: Identifier
    branch: Branch
    catalog_digest: Digest
    inventory_signature: Digest


def inventory_authorization(
    manager: DataFrameIOManager, instance: DagsterInstance, registered: DeclaredGlobalConfig, branch: Branch
) -> tuple[DataFrame[SourceCatalogTable] | DataFrame[SourceDocumentsTable], SourceInventoryAuthorization]:
    descriptor = BRANCH_BY_NAMESPACE[branch]
    stage = descriptor.declared_stages[0]
    schema = SourceDocumentsTable if descriptor.index is None else SourceCatalogTable
    binding = inventory_materialization(manager, instance, registered, branch)
    reference = manager.reference_at(asset_path(stage, branch), registered.dataset_id)
    if binding.catalog_digest != reference.digest:
        raise HumanReviewError("stale", "Source inventory reference differs from its materialization")
    value = manager.read_version(reference)
    if not isinstance(value, pd.DataFrame):
        raise HumanReviewError("conflict", "Source catalog is not a table")
    validate_source_receipt(value, registered.source_config(branch), manager)
    return schema.validate(value), binding


def inventory_materialization(
    manager: DataFrameIOManager, instance: DagsterInstance, registered: DeclaredGlobalConfig, branch: Branch
) -> SourceInventoryAuthorization:
    dataset_id = registered.dataset_id
    stage = BRANCH_BY_NAMESPACE[branch].declared_stages[0]
    config = registered.source_config(branch)
    require_source_rights(config)
    expected = manager.stage_signature(stage, collection_operation(branch, stage).project(config), {})
    key = AssetKey(asset_path(stage, branch))
    record = current_materialization(instance, key, dataset_id)
    if record is None or (materialization := record.asset_materialization) is None:
        raise HumanReviewError("pending", f"Materialize {stage} before processing its inventory")
    reference = materialized_reference(instance, record)
    if (
        materialization.metadata.get("input_fingerprint") != TextMetadataValue(expected)
        or manager.reference_at(key.path, dataset_id) != reference
    ):
        raise HumanReviewError("stale", "Source inventory changed")
    return SourceInventoryAuthorization(
        dataset_id=dataset_id,
        branch=branch,
        catalog_digest=reference.digest,
        inventory_signature=expected,
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
