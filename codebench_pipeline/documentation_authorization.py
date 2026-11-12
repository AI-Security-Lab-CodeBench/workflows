from pathlib import Path
from typing import Final

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, build_input_context
from pandera.typing import DataFrame
from pydantic import BaseModel, ConfigDict, ValidationError

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import SourceCatalogTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import Branch, asset_path
from codebench_pipeline.dataset_config import catalog_for_branch
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import Digest, Identifier

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
) -> tuple[DataFrame[SourceCatalogTable], SourceInventoryAuthorization]:
    spec = DatasetRegistry(home).get(dataset_id)
    config = catalog_for_branch(spec, branch)
    if not config.documents and config.manifest is None:
        raise HumanReviewError("pending", "No source inventory is configured")
    expected = stage_signature("source_catalog", config, {"category_rules": spec.rules})
    key = AssetKey(asset_path("source_catalog", branch))
    records = instance.fetch_materializations(
        AssetRecordsFilter(asset_key=key, asset_partitions=[dataset_id]), limit=1
    ).records
    if not records or records[0].asset_materialization is None:
        raise HumanReviewError("pending", "Materialize the source manifest catalog before processing it")
    if records[0].asset_materialization.metadata["input_fingerprint"].value != expected:
        raise HumanReviewError("stale", "Source inventory changed")
    manager = DataFrameIOManager(storage=spec.storage)
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
    return SourceCatalogTable.validate(value), SourceInventoryAuthorization(
        dataset_id=dataset_id, branch=branch, catalog_digest=reference.digest, inventory_signature=expected
    )


def inventory_binding(branch: Branch) -> str:
    return DOCUMENTATION_BINDING if branch == "documentation" else INVENTORY_BINDING


def frame_inventory_authorization(frame: pd.DataFrame, branch: Branch) -> SourceInventoryAuthorization:
    try:
        binding = SourceInventoryAuthorization.model_validate(frame.attrs.get(inventory_binding(branch)))
    except ValidationError as error:
        raise HumanReviewError("stale", "Output lacks its source inventory binding") from error
    if binding.branch != branch:
        raise HumanReviewError("stale", "Output belongs to another source branch")
    return binding
