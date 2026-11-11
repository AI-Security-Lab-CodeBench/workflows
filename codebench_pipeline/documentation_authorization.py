from pathlib import Path
from typing import Final

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, build_input_context
from pandera.typing import DataFrame
from pydantic import BaseModel, ConfigDict, ValidationError

from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import SourceCatalogTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import asset_path
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import Digest, Identifier

DOCUMENTATION_BINDING: Final = "codebench_documentation_sources"


class DocumentationAuthorization(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    dataset_id: Identifier
    catalog_digest: Digest
    inventory_signature: Digest


def documentation_authorization(
    home: Path, instance: DagsterInstance, dataset_id: str
) -> tuple[DataFrame[SourceCatalogTable], DocumentationAuthorization]:
    spec = DatasetRegistry(home).get(dataset_id)
    if spec.documentation_catalog is None:
        raise HumanReviewError("pending", "No documentation source inventory is configured")
    expected = stage_signature("source_catalog", spec.documentation_catalog, {"category_rules": spec.rules})
    key = AssetKey(asset_path("source_catalog", "documentation"))
    records = instance.fetch_materializations(
        AssetRecordsFilter(asset_key=key, asset_partitions=[dataset_id]), limit=1
    ).records
    if not records or records[0].asset_materialization is None:
        raise HumanReviewError("pending", "Discover documentation sources before processing them")
    if records[0].asset_materialization.metadata["input_fingerprint"].value != expected:
        raise HumanReviewError("stale", "Documentation source inventory changed")
    manager = DataFrameIOManager(storage=spec.storage)
    with build_input_context(
        asset_key=key,
        partition_key=dataset_id,
        asset_partitions_def=DATASET_PARTITIONS,
        instance=instance,
    ) as context:
        reference = manager.current_reference(context)
    value = manager.read_version(reference)
    if not isinstance(value, pd.DataFrame):
        raise HumanReviewError("conflict", "Documentation catalog is not a table")
    return SourceCatalogTable.validate(value), DocumentationAuthorization(
        dataset_id=dataset_id, catalog_digest=reference.digest, inventory_signature=expected
    )


def frame_documentation_authorization(frame: pd.DataFrame) -> DocumentationAuthorization:
    try:
        return DocumentationAuthorization.model_validate(frame.attrs.get(DOCUMENTATION_BINDING))
    except ValidationError as error:
        raise HumanReviewError("stale", "Documentation output lacks its source binding") from error
