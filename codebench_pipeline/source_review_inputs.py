from pathlib import Path
from typing import assert_never

import pandas as pd
from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, build_input_context

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.dataframe_io import DataFrameIOManager
from codebench_pipeline.dataframe_schemas import SourceCatalogTable
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_catalog import source_scope
from codebench_pipeline.dataset_engine import stage_signature
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.human_review import CompletionToken, HumanReviewError, ReviewSnapshot
from codebench_pipeline.human_review_transactions import HumanReviewTransaction
from codebench_pipeline.review_api_models import EvidenceResponse
from codebench_pipeline.review_stages import ReviewInput, ReviewStage
from codebench_pipeline.review_tables import ReviewTable, TableReviewSource
from codebench_pipeline.storage import digest, fingerprint


def source_stage() -> ReviewStage:
    return ReviewStage(
        stage_id="sources",
        title="Source selection",
        asset_keys=(
            ("datasets", "source_catalog"),
            ("datasets", "human_source_decisions"),
            ("datasets", "source_documents"),
        ),
        completion_label="Complete selection and continue",
        can_continue=True,
    )


class SourceReviewSource:
    def __init__(self, home: Path, instance: DagsterInstance) -> None:
        self.home = home.resolve()
        self.instance = instance
        self.tables = TableReviewSource(home, source_stage(), self._resolve)

    def _resolve(self, dataset_id: str) -> ReviewTable:
        spec = DatasetRegistry(self.home).get(dataset_id)
        if not spec.catalog.documents and spec.catalog.search_request is None:
            raise DatasetViewError(
                "not_ready",
                "Dataset inventory is unconfigured; discover or register corpus sources before human review.",
            )
        expected = stage_signature("source_catalog", spec.catalog, {"category_rules": spec.rules})
        key = AssetKey(["datasets", "source_catalog"])
        events = self.instance.fetch_materializations(
            AssetRecordsFilter(asset_key=key, asset_partitions=[dataset_id]), limit=1
        ).records
        if not events or events[0].asset_materialization is None:
            raise DatasetViewError("not_ready", "Discover sources before source review")
        event = events[0].asset_materialization
        if event.metadata["input_fingerprint"].value != expected:
            raise DatasetViewError("not_ready", "Source discovery changed; refresh the source catalog")
        manager = DataFrameIOManager(storage=spec.storage)
        with build_input_context(
            asset_key=key,
            partition_key=dataset_id,
            asset_partitions_def=DATASET_PARTITIONS,
            instance=self.instance,
        ) as context:
            reference = manager.current_reference(context)
        value = manager.read_version(reference)
        match value:
            case pd.DataFrame() as frame:
                catalog = SourceCatalogTable.validate(frame)
            case CategoryRules():
                raise HumanReviewError("conflict", "Source catalog is not a table")
            case _:
                assert_never(value)
        revision = fingerprint(
            {
                "source_catalog": reference.digest,
                "inputs": expected,
                "gate": digest(Path(__file__)),
            }
        )
        return ReviewTable(
            frame=catalog,
            id_column="source_id",
            revision=revision,
            evidence={"scope": source_scope(spec.rules), "catalog_digest": reference.digest},
            record_policy={
                "scope": source_scope(spec.rules),
                "model": spec.catalog.model.model_dump_json(),
                "implementation": {
                    name: digest(Path(__file__).parent / name)
                    for name in (
                        "source_review_inputs.py",
                        "dataset_catalog.py",
                        "human_review.py",
                        "human_review_transactions.py",
                        "review_tables.py",
                    )
                },
            },
            eligible_ids=tuple(
                str(value) for value in catalog.loc[catalog["decision"] == "selected", "source_id"]
            ),
        )

    def current(self, dataset_id: str) -> ReviewInput:
        return self.tables.current(dataset_id)

    def evidence(self, snapshot: ReviewSnapshot, candidate_id: str) -> EvidenceResponse:
        return self.tables.evidence(snapshot, candidate_id)


def source_authorization(
    source: SourceReviewSource, dataset_id: str, transaction: HumanReviewTransaction
) -> tuple[ReviewInput, CompletionToken]:
    resolved = source.current(dataset_id)
    batch = transaction.get_current_batch(dataset_id, "sources")
    if batch.snapshot != resolved.snapshot or batch.completion is None:
        raise HumanReviewError("pending", "Complete current source selection before using discovered sources")
    return resolved, transaction.authorize_completion(resolved.snapshot, batch.completion)
