"""Framework boundary for annotation projections and finalization tables."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd
import pandera.pandas as pa
from dagster import AssetIn, AssetKey, AssetsDefinition, DataVersion, Output, PythonObjectDagsterType, asset
from pandera.typing import DataFrame
from pydantic import JsonValue, PrivateAttr

from codebench_pipeline.config_assets import mapped_input
from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    FilteredCommandsTable,
    LabelingAttemptsTable,
    LabelingFailuresTable,
    ProposedAnnotationsTable,
)
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, TABLE_TYPES
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_config import DeduplicationPolicy
from codebench_pipeline.stage_configuration import (
    CategoryEligibilityConfig,
    DeclaredBranchConfig,
    DeduplicationConfig,
)
from codebench_pipeline.storage import IntegrityError

type Annotations = DataFrame[ProposedAnnotationsTable]
FINALIZATION_TABLE_IO: Final = "finalization_table_io_manager"
EVIDENCE_ATTRIBUTES: Final = {
    "deduplicated_annotations": "duplicates",
    "category_eligible_annotations": "category_exclusions",
}
PROJECTION_SCHEMAS: Final = {
    "filtered_commands": FilteredCommandsTable,
    "proposed_annotations": ProposedAnnotationsTable,
    "labeling_failures": LabelingFailuresTable,
    "merged_command_candidates": CommandCandidatesTable,
}


@dataclass(slots=True)
class CollectionCacheInput:
    signature: str
    cached: pd.DataFrame | None
    scratch: Path | None


class CollectionInput(DeclaredBranchConfig):
    _cache: CollectionCacheInput | None = PrivateAttr(default=None)

    def bind_cache(self, cache: CollectionCacheInput) -> None:
        self._cache = cache

    def output[Schema: pa.DataFrameModel](
        self, schema: type[Schema], compute: Callable[[Path], DataFrame[Schema]]
    ) -> Output[DataFrame[Schema]]:
        cache = self._cache
        if cache is None:
            raise IntegrityError("Collection input hooks did not prepare the table")
        if cache.cached is not None:
            table = schema.validate(cache.cached)
        elif cache.scratch is not None:
            table = schema.validate(compute(cache.scratch))
        else:
            raise IntegrityError("Collection cache miss has no attempt directory")
        return Output(table, data_version=DataVersion(cache.signature))


@dataclass(slots=True)
class MergeInput:
    frames: dict[Branch, DataFrame[CommandCandidatesTable]]
    binding: dict[str, JsonValue]
    signature: str | None = None
    cached: pd.DataFrame | None = None


MERGE_INPUT: Final = PythonObjectDagsterType(python_type=MergeInput, name="MergeInput")


@dataclass(slots=True)
class FilteringInput:
    candidates: DataFrame[CommandCandidatesTable]
    signature: str | None = None
    cached: pd.DataFrame | None = None


FILTERING_INPUT: Final = PythonObjectDagsterType(python_type=FilteringInput, name="FilteringInput")


@dataclass(frozen=True, slots=True)
class FinalizationTable:
    table: Annotations
    evidence: JsonValue


@dataclass(slots=True)
class FinalizationInput:
    annotations: Annotations
    signature: str | None = None
    cached: Annotations | None = None


_INPUT: Final = PythonObjectDagsterType(python_type=FinalizationInput, name="FinalizationInput")


@dataclass(slots=True)
class ProjectionInput:
    attempts: DataFrame[LabelingAttemptsTable]
    signature: str | None = None
    cached: pd.DataFrame | None = None


PROJECTION_INPUT: Final = PythonObjectDagsterType(python_type=ProjectionInput, name="ProjectionInput")


@dataclass(slots=True)
class LabelingInput:
    commands: DataFrame[FilteredCommandsTable]
    attempt: tuple[str, Path] | None = None
    scratch: Path | None = None


LABELING_INPUT: Final = PythonObjectDagsterType(python_type=LabelingInput, name="LabelingInput")


def projection_output[Schema: pa.DataFrameModel](
    loaded: ProjectionInput,
    schema: type[Schema],
    compute: Callable[[DataFrame[LabelingAttemptsTable]], DataFrame[Schema]],
) -> Output[DataFrame[Schema]]:
    if loaded.signature is None:
        raise IntegrityError("Projection input hooks did not prepare the table")
    table = compute(loaded.attempts) if loaded.cached is None else schema.validate(loaded.cached)
    return Output(table, data_version=DataVersion(loaded.signature))


def table_output(
    loaded: FinalizationInput, attribute: str, compute: Callable[[Annotations], FinalizationTable]
) -> Output[Annotations]:
    if loaded.signature is None:
        raise IntegrityError("Finalization input hooks did not prepare the table")
    if loaded.cached is not None:
        table = loaded.cached
    else:
        result = compute(loaded.annotations)
        table = result.table
        table.attrs[attribute] = result.evidence
    return Output(table, data_version=DataVersion(loaded.signature))


def finalization_table_assets(
    deduplicate: Callable[[Annotations, DeduplicationPolicy], FinalizationTable],
    select_eligible: Callable[[Annotations, str], FinalizationTable],
) -> tuple[AssetsDefinition, AssetsDefinition]:
    from codebench_pipeline.finalization_table_io import retain_failed_finalization

    @asset(
        key_prefix=["datasets"],
        group_name="deduplication",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key=FINALIZATION_TABLE_IO,
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        hooks={retain_failed_finalization},
        ins={
            "proposed_annotations": AssetIn(
                key=AssetKey(["datasets", "proposed_annotations"]),
                input_manager_key=FINALIZATION_TABLE_IO,
                dagster_type=_INPUT,
            ),
            "mapped_config": mapped_input(
                "deduplicated_annotations", DeduplicationConfig, input_manager_key=FINALIZATION_TABLE_IO
            ),
        },
    )
    def deduplicated_annotations(
        proposed_annotations: FinalizationInput,
        mapped_config: DeduplicationConfig,
    ) -> Output[Annotations]:
        return table_output(
            proposed_annotations,
            EVIDENCE_ATTRIBUTES["deduplicated_annotations"],
            lambda table: deduplicate(table, mapped_config.deduplication),
        )

    @asset(
        key_prefix=["datasets"],
        group_name="dataset_release",
        partitions_def=DATASET_PARTITIONS,
        io_manager_key=FINALIZATION_TABLE_IO,
        dagster_type=TABLE_TYPES[ProposedAnnotationsTable],
        hooks={retain_failed_finalization},
        ins={
            "deduplicated_annotations": AssetIn(
                key=AssetKey(["datasets", "deduplicated_annotations"]),
                input_manager_key=FINALIZATION_TABLE_IO,
                dagster_type=_INPUT,
            ),
            "mapped_config": mapped_input(
                "category_eligible_annotations",
                CategoryEligibilityConfig,
                input_manager_key=FINALIZATION_TABLE_IO,
            ),
        },
    )
    def category_eligible_annotations(
        deduplicated_annotations: FinalizationInput,
        mapped_config: CategoryEligibilityConfig,
    ) -> Output[Annotations]:
        return table_output(
            deduplicated_annotations,
            EVIDENCE_ATTRIBUTES["category_eligible_annotations"],
            lambda table: select_eligible(table, mapped_config.category_id),
        )

    return deduplicated_annotations, category_eligible_annotations
