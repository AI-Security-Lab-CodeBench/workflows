from collections.abc import Mapping

import pandas as pd
from dagster import Config
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import AssetSnapshot, DataFrameIOManager
from codebench_pipeline.dataset_engine import implementation_signature
from codebench_pipeline.models import Digest
from codebench_pipeline.stage_implementation import (
    STAGE_ROOTS,
    ProcessingChanged,
    ProcessingReceiptIncomplete,
    verify_processing,
)


def legacy_signature_matches(
    storage: DataFrameIOManager,
    stage: str,
    config: Config,
    inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
    recorded: str,
    evidence: Mapping[str, JsonValue] | None = None,
) -> bool:
    if stage not in STAGE_ROOTS:
        return False
    if evidence is not None and "stage_implementation" in evidence:
        implementation = TypeAdapter(dict[str, Digest]).validate_python(evidence["implementation"])
        stage_implementation = TypeAdapter(dict[str, Digest]).validate_python(
            evidence["stage_implementation"]
        )
    else:
        try:
            payload = storage.read_evidence(f"legacy-implementations/{recorded}.json")
        except FileNotFoundError:
            return False
        implementation = TypeAdapter(dict[str, Digest]).validate_json(payload)
        stage_implementation = implementation
    if implementation_signature(stage, config, inputs, stage_implementation) != recorded:
        return False
    try:
        verify_processing(stage, implementation, storage.read_evidence)
    except (ProcessingChanged, ProcessingReceiptIncomplete):
        return False
    return True
