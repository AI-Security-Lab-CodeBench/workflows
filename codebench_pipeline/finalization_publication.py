"""Materialization metadata for reuse of an immutable, admitted table attempt."""

from dataclasses import dataclass
from typing import Annotated, Final, Literal

from pydantic import Field

from codebench_pipeline.dataframe_io import AssetReference, UpstreamTableReference
from codebench_pipeline.models import Digest, FrozenModel

FINALIZATION_PUBLICATION_METADATA: Final = "codebench/finalization_publication"
TABLE_REGISTRATION_METADATA: Final = "codebench/registration_digest"


@dataclass(frozen=True, slots=True)
class TableOutputAdmission:
    output: AssetReference
    upstreams: tuple[UpstreamTableReference, ...]
    registration_digest: Digest


class StepAttempt(FrozenModel):
    run_id: str
    step_key: str
    retry_number: Annotated[int, Field(ge=0)]


class FinalizationPublicationV1(FrozenModel):
    version: Literal[1] = 1
    input_signature: Digest
    artifact_attempt: Annotated[str, Field(pattern=r"^[a-f0-9]{32}$")]
    output: AssetReference
    receipt_index_digest: Digest
    step_attempt: StepAttempt
    upstreams: tuple[UpstreamTableReference, ...]
    registration_digest: Digest
