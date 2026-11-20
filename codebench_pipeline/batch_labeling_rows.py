"""Row contracts shared by batch inference, grounding, and checkpoint storage."""

from dataclasses import dataclass

from pydantic import ConfigDict, JsonValue

from codebench_pipeline.grounding_tools import DocumentationQuery
from codebench_pipeline.labeling_outcomes import LabelingAttempt
from codebench_pipeline.models import FrozenModel, SemanticSnippet


class RowEvidence(FrozenModel):
    model_config = ConfigDict(strict=True)

    evidence_id: str
    snippet: SemanticSnippet
    provider_metadata: JsonValue = None


@dataclass(frozen=True, slots=True)
class BatchRow:
    attempt: LabelingAttempt
    evidence: list[RowEvidence]
    queries: list[DocumentationQuery]
    files: dict[str, bytes]
