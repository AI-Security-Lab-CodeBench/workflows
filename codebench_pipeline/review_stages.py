from dataclasses import dataclass
from typing import Protocol, Self

import pandas as pd
from pydantic import Field, model_validator

from codebench_pipeline.dataset_branches import asset_path
from codebench_pipeline.human_review import ReviewSnapshot, Text
from codebench_pipeline.models import Identifier
from codebench_pipeline.review_api_models import EvidenceResponse, ReviewBackend
from codebench_pipeline.table_query import QueryModel


class ReviewStage(QueryModel):
    stage_id: Identifier
    title: Text
    asset_keys: tuple[tuple[Text, ...], ...]
    completion_label: Text = "Complete review"
    can_continue: bool = False
    continuation_label: Text = "Continue pipeline"
    additional_publication_stages: tuple[Identifier, ...] = ()
    min_approvals: int = Field(default=0, ge=0)
    max_approvals: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def valid_bounds(self) -> Self:
        if self.max_approvals is not None and self.max_approvals < self.min_approvals:
            raise ValueError("Approval bounds are inconsistent")
        return self


class ReviewStageList(QueryModel):
    stages: tuple[ReviewStage, ...]


@dataclass(frozen=True, slots=True)
class ReviewInput:
    snapshot: ReviewSnapshot
    frame: pd.DataFrame
    id_column: str


class ReviewSource(Protocol):
    def current(self, dataset_id: str) -> ReviewInput:
        """Resolve authoritative records under the shared review transaction."""
        ...

    def evidence(self, snapshot: ReviewSnapshot, candidate_id: str) -> EvidenceResponse: ...


@dataclass(frozen=True, slots=True)
class ReviewRegistration:
    stage: ReviewStage
    backend: ReviewBackend


def annotation_stage() -> ReviewStage:
    return ReviewStage(
        stage_id="annotations",
        title="Dataset command review",
        asset_keys=tuple(
            tuple(asset_path(stage))
            for stage in (
                "deduplicated_annotations",
                "human_review_queue",
                "human_review_decisions",
                "dataset_release",
            )
        ),
        completion_label="Complete review and publish",
        can_continue=True,
        continuation_label="Continue publication",
    )
