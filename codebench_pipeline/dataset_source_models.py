from typing import Literal

from pydantic import Field

from codebench_pipeline.models import FrozenModel


class SourceChoice(FrozenModel):
    source_id: str
    decision: Literal["selected", "needs_review", "exclude"]
    analysis: str = Field(min_length=1)
    confidence: Literal["low", "medium", "high"]
    rationale: str = Field(min_length=1)


class SourceSelection(FrozenModel):
    sources: list[SourceChoice]


def is_documentation_source(source_type: str, provenance_type: str | None) -> bool:
    return not any(
        word in source_type.lower()
        for word in ("dataset", "corpus", "benchmark", "agent", "sft", "synthetic", "generated")
    ) and provenance_type in {
        "community_documentation",
        "vendor_documentation",
        "official_documentation",
        "maintainer_documentation",
        "forum",
        "forum_discussion",
        "forum_post",
    }
