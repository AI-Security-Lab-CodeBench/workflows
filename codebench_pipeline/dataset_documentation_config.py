from __future__ import annotations

from typing import Literal, assert_never

from pydantic import ConfigDict, Field, model_validator

from codebench_pipeline.dataset_seed_config import DocumentationSeed, SearchSeeds
from codebench_pipeline.model_config import ModelConfig
from codebench_pipeline.typed_config import TypedConfig


class DocumentationConfig(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    mode: Literal["provided_snippets", "keenable"] = "provided_snippets"
    snippets: list[DocumentationSeed] = Field(default_factory=list)
    search_request: SearchSeeds | None = None
    revision: str = Field(default="1", min_length=1)
    maximum_bytes: int = Field(default=1_048_576, gt=0, le=10_485_760)
    model: ModelConfig | None = None

    @model_validator(mode="after")
    def matching_inputs(self) -> DocumentationConfig:
        match self.mode:
            case "provided_snippets":
                if self.search_request is not None:
                    raise ValueError("Provided documentation forbids a search request")
                return self
            case "keenable":
                if self.search_request is None or self.snippets:
                    raise ValueError("Live documentation requires search, without provided snippets")
                return self
        assert_never(self.mode)
