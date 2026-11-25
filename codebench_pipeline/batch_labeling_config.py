from typing import Annotated, Literal

from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.grounding_tools import LiveGroundingConfig
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.typed_config import TypedConfig

BackendRevision = Annotated[str, Field(min_length=1)]
InferenceMode = Literal["guarded", "plain"]
RowConcurrency = Annotated[int, Field(ge=1, le=64)]
MaximumGroundingTurns = Annotated[int, Field(ge=0)]
ReasoningEffort = Annotated[str, Field(pattern=r"^(none|low|high|max)$")]


class BatchPromptConfig(TypedConfig):
    instructions: str = Field(default="", max_length=16000)
    reasoning_effort: ReasoningEffort | None = None
    row_concurrency: RowConcurrency = 8
    maximum_grounding_turns: MaximumGroundingTurns = 5


class BatchLabelingConfig(BatchPromptConfig):
    retry_revision: str = Field(default="1", min_length=1)
    backend_revision: BackendRevision
    inference_mode: InferenceMode = Field(default="guarded", exclude_if=lambda mode: mode == "guarded")
    grounding: LiveGroundingConfig
    capacity: CapacityConfig | None = Field(default=None, exclude_if=lambda capacity: capacity is None)

    @model_validator(mode="after")
    def guarded_requires_capacity(self) -> "BatchLabelingConfig":
        if self.inference_mode == "guarded" and self.capacity is None:
            raise PydanticCustomError("guarded_capacity", "Guarded labeling requires capacity settings")
        return self

    def require_capacity(self) -> CapacityConfig:
        if self.capacity is None:
            raise IntegrityError("Guarded labeling requires capacity settings")
        return self.capacity


class DeclaredBatchLabelingConfig(BatchPromptConfig):
    backend_revision: BackendRevision
    inference_mode: InferenceMode = "guarded"
    grounding: LiveGroundingConfig
    capacity: CapacityConfig | None = None

    def registered(self) -> BatchLabelingConfig:
        return BatchLabelingConfig(
            instructions=self.instructions,
            reasoning_effort=self.reasoning_effort,
            row_concurrency=self.row_concurrency,
            maximum_grounding_turns=self.maximum_grounding_turns,
            backend_revision=self.backend_revision,
            inference_mode=self.inference_mode,
            grounding=self.grounding,
            capacity=self.capacity,
        )
