from typing import Literal

from pydantic import Field

from codebench_pipeline.grounding_tools import GroundingConfig
from codebench_pipeline.inference_capacity import CapacityConfig
from codebench_pipeline.typed_config import TypedConfig


class BatchLabelingConfig(TypedConfig):
    instructions: str = Field(default="", max_length=16000)
    batch_size: int = Field(default=25, ge=1, le=50)
    reasoning_effort: str | None = Field(default=None, pattern=r"^(low|high|max)$")
    maximum_model_turns: int = Field(default=4, ge=2, le=8)
    maximum_grounding_rounds: int = Field(default=2, ge=0, le=4)
    retry_revision: str = Field(default="1", min_length=1)
    backend_revision: str = Field(min_length=1)
    inference_mode: Literal["guarded", "plain"] = Field(
        default="guarded", exclude_if=lambda mode: mode == "guarded"
    )
    grounding: GroundingConfig
    capacity: CapacityConfig
