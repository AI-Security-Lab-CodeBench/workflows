from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, model_validator

from .batch_labeling_config import InferenceMode, ReasoningEffort
from .inference_capacity import ScopedCapacityConfig
from .model_config import ModelConfig
from .storage import IntegrityError
from .typed_config import ConfigArtifact

if TYPE_CHECKING:
    from .stage_configuration import ScopedAnnotationConfig


class DiscoveryModelOverride(ConfigArtifact):
    provider: Literal["openai_compatible"]
    model: str
    base_url: str
    api_key_env: str | None = None
    temperature: float | None = None
    structured_output: Literal["json_schema", "json_object"] = "json_schema"

    @model_validator(mode="after")
    def valid_connection(self) -> Self:
        ModelConfig.model_validate(self.model_dump())
        return self


class DiscoveryOptions(ConfigArtifact):
    max_output_tokens: int = Field(default=256, ge=64, le=1024)
    timeout_seconds: float = Field(default=30.0, gt=0, le=60)
    model: DiscoveryModelOverride | None = Field(default=None, exclude_if=lambda model: model is None)

    def resolve_inference(self, labeling: ScopedAnnotationConfig) -> DiscoveryInference:
        override = self.model
        model, batch = labeling.model, labeling.batch
        uses_labeling_binding = override is None or (
            model.base_url is not None
            and override.base_url.rstrip("/") == model.base_url.rstrip("/")
            and override.model == model.model
            and override.api_key_env == model.api_key_env
        )
        if batch.inference_mode == "guarded" and not uses_labeling_binding:
            raise IntegrityError(
                "Guarded discovery must use the labeling endpoint, served model and credential binding"
            )
        connection = model.model_dump() if override is None else override.model_dump()
        return DiscoveryInference(
            model=ModelConfig.model_validate(
                {
                    **connection,
                    "max_output_tokens": self.max_output_tokens,
                    "timeout_seconds": self.timeout_seconds,
                }
            ),
            backend_revision=batch.backend_revision,
            inference_mode=batch.inference_mode,
            capacity=batch.capacity,
            reasoning_effort=batch.reasoning_effort if uses_labeling_binding else None,
        )


class DiscoveryInference(ConfigArtifact):
    model: ModelConfig
    backend_revision: str
    inference_mode: InferenceMode
    capacity: ScopedCapacityConfig | None
    reasoning_effort: ReasoningEffort | None = Field(default=None, exclude_if=lambda effort: effort is None)
