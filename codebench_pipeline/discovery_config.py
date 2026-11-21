from typing import Literal, Self

from pydantic import Field, model_validator

from .batch_labeling_config import InferenceMode
from .inference_capacity import ScopedCapacityConfig
from .model_config import ModelConfig
from .storage import IntegrityError
from .typed_config import ConfigArtifact


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

    def resolve_model(self, labeling: ModelConfig, mode: InferenceMode) -> ModelConfig:
        override = self.model
        if (
            override is not None
            and mode == "guarded"
            and (
                labeling.base_url is None
                or override.base_url.rstrip("/") != labeling.base_url.rstrip("/")
                or override.model != labeling.model
                or override.api_key_env != labeling.api_key_env
            )
        ):
            raise IntegrityError(
                "Guarded discovery must use the labeling endpoint, served model and credential binding"
            )
        connection = labeling.model_dump() if override is None else override.model_dump()
        return ModelConfig.model_validate(
            {
                **connection,
                "max_output_tokens": self.max_output_tokens,
                "timeout_seconds": self.timeout_seconds,
            }
        )


class DiscoveryInference(ConfigArtifact):
    model: ModelConfig
    backend_revision: str
    inference_mode: InferenceMode
    capacity: ScopedCapacityConfig | None
