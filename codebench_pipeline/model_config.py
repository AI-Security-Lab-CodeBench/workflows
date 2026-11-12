from __future__ import annotations

import re
from typing import Literal, assert_never
from urllib.parse import urlsplit

from pydantic import ConfigDict, Field, field_validator, model_validator

from codebench_pipeline.typed_config import TypedConfig


class ModelConfig(TypedConfig):
    """One explicit provider configuration; credentials remain in the environment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: Literal["native", "openai_compatible"]
    model: str
    base_url: str | None = None
    api_key_env: str | None = None
    timeout_seconds: float = Field(default=900, gt=0, allow_inf_nan=False, strict=True)
    max_output_tokens: int = Field(default=4096, gt=0, strict=True)
    temperature: float | None = Field(default=None, allow_inf_nan=False, strict=True)
    structured_output: Literal["json_schema", "json_object"] = "json_schema"

    @field_validator("model")
    @classmethod
    def nonblank_model(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("model must not be blank")
        return value

    @field_validator("api_key_env")
    @classmethod
    def environment_name(cls, value: str | None) -> str | None:
        if value is not None and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
            raise ValueError("api_key_env must be an environment variable name")
        return value

    @field_validator("base_url")
    @classmethod
    def endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or any(character.isspace() or ord(character) < 32 for character in value)
            or "\\" in value
        ):
            raise ValueError("base_url must be an HTTP URL without credentials, query or fragment")
        _ = parsed.port
        return value

    @model_validator(mode="after")
    def provider_fields(self) -> ModelConfig:
        match self.provider:
            case "native":
                if self.base_url is not None or self.api_key_env is not None:
                    raise ValueError("native forbids base_url and api_key_env")
                return self
            case "openai_compatible":
                if self.base_url is None:
                    raise ValueError("openai_compatible requires base_url")
                return self
        assert_never(self.provider)


class ModelRoles(TypedConfig):
    """The active pipeline labels commands; optional roles preserve older specifications."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    labeler: ModelConfig
    selector: ModelConfig | None = None
    adapter: ModelConfig | None = None
    reviewer: ModelConfig | None = None
