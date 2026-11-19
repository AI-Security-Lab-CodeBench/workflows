from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictBool, TypeAdapter, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.category_rules import Text
from codebench_pipeline.models import Digest
from codebench_pipeline.typed_config import TypedConfig

TRANSFORM_OPTIONS: Final = TypeAdapter(dict[str, JsonValue])


class SourceTransformConfig(TypedConfig):
    module_path: Text
    sha256: Digest
    entrypoint: str = Field(pattern=r"^[A-Za-z_][A-Za-z_0-9]*$")
    kind: Literal["function", "class"] = "function"
    trusted: bool = Field(strict=True)
    options_json: str = "{}"
    batch_size: int = Field(default=128, ge=1, le=10000)

    @model_validator(mode="after")
    def valid_options(self) -> "SourceTransformConfig":
        if self.trusted is not True:
            raise ValueError("Transformation code must be explicitly trusted")
        TRANSFORM_OPTIONS.validate_json(self.options_json, strict=True)
        return self


class PreparedSourceTransform(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    module_path: Text
    sha256: Digest
    entrypoint: str = Field(pattern=r"^[A-Za-z_][A-Za-z_0-9]*$")
    kind: Literal["function", "class"] = "function"
    trusted: StrictBool
    options: dict[str, JsonValue] = Field(default_factory=dict)
    batch_size: int | None = Field(default=None, ge=1, le=10000)

    @model_validator(mode="after")
    def explicitly_trusted(self) -> "PreparedSourceTransform":
        if self.trusted is not True:
            raise PydanticCustomError("transform_trust", "Transformation code must be explicitly trusted")
        return self

    def runtime_config(self) -> SourceTransformConfig:
        return SourceTransformConfig(
            module_path=self.module_path,
            sha256=self.sha256,
            entrypoint=self.entrypoint,
            kind=self.kind,
            trusted=self.trusted,
            options_json=TRANSFORM_OPTIONS.dump_json(self.options).decode("utf-8"),
            **({"batch_size": self.batch_size} if self.batch_size is not None else {}),
        )


@dataclass(frozen=True, slots=True)
class SourceRow:
    position: str
    value: JsonValue


@dataclass(frozen=True, slots=True)
class TransformContext:
    source_id: str
    source_url: str
    source_sha256: str
    options: Mapping[str, JsonValue]


class CanonicalSourceRow(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")

    command: Text
    context: str = ""
    source_position: Text
    transformations: list[Text] = Field(default_factory=list)


@runtime_checkable
class TransformFunction(Protocol):
    def __call__(
        self, rows: Sequence[SourceRow], context: TransformContext
    ) -> Iterable[CanonicalSourceRow | dict[str, JsonValue]]: ...


@runtime_checkable
class BatchTransform(Protocol):
    def transform(
        self, rows: Sequence[SourceRow], context: TransformContext
    ) -> Iterable[CanonicalSourceRow | dict[str, JsonValue]]: ...
