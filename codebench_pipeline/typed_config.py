from typing import Self

from dagster import Config
from pydantic import ConfigDict


class TypedConfig(Config):
    """Strict Dagster configuration with a typed nested-field descriptor."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    def __get__[Owner](self, instance: Owner | None, owner: type[Owner] | None = None) -> Self:
        return self
