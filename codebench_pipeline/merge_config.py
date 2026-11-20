from typing import Annotated, Final, Self

from pydantic import AliasChoices, Field, model_validator
from pydantic_core import PydanticCustomError

from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.typed_config import ConfigArtifact, TypedConfig

SourceWeight = Annotated[float, Field(strict=True, ge=0, le=1, allow_inf_nan=False)]
DEFAULT_KEENABLE_SOURCE_WEIGHT: Final = 0.3


class CollectionMergeConfig(TypedConfig, ConfigArtifact):
    dataset_source_weight: SourceWeight = 0.6
    documentation_source_weight: SourceWeight = 0.9
    keenable_source_weight: SourceWeight = Field(
        default=DEFAULT_KEENABLE_SOURCE_WEIGHT,
        validation_alias=AliasChoices("keenable_source_weight", "keenable_weight"),
    )
    repository_origin_weights: dict[str, SourceWeight] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_defaults(self) -> Self:
        if not self.keenable_source_weight < self.dataset_source_weight < self.documentation_source_weight:
            raise PydanticCustomError(
                "source_weights",
                "Default source weights must keep documentation above datasets above Keenable",
            )
        return self

    def weight(self, branch: Branch, origin_id: str | None) -> float:
        default = {
            "dataset": self.dataset_source_weight,
            "documentation": self.documentation_source_weight,
            "keenable": self.keenable_source_weight,
        }[branch]
        return default if origin_id is None else self.repository_origin_weights.get(origin_id, default)

    def check_origins(self, dataset: set[str], documentation: set[str]) -> None:
        unknown = self.repository_origin_weights.keys() - (dataset | documentation)
        if unknown:
            raise PydanticCustomError(
                "source_weights",
                "Source weight overrides must name declared repository origins: {unknown}",
                {"unknown": ", ".join(sorted(unknown))},
            )
        dataset_weights = [self.weight("dataset", origin) for origin in dataset]
        documentation_weights = [self.weight("documentation", origin) for origin in documentation]
        if (
            not self.keenable_source_weight
            < min(dataset_weights)
            <= max(dataset_weights)
            < min(documentation_weights)
        ):
            raise PydanticCustomError(
                "source_weights", "Source weights must keep documentation above datasets above Keenable"
            )
