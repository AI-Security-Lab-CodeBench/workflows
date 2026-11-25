from pathlib import Path
from typing import Annotated, Final, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, JsonValue, field_validator, model_validator

from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import sha256
from codebench_pipeline.rules_contracts import Category
from codebench_pipeline.storage import IntegrityError, open_directory, open_regular_file
from codebench_pipeline.typed_config import ConfigArtifact

_DEFAULT_CONFIGURATION: Final = Path(__file__).resolve().parents[1] / "business-configs" / "bashguard"
_MAX_CONFIG_BYTES: Final = 1024 * 1024


class SplitPin(ConfigArtifact):
    digest: Digest
    row_count: int = Field(gt=0)


class RulesRuntime(ConfigArtifact):
    base_url: str
    version: Annotated[str, Field(pattern=r"^sha256:[a-f0-9]{64}$")]

    @field_validator("base_url")
    @classmethod
    def require_endpoint(cls, value: str) -> str:
        endpoint = urlsplit(value)
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or endpoint.username is not None
            or endpoint.path not in {"", "/"}
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError("Expected a Bashguard HTTP origin without credentials")
        return value


class GuidePin(ConfigArtifact):
    path: Literal["guide.json"]
    sha256: Digest


class SampleCount(ConfigArtifact):
    count: int = Field(gt=0)


class SamplePercent(ConfigArtifact):
    percent: float = Field(gt=0, le=100)


type SampleSelection = SampleCount | SamplePercent
DEFAULT_SAMPLE: Final = SampleCount(count=200)


class RulesPolicy(ConfigArtifact):
    runtime: RulesRuntime
    prompt_guide: GuidePin
    categories: tuple[Category, ...] = Field(min_length=1)
    sample: SampleSelection = DEFAULT_SAMPLE

    @model_validator(mode="after")
    def require_distinct_categories(self) -> Self:
        if len(set(self.categories)) != len(self.categories):
            raise IntegrityError("Rules registration requires distinct categories")
        return self


class PromptGuide(ConfigArtifact):
    instructions: str = Field(min_length=1)
    examples: tuple[dict[str, JsonValue], ...] = Field(min_length=1)


class RegisteredRules(ConfigArtifact):
    registration: RulesPolicy
    digest: Digest
    guide: str

    @model_validator(mode="after")
    def verify_guide(self) -> Self:
        guide = self.guide.encode()
        if len(guide) > _MAX_CONFIG_BYTES or sha256(guide) != self.registration.prompt_guide.sha256:
            raise IntegrityError("Rules guide differs from the registered checksum")
        PromptGuide.model_validate_json(guide)
        return self

    @classmethod
    def approve(cls, payload: bytes, expected_sha256: str, guide: str) -> Self:
        if len(payload) > _MAX_CONFIG_BYTES or sha256(payload) != expected_sha256:
            raise IntegrityError("Rules registration digest mismatch")
        return cls(registration=RulesPolicy.model_validate_json(payload), digest=expected_sha256, guide=guide)


class HistoricalRulesPolicy(RulesPolicy):
    train: SplitPin | None = None
    test: SplitPin | None = None


class HistoricalRegisteredRules(ConfigArtifact):
    registration: HistoricalRulesPolicy
    digest: Digest
    guide: str


def read_rules_guide() -> str:
    with open_directory(_DEFAULT_CONFIGURATION) as handle, open_regular_file(handle, "guide.json") as stream:
        content = stream.read(_MAX_CONFIG_BYTES + 1)
    if len(content) > _MAX_CONFIG_BYTES:
        raise IntegrityError("Rules guide exceeds the registration size limit")
    return content.decode("utf-8")
