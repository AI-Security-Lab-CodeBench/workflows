from dataclasses import dataclass
from typing import Final, Literal

from pydantic import ConfigDict, Field

from codebench_pipeline.category_rules import StrictModel, Text


@dataclass(frozen=True, slots=True)
class PilotError(RuntimeError):
    reason: str

    def __str__(self) -> str:
        return self.reason


OPERATIONS: Final = {
    "terraform validate": "CAT-3.1",
    "terraform state list": "CAT-3.1",
    "docker volume ls": "CAT-3.3",
    "docker system df": "CAT-3.3",
}


class Sampling(StrictModel):
    temperature: float = Field(ge=0, le=2)
    top_p: float = Field(gt=0, le=1)
    max_tokens: int = Field(gt=0, le=4096)
    seed: int = Field(ge=0, lt=2**32)
    samples_per_seed: Literal[1] = 1


class Prompt(StrictModel):
    template: Literal["prompt.md"] = "prompt.md"
    version: Text
    candidates_per_call: int = Field(gt=0, le=2)


BackendMode = Literal["plain", "sglang_guarded"]


class Backend(StrictModel):
    mode: BackendMode
    provider: Literal["openai_compatible"] = "openai_compatible"
    base_url: Text | None = None
    model: Text | None = None
    timeout: float = Field(gt=0, le=300)
    retries: Literal[0] = 0
    json_mode: Literal[True] = True


class VendorConfig(StrictModel):
    """Only these reviewed settings cross into the vendor process."""

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    run_name: Literal["cat3_inspection_pilot"] = "cat3_inspection_pilot"
    split: Literal["train", "test"]
    seeds_path: Literal["seeds.jsonl"] = "seeds.jsonl"
    prompt: Prompt
    backend: Backend
    sampling: Sampling


class Seed(StrictModel):
    seed_id: Text
    split: Literal["train", "test"]
    risk: Text
    family_id: Text
    prompt: Text
    mechanism: Text
    source_kind: Literal["abstract_synthetic_spec"]
    status: Literal["approved_for_generation"]


class Candidate(StrictModel):
    command: Text
    util: Text
    description: Text
    class_name: Literal["allow"] = Field(alias="class")
    severity: Literal["low"]


class Candidates(StrictModel):
    candidates: list[Candidate] = Field(min_length=1, max_length=2)
