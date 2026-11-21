import json
from dataclasses import dataclass, field
from pathlib import Path
from re import fullmatch
from typing import Literal

from pydantic import Field, ValidationError

from .agent_events import InferenceError, parse_json
from .inference_capacity import CapacityConfig, CapacityUnavailable, guarded_complete
from .keenable_inspection import InspectionTaxonomy
from .keenable_policy import Digest, KeenableError, Record, digest
from .model_config import ModelConfig
from .models import Identifier
from .openai_transport import complete, parse_completion, strict_schema


class DiscoveryDecision(Record):
    action: Literal["query", "finalize"]
    seed: Identifier | None
    context: str | None = Field(default=None, exclude_if=lambda value: value is None)


class DiscoveryFeedback(Record):
    seed: Identifier
    results: int = Field(ge=0)
    accepted: int = Field(ge=0)
    new_candidates: int = Field(ge=0)
    query_id: Digest | None = None
    query_context: str | None = None
    new_sources: int = Field(default=0, ge=0)
    excerpt_context: tuple[str, ...] = ()


class DiscoveryTurn(Record):
    available: tuple[Identifier, ...]
    previous: tuple[DiscoveryFeedback, ...]
    remaining_searches: int = Field(gt=0)


class DiscoveryChoice(Record):
    decision: DiscoveryDecision
    request_sha256: Digest | None = None
    response_sha256: Digest | None = None
    provider_response_id: str | None = None
    query_id: Digest | None = None


@dataclass(frozen=True, slots=True)
class DiscoveryModel:
    config: ModelConfig
    key: str | None = field(repr=False)
    taxonomy: tuple[InspectionTaxonomy, ...]
    capacity: CapacityConfig | None = None

    def choose(self, turn: DiscoveryTurn, evidence: Path) -> DiscoveryChoice:
        config = self.config
        if config.api_key_env is not None and not self.key:
            raise KeenableError("discovery_model_credential_unavailable")
        if config.base_url is None:
            raise KeenableError("discovery_model_endpoint_unavailable")
        inspections = {item.seed.id: item.seed.inspection for item in self.taxonomy}
        available = [
            {
                **item.model_dump(mode="json"),
                "context_words": sorted(inspections[item.seed.id].context_words),
            }
            for item in self.taxonomy
        ]
        body = json.dumps(
            {
                "model": config.model,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Find benign read-only inspection examples. Return action=query with a seed ID "
                            "and context: write a new natural search phrase of two to eight words using "
                            "that seed's context vocabulary, without punctuation. You may select the same "
                            "seed again; use prior query IDs, context, novelty and budget to vary the search. "
                            "Risk and severity describe the taxonomy, not additional operations to search for. "
                            "To stop, return action=finalize with seed=null and context=null."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps({**turn.model_dump(mode="json"), "available": available}),
                    },
                ],
                "response_format": (
                    {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "discovery_decision",
                            "strict": True,
                            "schema": strict_schema(DiscoveryDecision.model_json_schema()),
                        },
                    }
                    if config.structured_output == "json_schema"
                    else {"type": "json_object"}
                ),
                "max_completion_tokens": config.max_output_tokens,
                **({"temperature": config.temperature} if config.temperature is not None else {}),
                "stream": False,
            }
        ).encode()
        try:
            raw = (
                guarded_complete(
                    config.base_url,
                    body,
                    self.key,
                    config.timeout_seconds,
                    self.capacity,
                    evidence,
                )
                if self.capacity is not None
                else complete(
                    config.base_url.rstrip("/") + "/chat/completions", body, self.key, config.timeout_seconds
                )
            ).body
            if len(raw) > 64 * 1024:
                raise KeenableError("discovery_model_response_budget")
            completion = parse_completion(raw.decode())
            decision = DiscoveryDecision.model_validate(parse_json(completion.text()))
        except CapacityUnavailable as error:
            raise KeenableError(f"discovery_capacity_{error.reason}") from None
        except (OSError, InferenceError, ValidationError, UnicodeError, json.JSONDecodeError):
            raise KeenableError("discovery_model_response_invalid") from None
        if decision.action == "query":
            seed, context = decision.seed, decision.context
            if seed is None or seed not in turn.available:
                raise KeenableError("discovery_seed_unavailable")
            if (
                context is None
                or len(context) > 100
                or not fullmatch(r"[a-zA-Z0-9]+(?: [a-zA-Z0-9]+){1,7}", context)
                or not set(context.lower().split()) <= inspections[seed].context_words
            ):
                raise KeenableError("discovery_context_outside_inspection")
            decision = decision.model_copy(update={"context": context.lower()})
        elif decision.seed is not None or decision.context is not None:
            raise KeenableError("discovery_finalize_with_query")
        return DiscoveryChoice(
            decision=decision,
            request_sha256=digest(body),
            response_sha256=digest(raw),
            provider_response_id=digest(completion.id.encode()),
        )
