from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, Literal, Self

from anyio import run, to_thread
from pandera.typing import DataFrame
from pydantic import Field, model_validator

from scripts.dataset_schema import SECRET_PATTERNS

from .dataframe_schemas import CommandCandidatesTable, validated_frame
from .keenable_decisions import (
    DiscoveryChoice,
    DiscoveryDecision,
    DiscoveryFeedback,
    DiscoveryModel,
    DiscoveryTurn,
)
from .keenable_inspection import INSPECTIONS, AuthoredInspection, Seed, shell_spans
from .keenable_mcp import search_session, search_snippets
from .keenable_policy import (
    Digest,
    KeenableError,
    KeenableSnippetSettings,
    Record,
    canonical_url,
    digest,
)
from .keenable_tables import PROVIDER_RECEIPT, provider_source_id

_PARSER: Final = "keenable-inspection-shell-block-v1"


class SnippetCandidateReceipt(Record):
    candidate_id: Digest
    source_id: str
    url: str
    body_sha256: Digest
    start: int = Field(ge=0)
    end: int = Field(gt=0)
    span_sha256: Digest
    command: str
    description: str
    source_type: Literal["web_page"] = "web_page"
    license: Literal["keenable"] = "keenable"


class _SearchObservation(Record):
    rank: int
    url_sha256: Digest
    outcome: Literal["extracted", "no_commands", "outside_inspection", "invalid_url", "sensitive"]
    candidate_ids: tuple[Digest, ...] = ()


class _SearchReceipt(Record):
    query: str
    hits: tuple[_SearchObservation, ...]
    choice: DiscoveryChoice | None = Field(default=None, exclude_if=lambda value: value is None)
    feedback: DiscoveryFeedback | None = Field(default=None, exclude_if=lambda value: value is None)


class SnippetCollectionReceipt(Record):
    schema_version: Literal["keenable-snippets-v1"] = "keenable-snippets-v1"
    settings_sha256: Digest
    candidates: dict[Digest, SnippetCandidateReceipt]
    snippets: dict[Digest, str]
    searches: tuple[_SearchReceipt, ...]
    stop: Literal["budget", "seeds_exhausted", "finalized"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    finalize: DiscoveryChoice | None = Field(default=None, exclude_if=lambda value: value is None)

    def read_body(self, body_sha256: str) -> bytes:
        """Read exact snippet bytes by content hash without network or page lookup."""
        text = self.snippets.get(body_sha256)
        if text is None or digest(body := text.encode()) != body_sha256:
            raise KeenableError("snippet_body_integrity")
        return body

    @model_validator(mode="after")
    def binds_candidates(self) -> Self:
        for key, candidate in self.candidates.items():
            body = self.read_body(candidate.body_sha256)
            literal = body[candidate.start : candidate.end]
            if (
                key != candidate.candidate_id
                or key != _candidate_id(candidate.url, candidate.body_sha256, candidate.start, candidate.end)
                or candidate.source_id != provider_source_id(candidate.url)
                or not candidate.start < candidate.end <= len(body)
                or literal != candidate.command.encode()
                or digest(literal) != candidate.span_sha256
            ):
                raise ValueError("Keenable candidate does not bind to its snippet")
        return self


def _candidate_id(url: str, body_sha256: str, start: int, end: int) -> str:
    return digest(f"{url}\n{body_sha256}\n{start}:{end}".encode())


@dataclass(frozen=True, slots=True)
class DiscoveryRun:
    evidence: Path
    api_key: str = field(repr=False)
    model: DiscoveryModel | None = field(default=None, repr=False)


async def _collect(
    settings: KeenableSnippetSettings, access: DiscoveryRun
) -> DataFrame[CommandCandidatesTable]:
    if settings.queries:
        raise KeenableError("raw_discovery_queries_unsupported")
    candidates: dict[str, SnippetCandidateReceipt] = {}
    snippets: dict[str, str] = {}
    searches: list[_SearchReceipt] = []
    feedback: list[DiscoveryFeedback] = []
    seeds: tuple[Seed, ...] = tuple(
        seed.id if isinstance(seed, AuthoredInspection) else seed for seed in settings.seeds or ()
    )
    model = access.model
    if settings.authored_seeds and model is None:
        raise KeenableError("discovery_model_required")
    if model is not None and tuple(item.seed for item in model.taxonomy) != settings.authored_seeds:
        raise KeenableError("discovery_taxonomy_binding_mismatch")
    budget = settings.max_search_calls or len(seeds)
    limit = budget if model is not None else min(budget, len(seeds))
    finalize = None
    settings_sha256 = digest(settings.model_dump_json().encode())
    seen_queries: set[Digest] = set()
    seen_sources: set[str] = set()
    model_key = model.key if model is not None else None
    async with AsyncExitStack() as stack:
        session = None
        while len(searches) < limit:
            turn = DiscoveryTurn(
                available=seeds, previous=tuple(feedback), remaining_searches=limit - len(searches)
            )
            choice = (
                await to_thread.run_sync(model.choose, turn, access.evidence)
                if model is not None
                else DiscoveryChoice(decision=DiscoveryDecision(action="query", seed=seeds[len(searches)]))
            )
            turn_number = len(searches) + 1
            decision_path = access.evidence / f"discovery-{turn_number:02}-decision.json"
            seed = choice.decision.seed
            if seed is None:
                finalize = choice
                decision_path.write_text(choice.model_dump_json(), encoding="utf-8")
                break
            inspection = INSPECTIONS[seed]
            context = choice.decision.context
            query = inspection.query if context is None else f"{inspection.query} Focus: {context}."
            query_id = digest(query.encode())
            if query_id in seen_queries:
                raise KeenableError("discovery_query_already_used")
            seen_queries.add(query_id)
            choice = choice.model_copy(update={"query_id": query_id})
            decision_path.write_text(choice.model_dump_json(), encoding="utf-8")
            if session is None:
                session = await stack.enter_async_context(search_session(access.api_key))
            result = await search_snippets(
                session, query, (settings.max_results, settings.snippet_max_length)
            )
            hits: list[_SearchObservation] = []
            candidate_count = len(candidates)
            accepted: set[str] = set()
            observed_sources: set[str] = set()
            excerpt_context: dict[str, None] = {}
            for rank, hit in enumerate(result.results, 1):
                url_sha256 = digest(hit.url.encode())
                if (canonical := canonical_url(hit.url)) is None:
                    hits.append(_SearchObservation(rank=rank, url_sha256=url_sha256, outcome="invalid_url"))
                    continue
                if any(
                    access.api_key in value
                    or (model_key and model_key in value)
                    or any(pattern.search(value) for pattern in SECRET_PATTERNS)
                    for value in (hit.url, hit.snippet, hit.description)
                ):
                    hits.append(_SearchObservation(rank=rank, url_sha256=url_sha256, outcome="sensitive"))
                    continue
                observed_sources.add(canonical)
                for phrase in inspection.excerpt_context(f"{hit.description}\n{hit.snippet}"):
                    if len(excerpt_context) < 4:
                        excerpt_context[phrase] = None
                body = hit.snippet.encode()
                body_sha256 = digest(body)
                spans = shell_spans(body)
                ids: list[Digest] = []
                for span in spans:
                    literal = body[span.start : span.end]
                    command = literal.decode()
                    if not inspection.command.fullmatch(command):
                        continue
                    key = _candidate_id(hit.url, body_sha256, span.start, span.end)
                    ids.append(key)
                    if key not in candidates:
                        candidates[key] = SnippetCandidateReceipt(
                            candidate_id=key,
                            source_id=provider_source_id(hit.url),
                            url=hit.url,
                            body_sha256=body_sha256,
                            start=span.start,
                            end=span.end,
                            span_sha256=digest(literal),
                            command=command,
                            description=hit.description or span.description,
                        )
                if ids:
                    snippets[body_sha256] = hit.snippet
                    accepted.update(ids)
                hits.append(
                    _SearchObservation(
                        rank=rank,
                        url_sha256=url_sha256,
                        outcome="extracted" if ids else "outside_inspection" if spans else "no_commands",
                        candidate_ids=tuple(ids),
                    )
                )
            observed = DiscoveryFeedback(
                seed=seed,
                results=len(hits),
                accepted=len(accepted),
                new_candidates=len(candidates) - candidate_count,
                query_id=query_id,
                query_context=context,
                new_sources=len(observed_sources - seen_sources),
                excerpt_context=tuple(excerpt_context),
            )
            seen_sources.update(observed_sources)
            feedback.append(observed)
            searches.append(_SearchReceipt(query=query, hits=tuple(hits), choice=choice, feedback=observed))
            (access.evidence / f"discovery-{turn_number:02}-result.json").write_text(
                SnippetCollectionReceipt(
                    settings_sha256=settings_sha256,
                    candidates=candidates,
                    snippets=snippets,
                    searches=tuple(searches),
                ).model_dump_json(),
                encoding="utf-8",
            )
    frame = validated_frame(
        [
            {
                "candidate_id": candidate.candidate_id,
                "command": candidate.command,
                "util": candidate.command.split()[0],
                "context": candidate.description,
                "source_id": candidate.source_id,
                "source_url": candidate.url,
                "source_position": f"bytes:{candidate.start}:{candidate.end}",
                "transformations": [_PARSER],
                "risk_candidates": [],
            }
            for candidate in candidates.values()
        ],
        CommandCandidatesTable,
    )
    frame.attrs[PROVIDER_RECEIPT] = SnippetCollectionReceipt(
        settings_sha256=settings_sha256,
        candidates=candidates,
        snippets=snippets,
        searches=tuple(searches),
        stop="finalized"
        if finalize is not None
        else "budget"
        if limit == budget and budget
        else "seeds_exhausted",
        finalize=finalize,
    ).model_dump_json()
    return frame


def provider_snippet_candidates(
    settings: KeenableSnippetSettings, access: DiscoveryRun
) -> DataFrame[CommandCandidatesTable]:
    """Build canonical candidates from indexed Keenable excerpts with offline-verifiable evidence."""
    try:
        return run(_collect, settings, access)
    except ExceptionGroup as errors:
        cause: BaseException = errors
        while isinstance(cause, BaseExceptionGroup) and len(cause.exceptions) == 1:
            cause = cause.exceptions[0]
        if isinstance(cause, KeenableError):
            raise cause from None
        raise
