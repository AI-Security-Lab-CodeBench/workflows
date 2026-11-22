from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, Literal, Self

from anyio import run, to_thread
from pandera.typing import DataFrame
from pydantic import Field, JsonValue, TypeAdapter, model_validator

from scripts.dataset_schema import SECRET_PATTERNS

from .dataframe_io import frame_bytes
from .dataframe_schemas import CommandCandidatesTable, frame_records, validated_frame
from .keenable_decisions import (
    DiscoveryChoice,
    DiscoveryDecision,
    DiscoveryFeedback,
    DiscoveryModel,
    DiscoveryTurn,
)
from .keenable_inspection import INSPECTIONS, AuthoredInspection, shell_spans
from .keenable_mcp import SearchResult, search_session, search_snippets
from .keenable_policy import (
    Digest,
    KeenableError,
    KeenableSnippetSettings,
    Record,
    canonical_url,
    digest,
)
from .keenable_provider import KeenableIncident, KeenableTransport
from .keenable_tables import PROVIDER_RECEIPT, provider_source_id

_PARSER: Final = "keenable-inspection-shell-block-v1"
_INCIDENTS = TypeAdapter(list[KeenableIncident])
_JSON = TypeAdapter(JsonValue)


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


def _candidate_row(candidate: SnippetCandidateReceipt) -> dict[str, JsonValue]:
    return {
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


@dataclass(frozen=True, slots=True)
class DiscoveryRun:
    evidence: Path
    transport: KeenableTransport = field(repr=False)
    model: DiscoveryModel | None = field(default=None, repr=False)

    def contains_credential(self, text: str) -> bool:
        model_key = self.model.key if self.model is not None else None
        return self.transport.credentials.contains(text) or bool(model_key and model_key in text)

    def sensitive(self, text: str) -> bool:
        return self.contains_credential(text) or any(pattern.search(text) for pattern in SECRET_PATTERNS)

    def contains_decoded_credential(self, value: JsonValue) -> bool:
        match value:
            case str():
                return self.contains_credential(value)
            case list():
                return any(self.contains_decoded_credential(item) for item in value)
            case dict():
                return any(
                    self.contains_credential(key) or self.contains_decoded_credential(item)
                    for key, item in value.items()
                )
            case _:
                return False

    def contains_output_credential(self, body: bytes) -> bool:
        return self.contains_credential(body.decode("utf-8")) or self.contains_decoded_credential(
            _JSON.validate_json(body)
        )

    def check_output(self, body: bytes) -> None:
        if self.contains_output_credential(body):
            raise KeenableError("sensitive_discovery_output")

    def write_output(self, path: Path, body: bytes) -> None:
        self.check_output(body)
        path.write_bytes(body)

    async def search(self, query: str, limits: tuple[int, int], turn: int) -> SearchResult:
        async def operation(key: str) -> SearchResult:
            async with search_session(key) as session:
                return await search_snippets(session, query, limits)

        incidents: list[KeenableIncident] = []
        try:
            return await self.transport.call("search", operation, audit=incidents.append)
        finally:
            if incidents:
                path = self.evidence / f"discovery-{turn:02}-provider-incidents.json"
                self.write_output(path, _INCIDENTS.dump_json(incidents))


async def _collect(
    settings: KeenableSnippetSettings, access: DiscoveryRun
) -> DataFrame[CommandCandidatesTable]:
    if settings.queries:
        raise KeenableError("raw_discovery_queries_unsupported")
    candidates: dict[str, SnippetCandidateReceipt] = {}
    snippets: dict[str, str] = {}
    searches: list[_SearchReceipt] = []
    feedback: list[DiscoveryFeedback] = []
    inspections = dict(
        (seed.id, seed.inspection) if isinstance(seed, AuthoredInspection) else (seed, INSPECTIONS[seed])
        for seed in settings.seeds or ()
    )
    seeds = tuple(inspections)
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
    while len(searches) < limit:
        turn = DiscoveryTurn(
            available=seeds, previous=tuple(feedback), remaining_searches=limit - len(searches)
        )
        choice = (
            await to_thread.run_sync(model.choose, turn, access.evidence, access.check_output)
            if model is not None
            else DiscoveryChoice(decision=DiscoveryDecision(action="query", seed=seeds[len(searches)]))
        )
        turn_number = len(searches) + 1
        decision_path = access.evidence / f"discovery-{turn_number:02}-decision.json"
        seed = choice.decision.seed
        if seed is None:
            finalize = choice
            access.write_output(decision_path, choice.model_dump_json().encode())
            break
        inspection = inspections[seed]
        context = choice.decision.context
        query = inspection.query if context is None else f"{inspection.query} Focus: {context}."
        if (
            access.sensitive(seed)
            or access.sensitive(query)
            or access.contains_output_credential(_JSON.dump_json(query))
        ):
            raise KeenableError("sensitive_discovery_query")
        query_id = digest(query.encode())
        if query_id in seen_queries:
            raise KeenableError("discovery_query_already_used")
        seen_queries.add(query_id)
        choice = choice.model_copy(update={"query_id": query_id})
        access.write_output(decision_path, choice.model_dump_json().encode())
        result = await access.search(query, (settings.max_results, settings.snippet_max_length), turn_number)
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
                access.sensitive(value)
                for value in (hit.url, hit.snippet, hit.description, hit.model_dump_json(), canonical)
            ):
                hits.append(_SearchObservation(rank=rank, url_sha256=url_sha256, outcome="sensitive"))
                continue
            phrases = inspection.excerpt_context(f"{hit.description}\n{hit.snippet}")
            if access.contains_output_credential(_JSON.dump_json(list(phrases))):
                hits.append(_SearchObservation(rank=rank, url_sha256=url_sha256, outcome="sensitive"))
                continue
            body = hit.snippet.encode()
            body_sha256 = digest(body)
            spans = shell_spans(body)
            extracted: dict[Digest, SnippetCandidateReceipt] = {}
            for span in spans:
                literal = body[span.start : span.end]
                command = literal.decode()
                if not inspection.command.fullmatch(command):
                    continue
                key = _candidate_id(hit.url, body_sha256, span.start, span.end)
                extracted[key] = SnippetCandidateReceipt(
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
            if any(
                access.contains_output_credential(candidate.model_dump_json().encode())
                or access.contains_output_credential(_JSON.dump_json(_candidate_row(candidate)))
                for candidate in extracted.values()
            ):
                hits.append(_SearchObservation(rank=rank, url_sha256=url_sha256, outcome="sensitive"))
                continue
            observed_sources.add(canonical)
            for phrase in phrases:
                if len(excerpt_context) < 4:
                    excerpt_context[phrase] = None
            if extracted:
                snippets[body_sha256] = hit.snippet
                accepted.update(extracted)
                for key, candidate in extracted.items():
                    candidates.setdefault(key, candidate)
            hits.append(
                _SearchObservation(
                    rank=rank,
                    url_sha256=url_sha256,
                    outcome="extracted" if extracted else "outside_inspection" if spans else "no_commands",
                    candidate_ids=tuple(extracted),
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
        access.write_output(
            access.evidence / f"discovery-{turn_number:02}-result.json",
            SnippetCollectionReceipt(
                settings_sha256=settings_sha256,
                candidates=candidates,
                snippets=snippets,
                searches=tuple(searches),
            )
            .model_dump_json()
            .encode(),
        )
    frame = validated_frame(
        [_candidate_row(candidate) for candidate in candidates.values()], CommandCandidatesTable
    )
    receipt = SnippetCollectionReceipt(
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
    access.check_output(receipt.encode())
    frame.attrs[PROVIDER_RECEIPT] = receipt
    if any(
        access.contains_decoded_credential(row) for row in frame_records(frame)
    ) or access.contains_credential(frame_bytes(frame).decode("utf-8", errors="surrogateescape")):
        raise KeenableError("sensitive_discovery_output")
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
