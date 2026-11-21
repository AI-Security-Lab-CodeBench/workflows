from dataclasses import dataclass
from re import compile
from typing import Final, Literal, Self

from anyio import run
from pandera.typing import DataFrame
from pydantic import Field, model_validator

from scripts.dataset_schema import SECRET_PATTERNS

from .dataframe_schemas import CommandCandidatesTable, validated_frame
from .keenable_mcp import search_session, search_snippets
from .keenable_policy import (
    COMMANDS,
    Digest,
    KeenableError,
    KeenableSnippetSettings,
    Record,
    canonical_url,
    digest,
)
from .keenable_tables import PROVIDER_RECEIPT, provider_source_id

_UTILITIES: Final = frozenset(
    command.split()[0].encode() for commands in COMMANDS.values() for command in commands
)
_FENCE: Final = compile(rb"^ {0,3}(`{3,}|~{3,})([^\r\n]*)")
_SHELL: Final = frozenset({b"bash", b"shell", b"sh", b"zsh"})
_CONSOLE: Final = frozenset({b"console", b"shell-session", b"terminal"})
_WORD: Final = compile(rb"(?:[a-zA-Z0-9_./+-]+|\\(?:dt|dn))(?=[ \t]|$)")
_PARSER: Final = "keenable-snippet-shell-block-v1"


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
    outcome: Literal["extracted", "no_commands", "invalid_url", "sensitive"]
    candidate_ids: tuple[Digest, ...] = ()


class _SearchReceipt(Record):
    query: str
    hits: tuple[_SearchObservation, ...]


class SnippetCollectionReceipt(Record):
    schema_version: Literal["keenable-snippets-v1"] = "keenable-snippets-v1"
    settings_sha256: Digest
    candidates: dict[Digest, SnippetCandidateReceipt]
    snippets: dict[Digest, str]
    searches: tuple[_SearchReceipt, ...]

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


@dataclass(frozen=True, slots=True)
class _Span:
    start: int
    end: int
    util: str
    description: str


def _shell_spans(body: bytes) -> tuple[_Span, ...]:
    """Extract complete shell fences while preserving the command bytes and offsets."""
    spans: list[_Span] = []
    pending: list[_Span] = []
    fence = b""
    language = b""
    description = ""
    preceding_prose = ""
    offset = 0
    continued = False
    for line in body.splitlines(keepends=True):
        start, offset = offset, offset + len(line)
        if delimiter := _FENCE.fullmatch(line.rstrip(b"\r\n")):
            marker, info = delimiter.groups()
            if not fence:
                fence, language = marker, info.strip().split(b" ", 1)[0].lower()
                description = preceding_prose
                preceding_prose = ""
                continued = False
            elif marker.startswith(fence) and not info.strip():
                spans.extend(pending)
                pending.clear()
                fence = b""
            continue
        if not fence:
            if line.strip():
                preceding_prose = line.strip().decode()
            continue
        if language not in _SHELL | _CONSOLE | {b""}:
            continue
        raw = line.strip(b" \t\r\n")
        if continued or raw.endswith(b"\\"):
            continued = raw.endswith(b"\\")
            continue
        if raw.startswith(b"# "):
            description = raw[2:].decode()
            continue
        command = raw.removeprefix(b"$ ")
        if command == raw and language in _CONSOLE:
            continue
        utility = _WORD.match(command)
        if (
            utility is None
            or utility[0].endswith(b"/")
            or (command == raw and not language and utility[0] not in _UTILITIES)
        ):
            continue
        command_start = start + line.index(command)
        pending.append(_Span(command_start, command_start + len(command), utility[0].decode(), description))
    return tuple(spans)


def _candidate_id(url: str, body_sha256: str, start: int, end: int) -> str:
    return digest(f"{url}\n{body_sha256}\n{start}:{end}".encode())


async def _collect(settings: KeenableSnippetSettings, api_key: str) -> DataFrame[CommandCandidatesTable]:
    candidates: dict[str, SnippetCandidateReceipt] = {}
    snippets: dict[str, str] = {}
    utilities: dict[str, str] = {}
    searches: list[_SearchReceipt] = []
    if settings.queries:
        async with search_session(api_key) as session:
            for query in settings.queries:
                result = await search_snippets(
                    session, query, (settings.max_results, settings.snippet_max_length)
                )
                hits: list[_SearchObservation] = []
                for rank, hit in enumerate(result.results, 1):
                    url_sha256 = digest(hit.url.encode())
                    if canonical_url(hit.url) is None:
                        hits.append(
                            _SearchObservation(rank=rank, url_sha256=url_sha256, outcome="invalid_url")
                        )
                        continue
                    if any(
                        api_key in value or any(pattern.search(value) for pattern in SECRET_PATTERNS)
                        for value in (hit.url, hit.snippet, hit.description)
                    ):
                        hits.append(_SearchObservation(rank=rank, url_sha256=url_sha256, outcome="sensitive"))
                        continue
                    body = hit.snippet.encode()
                    body_sha256 = digest(body)
                    ids: list[str] = []
                    for span in _shell_spans(body):
                        key = _candidate_id(hit.url, body_sha256, span.start, span.end)
                        ids.append(key)
                        if key not in candidates:
                            literal = body[span.start : span.end]
                            candidates[key] = SnippetCandidateReceipt(
                                candidate_id=key,
                                source_id=provider_source_id(hit.url),
                                url=hit.url,
                                body_sha256=body_sha256,
                                start=span.start,
                                end=span.end,
                                span_sha256=digest(literal),
                                command=literal.decode(),
                                description=hit.description or span.description,
                            )
                            utilities[key] = span.util
                    if ids:
                        snippets[body_sha256] = hit.snippet
                    hits.append(
                        _SearchObservation(
                            rank=rank,
                            url_sha256=url_sha256,
                            outcome="extracted" if ids else "no_commands",
                            candidate_ids=tuple(ids),
                        )
                    )
                searches.append(_SearchReceipt(query=query, hits=tuple(hits)))
    frame = validated_frame(
        [
            {
                "candidate_id": candidate.candidate_id,
                "command": candidate.command,
                "util": utilities[candidate.candidate_id],
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
        settings_sha256=digest(settings.model_dump_json().encode()),
        candidates=candidates,
        snippets=snippets,
        searches=tuple(searches),
    ).model_dump_json()
    return frame


def provider_snippet_candidates(
    settings: KeenableSnippetSettings, api_key: str
) -> DataFrame[CommandCandidatesTable]:
    """Build canonical candidates from indexed Keenable excerpts with offline-verifiable evidence."""
    try:
        return run(_collect, settings, api_key)
    except ExceptionGroup as errors:
        cause: BaseException = errors
        while isinstance(cause, BaseExceptionGroup) and len(cause.exceptions) == 1:
            cause = cause.exceptions[0]
        if isinstance(cause, KeenableError):
            raise cause from None
        raise
