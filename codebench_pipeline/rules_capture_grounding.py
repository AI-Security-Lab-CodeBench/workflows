from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Final, Literal
from urllib.parse import urlsplit

from pydantic import Field

from codebench_pipeline.grounding_tools import GroundingCredentials, GroundingFiles, LiveGroundingConfig
from codebench_pipeline.keenable_provider import KeenableIncident
from codebench_pipeline.live_grounding import LiveGrounding, LivePage, LiveReceipt, PageFetchInput, QueryInput
from codebench_pipeline.rules_capture_facts import PUBLIC_PROGRAMS
from codebench_pipeline.rules_contracts import Diagnostic, EvaluationError
from codebench_pipeline.typed_config import ConfigArtifact

_CALL_LIMIT: Final = 8
_BATCH_ATTEMPTS: Final = 4
_EXCERPT_LIMIT: Final = 3000
_TOPICS: Final = {
    "operands": "documented operand roles and option boundary",
    "options": "documented options, option arguments and semantics",
    "program": "documented distinction between embedded program, program file and input files",
}


@dataclass(frozen=True, slots=True)
class CaptureGrounding:
    config: LiveGroundingConfig
    credentials: GroundingCredentials

    @contextmanager
    def session(self, prior_calls: int) -> Generator[LiveGrounding]:
        with TemporaryDirectory(prefix="codebench-capture-grounding-") as directory:
            yield LiveGrounding(
                self.config, self.credentials, GroundingFiles(Path(directory)), max(0, _CALL_LIMIT - prior_calls)
            )


class CaptureDocumentationEvidence(ConfigArtifact):
    receipts: tuple[LiveReceipt, ...]
    pages: tuple[LivePage, ...]
    incidents: dict[int, tuple[KeenableIncident, ...]]


class DocumentationSearch(ConfigArtifact):
    program: str = Field(pattern=r"^[a-z][a-z0-9-]{0,31}$")
    topic: Literal["operands", "options", "program"]


class DocumentationFetch(ConfigArtifact):
    handle: str = Field(pattern=r"^doc-[0-9]+-[1-8]$")


class DocumentationHit(ConfigArtifact):
    handle: str
    url: str


class DocumentationDiscovery(ConfigArtifact):
    kind: Literal["search_results"]
    hits: tuple[DocumentationHit, ...]


class DocumentationPage(ConfigArtifact):
    kind: Literal["fetched_page"]
    page_id: str
    url: str
    excerpt: str


type DocumentationFeedback = DocumentationDiscovery | DocumentationPage


@dataclass(slots=True)
class BatchDocumentation:
    live: LiveGrounding
    batch_id: str
    programs: frozenset[str]
    start: int
    attempts: int = 0
    searched: set[DocumentationSearch] = field(default_factory=set)
    handles: dict[str, str] = field(default_factory=dict)

    def _available(self) -> bool:
        return self.attempts < _BATCH_ATTEMPTS and len(self.live.history) < self.live.limit

    def choices(self) -> tuple[DocumentationSearch, ...]:
        if not self._available():
            return ()
        return tuple(choice for program in sorted(self.programs) for choice in (
            DocumentationSearch(program=program, topic="operands"),
            DocumentationSearch(program=program, topic="options"),
            *((DocumentationSearch(program=program, topic="program"),) if program == "awk" else ()),
        ) if choice not in self.searched)

    def can_fetch(self) -> bool:
        return self._available() and bool(self.handles)

    def _begin(self) -> None:
        if not self._available():
            raise EvaluationError(Diagnostic(
                code="grounding_budget_exhausted", phase="documentation",
                message="No more lookups are available. Use masked train evidence or abstain.",
            ))
        self.attempts += 1

    def search(self, arguments: str) -> DocumentationDiscovery:
        self._begin()
        request = DocumentationSearch.model_validate_json(arguments)
        family = PUBLIC_PROGRAMS.get(request.program)
        if family is None or request.program not in self.programs or (request.topic == "program" and request.program != "awk"):
            raise EvaluationError(Diagnostic(
                code="documentation_scope", message="Copy a program/topic pair from documentation_choices. "
                "The program topic is only for awk; otherwise use operands or options.",
            ))
        if request in self.searched:
            raise EvaluationError(Diagnostic(
                code="documentation_repeat", message="That pair was already searched. Fetch an offered handle, "
                "choose a different listed pair, or work from the batch evidence.",
            ))
        self.searched.add(request)
        query = f"{family} {request.program} manual {_TOPICS[request.topic]}"
        receipt = self.live.call(
            "query", QueryInput(query=query, candidate_ids=[self.batch_id]).model_dump_json(), {self.batch_id}
        )
        self._require_success(receipt)
        hits = tuple(DocumentationHit(handle=f"doc-{receipt.index}-{index}", url=hit.url)
                     for index, hit in enumerate(receipt.discovery, 1) if not urlsplit(hit.url).query)
        self.handles.update((hit.handle, hit.url) for hit in hits)
        return DocumentationDiscovery(kind="search_results", hits=hits)

    def fetch(self, arguments: str) -> DocumentationPage:
        self._begin()
        request = DocumentationFetch.model_validate_json(arguments)
        if (url := self.handles.pop(request.handle, None)) is not None:
            return self._fetch(url)
        raise EvaluationError(Diagnostic(
            code="documentation_scope", message="Copy an unused handle from documentation.hits in this batch.",
        ))

    def _fetch(self, url: str) -> DocumentationPage:
        receipt = self.live.call(
            "page_fetch", PageFetchInput(url=url, candidate_ids=[self.batch_id]).model_dump_json(), {self.batch_id},
        )
        self._require_success(receipt)
        if receipt.page_id is None:
            raise EvaluationError(Diagnostic(code="documentation_response", message="No fetched page was retained."))
        page = self.live.pages[receipt.page_id]
        return DocumentationPage(
            kind="fetched_page", page_id=receipt.page_id, url=page.url, excerpt=page.content[:_EXCERPT_LIMIT],
        )

    def evidence(self) -> CaptureDocumentationEvidence:
        receipts = tuple(self.live.history[self.start:])
        return CaptureDocumentationEvidence(
            receipts=receipts, pages=tuple(self.live.pages[item.page_id] for item in receipts if item.page_id is not None),
            incidents={item.index: self.live.incidents[item.index] for item in receipts if item.index in self.live.incidents},
        )

    @staticmethod
    def _require_success(receipt: LiveReceipt) -> None:
        if receipt.error is not None:
            raise EvaluationError(Diagnostic(
                code=receipt.error, phase="documentation", message="Documentation lookup did not return evidence.",
            ))
