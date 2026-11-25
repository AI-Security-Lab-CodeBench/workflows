from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
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
    search_receipt: int = Field(ge=0)
    result: int = Field(ge=1, le=8)


class DocumentationHit(ConfigArtifact):
    result: int
    url: str


class DocumentationFeedback(ConfigArtifact):
    receipt: int
    hits: tuple[DocumentationHit, ...] = ()
    page_id: str | None = None
    url: str | None = None
    excerpt: str | None = None


@dataclass(frozen=True, slots=True)
class BatchDocumentation:
    live: LiveGrounding
    batch_id: str
    programs: frozenset[str]
    start: int

    def search(self, arguments: str) -> DocumentationFeedback:
        request = DocumentationSearch.model_validate_json(arguments)
        family = PUBLIC_PROGRAMS.get(request.program)
        if family is None or request.program not in self.programs or (request.topic == "program" and request.program != "awk"):
            raise EvaluationError(Diagnostic(
                code="documentation_scope", message="Select a supported public operation present in this batch.",
            ))
        query = f"{family} {request.program} manual {_TOPICS[request.topic]}"
        receipt = self.live.call(
            "query", QueryInput(query=query, candidate_ids=[self.batch_id]).model_dump_json(), {self.batch_id}
        )
        self._require_success(receipt)
        return DocumentationFeedback(
            receipt=receipt.index,
            hits=tuple(DocumentationHit(result=index, url=hit.url) for index, hit in enumerate(receipt.discovery, 1)
                       if not urlsplit(hit.url).query),
        )

    def fetch(self, arguments: str) -> DocumentationFeedback:
        request = DocumentationFetch.model_validate_json(arguments)
        if self.start <= request.search_receipt < len(self.live.history):
            search = self.live.history[request.search_receipt]
            if search.tool == "query" and request.result <= len(search.discovery):
                url = search.discovery[request.result - 1].url
                if not urlsplit(url).query:
                    return self._fetch(url)
        raise EvaluationError(Diagnostic(
            code="documentation_scope", message="Use a discovery result from a search in this batch.",
        ))

    def _fetch(self, url: str) -> DocumentationFeedback:
        receipt = self.live.call(
            "page_fetch", PageFetchInput(url=url, candidate_ids=[self.batch_id]).model_dump_json(), {self.batch_id},
        )
        self._require_success(receipt)
        if receipt.page_id is None:
            raise EvaluationError(Diagnostic(code="documentation_response", message="No fetched page was retained."))
        page = self.live.pages[receipt.page_id]
        return DocumentationFeedback(
            receipt=receipt.index, page_id=receipt.page_id, url=page.url, excerpt=page.content[:_EXCERPT_LIMIT],
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
