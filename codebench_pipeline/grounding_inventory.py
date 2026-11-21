from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from functools import cached_property
from hashlib import sha256

from pandera.typing import DataFrame

from codebench_pipeline.dataframe_schemas import SourceDocumentsTable
from codebench_pipeline.grounding_passages import OperationPassage, operation_passages
from codebench_pipeline.grounding_sources import (
    CandidateEvidence,
    RepositoryBranchEvidence,
    origin_binding_payload,
)
from codebench_pipeline.keenable_policy import RESPONSE_BYTES, canonical_url, record_digest
from codebench_pipeline.keenable_receipts import CaptureLock, VerifiedPage
from codebench_pipeline.repository_acquisition import CAPTURED_SOURCES, CATALOG_SOURCES
from codebench_pipeline.repository_receipts import RepositoryReceipt, RepositoryRun, canonical, read_receipt
from codebench_pipeline.repository_tree import git_blob_sha1, raw_url
from codebench_pipeline.storage import IntegrityError, fingerprint
from scripts.dataset_schema import SECRET_PATTERNS


@dataclass(frozen=True, slots=True)
class DocumentationCapture:
    url: str
    binding: str
    sha256: str
    git_blob: str
    size: int
    run: RepositoryRun

    def read(self) -> bytes:
        body = self.run.read_capture(f"blobs/{self.git_blob}", self.size)
        if (
            len(body) != self.size
            or sha256(body).hexdigest() != self.sha256
            or git_blob_sha1(body) != self.git_blob
        ):
            raise IntegrityError("Documentation inventory capture digest mismatch")
        return body


@dataclass(frozen=True, slots=True)
class SharedPassage:
    url: str
    origin_binding: str
    origin_sha256: str
    passage: OperationPassage
    quote: str


def shared_passages(url: str, binding: str, body: bytes) -> Iterator[SharedPassage]:
    if canonical_url(url) != url:
        return
    origin_sha256 = sha256(body).hexdigest()
    for passage in operation_passages(body):
        quote = body[passage.start : passage.end].decode()
        if not any(pattern.search(quote) for pattern in SECRET_PATTERNS):
            yield SharedPassage(url, binding, origin_sha256, passage, quote)


@dataclass(frozen=True)
class SharedOperations:
    sources: CandidateEvidence
    pages: Mapping[str, bytes]
    documentation: tuple[DocumentationCapture, ...]

    @cached_property
    def approved_pages(self) -> dict[str, VerifiedPage]:
        if not isinstance(self.sources.lock, CaptureLock) or self.sources.provider is None:
            return {}
        declaration = self.sources.provider.declaration
        return {
            page.url: page
            for page in self.sources.lock.pages
            if page.case is not None
            and canonical_url(page.url) == page.url
            and declaration.authority(page.url, page.case) is not None
        }

    def _passages(self) -> Iterable[SharedPassage]:
        for page in self.approved_pages.values():
            assert isinstance(self.sources.lock, CaptureLock)
            binding = fingerprint(
                {"declaration": self.sources.lock.declaration_sha256, "page": record_digest(page)}
            )
            yield from shared_passages(page.url, binding, self.pages[page.url])
        for document in self.documentation:
            yield from shared_passages(document.url, document.binding, document.read())

    @cached_property
    def index(self) -> dict[tuple[str, ...], list[SharedPassage]]:
        index: dict[tuple[str, ...], list[SharedPassage]] = {}
        for item in self._passages():
            index.setdefault(item.passage.route, []).append(item)
        return index

    def operation(self, tokens: tuple[str, ...]) -> Iterator[SharedPassage]:
        for key in dict.fromkeys((tokens[:2], tokens[:1])):
            yield from self.index.get(key, ())


def documentation_captures(
    branch: RepositoryBranchEvidence,
    documentation_capture: DataFrame[SourceDocumentsTable],
    run: RepositoryRun,
) -> tuple[DocumentationCapture, ...]:
    """Verify the explicit capture and scope receipts against the terminal candidate receipt."""
    terminal = branch.receipt
    if terminal.branch != "documentation" or len(terminal.upstream) != 2:
        raise IntegrityError("Documentation candidate receipt lacks its source inventory")
    documents = read_receipt(documentation_capture, "source_documents", run)
    if len(documents.upstream) != 1:
        raise IntegrityError("Documentation source receipt lacks its catalog")
    scope = documentation_capture.attrs.get("documentation_scope_receipt_json")
    if not isinstance(scope, str):
        raise IntegrityError("Documentation capture lacks its scope receipt")
    catalog = RepositoryReceipt.model_validate_json(scope)
    if run.read_evidence(f"receipts/{catalog.digest}.json") != catalog.body:
        raise IntegrityError("Documentation scope differs from its verified receipt")
    for stage, digest, receipt in (
        ("source_documents", terminal.upstream[0], documents),
        ("source_catalog", documents.upstream[0], catalog),
    ):
        if (
            receipt.stage != stage
            or receipt.digest != digest
            or receipt.branch != terminal.branch
            or receipt.dataset_id != terminal.dataset_id
            or receipt.config_digest != terminal.config_digest
            or receipt.observations.get("complete") is not True
            or receipt.observations["origins"] != terminal.observations["origins"]
        ):
            raise IntegrityError("Documentation inventory has no current verified receipt")
    limit = min(branch.config.maximum_file_bytes, RESPONSE_BYTES)
    origins = {origin.origin_id: branch.current_origin(origin.origin_id) for origin in branch.config.origins}
    sources = {
        row.source_id: row
        for row in CATALOG_SOURCES.validate_python(catalog.observations["sources"])
        if not row.exclusion
    }
    captures: list[DocumentationCapture] = []
    for capture in CAPTURED_SOURCES.validate_python(documents.observations["captures"]):
        source = sources[capture.source_id]
        if (source.origin_id, source.git_blob_sha1, source.size) != (
            capture.origin_id,
            capture.git_blob_sha1,
            capture.size,
        ) or capture.capture_key != f"blobs/{source.git_blob_sha1}":
            raise IntegrityError("Documentation capture is outside its declared inventory")
        if 0 < capture.size <= limit:
            origin = origins[source.origin_id]
            captures.append(
                DocumentationCapture(
                    url=raw_url(origin, source.path),
                    binding=sha256(
                        b'{"capture_sha256":'
                        + canonical(capture.sha256)
                        + b',"origin":'
                        + origin_binding_payload(run, origin.origin_sha256)
                        + b',"source":'
                        + canonical(source.model_dump(mode="json"))
                        + b"}"
                    ).hexdigest(),
                    sha256=capture.sha256,
                    git_blob=source.git_blob_sha1,
                    size=capture.size,
                    run=run,
                )
            )
    return tuple(captures)
