from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import JsonValue

from codebench_pipeline.batch_labeling import RowEvidence
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_inventory import SharedOperations, shared_passages
from codebench_pipeline.grounding_passages import Applicability
from codebench_pipeline.grounding_policy import GroundingRecord, OperationGrant, command_tokens
from codebench_pipeline.grounding_sources import CandidateCapture, CandidateEvidence
from codebench_pipeline.grounding_tools import (
    DocumentationQuery,
    GroundingBroker,
    GroundingConfig,
)
from codebench_pipeline.keenable_policy import RESPONSE_BYTES, canonical_url
from codebench_pipeline.models import Digest, SemanticSnippet
from codebench_pipeline.repository_receipts import canonical, publish_bytes
from codebench_pipeline.storage import IntegrityError, digest, fingerprint
from scripts.dataset_schema import SECRET_PATTERNS


def trusted_origin(config: GroundingConfig, url: str) -> bool:
    return canonical_url(url) == url and (
        urlsplit(url).hostname in {domain.lower() for domain in config.allowed_domains}
        or any(
            url.rstrip("/") == prefix.rstrip("/") or url.startswith(prefix.rstrip("/") + "/")
            for prefix in config.trusted_url_prefixes
        )
    )


def admitted_excerpt(
    candidate: CandidateRecord, url: str, quote: str, binding: str, applicability: str
) -> RowEvidence:
    snippet = SemanticSnippet(
        utility=candidate.util,
        subcommands=[candidate.command],
        url=url,
        retrieved_at="content-addressed",
        snippet=quote,
        version_limits=applicability,
    )
    return RowEvidence(
        evidence_id=fingerprint(
            {
                "candidate_id": candidate.candidate_id,
                "binding": binding,
                "snippet": snippet.model_dump(mode="json"),
            }
        ),
        snippet=snippet,
        provider_metadata=binding,
    )


@dataclass(frozen=True, slots=True)
class GuardedEvidence:
    sources: CandidateEvidence
    config: GroundingConfig
    root: Path
    shared_operations: SharedOperations | None = None

    @property
    def source_limited(self) -> bool:
        return self.sources.provider.declaration.source_scoped

    def source(self, candidate: CandidateRecord) -> tuple[str, list[RowEvidence]]:
        capture = self.sources.resolve(candidate)
        binding = fingerprint(
            {
                "source": capture.source_binding,
                "source_versions": None if capture.versions is None else list(capture.versions),
                "implementation": {
                    path.name: digest(path) for path in Path(__file__).parent.glob("grounding*.py")
                },
            }
        )
        if not self.source_limited:
            return binding, []
        admitted = self._shared_evidence(candidate, capture)
        return fingerprint({"source": binding, "shared": [item.evidence_id for item in admitted]}), admitted

    def preflight(self, broker: GroundingBroker, candidate: CandidateRecord) -> list[RowEvidence]:
        return self.lookup(broker, None, [candidate]).get(candidate.candidate_id, [])

    def _capture(self, candidate: CandidateRecord, receipt: OperationReceipt) -> RowEvidence:
        path = self.root / "origin-bytes" / receipt.grant.capture_sha256
        with path.open("rb") as stream:
            body = stream.read(RESPONSE_BYTES + 1)
        if len(body) > RESPONSE_BYTES:
            raise IntegrityError("Operation origin capture exceeds its byte budget")
        return self._excerpt(candidate, receipt, body)

    def _excerpt(self, candidate: CandidateRecord, receipt: OperationReceipt, body: bytes) -> RowEvidence:
        grant = receipt.grant
        if sha256(body).hexdigest() != grant.capture_sha256:
            raise IntegrityError("Operation origin capture digest mismatch")
        if grant.byte_end > len(body):
            raise IntegrityError("Operation quote exceeds its authorized origin bytes")
        quote = body[grant.byte_start : grant.byte_end].decode()
        if not quote.strip() or any(pattern.search(quote) for pattern in SECRET_PATTERNS):
            raise IntegrityError("Authorized operation span is empty or sensitive")
        return admitted_excerpt(candidate, grant.url, quote, receipt.digest, grant.applicability)

    def _authorizations(self, candidate: CandidateRecord, capture: CandidateCapture) -> list[OperationGrant]:
        authorized: list[OperationGrant] = []
        now = datetime.now(UTC)
        for grant in self.config.operation_grants:
            if (
                not grant.retain_origin_bytes
                or not trusted_origin(self.config, grant.url)
                or (grant.library_id is not None and grant.library_id not in self.config.pinned_library_ids)
                or not grant.applies(candidate, now)
                or not capture.covered_by(grant, candidate.source_position)
                or (
                    grant.source_revision is not None
                    and (grant.url, grant.capture_sha256, grant.source_revision)
                    != (capture.url, capture.capture_sha256, capture.revision)
                )
            ):
                continue
            try:
                with (self.root / "rights" / f"{grant.rights_sha256}.json").open("rb") as stream:
                    rights = stream.read(65537)
            except FileNotFoundError:
                continue
            if len(rights) <= 65536 and sha256(rights).hexdigest() == grant.rights_sha256:
                authorized.append(grant)
        return authorized

    def verify(self, candidate: CandidateRecord, evidence: RowEvidence, quote: str) -> None:
        if quote != evidence.snippet.snippet:
            raise ValueError("Citation must reproduce the complete admitted passage")
        capture = self.sources.resolve(candidate)
        if self.source_limited:
            if evidence in self._shared_evidence(candidate, capture, verify=True):
                return
            raise ValueError("Citation lacks a current source-level operation passage")
        authorized = self._authorizations(candidate, capture)
        if authorized:
            path = self.root / "verified-operations" / f"{evidence.evidence_id}.json"
            if path.stem == evidence.evidence_id and len(evidence.evidence_id) == 64:
                payload = path.read_bytes()
                if sha256(payload).hexdigest() != evidence.provider_metadata:
                    raise IntegrityError("Operation admission receipt differs from its verified digest")
                receipt = OperationReceipt.model_validate_json(payload)
                if (
                    receipt.grant in authorized
                    and (receipt.source_binding, receipt.source_versions, receipt.returned_version)
                    == (capture.source_binding, capture.versions, receipt.grant.requested_version)
                    and evidence == self._capture(candidate, receipt)
                ):
                    return
        raise ValueError("Citation lacks a current candidate or exact-operation origin authorization")

    def lookup(
        self, broker: GroundingBroker, query: DocumentationQuery | None, candidates: list[CandidateRecord]
    ) -> dict[str, list[RowEvidence]]:
        if not self.source_limited:
            return {}
        return {
            candidate.candidate_id: items
            for candidate in candidates
            if (items := self._shared_evidence(candidate, self.sources.resolve(candidate)))
        }

    def _shared_evidence(
        self, candidate: CandidateRecord, capture: CandidateCapture, *, verify: bool = False
    ) -> list[RowEvidence]:
        if capture.versions != ():
            return []
        tokens = tuple(command_tokens(candidate))
        if not tokens or len(candidate.command.encode()) > 256:
            return []
        branch, _, original_id = candidate.candidate_id.partition(":")
        if branch == "keenable":
            origin = self.sources.provider_rows[original_id]
            start, end = origin.start, origin.end
        else:
            span = self.sources.repositories[branch].rows[original_id].source_span
            start, end = span.byte_start, span.byte_end
        if Applicability(capture.body).overlaps(start, end):
            return []
        self.sources.provider.declaration.check_current(datetime.now(UTC))
        operations = self.shared_operations
        if operations is None:
            raise IntegrityError("Source-level grounding requires a verified shared operation index")
        own = shared_passages(capture.url, capture.source_binding, capture.body)
        selected = next((entry for entry in own if entry.passage.matches(tokens)), None)
        if selected is None:
            selected = next(
                (
                    entry
                    for entry in operations.operation(tokens)
                    if entry.url != capture.url and entry.passage.matches(tokens)
                ),
                None,
            )
        if selected is None:
            return []
        receipt: dict[str, JsonValue] = {
            "candidate_source": capture.source_binding,
            "origin_binding": selected.origin_binding,
            "origin_sha256": selected.origin_sha256,
            "url": selected.url,
            "byte_start": selected.passage.start,
            "byte_end": selected.passage.end,
            "matcher": "verified-container-applicability-v5",
        }
        receipt_digest = fingerprint(receipt)
        path = self.root / "shared-operations" / f"{receipt_digest}.json"
        payload = canonical(receipt)
        if verify:
            if path.read_bytes() != payload:
                raise IntegrityError("Shared operation admission receipt differs from its verified bytes")
        else:
            publish_bytes(path, payload)
        return [
            admitted_excerpt(
                candidate,
                selected.url,
                selected.quote,
                receipt_digest,
                "No restriction detected in capture context",
            )
        ]


class OperationReceipt(GroundingRecord):
    grant: OperationGrant
    source_binding: str
    source_versions: tuple[str, ...] | None
    returned_version: str | None
    lead_sha256: Digest

    @property
    def body(self) -> bytes:
        return canonical(self.model_dump(mode="json"))

    @property
    def digest(self) -> str:
        return sha256(self.body).hexdigest()
