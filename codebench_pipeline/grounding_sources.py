import re
from dataclasses import dataclass
from hashlib import sha256
from typing import Final, assert_never

import pandas as pd
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.collection_config import DeclaredCollection
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.grounding_policy import (
    GroundingRecord,
    OperationGrant,
    SourceVersion,
    VersionIndependent,
    version_constraints,
)
from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_receipts import CandidateReceipt, CaptureLock
from codebench_pipeline.keenable_tables import CANDIDATE_RECEIPTS, provider_source_id
from codebench_pipeline.labeling_stage_settings import (
    LabelingSources,
    RepositoryEvidenceOrigin,
    RepositoryEvidenceSettings,
)
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_config import check_path
from codebench_pipeline.repository_etl import SourceSpan, source_span
from codebench_pipeline.repository_receipts import RepositoryReceipt, RepositoryRun, canonical
from codebench_pipeline.repository_tree import tree_url
from codebench_pipeline.storage import IntegrityError, fingerprint

BRANCH_METADATA: Final = TypeAdapter(dict[str, dict[str, JsonValue]])
_FENCE: Final = re.compile(r"(?m)^[ \t]*(?:`{3,}|~{3,})([^\n]*)\n")
_ORIGIN_BINDINGS: Final = "origin-bindings"


def persist_repository_origin_bindings(collection: DeclaredCollection, run: RepositoryRun) -> None:
    """Retain the canonical origin bytes needed to reproduce published binding IDs."""
    for branch in ("dataset", "documentation"):
        for origin in collection.repository_config(branch).origins(branch):
            payload = canonical(origin.model_dump(mode="json"))
            run.write_evidence(f"{_ORIGIN_BINDINGS}/{sha256(payload).hexdigest()}.json", payload)


def origin_binding_payload(run: RepositoryRun, origin_sha256: Digest) -> bytes:
    payload = run.read_evidence(f"{_ORIGIN_BINDINGS}/{origin_sha256}.json")
    if sha256(payload).hexdigest() != origin_sha256:
        raise IntegrityError("Repository origin binding bytes differ from the authorized digest")
    return payload


def source_versions(body: bytes, command_start: int) -> tuple[str, ...] | None:
    """Read the bounded prose introducing the captured command, without crossing a prior code block."""
    prefix = body[max(0, command_start - 1200) : command_start].decode(errors="ignore")
    sections = _FENCE.split(prefix)
    if len(sections) > 1 and (sections[-2].strip() or not sections[-1].strip()):
        return version_constraints(sections[-3])
    return version_constraints(sections[-1])


class RepositoryCandidate(GroundingRecord):
    source_id: str
    origin_id: str
    source_url: str
    capture_key: str
    capture_sha256: Digest
    source_position: str
    source_span: SourceSpan
    license: str
    attribution: str


REPOSITORY_CANDIDATES: Final = TypeAdapter(dict[str, RepositoryCandidate])


@dataclass(frozen=True, slots=True)
class CandidateCapture:
    origin_id: str
    source_binding: str
    url: str
    body: bytes
    capture_sha256: str
    revision: str
    versions: tuple[str, ...] | None

    def covered_by(self, grant: OperationGrant, position: str) -> bool:
        scope = grant.version_scope
        match scope:
            case None:
                return False
            case VersionIndependent():
                return True
            case SourceVersion():
                return (
                    scope.source_url,
                    scope.source_capture_sha256,
                    scope.source_position,
                    scope.source_revision,
                ) == (self.url, self.capture_sha256, position, self.revision)
        assert_never(scope)


@dataclass(frozen=True, slots=True)
class RepositoryBranchEvidence:
    config: RepositoryEvidenceSettings
    receipt: RepositoryReceipt
    rows: dict[str, RepositoryCandidate]

    def current_origin(self, origin_id: str) -> RepositoryEvidenceOrigin:
        origins = {origin.origin_id: origin for origin in self.config.origins}
        origin = origins[origin_id]
        observed = self.receipt.observations["origins"]
        if not isinstance(observed, dict) or not isinstance(proof := observed.get(origin.origin_id), dict):
            raise IntegrityError("Repository origin evidence is missing")
        rights = proof.get("rights")
        if (
            (proof.get("repository"), proof.get("commit"), proof.get("tree_digest"), proof.get("tree_url"))
            != (origin.repository, origin.commit, origin.tree_digest, tree_url(origin))
            or not isinstance(rights, list)
            or [(item.get("path"), item.get("sha256")) for item in rights if isinstance(item, dict)]
            != [(item.path, item.sha256) for item in origin.rights]
        ):
            raise IntegrityError("Repository origin differs from its current rights authorization")
        return origin

    def candidate(self, row: CandidateRecord, original_id: str, run: RepositoryRun) -> CandidateCapture:
        source = self.rows[original_id]
        origin = self.current_origin(source.origin_id)
        if (
            origin.license_id != source.license
            or origin.attribution != source.attribution
            or (row.source_id, row.source_url, row.source_position)
            != (f"{self.receipt.branch}:{source.source_id}", source.source_url, source.source_position)
        ):
            raise IntegrityError("Candidate provenance differs from its current repository authorization")
        check_path(source.capture_key)
        body = run.read_capture(source.capture_key, self.config.maximum_file_bytes)
        if len(body) > self.config.maximum_file_bytes or sha256(body).hexdigest() != source.capture_sha256:
            raise IntegrityError("Repository candidate capture digest mismatch")
        start, end = source.source_span.byte_start, source.source_span.byte_end
        if (
            not 0 <= start < end <= len(body)
            or source_span(body.decode(), row.source_position) != source.source_span
        ):
            raise IntegrityError("Repository candidate span is outside its verified capture")
        span = body[start:end].decode()
        if row.command not in span:
            raise IntegrityError("Repository candidate command is absent from its source span")
        binding = sha256(
            b'{"origin":'
            + origin_binding_payload(run, origin.origin_sha256)
            + b',"source":'
            + canonical(source.model_dump(mode="json"))
            + b"}"
        ).hexdigest()
        return CandidateCapture(
            origin.origin_id,
            binding,
            source.source_url,
            body,
            source.capture_sha256,
            origin.commit,
            source_versions(body, start),
        )


@dataclass(frozen=True, slots=True)
class CandidateEvidence:
    repositories: dict[str, RepositoryBranchEvidence]
    provider: KeenableCollection
    lock: CaptureLock
    provider_rows: dict[str, CandidateReceipt]
    source_scoped: bool
    run: RepositoryRun

    @classmethod
    def from_frame(
        cls, frame: pd.DataFrame, run: RepositoryRun, collection: LabelingSources
    ) -> "CandidateEvidence":
        attrs = BRANCH_METADATA.validate_python(frame.attrs.get("branch_metadata"))
        repositories: dict[str, RepositoryBranchEvidence] = {}
        for branch, config in (
            ("dataset", collection.existing_datasets),
            ("documentation", collection.documentation_crawl),
        ):
            receipt = RepositoryReceipt.model_validate_json(str(attrs[branch]["repository_receipt_json"]))
            if (
                receipt.branch != branch
                or receipt.dataset_id != run.dataset_id
                or receipt.stage != "command_candidates"
                or receipt.config_digest != config.receipt_config_digest
                or receipt.observations.get("complete") is not True
                or run.read_evidence(f"receipts/{receipt.digest}.json") != receipt.body
            ):
                raise IntegrityError("Repository terminal receipt does not match the registered branch")
            rows = REPOSITORY_CANDIDATES.validate_python(receipt.observations["candidates"])
            repositories[branch] = RepositoryBranchEvidence(config, receipt, rows)
        access = collection.keenable_use_cases
        provider = KeenableCollection(run.dataset_home, access)
        lock = CaptureLock.model_validate_json(str(attrs["keenable"]["keenable_capture_lock"]))
        provider.adapters(lock)
        return cls(
            repositories,
            provider,
            lock,
            {
                item.candidate_id: item
                for item in CANDIDATE_RECEIPTS.validate_json(str(attrs["keenable"]["keenable_receipt_json"]))
            },
            not access.legacy_reviewed_spans,
            run,
        )

    def resolve(self, candidate: CandidateRecord) -> CandidateCapture:
        branch, _, original_id = candidate.candidate_id.partition(":")
        if branch != "keenable":
            return self.repositories[branch].candidate(candidate, original_id, self.run)
        source = self.provider_rows[original_id]
        body = self.provider.read_body(self.lock, source.url)
        literal = body[source.start : source.end]
        if (
            (candidate.source_id, candidate.source_url, candidate.source_position, candidate.command)
            != (
                f"keenable:{provider_source_id(source.url)}",
                source.url,
                f"bytes:{source.start}:{source.end}",
                source.command,
            )
            or not 0 <= source.start < source.end <= len(body)
            or literal != source.command.encode()
            or sha256(literal).hexdigest() != source.span_sha256
            or sha256(body).hexdigest() != source.body_sha256
        ):
            raise IntegrityError("Keenable candidate differs from its verified capture")
        binding = fingerprint(
            {"source": source.model_dump(mode="json"), "declaration": self.lock.declaration_sha256}
        )
        return CandidateCapture(
            source.origin_id,
            binding,
            source.url,
            body,
            source.body_sha256,
            self.provider.declaration.generation,
            source_versions(body, source.start),
        )
