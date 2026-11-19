from __future__ import annotations

import shlex
from http.client import HTTPException

from pandera.typing import DataFrame
from pydantic import JsonValue, ValidationError

from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    DocumentAdaptersTable,
    SourceCatalogTable,
    SourceDocumentsTable,
    frame_records,
    validated_frame,
)
from codebench_pipeline.dataset_records import CandidateRecord
from codebench_pipeline.repository_acquisition import (
    CAPTURED_SOURCES,
    CATALOG_SOURCES,
    CaptureRuntime,
    capture_sources,
    catalog_origin,
    origin_receipt_failure,
)
from codebench_pipeline.repository_branch_config import RepositoryAcquisitionConfig
from codebench_pipeline.repository_config import RepositoryBranch
from codebench_pipeline.repository_config import load_repository_config as load_repository_config
from codebench_pipeline.repository_etl import (
    ADAPTER_BINDINGS,
    authorize_adapter,
    repository_rows,
    source_span,
)
from codebench_pipeline.repository_receipts import (
    RepositoryReceipt,
    RepositoryRun,
    read_receipt,
    record_receipt,
    sha256,
)
from codebench_pipeline.repository_tree import raw_url
from codebench_pipeline.source_transform_contract import TRANSFORM_OPTIONS, TransformContext
from codebench_pipeline.storage import IntegrityError


def repository_catalog(
    config: RepositoryAcquisitionConfig, branch: RepositoryBranch, run: RepositoryRun
) -> DataFrame[SourceCatalogTable]:
    """Enumerate a complete pinned scope and record policy and origin evidence."""
    rows: list[dict[str, JsonValue]] = []
    sources: list[JsonValue] = []
    origins: dict[str, JsonValue] = {}
    for origin in config.origins(branch):
        try:
            selected, evidence = catalog_origin(origin, run, config.maximum_file_bytes)
        except (ValidationError, IntegrityError, OSError, HTTPException) as error:
            origin_receipt_failure(run, origin, error)
            raise
        origins[origin.origin_id] = evidence
        for source in selected:
            sources.append(source.model_dump(mode="json"))
            rows.append(
                {
                    "source_id": source.source_id,
                    "source_url": raw_url(origin, source.path),
                    "license": origin.license_id,
                    "license_url": raw_url(origin, origin.license.path),
                    "attribution": origin.attribution,
                    "revision": origin.commit,
                    "retrieved_at": "awaiting capture",
                    "source_type": origin.source_type,
                    "provenance_type": origin.provenance_type,
                    "source_locator": source.path,
                    "sha256": "",
                    "format": origin.format,
                    "text_field": "$",
                    "context_field": "",
                    "sample": "",
                    "captured_content": "",
                    "decision": "exclude" if source.exclusion else "selected",
                    "analysis": "Complete pinned repository tree and rights evidence verified.",
                    "confidence": "not_applicable",
                    "rationale": source.exclusion or origin.use,
                }
            )
    return record_receipt(
        validated_frame(rows, SourceCatalogTable),
        RepositoryReceipt(
            stage="source_catalog",
            branch=branch,
            dataset_id=run.dataset_id,
            run_id=run.run_id,
            config_digest=config.branch_digest(branch),
            table_digest="0" * 64,
            upstream=(),
            observations={"complete": True, "origins": origins, "sources": sources},
        ),
        run,
    )


def repository_documents(
    catalog: DataFrame[SourceCatalogTable], config: RepositoryAcquisitionConfig, run: RepositoryRun
) -> DataFrame[SourceDocumentsTable]:
    """Capture the complete authorized scope with byte-level integrity and replay."""
    receipt = read_receipt(catalog, "source_catalog", run)
    if receipt.config_digest != config.branch_digest(receipt.branch):
        raise IntegrityError("Repository catalog is stale for the declared configuration")
    sources = CATALOG_SOURCES.validate_python(receipt.observations["sources"])
    indexed = {str(row["source_id"]): row for row in frame_records(catalog)}
    rows: list[dict[str, JsonValue]] = []
    captures: list[JsonValue] = []
    runtime = CaptureRuntime(limit=config.maximum_file_bytes, workers=config.workers, run=run)
    inherited = SourceDocumentsTable.to_schema().columns.keys() - {"content", "sha256", "retrieved_at"}
    for origin in config.origins(receipt.branch):
        eligible = tuple(
            source for source in sources if source.origin_id == origin.origin_id and not source.exclusion
        )
        for capture, text in capture_sources(eligible, origin, runtime):
            row = indexed[capture.source_id]
            rows.append(
                {
                    **{key: row[key] for key in inherited},
                    "content": text,
                    "sha256": capture.sha256,
                    "retrieved_at": capture.retrieved_at,
                }
            )
            captures.append(capture.model_dump(mode="json"))
    return record_receipt(
        validated_frame(rows, SourceDocumentsTable),
        receipt.follow(
            run, "source_documents", {"origins": receipt.observations["origins"], "captures": captures}
        ),
        run,
    )


def repository_adapters(
    documents: DataFrame[SourceDocumentsTable], config: RepositoryAcquisitionConfig, run: RepositoryRun
) -> DataFrame[DocumentAdaptersTable]:
    """Resolve adapters against the independent deployment allowlist before imports."""
    receipt = read_receipt(documents, "source_documents", run)
    if receipt.config_digest != config.branch_digest(receipt.branch):
        raise IntegrityError("Repository documents are stale for the declared configuration")
    origins = {
        origin.origin_id: authorize_adapter(origin.transform) for origin in config.origins(receipt.branch)
    }
    captures = CAPTURED_SOURCES.validate_python(receipt.observations["captures"])
    bindings: dict[str, JsonValue] = {
        capture.source_id: origins[capture.origin_id].model_dump(mode="json") for capture in captures
    }
    records = [
        {
            "source_id": row["source_id"],
            "format": row["format"],
            "text_field": row["text_field"],
            "context_field": row["context_field"],
            "rationale": "Pinned repository adapter and capture.",
        }
        for row in frame_records(documents)
    ]
    return record_receipt(
        validated_frame(records, DocumentAdaptersTable),
        receipt.follow(run, "document_adapters", {"bindings": bindings}),
        run,
    )


def repository_candidates(
    documents: DataFrame[SourceDocumentsTable],
    adapters: DataFrame[DocumentAdaptersTable],
    config: RepositoryAcquisitionConfig,
    run: RepositoryRun,
) -> DataFrame[CommandCandidatesTable]:
    """Extract inert examples with capture, adapter and exact source span bindings."""
    receipt = read_receipt(documents, "source_documents", run)
    adapter_receipt = read_receipt(adapters, "document_adapters", run)
    expected = config.branch_digest(receipt.branch)
    if (receipt.config_digest, adapter_receipt.config_digest) != (expected, expected):
        raise IntegrityError("Repository candidate inputs are stale for the declared configuration")
    if adapter_receipt.upstream != (receipt.digest,) or adapter_receipt.branch != receipt.branch:
        raise IntegrityError("Repository adapters were bound to different captures")
    bindings = ADAPTER_BINDINGS.validate_python(adapter_receipt.observations["bindings"])
    captures = {
        capture.source_id: capture
        for capture in CAPTURED_SOURCES.validate_python(receipt.observations["captures"])
    }
    approved = {
        origin.origin_id: authorize_adapter(origin.transform) for origin in config.origins(receipt.branch)
    }
    rows: list[dict[str, JsonValue]] = []
    evidence: dict[str, JsonValue] = {}
    empty: list[JsonValue] = []
    for document in frame_records(documents):
        source_id = str(document["source_id"])
        capture = captures[source_id]
        adapter = approved[capture.origin_id]
        if bindings[source_id] != adapter:
            raise IntegrityError(f"Repository adapter identity no longer matches deployment: {source_id}")
        text, url = str(document["content"]), str(document["source_url"])
        body = text.encode()
        if sha256(body) != capture.sha256 or (run.root / capture.capture_key).read_bytes() != body:
            raise IntegrityError(f"Repository capture differs from its persisted bytes: {source_id}")
        options = TRANSFORM_OPTIONS.validate_json(adapter.transform.options_json)
        context = TransformContext(source_id, url, capture.sha256, options)
        previous_count = len(rows)
        for row in repository_rows(text, adapter, context):
            try:
                words = shlex.split(row.command)
            except ValueError:
                words = row.command.split(maxsplit=1)
            if not words:
                continue
            candidate_id = sha256(f"{source_id}\n{row.source_position}\n{row.command}".encode())
            candidate = CandidateRecord(
                candidate_id=candidate_id,
                command=row.command,
                util=words[0].rsplit("/", 1)[-1] or words[0],
                context=row.context,
                source_id=source_id,
                source_url=url,
                source_position=row.source_position,
                transformations=row.transformations,
                risk_candidates=[],
            )
            rows.append(
                {
                    **candidate.model_dump(mode="json"),
                    "source_weight": config.source_weight(receipt.branch, capture.origin_id),
                }
            )
            evidence[candidate_id] = {
                "source_id": source_id,
                "origin_id": capture.origin_id,
                "source_url": url,
                "capture_key": capture.capture_key,
                "capture_sha256": capture.sha256,
                "source_position": row.source_position,
                "source_span": source_span(text, row.source_position).model_dump(mode="json"),
                "license": document["license"],
                "attribution": document["attribution"],
            }
        if len(rows) == previous_count:
            empty.append(source_id)
    return record_receipt(
        validated_frame(rows, CommandCandidatesTable),
        receipt.follow(
            run,
            "command_candidates",
            {
                "origins": receipt.observations["origins"],
                "candidates": evidence,
                "no_extractable_examples": empty,
            },
        ).model_copy(update={"upstream": (receipt.digest, adapter_receipt.digest)}),
        run,
    )
