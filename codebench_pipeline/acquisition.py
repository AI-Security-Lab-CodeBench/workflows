from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.request import Request, urlopen

from scripts.dataset_schema import SECRET_PATTERNS

from .agents import propose_adapter
from .discovery_models import (
    AcquisitionReceipt,
    DiscoveryError,
    DiscoveryFailure,
    DiscoveryOutput,
    InspectedSelection,
    SemanticsDocument,
    is_http_url,
    save_model,
)
from .discovery_selection import gate_selection
from .models import DatasetSpec, SourceDocument


def acquire(
    discovery: Path | DiscoveryOutput,
    output: Path,
    dataset_id: str,
    revision: str,
    semantics: Path,
    model: str,
) -> DatasetSpec:
    """Acquire full inert UTF-8 documents and register a local dataset.json.

    Accepts discovery.json or the typed discover result. Only inspected proposals
    with explicit permitted reuse and human provenance qualify. Semantics is an
    existing JSONL of documentation snippets, validated and copied verbatim.
    CODEBENCH_DOWNLOAD_MAX_BYTES (default 10 MiB) caps each full source; oversized,
    redirected, non-UTF-8 or drifted sources fail rather than becoming snapshots.
    Partial downloads, inference evidence and acquisition failures remain in the
    fresh output directory. dataset.json is written only after all inputs exist.
    """
    parsed = (
        DiscoveryOutput.model_validate_json(discovery.read_text(encoding="utf-8"))
        if isinstance(discovery, Path)
        else discovery
    )
    selection = gate_selection(InspectedSelection(sources=parsed.sources), parsed.inspected)
    eligible = [source for source in selection.sources if source.decision == "proposed"]
    if not eligible:
        raise DiscoveryError("No eligible inspected sources to acquire")
    semantics_bytes = semantics.read_bytes()
    docs = [
        SemanticsDocument.model_validate_json(line)
        for line in semantics_bytes.decode("utf-8").splitlines()
        if line.strip()
    ]
    if not docs:
        raise DiscoveryError("Acquisition requires supplied subcommand documentation")
    limit = int(os.environ.get("CODEBENCH_DOWNLOAD_MAX_BYTES", str(10 * 1024 * 1024)))
    if limit <= 0:
        raise DiscoveryError("Download byte limit must be positive")
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    save_model(output / "discovery.json", parsed)
    documents: list[SourceDocument] = []
    try:
        for index, source in enumerate(eligible):
            if not is_http_url(source.url):
                raise DiscoveryError("Acquisition URL must be HTTP(S) without credentials")
            target = output / f"source-{index:04d}"
            target.mkdir()
            with urlopen(
                Request(source.url, headers={"User-Agent": "CodeBench/1.0"}), timeout=60
            ) as response:
                final_url = response.geturl()
                content_type = response.headers.get_content_type()
                body = response.read(limit + 1)
            if final_url != source.url:
                raise DiscoveryError("Redirected source must be independently inspected before acquisition")
            if len(body) > limit:
                raise DiscoveryError("Source exceeds full-snapshot byte limit")
            text = body.decode("utf-8")
            if any(pattern.search(text) for pattern in SECRET_PATTERNS):
                raise DiscoveryError("Source contains a sensitive-value indicator; content was not persisted")
            if not text.strip() or source.commands_evidence not in text or source.origin_evidence not in text:
                raise DiscoveryError(
                    "Full source no longer contains inspected command or provenance evidence"
                )
            local_path = target / "source.txt"
            with local_path.open("xb") as stream:
                stream.write(body)
            save_model(
                target / "receipt.json",
                AcquisitionReceipt(
                    source_id=source.source_id,
                    requested_url=source.url,
                    final_url=final_url,
                    retrieved_at=datetime.now(UTC).isoformat(),
                    sha256=hashlib.sha256(body).hexdigest(),
                    bytes=len(body),
                    content_type=content_type,
                ),
            )
            adapter = propose_adapter(text[:20000], target / "adapter", model)
            provenance: Literal["human_authored", "human_collected"]
            if source.origin == "human_authored":
                provenance = "human_authored"
            elif source.origin == "human_collected":
                provenance = "human_collected"
            else:
                raise DiscoveryError("Acquisition requires inspected human provenance")
            documents.append(
                SourceDocument(
                    source_id=source.source_id,
                    url=source.url,
                    local_path=local_path,
                    license=source.license,
                    license_url=source.license_url,
                    source_type=source.source_type,
                    accessibility="accessible",
                    reuse_decision="permitted",
                    commands_present=True,
                    revision=source.revision,
                    retrieved_at=datetime.now(UTC).isoformat(),
                    provenance_type=provenance,
                    format=adapter.format,
                    text_field=adapter.text_field,
                    context_field=adapter.context_field,
                    source_locator=source.commands_evidence,
                    sha256=hashlib.sha256(body).hexdigest(),
                )
            )
        manifest = output / "documents.jsonl"
        with manifest.open("x", encoding="utf-8") as stream:
            for document in documents:
                stream.write(document.model_dump_json() + "\n")
        semantics_path = output / "semantics.jsonl"
        with semantics_path.open("xb") as stream:
            stream.write(semantics_bytes)
        dataset = DatasetSpec(
            dataset_id=dataset_id,
            revision=revision,
            documents=manifest,
            semantics=semantics_path,
            label_model=model,
            review_model=model,
            use_document_adapters=True,
        )
        save_model(output / "dataset.json", dataset)
        return dataset
    except (OSError, ValueError, DiscoveryError, RuntimeError) as error:
        save_model(
            output / "failure.json",
            DiscoveryFailure(operation="acquire", target=dataset_id, error=str(error)),
        )
        raise
