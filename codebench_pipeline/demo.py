from __future__ import annotations

import shutil
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from codebench_pipeline.models import DatasetSpec, ReplaySpec, SourceDocument
from codebench_pipeline.storage import IntegrityError


class CandidatePointer(BaseModel):
    model_config = ConfigDict(extra="ignore")
    candidate_id: str
    source_ids: list[str]


class PacketPointer(BaseModel):
    model_config = ConfigDict(extra="ignore")
    candidate_ids: list[str]


class SourcePointer(BaseModel):
    model_config = ConfigDict(extra="ignore")
    source_id: str


def prepare_demo(repo: Path, destination: Path, packet_name: str) -> DatasetSpec:
    if destination.exists():
        return DatasetSpec.model_validate_json(
            (destination / "dataset.json").read_text(encoding="utf-8")
        ).model_copy(update={"adapter": None, "use_document_adapters": True})
    historical = repo / "data/pipeline/label-only-20261105"
    exchange = historical / "complete-exchange"
    packet_path = exchange / "packets" / packet_name
    ids = set(PacketPointer.model_validate_json(packet_path.read_bytes()).candidate_ids)
    annotation = historical / "historical-recovery/annotation"
    candidate_lines: list[str] = []
    source_ids: set[str] = set()
    for line in (annotation / "candidates.jsonl").read_text(encoding="utf-8").splitlines():
        pointer = CandidatePointer.model_validate_json(line)
        if pointer.candidate_id in ids:
            candidate_lines.append(line)
            source_ids.update(pointer.source_ids)
    if len(candidate_lines) != len(ids):
        raise IntegrityError("Historical packet candidates are missing")
    source_lines = [
        line
        for line in (annotation / "sources.jsonl").read_text(encoding="utf-8").splitlines()
        if SourcePointer.model_validate_json(line).source_id in source_ids
    ]
    manifest = repo / "research/pipeline/all-documents.jsonl"
    document_lines: list[str] = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if SourcePointer.model_validate_json(line).source_id in source_ids:
            document = SourceDocument.model_validate_json(line)
            document = document.model_copy(
                update={"local_path": (manifest.parent / document.local_path).resolve()}
            )
            document_lines.append(document.model_dump_json(exclude_none=True))
    if len(document_lines) != len(source_ids):
        raise IntegrityError("Demo packet includes sources without preserved document manifests")
    destination.mkdir(parents=True)
    for name, lines in (
        ("candidates.jsonl", candidate_lines),
        ("sources.jsonl", source_lines),
        ("documents.jsonl", document_lines),
    ):
        (destination / name).write_text("\n".join(lines) + "\n", encoding="utf-8")
    for directory in ("packets", "responses", "review-packets", "reviews"):
        (destination / directory).mkdir()
        shutil.copyfile(exchange / directory / packet_name, destination / directory / packet_name)
    semantics = repo / "research/pipeline/semantics-v2.jsonl"
    replay = ReplaySpec(
        candidates=destination / "candidates.jsonl",
        sources=destination / "sources.jsonl",
        semantics=semantics,
        packets=destination / "packets",
        responses=destination / "responses",
        review_packets=destination / "review-packets",
        reviews=destination / "reviews",
    )
    spec = DatasetSpec(
        dataset_id=packet_path.stem,
        revision="historical-2026-11-05",
        documents=destination / "documents.jsonl",
        semantics=semantics,
        use_document_adapters=True,
        replay=replay,
    )
    (destination / "dataset.json").write_text(spec.model_dump_json(indent=2), encoding="utf-8")
    return spec
