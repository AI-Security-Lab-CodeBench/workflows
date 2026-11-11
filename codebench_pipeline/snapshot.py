from __future__ import annotations

from pathlib import Path

from pydantic import JsonValue

from codebench_pipeline.models import DatasetSpec, SourceDocument
from codebench_pipeline.storage import IntegrityError, digest
from scripts.dataset_schema import SECRET_PATTERNS


def documents(spec: DatasetSpec) -> list[SourceDocument]:
    records = [
        SourceDocument.model_validate_json(line)
        for line in spec.documents.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    identifiers = [record.source_id for record in records]
    if len(identifiers) != len(set(identifiers)):
        raise IntegrityError("Duplicate source document identity")
    return records


def source_path(spec: DatasetSpec, document: SourceDocument) -> Path:
    return (spec.documents.parent / document.local_path).resolve()


def source_inputs(spec: DatasetSpec) -> dict[str, str]:
    result = {"documents": digest(spec.documents)}
    for document in documents(spec):
        value = digest(source_path(spec, document))
        if document.sha256 and value != document.sha256:
            raise IntegrityError(f"Source checksum mismatch: {document.source_id}")
        result[document.source_id] = value
    return result


def build_snapshot(spec: DatasetSpec, out: Path) -> dict[str, JsonValue]:
    entries = documents(spec)
    (out / "documents").mkdir()
    manifest: list[str] = []
    for index, document in enumerate(entries):
        body = source_path(spec, document).read_bytes()
        text = body.decode("utf-8")
        if any(pattern.search(text) for pattern in SECRET_PATTERNS):
            raise IntegrityError(f"Source contains a sensitive-value indicator: {document.source_id}")
        relative = Path("documents") / f"{index:05d}.txt"
        (out / relative).write_bytes(body)
        updated = document.model_copy(update={"local_path": relative})
        manifest.append(updated.model_dump_json(exclude_none=True))
    (out / "documents.jsonl").write_text("\n".join(manifest) + "\n", encoding="utf-8")
    return {
        "documents": len(entries),
        "mode": "historical_replay" if spec.replay else "fresh",
    }
