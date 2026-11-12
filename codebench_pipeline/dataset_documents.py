import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPException
from pathlib import Path

import pandera.pandas as pa
from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.dataframe_schemas import (
    DocumentAdaptersTable,
    SourceCatalogTable,
    SourceDocumentsTable,
    frame_records,
    validated_frame,
)
from codebench_pipeline.dataset_config import AdapterConfig, SnapshotConfig
from codebench_pipeline.dataset_seed_config import SourceSeed
from codebench_pipeline.models import AdapterSpec
from codebench_pipeline.source_retrieval import RetrievedSource, retrieve_source
from codebench_pipeline.storage import IntegrityError


@pa.check_types
def snapshot_documents(
    catalog: DataFrame[SourceCatalogTable], config: SnapshotConfig,
    *, cache: Path | None = None, evidence: Path | None = None,
) -> DataFrame[SourceDocumentsTable]:
    supplied = {source.source_id: source for source in config.documents}
    selected = [row for row in frame_records(catalog) if row["decision"] == "selected"]
    if any(str(row["source_id"]) not in supplied for row in selected):
        raise IntegrityError("Every acquired source must belong to the category source manifest")

    def retrieve(source: SourceSeed) -> RetrievedSource:
        try:
            return retrieve_source(source, config.maximum_bytes, cache, evidence)
        except (OSError, ValueError, IntegrityError, HTTPException) as error:
            if evidence is not None:
                evidence.mkdir(parents=True, exist_ok=True)
                identity = hashlib.sha256(source.source_id.encode()).hexdigest()
                (evidence / f"{identity}.failure.json").write_text(
                    json.dumps({
                        "source_id": source.source_id,
                        "url": source.url,
                        "complete": False,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    }),
                    encoding="utf-8",
                )
            raise

    sources = [supplied[str(row["source_id"])] for row in selected]
    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        retrieved = list(executor.map(retrieve, sources))
    rows = [
        {
            **{key: value for key, value in row.items()
               if key not in {"sample", "captured_content", "decision", "analysis", "confidence", "rationale"}},
            "content": result.content,
            "sha256": result.sha256,
        }
        for row, result in zip(selected, retrieved, strict=True)
    ]
    frame = validated_frame(rows, SourceDocumentsTable)
    frame.attrs["source_transformations_json"] = json.dumps(
        {source.source_id: source.transformations for source in sources}, ensure_ascii=False
    )
    frame.attrs["source_inputs_json"] = json.dumps(
        [source.model_dump(mode="json") for source in sources], ensure_ascii=False
    )
    return frame


@pa.check_types
def document_adapters(
    documents: DataFrame[SourceDocumentsTable], config: AdapterConfig, evidence: Path
) -> DataFrame[DocumentAdaptersTable]:
    rows: list[dict[str, JsonValue]] = []
    for document in frame_records(documents):
        adapter = AdapterSpec.model_validate({
            "format": document["format"], "text_field": document["text_field"],
            "context_field": document["context_field"], "rationale": "Declared category manifest mapping.",
        })
        rows.append({"source_id": document["source_id"], **adapter.model_dump(mode="json")})
    return validated_frame(rows, DocumentAdaptersTable)
