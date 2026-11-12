import json
from pathlib import Path

from pandera.typing import DataFrame
from pydantic import JsonValue

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_schemas import SourceCatalogTable, validated_frame
from codebench_pipeline.dataset_branches import Branch
from codebench_pipeline.dataset_config import CatalogConfig
from codebench_pipeline.dataset_seed_config import SourceSeed
from codebench_pipeline.dataset_source_models import SourceChoice, is_documentation_source
from codebench_pipeline.discovery_models import SourceProposal
from codebench_pipeline.models import SourceDocument
from codebench_pipeline.storage import IntegrityError


def source_scope(rules: CategoryRules) -> dict[str, JsonValue]:
    return {
        "category": rules.canonical,
        "risks": [risk.model_dump(mode="json") for risk in rules.risks],
    }


def provided_documents(seeds: list[SourceSeed]) -> list[SourceDocument]:
    records = [seed.to_source_document() for seed in seeds]
    if len(records) != len({record.source_id for record in records}):
        raise IntegrityError("Duplicate source identifiers")
    return records


def catalog_row(
    source: SourceDocument | SourceSeed | SourceProposal, choice: SourceChoice | None, sample: str
) -> dict[str, JsonValue]:
    return {
        "source_id": source.source_id,
        "source_url": source.url,
        "license": source.license,
        "license_url": source.license_url,
        "attribution": source.attribution if isinstance(source, (SourceDocument, SourceSeed)) else source.url,
        "revision": source.revision,
        "retrieved_at": source.retrieved_at
        if isinstance(source, (SourceDocument, SourceSeed))
        else "recorded in discovery evidence",
        "source_type": source.source_type,
        "provenance_type": (source.provenance_type or "")
        if isinstance(source, (SourceDocument, SourceSeed))
        else source.origin,
        "source_locator": source.source_locator
        if isinstance(source, (SourceDocument, SourceSeed))
        else source.commands_evidence,
        "sha256": (source.sha256 or "") if isinstance(source, (SourceDocument, SourceSeed)) else "",
        "format": source.format if isinstance(source, (SourceDocument, SourceSeed)) else "text",
        "text_field": source.text_field if isinstance(source, (SourceDocument, SourceSeed)) else "$",
        "context_field": source.context_field if isinstance(source, (SourceDocument, SourceSeed)) else "",
        "sample": sample,
        "captured_content": "",
        "decision": choice.decision if choice is not None else "selected",
        "analysis": choice.analysis if choice is not None else "Category source manifest input; no model selection.",
        "confidence": choice.confidence if choice is not None else "not_applicable",
        "rationale": choice.rationale if choice is not None else "Declared provenance and reuse metadata validated.",
    }


def build_catalog(
    config: CatalogConfig, rules: CategoryRules, evidence: Path, *, branch: Branch = "dataset"
) -> DataFrame[SourceCatalogTable]:
    if branch == "documentation" and any(
        not is_documentation_source(source.source_type, source.provenance_type)
        for source in config.documents
    ):
        raise IntegrityError("Dataset corpora cannot enter the documentation branch")
    frame = validated_frame(
        [catalog_row(source, None, "") for source in config.documents], SourceCatalogTable
    )
    frame.attrs["source_inputs_json"] = json.dumps(
        [source.model_dump(mode="json") for source in config.documents], ensure_ascii=False
    )
    if config.manifest is not None:
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "source-manifest.json").write_bytes(Path(config.manifest).read_bytes())
    return frame
