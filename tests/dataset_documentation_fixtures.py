from pathlib import Path

from pandera.typing import DataFrame
from pydantic import JsonValue
from test_dataset_rules import make_candidate

from codebench_pipeline.dataframe_schemas import FilteredCommandsTable, validated_frame
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_seed_config import DocumentationSeed, SearchSeeds
from codebench_pipeline.discovery_models import DiscoveryOutput, InspectedPage, SourceProposal
from codebench_pipeline.model_config import ModelConfig


def commands(*texts: str, decision: str = "eligible") -> DataFrame[FilteredCommandsTable]:
    rows: list[dict[str, JsonValue]] = []
    for index, text in enumerate(texts):
        row = make_candidate(str(index)).model_dump(mode="json")
        row.update(command=text, filter_decision=decision, filter_reason="")
        rows.append(row)
    return validated_frame(rows, FilteredCommandsTable)


def seed() -> DocumentationSeed:
    return DocumentationSeed(
        utility="probe",
        subcommands=["inspect"],
        url="https://example.org/docs",
        retrieved_at="2026-11-10",
        snippet="probe inspect reads metadata.",
        version_limits="Unverified releases.",
    )


def live(maximum_bytes: int = 8000) -> DocumentationConfig:
    return DocumentationConfig(
        mode="keenable",
        maximum_bytes=maximum_bytes,
        search_request=SearchSeeds.model_validate(
            {"searches": [{"risk": "CAT-8.1", "queries": ["probe documentation license"]}], "max_chars": 1000}
        ),
        model=ModelConfig(provider="native", model="fixture/small"),
    )


def discovery(root: Path, reuse: str = "permitted") -> DiscoveryOutput:
    page = InspectedPage(
        url=seed().url,
        risk="CAT-8.1",
        retrieved_at=seed().retrieved_at,
        evidence_path=root / "fetched.json",
        text="Maintainer documentation. MIT license. probe inspect reads metadata. probe erase removes data.",
    )
    source = SourceProposal.model_validate(
        {
            "source_id": "docs",
            "risk": "CAT-8.1",
            "url": page.url,
            "source_type": "documentation",
            "origin": "human_authored",
            "origin_evidence": "Maintainer documentation.",
            "commands_evidence": "probe inspect",
            "license": "MIT",
            "license_url": page.url,
            "license_evidence": "MIT license.",
            "reuse_decision": reuse,
            "revision": "inspection capture",
            "decision": "proposed",
            "rationale": "Relevant semantics.",
            "analysis": "The maintainer page describes the actual operation.",
            "confidence": "high",
        }
    )
    return DiscoveryOutput(sources=[source], inspected=[page], adapter_requests=[], failures=[])


def proposal(**changes: JsonValue) -> dict[str, JsonValue]:
    value: dict[str, JsonValue] = {
        "utility": "probe",
        "subcommands": ["inspect"],
        "url": seed().url,
        "snippet": seed().snippet,
    }
    value.update(changes)
    return value
