from __future__ import annotations

import hashlib
from pathlib import Path
from time import time
from typing import Literal
from uuid import uuid4

from dagster import AssetKey, AssetMaterialization, DagsterEvent, DagsterEventType, DagsterInstance
from dagster._core.events import StepMaterializationData
from dagster._core.events.log import EventLogEntry
from dagster._core.storage.dagster_run import DagsterRun
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import AssetReference
from codebench_pipeline.dataset_config import CatalogInputs, DatasetBuildSpec, PublicationConfig
from codebench_pipeline.dataset_documentation_config import DocumentationConfig
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.model_config import ModelRoles
from codebench_pipeline.storage import fingerprint

FixtureCategory = Literal["CAT-99", "CAT-98"]


def record_artifact(
    instance: DagsterInstance,
    dataset_id: str,
    asset_key: AssetKey,
    reference: AssetReference,
    storage_config: dict[str, JsonValue],
) -> None:
    run_id = str(uuid4())
    job_name = "fixture_materialization"
    instance.add_run(
        DagsterRun(
            job_name=job_name,
            run_id=run_id,
            tags={"dagster/partition": dataset_id},
            run_config={"resources": {"dataset_io_manager": {"config": storage_config}}},
        )
    )
    instance.store_event(
        EventLogEntry(
            error_info=None,
            level="INFO",
            user_message="Fixture materialization",
            run_id=run_id,
            timestamp=time(),
            job_name=job_name,
            dagster_event=DagsterEvent(
                event_type_value=DagsterEventType.ASSET_MATERIALIZATION.value,
                job_name=job_name,
                event_specific_data=StepMaterializationData(
                    AssetMaterialization(
                        asset_key=asset_key,
                        partition=dataset_id,
                        metadata={
                            "codebench/content_digest": reference.digest,
                            "codebench/storage_format": reference.format,
                        },
                    )
                ),
            ),
        )
    )


class PreparedSourceFixture(CatalogInputs):
    revision: str = "1"


class PreparedFileFixture(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_id: str
    revision: str
    rules: CategoryRules
    models: ModelRoles
    seeds: PreparedSourceFixture
    documentation_sources: PreparedSourceFixture | None = None
    documentation: DocumentationConfig
    publication: PublicationConfig = Field(default_factory=PublicationConfig)


def source_bytes(category: FixtureCategory = "CAT-99") -> bytes:
    commands = (
        ("git status", "git status --show-stash")
        if category == "CAT-99"
        else (
            "terraform destroy",
            "terraform destroy -auto-approve",
            "terraform destroy -compact-warnings",
        )
    )
    return ("# Inert test commands\n\n" + "\n\n".join(f"`{command}`" for command in commands) + "\n").encode()


def example_config(category: FixtureCategory = "CAT-99") -> PreparedFileFixture:
    utility, subcommand = ("git", "status") if category == "CAT-99" else ("terraform", "destroy")
    return PreparedFileFixture.model_validate(
        {
            "dataset_id": f"fixture_{category.lower().replace('-', '_')}",
            "revision": "fixture-v1",
            "rules": {
                "category_id": category,
                "category_name": "Inert fixture operations",
                "version": "fixture-v1",
                "policy": "Assess supplied evidence independently; never execute commands.",
                "description_language": "English",
                "risks": [
                    {
                        "id": f"{category}.1",
                        "name": "Fixture operations",
                        "definition": "Inspection or destruction represented as inert test data.",
                        "command_patterns": [rf"^{utility}\s+{subcommand}\b"],
                        "shell_routes": {utility: rf"^{subcommand}(?: |$)"},
                        "guidance": "Assess the visible flags and context.",
                    }
                ],
                "classes": [
                    {"name": name, "definition": f"Fixture {name} judgment"}
                    for name in ("allow", "deny", "ask")
                ],
                "severities": [
                    {"name": name, "definition": f"Fixture {name} impact"}
                    for name in ("low", "medium", "high", "critical")
                ],
            },
            "models": {
                role: {"provider": "native", "model": f"fixture/{role}"}
                for role in ("selector", "adapter", "labeler", "reviewer")
            },
            "seeds": {"mode": "provided_sources"},
            "documentation_sources": {
                "mode": "provided_sources",
                "documents": [
                    {
                        "source_id": f"fixture-{utility}",
                        "url": f"https://example.org/{utility}/commands",
                        "local_path": "source.md",
                        "license": "CC0-1.0",
                        "license_url": "https://example.org/license",
                        "source_type": "maintainer_documentation",
                        "provenance_type": "community_documentation",
                        "accessibility": "accessible",
                        "reuse_decision": "permitted",
                        "commands_present": True,
                        "revision": "fixture-v1",
                        "retrieved_at": "2026-11-01",
                        "format": "markdown",
                        "source_locator": "source.md",
                        "sha256": hashlib.sha256(source_bytes(category)).hexdigest(),
                        "attribution": "Authored inert test fixture",
                    }
                ],
            },
            "documentation": {
                "snippets": [
                    {
                        "utility": utility,
                        "subcommands": [subcommand],
                        "url": f"https://example.org/{utility}/{subcommand}",
                        "retrieved_at": "2026-11-01",
                        "snippet": (
                            "Status inspects repository state; --show-stash includes stash counts."
                            if utility == "git"
                            else "Destroy removes infrastructure; -auto-approve bypasses confirmation; "
                            "-compact-warnings changes warning presentation."
                        ),
                        "version_limits": "Authored fixture, not installed tool evidence.",
                    }
                ]
                + (
                    [
                        {
                            "utility": "terraform",
                            "subcommands": ["apply", "destroy"],
                            "url": "https://example.org/terraform/apply",
                            "retrieved_at": "2026-11-01",
                            "snippet": "Auto approval skips confirmation, not authorization. "
                            "Saved plans can also bypass confirmation.",
                            "version_limits": "Authored fixture, not installed tool evidence.",
                        }
                    ]
                    if category == "CAT-98"
                    else []
                )
            },
        }
    )


def write_fixture(root: Path, category: FixtureCategory = "CAT-99") -> Path:
    directory = root / category.lower()
    directory.mkdir(exist_ok=True)
    (directory / "source.md").write_bytes(source_bytes(category))
    path = directory / "dataset.json"
    path.write_text(example_config(category).model_dump_json(), encoding="utf-8")
    return path


def seed_prepared_snapshot(
    home: Path, spec: DatasetBuildSpec, documentation: DocumentationConfig
) -> tuple[DatasetBuildSpec, str]:
    archived = spec.model_copy(update={"declared_collection": None, "documentation": documentation})
    version = fingerprint(archived.model_dump_json())
    directory = DatasetRegistry(home).root / archived.dataset_id
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{version}.json").write_text(archived.model_dump_json(), encoding="utf-8")
    (directory / "current").write_text(version, encoding="ascii")
    return archived, version
