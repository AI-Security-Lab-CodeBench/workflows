import hashlib
import json
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypeVar

import pandas as pd
import pandera.pandas as pa
from dagster import AssetExecutionContext, AssetObservation, Config, DataVersion, MetadataValue, Output
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataframe_io import DataFrameIOManager, frame_bytes
from codebench_pipeline.dataset_branches import asset_branch, stage_name
from codebench_pipeline.dataset_catalog import provided_documents, source_scope
from codebench_pipeline.dataset_config import CatalogConfig, SnapshotConfig
from codebench_pipeline.storage import IntegrityError, digest, fingerprint

Schema = TypeVar("Schema", bound=pa.DataFrameModel)


def code_versions() -> dict[str, str]:
    package = Path(__file__).parent
    paths = [
        *package.glob("dataset_*.py"),
        *package.glob("dataframe_*.py"),
        *package.glob("source_review*.py"),
        *package.glob("discovery*.py"),
        package / "automatic_release.py",
        package / "annotation_cache.py",
        package / "publication_authorization.py",
        package / "review_policy.py",
        package / "documentation_authorization.py",
        package / "typed_config.py",
        package / "category_rules.py",
        package / "model_config.py",
        package / "inference.py",
        package / "models.py",
        package / "storage.py",
        package / "object_store.py",
        package / "agent_events.py",
        package / "agents.py",
        *package.glob("*transport.py"),
        package.parent / "scripts/dataset_schema.py",
        package.parent / "pyproject.toml",
        package.parent / "uv.lock",
    ]
    return {str(path.relative_to(package.parent)): digest(path) for path in paths if path.is_file()}


def stage_signature(stage: str, config: Config, inputs: Mapping[str, pd.DataFrame | CategoryRules]) -> str:
    values: dict[str, JsonValue] = {
        "configuration": config.model_dump_json(),
        "implementation": {key: value for key, value in code_versions().items()},
    }
    if stage in {"human_review_queue", "automatic_dataset_release"}:
        from codebench_pipeline.review_snapshots import REVIEW_POLICY_FILES

        values["publication_policy"] = {
            name: digest(Path(__file__).parent / name) for name in REVIEW_POLICY_FILES
        }
    for name, value in inputs.items():
        if isinstance(value, CategoryRules):
            if stage == "source_catalog":
                values[name] = source_scope(value)
            elif stage == "filtered_commands":
                values[name] = {
                    "category": value.category_id,
                    "patterns": {
                        risk.id: [pattern for pattern in risk.command_patterns] for risk in value.risks
                    },
                }
            else:
                values[name] = value.model_dump_json()
        else:
            values[name] = hashlib.sha256(frame_bytes(value)).hexdigest()
    if isinstance(config, (CatalogConfig, SnapshotConfig)):
        values["sources"] = {
            doc.source_id: digest(doc.local_path) for doc in provided_documents(config.documents)
        }
    return fingerprint(values)


def cached_table(
    context: AssetExecutionContext,
    storage: DataFrameIOManager,
    config: Config,
    inputs: Mapping[str, pd.DataFrame | CategoryRules],
    schema: type[Schema],
    compute: Callable[[Path], DataFrame[Schema]],
) -> Output[DataFrame[Schema]]:
    from codebench_pipeline.documentation_authorization import inventory_binding
    from codebench_pipeline.source_review_guard import SOURCE_GATED_STAGES, authorize_compute

    stage = context.asset_key.path[-1]
    source_token = authorize_compute(context, storage, inputs) if stage in SOURCE_GATED_STAGES else None
    signature = stage_signature(stage, config, inputs)
    branch = asset_branch(context.asset_key.path)
    key = f"{context.partition_key}/{stage_name(stage, branch)}/{signature}"
    versions = code_versions()

    def build() -> pd.DataFrame:
        attempt = uuid.uuid4().hex
        with TemporaryDirectory(prefix="dataset-inference-") as temporary:
            evidence = Path(temporary)
            try:
                frame = schema.validate(compute(evidence))
                if stage == "source_catalog":
                    frame.attrs["codebench_source_inventory"] = {
                        "branch": branch,
                        "signature": signature,
                    }
                if source_token is not None:
                    frame.attrs[inventory_binding(branch)] = source_token.model_dump(mode="json")
            finally:
                receipts: dict[str, JsonValue] = {}
                for path in sorted(evidence.rglob("*")):
                    if path.is_file():
                        name = path.relative_to(evidence).as_posix()
                        receipts[name] = storage.write_evidence(f"{key}/{attempt}/{name}", path.read_bytes())
                storage.write_evidence(
                    f"{key}/{attempt}/receipts.json", json.dumps(receipts, ensure_ascii=False).encode()
                )
                context.log_event(
                    AssetObservation(
                        asset_key=context.asset_key,
                        partition=context.partition_key,
                        metadata={"inference_attempt": attempt, "receipts": MetadataValue.json(receipts)},
                    )
                )
            if stage_signature(stage, config, inputs) != signature:
                raise IntegrityError("Inputs or implementation changed during dataset materialization")
            repo = Path(__file__).parent.parent
            for name, expected in versions.items():
                payload = (repo / name).read_bytes()
                if hashlib.sha256(payload).hexdigest() != expected:
                    raise IntegrityError(f"Implementation snapshot changed: {name}")
                storage.write_evidence(f"implementation/{expected}/{name}", payload)
            storage.write_evidence(f"{key}/stage-config.json", config.model_dump_json(indent=2).encode())
            storage.write_evidence(
                f"{key}/evidence-index.json",
                json.dumps(
                    {"attempt": attempt, "receipts": receipts, "config": config.model_dump(mode="json")},
                    ensure_ascii=False,
                ).encode(),
            )
            return frame

    frame, hit = storage.cached_frame(key, build)
    validated = schema.validate(frame)
    evidence_index = TypeAdapter(dict[str, JsonValue]).validate_json(
        storage.read_evidence(f"{key}/evidence-index.json")
    )
    return Output(
        validated,
        data_version=DataVersion(signature),
        metadata={
            "cache_hit": hit,
            "input_fingerprint": signature,
            "schema": schema.__name__,
            "evidence": MetadataValue.json(evidence_index),
        },
    )
