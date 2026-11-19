import hashlib
import json
import uuid
from collections.abc import Callable, Collection, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypeVar

import pandas as pd
import pandera.pandas as pa
from dagster import AssetExecutionContext, AssetObservation, Config, DataVersion, MetadataValue, Output
from pandera.typing import DataFrame
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.dataframe_io import AssetSnapshot, DataFrameIOManager, frame_bytes
from codebench_pipeline.dataset_branches import asset_branch, stage_name
from codebench_pipeline.dataset_catalog import source_scope
from codebench_pipeline.dataset_config import CatalogConfig, DocumentationConfig, SnapshotConfig
from codebench_pipeline.source_transforms import transformation_bytes
from codebench_pipeline.stage_implementation import STAGE_ROOTS, stage_versions, verify_processing
from codebench_pipeline.storage import IntegrityError, digest, fingerprint

Schema = TypeVar("Schema", bound=pa.DataFrameModel)


def code_versions(stage: str | None = None) -> dict[str, str]:
    if stage is not None and stage in STAGE_ROOTS:
        return stage_versions(stage)
    package = Path(__file__).parent
    paths = [
        *package.glob("dataset_*.py"),
        *package.glob("dataframe_*.py"),
        *package.glob("source_*.py"),
        *package.glob("repository*.py"),
        *package.glob("keenable*.py"),
        *package.glob("grounding*.py"),
        *package.glob("guarded_publication*.py"),
        package / "collection_config.py",
        package / "collection_descriptions.py",
        package / "collection_merge.py",
        package / "declared_dataset_bundle.py",
        package / "declared_source_assets.py",
        *package.glob("discovery*.py"),
        package / "annotation_cache.py",
        package / "publication_authorization.py",
        package / "review_policy.py",
        package / "documentation_authorization.py",
        package / "typed_config.py",
        package / "category_rules.py",
        package / "category_eligibility.py",
        package / "label_corrections.py",
        package / "stage_implementation.py",
        package / "stage_cache_compatibility.py",
        package / "business_config.py",
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


def receipt_source_versions(scoped: Mapping[str, str]) -> dict[str, str]:
    """Capture whole source files for every scoped implementation dependency."""
    root = Path(__file__).parent.parent
    versions = code_versions()
    for name in {entry.split(":", 1)[0] for entry in scoped} - versions.keys():
        versions[name] = digest(root / name)
    return versions


def stage_signature(
    stage: str, config: Config, inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot]
) -> str:
    """Fingerprint inputs; snapshots must be unmodified, integrity-verified storage reads."""
    return implementation_signature(stage, config, inputs, stage_code_versions(stage, config, inputs.keys()))


def stage_code_versions(stage: str, config: Config, inputs: Collection[str]) -> dict[str, str]:
    if stage == "labeling_attempts" and "commands" in inputs and "documentation" not in inputs:
        versions = code_versions("guarded_labeling_attempts")
    else:
        versions = code_versions(stage)
    if isinstance(config, DeclaredSourceStageConfig):
        package = Path(__file__).parent
        pattern = "keenable*" if config.branch == "keenable" else "repository*"
        for path in (*package.glob(f"{pattern}.py"), package / "declared_source_assets.py"):
            versions[f"codebench_pipeline/{path.name}"] = digest(path)
    return versions


def implementation_signature(
    stage: str,
    config: Config,
    inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
    implementation: Mapping[str, str],
) -> str:
    values: dict[str, JsonValue] = {
        "configuration": (
            {"branch": config.branch, "declaration": config.declaration_digest}
            if isinstance(config, DeclaredSourceStageConfig)
            else config.model_dump_json()
        ),
        "implementation": dict(implementation),
    }
    if stage in {"human_review_queue", "automatic_dataset_release"}:
        from codebench_pipeline.review_snapshots import REVIEW_POLICY_FILES

        values["publication_policy"] = {
            name: digest(Path(__file__).parent / name) for name in REVIEW_POLICY_FILES
        }
    for name, value in inputs.items():
        if isinstance(value, AssetSnapshot):
            if isinstance(value.value, CategoryRules):
                value = value.value
            else:
                values[name] = value.reference.digest
                continue
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
        transforms = {
            (source.transform.module_path, source.transform.sha256): source.transform
            for source in config.documents
            if source.transform is not None
        }
        values["transformation_code"] = {
            f"{path}:{expected}": hashlib.sha256(transformation_bytes(transform)).hexdigest()
            for (path, expected), transform in transforms.items()
        }
        values["sources"] = {
            source.source_id: digest(Path(source.local_path))
            if source.local_path is not None
            else {
                "url": source.url,
                "sha256": source.sha256,
                "git_blob_sha1": source.git_blob_sha1,
            }
            for source in config.documents
        }
    if isinstance(config, (CatalogConfig, DocumentationConfig)) and config.manifest is not None:
        values["manifest_sha256"] = digest(Path(config.manifest))
    return fingerprint(values)


def verified_legacy_table(
    storage: DataFrameIOManager,
    key: str,
    *,
    stage: str,
    config: Config,
    inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
    implementation: Mapping[str, str],
    content_digest: str,
) -> pd.DataFrame:
    """Read an equivalent legacy cache without computing or publishing authorization."""
    if stage not in STAGE_ROOTS:
        raise IntegrityError(f"Legacy reuse is not supported for stage: {stage}")
    signature = implementation_signature(stage, config, inputs, implementation)
    if key.rsplit("/", 1)[-1] != signature or key.rsplit("/", 2)[-2] != stage:
        raise IntegrityError("Legacy fingerprint does not match current inputs and configuration")

    verify_processing(stage, implementation, storage.read_evidence)
    if TypeAdapter(dict[str, JsonValue]).validate_json(storage.read_evidence(f"{key}/evidence-index.json"))[
        "config"
    ] != config.model_dump(mode="json"):
        raise IntegrityError("Legacy evidence configuration differs from current configuration")
    if TypeAdapter(JsonValue).validate_json(storage.read_evidence(f"{key}/stage-config.json")) != (
        config.model_dump(mode="json")
    ):
        raise IntegrityError("Legacy stage configuration differs from current configuration")

    def missing() -> pd.DataFrame:
        raise IntegrityError("Verified legacy cache is missing; processing was not started")

    frame, _ = storage.cached_frame(key, missing)
    if hashlib.sha256(frame_bytes(frame)).hexdigest() != content_digest:
        raise IntegrityError("Legacy cache differs from the recorded materialization output")
    return frame


def cached_table(
    context: AssetExecutionContext,
    storage: DataFrameIOManager,
    config: Config,
    inputs: Mapping[str, pd.DataFrame | CategoryRules],
    schema: type[Schema],
    compute: Callable[[Path], DataFrame[Schema]],
) -> Output[DataFrame[Schema]]:
    from codebench_pipeline.dataset_branches import FINALIZATION_STAGES
    from codebench_pipeline.dataset_finalization import FINALIZATION_BINDING, authorize_finalization_output
    from codebench_pipeline.documentation_authorization import inventory_binding
    from codebench_pipeline.source_evidence import retain_source_receipt, validate_source_receipt
    from codebench_pipeline.source_review_guard import SOURCE_GATED_STAGES, authorize_compute

    stage = context.asset_key.path[-1]
    shared_inputs = [
        value
        for value in inputs.values()
        if isinstance(value, pd.DataFrame) and FINALIZATION_BINDING in value.attrs
    ]
    if stage in FINALIZATION_STAGES and stage != "merged_command_candidates":
        if storage.review_home is None or not shared_inputs:
            raise IntegrityError("Shared processing requires current merged collection inputs")
        for value in shared_inputs:
            authorize_finalization_output(
                Path(storage.review_home), context.instance, context.partition_key, value, complete=True
            )
    source_token = authorize_compute(context, storage, inputs) if stage in SOURCE_GATED_STAGES else None
    scoped_versions = stage_code_versions(stage, config, inputs.keys())
    signature = implementation_signature(stage, config, inputs, scoped_versions)
    branch = asset_branch(context.asset_key.path)
    key = f"{context.partition_key}/{stage_name(stage, branch)}/{signature}"
    versions = receipt_source_versions(scoped_versions)

    def build() -> pd.DataFrame:
        attempt = uuid.uuid4().hex
        with TemporaryDirectory(prefix="dataset-inference-") as temporary:
            evidence = Path(temporary)
            try:
                frame = schema.validate(compute(evidence))
                if shared_inputs:
                    frame.attrs = {**shared_inputs[0].attrs, **frame.attrs}
                if stage == "source_catalog":
                    frame.attrs["codebench_source_inventory"] = {
                        "branch": branch,
                        "signature": signature,
                    }
                if source_token is not None:
                    frame.attrs[inventory_binding(branch)] = source_token.model_dump(mode="json")
                if isinstance(config, DeclaredSourceStageConfig):
                    retain_source_receipt(frame, config, storage)
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
                    {
                        "attempt": attempt,
                        "receipts": receipts,
                        "config": config.model_dump(mode="json"),
                        "implementation": versions,
                        "stage_implementation": scoped_versions,
                    },
                    ensure_ascii=False,
                ).encode(),
            )
            return frame

    frame, hit = storage.cached_frame(key, build)
    if isinstance(config, DeclaredSourceStageConfig):
        validate_source_receipt(frame, config, storage)
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
