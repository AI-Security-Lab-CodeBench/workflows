import hashlib
import json
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, TypeVar

import pandas as pd
import pandera.pandas as pa
from dagster import (
    AssetExecutionContext,
    AssetObservation,
    DagsterInstance,
    DataVersion,
    JsonMetadataValue,
    MetadataValue,
    Output,
)
from pandera.typing import DataFrame
from pydantic import BaseModel, JsonValue, RootModel, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import DeclaredSourceStageConfig
from codebench_pipeline.collection_stage_settings import collection_settings_branch
from codebench_pipeline.dataframe_io import AssetReference, AssetSnapshot, DataFrameIOManager, frame_bytes
from codebench_pipeline.dataset_branches import Branch, asset_branch, stage_name
from codebench_pipeline.dataset_catalog import source_scope
from codebench_pipeline.stage_configuration import DeclaredGlobalConfig
from codebench_pipeline.stage_implementation import (
    STAGE_ROOTS,
    ImplementationSource,
    processing_versions,
    stage_versions,
    verify_processing,
)
from codebench_pipeline.storage import IntegrityError, fingerprint

Schema = TypeVar("Schema", bound=pa.DataFrameModel)


@dataclass(frozen=True, slots=True)
class ReusedTable[TableSchema: pa.DataFrameModel]:
    table: DataFrame[TableSchema]
    signature: str
    evidence: dict[str, JsonValue]
    reference: AssetReference


class TableMetadataSink(Protocol):
    def add_output_metadata(self, metadata: Mapping[str, bool | str | JsonMetadataValue]) -> None: ...


def record_table_output_metadata(
    output: TableMetadataSink, metadata: dict[str, bool | str | JsonMetadataValue]
) -> None:
    output.add_output_metadata(metadata)


def code_versions(
    stage: str | None = None, *, source: ImplementationSource = ImplementationSource()
) -> dict[str, str]:
    if stage is not None and stage in STAGE_ROOTS:
        return stage_versions(stage, source)
    patterns = (
        "dataset_*.py",
        "dataframe_*.py",
        "source_*.py",
        "repository*.py",
        "keenable*.py",
        "grounding*.py",
        "guarded_publication*.py",
        "collection_config.py",
        "collection_descriptions.py",
        "collection_merge.py",
        "declared_dataset_bundle.py",
        "declared_source_assets.py",
        "discovery*.py",
        "annotation_cache.py",
        "publication_authorization.py",
        "review_policy.py",
        "documentation_authorization.py",
        "typed_config.py",
        "category_rules.py",
        "category_eligibility.py",
        "label_corrections.py",
        "stage_implementation.py",
        "business_config.py",
        "model_config.py",
        "inference.py",
        "models.py",
        "storage.py",
        "object_store.py",
        "agent_events.py",
        "*transport.py",
    )
    paths = (
        *(name for pattern in patterns for name in source.package_matches(pattern)),
        "scripts/dataset_schema.py",
        "pyproject.toml",
        "uv.lock",
    )
    return {name: source.digest(name) for name in paths if source.is_file(name)}


def receipt_source_versions(
    scoped: Mapping[str, str],
    *,
    source: ImplementationSource = ImplementationSource(),
    include_legacy_inventory: bool = True,
) -> dict[str, str]:
    """Capture whole source files for every scoped implementation dependency."""
    versions = code_versions(source=source) if include_legacy_inventory else {}
    for name in {entry.split(":", 1)[0] for entry in scoped} - versions.keys():
        versions[name] = source.digest(name)
    return versions


def stage_signature(
    stage: str,
    config: BaseModel,
    inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
    *,
    source: ImplementationSource = ImplementationSource(),
    storage_identity: str | None = None,
) -> str:
    """Fingerprint inputs; snapshots must be unmodified, integrity-verified storage reads."""
    return implementation_signature(
        stage,
        config,
        inputs,
        stage_code_versions(stage, config, source),
        review_policy=review_policy_versions(source) if stage == "human_review_queue" else None,
        storage_identity=storage_identity,
    )


def stage_code_versions(stage: str, config: BaseModel, source: ImplementationSource) -> dict[str, str]:
    return scoped_code_versions(
        stage,
        source,
        config.branch
        if isinstance(config, DeclaredSourceStageConfig)
        else collection_settings_branch(config),
    )


def scoped_code_versions(stage: str, source: ImplementationSource, branch: Branch | None) -> dict[str, str]:
    scope = stage if branch is None else stage_name(stage.rsplit("/", 1)[-1], branch)
    versions = code_versions(scope, source=source)
    if branch is not None or stage == "labeling_attempts":
        for module in ("dataframe_io", "dataframe_schemas", "dataset_storage_io", "object_store", "storage"):
            filename = f"codebench_pipeline/{module}.py"
            versions[filename] = source.digest(filename)
    if stage in {"deduplicated_annotations", "category_eligible_annotations"}:
        for module in ("finalization_table_asset", "finalization_table_io"):
            filename = f"codebench_pipeline/{module}.py"
            versions[filename] = source.digest(filename)
    return versions


def review_policy_versions(source: ImplementationSource) -> dict[str, str]:
    from codebench_pipeline.review_snapshots import REVIEW_POLICY_FILES

    return {name: source.digest(f"codebench_pipeline/{name}") for name in REVIEW_POLICY_FILES}


def implementation_signature(
    stage: str,
    config: BaseModel,
    inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
    implementation: Mapping[str, str],
    *,
    review_policy: Mapping[str, str] | None = None,
    storage_identity: str | None = None,
) -> str:
    values: dict[str, JsonValue] = {
        "configuration": (
            {"branch": config.branch, "declaration": config.declaration_digest}
            if isinstance(config, DeclaredSourceStageConfig)
            else config.model_dump_json()
        ),
        "implementation": dict(implementation),
    }
    if stage == "labeling_attempts" and storage_identity is not None:
        values["output_io"] = fingerprint(storage_identity)
    if stage == "human_review_queue":
        if review_policy is None:
            raise IntegrityError("Review policy implementation is not bound to the stage signature")
        values["publication_policy"] = {name: digest for name, digest in review_policy.items()}
    for name, value in inputs.items():
        if isinstance(value, AssetSnapshot):
            if isinstance(value.value, CategoryRules):
                value = value.value
            else:
                values[name] = value.reference.digest
                continue
        if isinstance(value, CategoryRules):
            if isinstance(config, DeclaredSourceStageConfig):
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
    return fingerprint(values)


def verified_legacy_table(
    storage: DataFrameIOManager,
    key: str,
    *,
    stage: str,
    config: BaseModel,
    inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
    implementation: Mapping[str, str],
    content_digest: str,
    check_processing: Callable[[str, Mapping[str, str], Callable[[str], bytes]], None] = verify_processing,
    receipt_implementation: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Read an equivalent legacy cache without computing or publishing authorization."""
    if stage not in STAGE_ROOTS:
        raise IntegrityError(f"Legacy reuse is not supported for stage: {stage}")
    signature = implementation_signature(stage, config, inputs, implementation)
    if key.rsplit("/", 1)[-1] != signature or key.rsplit("/", 2)[-2] != stage:
        raise IntegrityError("Legacy fingerprint does not match current inputs and configuration")

    check_processing(
        stage,
        implementation if receipt_implementation is None else receipt_implementation,
        storage.read_evidence,
    )
    if TypeAdapter(dict[str, JsonValue]).validate_json(storage.read_evidence(f"{key}/evidence-index.json"))[
        "config"
    ] != config.model_dump(mode="json"):
        raise IntegrityError("Legacy evidence configuration differs from current configuration")
    if RootModel[JsonValue].model_validate_json(storage.read_evidence(f"{key}/stage-config.json")).root != (
        config.model_dump(mode="json")
    ):
        raise IntegrityError("Legacy stage configuration differs from current configuration")

    def missing() -> pd.DataFrame:
        raise IntegrityError("Verified legacy cache is missing; processing was not started")

    frame, _ = storage.cached_frame(key, missing)
    if hashlib.sha256(frame_bytes(frame)).hexdigest() != content_digest:
        raise IntegrityError("Legacy cache differs from the recorded materialization output")
    return frame


def authorize_table_inputs(
    storage: DataFrameIOManager,
    instance: DagsterInstance,
    stage: str,
    inputs: Mapping[str, pd.DataFrame | CategoryRules],
    *,
    run_id: str,
    partition: str,
    registered: DeclaredGlobalConfig | None = None,
) -> list[pd.DataFrame]:
    from codebench_pipeline.dataset_branches import FINALIZATION_STAGES
    from codebench_pipeline.dataset_finalization import FINALIZATION_BINDING, authorize_finalization_output

    shared_inputs = [
        value
        for value in inputs.values()
        if isinstance(value, pd.DataFrame) and FINALIZATION_BINDING in value.attrs
    ]
    if stage in FINALIZATION_STAGES and stage != "merged_command_candidates":
        if storage.review_home is None or not shared_inputs:
            raise IntegrityError("Shared processing requires current merged collection inputs")
        guard_config = registered or storage.run_configuration(run_id, partition)
        for value in shared_inputs:
            authorize_finalization_output(
                manager=storage,
                instance=instance,
                registered=guard_config,
                frame=value,
                complete=True,
            )
    return shared_inputs


@dataclass(frozen=True, slots=True)
class TableCache:
    stage: str
    key: str
    signature: str
    source: ImplementationSource
    versions: dict[str, str]
    scoped_versions: dict[str, str]
    settings_json: str
    settings: dict[str, JsonValue]
    current_signature: Callable[[DataFrameIOManager], str]

    def retain_index(
        self, storage: DataFrameIOManager, index: dict[str, JsonValue], *, attempt: str | None = None
    ) -> tuple[dict[str, JsonValue], str]:
        if self.current_signature(storage) != self.signature:
            raise IntegrityError("Inputs or implementation changed during dataset materialization")
        payloads = {name: self.source.read(name) for name in self.versions}
        for name, payload in payloads.items():
            if hashlib.sha256(payload).hexdigest() != self.versions[name]:
                raise IntegrityError(f"Implementation snapshot changed: {name}")
        if self.stage in STAGE_ROOTS and any(
            self.scoped_versions[name] != version
            for name, version in processing_versions(self.stage, payloads.__getitem__).items()
        ):
            raise IntegrityError("Retained source differs from the scoped processing implementation")
        for name, payload in payloads.items():
            storage.write_evidence(f"implementation/{self.versions[name]}/{name}", payload)
        prefix = self.key if attempt is None else f"{self.key}/{attempt}"
        storage.write_evidence(f"{prefix}/stage-config.json", self.settings_json.encode())
        retained: dict[str, JsonValue] = {
            **index,
            "config": self.settings,
            "implementation": dict[str, JsonValue](self.versions),
            "stage_implementation": dict[str, JsonValue](self.scoped_versions),
        }
        payload = json.dumps(retained, ensure_ascii=False).encode()
        storage.write_evidence(f"{prefix}/evidence-index.json", payload)
        return retained, hashlib.sha256(payload).hexdigest()

    def retain_attempt(
        self, storage: DataFrameIOManager, attempt: str, scratch: Path | None
    ) -> dict[str, JsonValue]:
        prefix = f"{self.key}/{attempt}"
        receipts = {} if scratch is None else storage.retain_attempt_evidence(scratch, prefix)
        storage.write_evidence(f"{prefix}/receipts.json", json.dumps(receipts, ensure_ascii=False).encode())
        return receipts

    def metadata(
        self, storage: DataFrameIOManager, schema: type[pa.DataFrameModel], *, hit: bool
    ) -> dict[str, bool | str | JsonMetadataValue]:
        index = TypeAdapter(dict[str, JsonValue]).validate_json(
            storage.read_evidence(f"{self.key}/evidence-index.json")
        )
        return self.output_metadata(schema, hit=hit, evidence=index)

    def output_metadata(
        self, schema: type[pa.DataFrameModel], *, hit: bool, evidence: dict[str, JsonValue]
    ) -> dict[str, bool | str | JsonMetadataValue]:
        return {
            "cache_hit": hit,
            "input_fingerprint": self.signature,
            "schema": schema.__name__,
            "evidence": MetadataValue.json(evidence),
        }


def prepare_table_cache(
    storage: DataFrameIOManager,
    stage: str,
    partition: str,
    branch: Branch,
    config: BaseModel,
    inputs: Mapping[str, pd.DataFrame | CategoryRules | AssetSnapshot],
) -> TableCache:
    source = storage.implementation_source
    scoped = stage_code_versions(stage, config, source)
    signature = implementation_signature(
        stage,
        config,
        inputs,
        scoped,
        review_policy=review_policy_versions(source) if stage == "human_review_queue" else None,
        storage_identity=storage.storage.model_dump_json(),
    )
    return TableCache(
        stage_name(stage, branch),
        f"{partition}/{stage_name(stage, branch)}/{signature}",
        signature,
        source,
        receipt_source_versions(
            scoped,
            source=source,
            include_legacy_inventory=(
                not isinstance(config, DeclaredSourceStageConfig)
                and collection_settings_branch(config) is None
            ),
        ),
        scoped,
        config.model_dump_json(indent=2),
        TypeAdapter(dict[str, JsonValue]).validate_python(config.model_dump(mode="json")),
        lambda storage: storage.stage_signature(stage, config, inputs),
    )


def cached_table(
    context: AssetExecutionContext,
    storage: DataFrameIOManager,
    config: BaseModel,
    inputs: Mapping[str, pd.DataFrame | CategoryRules],
    schema: type[Schema],
    compute: Callable[[Path], DataFrame[Schema]],
    *,
    registered: DeclaredGlobalConfig | None = None,
    reuse_completed: bool = True,
) -> Output[DataFrame[Schema]]:
    stage = context.asset_key.path[-1]
    shared_inputs = authorize_table_inputs(
        storage,
        context.instance,
        stage,
        inputs,
        run_id=context.run_id,
        partition=context.partition_key,
        registered=registered,
    )
    branch = asset_branch(context.asset_key.path)
    cache = prepare_table_cache(storage, stage, context.partition_key, branch, config, inputs)
    signature = cache.signature
    retained_index: dict[str, JsonValue] = {}

    def build() -> pd.DataFrame:
        nonlocal retained_index
        attempt = uuid.uuid4().hex
        with TemporaryDirectory(prefix="dataset-inference-") as temporary:
            evidence = Path(temporary)
            try:
                frame = schema.validate(compute(evidence))
                if shared_inputs:
                    frame.attrs = {**shared_inputs[0].attrs, **frame.attrs}
            finally:
                receipts = cache.retain_attempt(storage, attempt, evidence)
                context.log_event(
                    AssetObservation(
                        asset_key=context.asset_key,
                        partition=context.partition_key,
                        metadata={"inference_attempt": attempt, "receipts": MetadataValue.json(receipts)},
                    )
                )
            retained_index, _ = cache.retain_index(
                storage,
                {"attempt": attempt, "receipts": receipts},
                attempt=None if reuse_completed else attempt,
            )
            return frame

    if reuse_completed:
        frame, hit = storage.cached_frame(cache.key, build)
        metadata = cache.metadata(storage, schema, hit=hit)
    else:
        frame = build()
        metadata = cache.output_metadata(schema, hit=False, evidence=retained_index)
    validated = schema.validate(frame)
    return Output(
        validated,
        data_version=DataVersion(signature),
        metadata=metadata,
    )
