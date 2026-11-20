import os
from collections.abc import Callable, Iterable
from functools import cache
from pathlib import Path
from typing import Final

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    Config,
    ConfigurableIOManager,
    DagsterInstance,
    DataVersion,
    EventLogRecord,
    InputContext,
    Output,
    OutputContext,
    PythonObjectDagsterType,
    TextMetadataValue,
    asset,
)
from dagster._core.definitions.data_version import extract_data_version_from_entry
from pydantic import BaseModel, JsonValue, RootModel

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.collection_config import CollectionMergeConfig
from codebench_pipeline.collection_stage_settings import (
    COLLECTION_OPERATIONS,
    CollectionOperation,
    collection_operation,
)
from codebench_pipeline.config_directory import ConfigDirectorySnapshot
from codebench_pipeline.dagster_table_metadata import MetadataSink
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS
from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    FINALIZATION_GROUPS,
    Branch,
)
from codebench_pipeline.dataset_config import DatasetBuildSpec, DeduplicationPolicy
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.declared_dataset_bundle import compile_directory
from codebench_pipeline.grounding_sources import persist_repository_origin_bindings
from codebench_pipeline.input_provenance import (
    current_materialization,
    input_materialization,
    producer_run_id,
)
from codebench_pipeline.labeling_stage_settings import ProposalRules
from codebench_pipeline.object_store import sha256
from codebench_pipeline.repository_receipts import RepositoryRun
from codebench_pipeline.stage_configuration import (
    CategoryEligibilityConfig,
    ConfigArtifact,
    DatasetReleaseConfig,
    DeclaredGlobalConfig,
    FilterConfig,
    GuardedLabelingConfig,
    ReviewQueueConfig,
)
from codebench_pipeline.storage import IntegrityError, fingerprint

GLOBAL_CONFIG: Final = AssetKey(["datasets", "global_config"])
CONFIG_DIRECTORY: Final = AssetKey(["datasets", "config_directory"])
_INSTALLED_AUTHORING_ROOT: Final = Path(__file__).resolve().parents[1] / "business-configs"
_CONFIG_DIRECTORY_TYPE: Final = PythonObjectDagsterType(
    name="ConfigDirectorySnapshot", python_type=ConfigDirectorySnapshot
)
COLLECTION_SCOPES: Final = {branch.namespace: branch.group for branch in COLLECTION_BRANCHES}
RegisteredConfig = DeclaredGlobalConfig
REGISTERED_CONFIG_TYPE: Final = PythonObjectDagsterType(name="RegisteredConfig", python_type=RegisteredConfig)


class GlobalConfig(Config):
    expected_revision: str = ""

    @classmethod
    def for_spec(cls, spec: DatasetBuildSpec) -> "GlobalConfig":
        if not spec.has_declared_collection:
            raise IntegrityError("Expected a declared collection specification")
        return cls(expected_revision=fingerprint(spec.model_dump_json()))


def mapping_key(scope: str) -> AssetKey:
    return AssetKey(["datasets", f"map_{scope}_config"])


def collection_mapping_key(branch: Branch, stage: str) -> AssetKey:
    return mapping_key(collection_operation(branch, stage).mapping_scope)


def registered_input() -> AssetIn:
    return AssetIn(key=GLOBAL_CONFIG, dagster_type=REGISTERED_CONFIG_TYPE)


def require_registered_rules(registered: RegisteredConfig, category_rules: CategoryRules) -> None:
    if category_rules != registered.rules:
        raise IntegrityError("Category rules differ from the run configuration; replan the build")


def configured_selection(stages: Iterable[AssetKey]) -> list[AssetKey]:
    from codebench_pipeline.definitions import defs

    selection = list(stages)
    if not selection:
        return []
    graph = defs.resolve_asset_graph()
    mappings = sorted(
        {
            parent
            for key in selection
            for parent in graph.get(key).parent_keys
            if parent != CONFIG_DIRECTORY and graph.get(parent).io_manager_key == "config_run_io_manager"
        },
        key=AssetKey.to_user_string,
    )
    return list(dict.fromkeys([*selection, *mappings, GLOBAL_CONFIG]))


@cache
def _contract_type(contract: type[ConfigArtifact]) -> PythonObjectDagsterType:
    return PythonObjectDagsterType(python_type=contract, name=contract.__name__)


def mapped_input(
    scope: str, contract: type[ConfigArtifact], *, input_manager_key: str | None = None
) -> AssetIn:
    return AssetIn(
        key=mapping_key(scope), dagster_type=_contract_type(contract), input_manager_key=input_manager_key
    )


def current_configuration_record(instance: DagsterInstance, key: AssetKey, partition: str) -> EventLogRecord:
    from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded

    record = current_materialization(instance, key, partition)
    if record is None or not materialization_attempt_succeeded(
        instance, record, output_name="result", retry_number="native"
    ):
        raise IntegrityError(f"No successful configuration for {key.to_user_string()}; materialize it first")
    return record


def _record_config_digest(context: MetadataSink, payload: bytes) -> None:
    context.add_output_metadata({"codebench/config_digest": sha256(payload)})


class ConfigRunIOManager(ConfigurableIOManager):
    """Read config from its producing run, or the partition's observed directory."""

    home: str

    def handle_output(self, context: OutputContext, obj: BaseModel) -> None:
        path = Path(self.home, "configuration-runs", context.run_id, *context.asset_key.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = obj.model_dump_json(by_alias=True).encode()
        path.write_bytes(payload)
        _record_config_digest(context, payload)

    def load_input(self, context: InputContext) -> BaseModel | None:
        if context.asset_key == CONFIG_DIRECTORY:
            record = input_materialization(context)
            if record is None:
                if producer_run_id(context.upstream_output) is not None:
                    raise IntegrityError(
                        "Missing configuration directory materialization for the upstream run"
                    )
                return None
            value = self.directory(record.run_id)
            version = extract_data_version_from_entry(record.event_log_entry)
            if version is None or version.value != value.digest:
                raise IntegrityError("Persisted configuration directory differs from its materialization")
            return value
        if context.asset_key == GLOBAL_CONFIG:
            return RegisteredConfig.model_validate_json(self._upstream_payload(context), strict=True)
        return self.load_scope(context)

    def load_scope(self, context: InputContext) -> ConfigArtifact:
        contract = context.dagster_type.typing_type
        if not isinstance(contract, type) or not issubclass(contract, ConfigArtifact):
            raise IntegrityError(f"No scoped configuration contract for {context.asset_key.to_user_string()}")
        return self.load_projected_scope(context, contract)

    def load_projected_scope[Contract: ConfigArtifact](
        self, context: InputContext, contract: type[Contract]
    ) -> Contract:
        return contract.model_validate_json(self._upstream_payload(context), strict=True)

    def _upstream_payload(self, context: InputContext) -> bytes:
        upstream = context.upstream_output
        if upstream is None:
            return self._read_run_output(context.step_context.run_id, context.asset_key)
        from codebench_pipeline.finalization_cache_reuse import materialization_attempt_succeeded

        record = input_materialization(context)
        if record is None or not materialization_attempt_succeeded(
            context.instance,
            record,
            output_name="result" if producer_run_id(upstream) is None else upstream.name,
            retry_number="native",
        ):
            raise IntegrityError(f"No successful configuration for {context.asset_key.to_user_string()}")
        return self._read_materialized(record)

    def registered(self, run_id: str, *, materialization: EventLogRecord | None = None) -> RegisteredConfig:
        payload = (
            self._read_run_output(run_id, GLOBAL_CONFIG)
            if materialization is None
            else self._read_materialized(materialization)
        )
        return RegisteredConfig.model_validate_json(payload, strict=True)

    def directory(self, run_id: str) -> ConfigDirectorySnapshot:
        return ConfigDirectorySnapshot.model_validate_json(
            self._read_run_output(run_id, CONFIG_DIRECTORY), strict=True
        )

    def _read_run_output(self, run_id: str, key: AssetKey) -> bytes:
        path = Path(self.home, "configuration-runs", run_id, *key.path)
        try:
            return path.read_bytes()
        except FileNotFoundError as error:
            raise IntegrityError(
                f"Missing configuration {key.to_user_string()} in this run; "
                "include configuration assets when selecting processing stages"
            ) from error

    def _read_materialized(self, record: EventLogRecord) -> bytes:
        materialization = record.asset_materialization
        if materialization is None:
            raise IntegrityError("Configuration producer has no materialization")
        payload = self._read_run_output(record.run_id, materialization.asset_key)
        if materialization.metadata.get("codebench/config_digest") != TextMetadataValue(sha256(payload)):
            raise IntegrityError("Persisted configuration differs from its producer evidence")
        return payload


def config_directory_asset(home: Path, authoring_root: Path | None = None) -> AssetsDefinition:
    @asset(
        key=CONFIG_DIRECTORY,
        partitions_def=DATASET_PARTITIONS,
        group_name="configuration",
        io_manager_key="config_run_io_manager",
        dagster_type=_CONFIG_DIRECTORY_TYPE,
        description="**How it works:** Read the declared category's authored files under the runtime "
        "configuration root. Publish the observed relative filenames and exact contents under a native "
        "data version. Observation does not approve changes to registered policy or source rights.",
    )
    def config_directory(context: AssetExecutionContext) -> Output[ConfigDirectorySnapshot]:
        spec = DatasetRegistry(home).get(context.partition_key)
        if spec.config_directory is None:
            raise IntegrityError("This registration does not authorize a live configuration directory")
        root = authoring_root
        if root is None:
            root = Path(os.environ.get("CODEBENCH_CONFIG_ROOT", str(_INSTALLED_AUTHORING_ROOT)))
        snapshot = ConfigDirectorySnapshot.observe(root, spec.config_directory.category_id)
        return Output(
            snapshot,
            data_version=DataVersion(snapshot.digest),
            metadata={"scope": snapshot.scope, "relative_files": list(sorted(snapshot.files))},
        )

    return config_directory


def configuration_assets(home: Path, *, authoring_root: Path | None = None) -> list[AssetsDefinition]:
    @asset(
        key=GLOBAL_CONFIG,
        partitions_def=DATASET_PARTITIONS,
        group_name="configuration",
        io_manager_key="config_run_io_manager",
        dagster_type=REGISTERED_CONFIG_TYPE,
        ins={"directory": AssetIn(key=CONFIG_DIRECTORY)},
        description="**How it works:** Read the validated registration for this dataset partition. "
        "Require the persisted directory input to match the approved content hash, then publish "
        "the captured bytes and validated settings for downstream stages.",
    )
    def global_config(
        context: AssetExecutionContext, config: GlobalConfig, directory: ConfigDirectorySnapshot | None
    ) -> Output[RegisteredConfig]:
        spec = DatasetRegistry(home).get(context.partition_key)
        if config.expected_revision and config.expected_revision != fingerprint(spec.model_dump_json()):
            raise IntegrityError("Run configuration no longer matches the registered specification")
        approved = spec.config_directory
        if directory is not None and (approved is None or directory.digest != approved.digest):
            raise IntegrityError("Observed configuration directory changed; authorize the captured revision")
        if directory is not None:
            collection = spec.declared_collection
            assert collection is not None
            rules, compiled_collection = compile_directory(
                directory.require_complete(spec.rules.category_id),
                spec.dataset_id,
                collection.merge_keenable_weight,
            )
            if rules != spec.rules or compiled_collection != collection:
                raise IntegrityError("Registered settings differ from the authorized directory capture")
            spec = spec.model_copy(update={"rules": rules, "declared_collection": compiled_collection})
        registered = RegisteredConfig.from_spec(spec, directory=approved if directory is None else directory)
        persist_repository_origin_bindings(
            registered.scopes.labeling_attempts.collection,
            RepositoryRun(home, registered.dataset_id, context.run_id),
        )
        applied = registered.config_directory
        return Output(
            registered,
            data_version=DataVersion(fingerprint(registered.model_dump_json())),
            metadata={}
            if applied is None
            else {
                "config_directory_scope": applied.scope,
                "config_directory_digest": applied.digest,
                "config_directory_files": len(applied.files),
            },
        )

    def mapping[Contract: ConfigArtifact](
        scope: str,
        contract: type[Contract],
        projection: Callable[[RegisteredConfig], Contract],
        *,
        group_name: str | None = None,
    ) -> AssetsDefinition:
        def project(registered: RegisteredConfig) -> Output[Contract]:
            value = projection(registered)
            effective = RootModel[JsonValue].model_validate_json(value.model_dump_json(by_alias=True))
            return Output(value, data_version=DataVersion(fingerprint(effective.root)))

        return asset(
            key=mapping_key(scope),
            partitions_def=DATASET_PARTITIONS,
            group_name=FINALIZATION_GROUPS[scope] if group_name is None else group_name,
            io_manager_key="config_run_io_manager",
            dagster_type=_contract_type(contract),
            ins={"registered": registered_input()},
            description=(
                f"**How it works:** Project the registered settings for {scope} into "
                f"`{contract.__name__}`. Its fields are "
                + ", ".join(f"`{name}`" for name in contract.model_fields)
                + ". The data version identifies the effective settings."
            ),
        )(project)

    def declared_branch(operation: CollectionOperation) -> AssetsDefinition:
        return mapping(
            operation.mapping_scope,
            operation.contract,
            lambda registered: operation.project(registered.source_config(operation.branch)),
            group_name=COLLECTION_SCOPES[operation.branch],
        )

    return [
        config_directory_asset(home, authoring_root),
        global_config,
        *(declared_branch(operation) for operation in COLLECTION_OPERATIONS),
        mapping(
            "merged_command_candidates",
            CollectionMergeConfig,
            lambda registered: registered.scopes.merged_command_candidates.merged_command_candidates,
        ),
        mapping(
            "labeling_attempts",
            GuardedLabelingConfig,
            RegisteredConfig.labeling_config,
        ),
        mapping(
            "filtered_commands",
            FilterConfig,
            RegisteredConfig.filter_config,
        ),
        mapping(
            "proposed_annotations",
            ProposalRules,
            RegisteredConfig.proposal_config,
        ),
        mapping(
            "deduplicated_annotations",
            DeduplicationPolicy,
            lambda registered: registered.scopes.deduplicated_annotations.deduplication,
        ),
        mapping(
            "category_eligible_annotations",
            CategoryEligibilityConfig,
            RegisteredConfig.eligibility_config,
        ),
        mapping(
            "human_review_queue",
            ReviewQueueConfig,
            RegisteredConfig.review_queue_config,
        ),
        mapping(
            "dataset_release",
            DatasetReleaseConfig,
            RegisteredConfig.release_config,
        ),
    ]
