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
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, rules_input
from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    FINALIZATION_GROUPS,
    Branch,
)
from codebench_pipeline.dataset_config import DatasetBuildSpec, DeduplicationPolicy
from codebench_pipeline.dataset_identity import CODEBENCH_DATASET_ID
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
from codebench_pipeline.rules_registration_config import RegisteredRules
from codebench_pipeline.stage_configuration import (
    CategoryEligibilityConfig,
    ConfigArtifact,
    DatasetReleaseConfig,
    DeclaredGlobalConfig,
    FilterConfig,
    GuardedLabelingConfig,
    ReviewQueueConfig,
    category_eligibility_settings,
    dataset_release_settings,
)
from codebench_pipeline.storage import IntegrityError, fingerprint, open_directory, open_regular_file

GLOBAL_CONFIG: Final = AssetKey(["datasets", "global_config"])
CONFIG_DIRECTORY: Final = AssetKey(["datasets", "config_directory"])
_INSTALLED_AUTHORING_ROOT: Final = Path(__file__).resolve().parents[1] / "business-configs"
_CONFIG_DIRECTORY_TYPE: Final = PythonObjectDagsterType(
    name="ConfigDirectorySnapshot", python_type=ConfigDirectorySnapshot
)
COLLECTION_SCOPES: Final = {branch.namespace: branch.group for branch in COLLECTION_BRANCHES}
RegisteredConfig = DeclaredGlobalConfig
REGISTERED_CONFIG_TYPE: Final = PythonObjectDagsterType(name="RegisteredConfig", python_type=RegisteredConfig)


def rules_policy_path(home: Path) -> Path:
    return home / "rules-configuration" / CODEBENCH_DATASET_ID / "approved.json"


class GlobalConfig(Config):
    expected_revision: str = ""
    expected_rules_digest: str = ""

    @classmethod
    def for_spec(cls, spec: DatasetBuildSpec) -> "GlobalConfig":
        if not spec.has_declared_collection:
            raise IntegrityError("Expected a declared collection specification")
        return cls(expected_revision=fingerprint(spec.model_dump_json()))


def mapping_key(scope: str) -> AssetKey:
    return AssetKey(["datasets", f"map_{scope}_config"])


SPLIT_CATEGORY_CONFIGS: Final = frozenset((split, "stats") for split in ("train", "test")) | frozenset(
    tuple(mapping_key(f"{split}_{scope}").path) for split in ("train", "test") for scope in ("load", "counts")
)
SINGLETON_RUN_CONFIGS: Final = frozenset({("codebench", "full_dataset")}) | frozenset(
    tuple(mapping_key(f"{split}_merge").path) for split in ("train", "test")
)
PRIVATE_RULES_RUN_ARTIFACTS: Final = frozenset(
    ("codebench", name)
    for name in ("rules_registration", "rules_capture", "rules_dedup", "rules_finalize",
                 "rules_train_sample", "rules_test_sample")
) | frozenset(
    tuple(mapping_key(name).path) for name in (
        "rules_capture", "rules_dedup", "rules_finalize", "rules_eval", "rules_train_sample", "rules_test_sample",
    )
)
PARTITIONED_RUN_CONFIGS: Final = SPLIT_CATEGORY_CONFIGS | SINGLETON_RUN_CONFIGS | PRIVATE_RULES_RUN_ARTIFACTS


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
def mapping_type(contract: type[ConfigArtifact]) -> PythonObjectDagsterType:
    return PythonObjectDagsterType(python_type=contract, name=contract.__name__)


def mapped_input(
    scope: str, contract: type[ConfigArtifact], *, input_manager_key: str | None = None
) -> AssetIn:
    return AssetIn(
        key=mapping_key(scope), dagster_type=mapping_type(contract), input_manager_key=input_manager_key
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


def _run_output_partition(context: InputContext | OutputContext) -> str | None:
    if tuple(context.asset_key.path) in PARTITIONED_RUN_CONFIGS and context.has_asset_partitions:
        return context.asset_partition_key
    return None


class ConfigRunIOManager(ConfigurableIOManager):
    """Read config from its producing run, or the partition's observed directory."""

    home: str

    def run_output_path(self, run_id: str, key: AssetKey, partition: str | None) -> Path:
        if tuple(key.path) not in PARTITIONED_RUN_CONFIGS:
            return Path(self.home, "configuration-runs", run_id, *key.path)
        if partition is None:
            raise IntegrityError("Partitioned configuration requires an asset partition")
        return Path(
            self.home, "configuration-runs", run_id, "partitions", sha256(partition.encode()), *key.path
        )

    def handle_output(self, context: OutputContext, obj: BaseModel) -> None:
        path = self.run_output_path(context.run_id, context.asset_key, _run_output_partition(context))
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = obj.model_dump_json(by_alias=True).encode()
        if context.asset_key == GLOBAL_CONFIG or tuple(context.asset_key.path) in PRIVATE_RULES_RUN_ARTIFACTS:
            self.write_private(path, payload)
        else:
            path.write_bytes(payload)
        _record_config_digest(context, payload)

    @staticmethod
    def write_private(path: Path, payload: bytes, *, exclusive: bool = False) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flag = os.O_EXCL if exclusive else os.O_TRUNC
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | flag, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)

    def read_private(self, path: Path) -> bytes:
        with open_directory(path.parent) as directory, open_regular_file(directory, path.name) as stream:
            return stream.read()

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

    def materialized_scope[Contract: ConfigArtifact](
        self, record: EventLogRecord, contract: type[Contract]
    ) -> Contract:
        return contract.model_validate_json(self._read_materialized(record), strict=True)

    def _upstream_payload(self, context: InputContext) -> bytes:
        upstream = context.upstream_output
        if upstream is None:
            return self._read_run_output(
                context.step_context.run_id, context.asset_key, _run_output_partition(context)
            )
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

    def _read_run_output(self, run_id: str, key: AssetKey, partition: str | None = None) -> bytes:
        path = self.run_output_path(run_id, key, partition)
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
        payload = self._read_run_output(record.run_id, materialization.asset_key, materialization.partition)
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
                collection.merge_config,
            )
            if rules != spec.rules or compiled_collection != collection:
                raise IntegrityError("Registered settings differ from the authorized directory capture")
            spec = spec.model_copy(update={"rules": rules, "declared_collection": compiled_collection})
        rules_learning = None
        if spec.dataset_id == CODEBENCH_DATASET_ID:
            try:
                payload = ConfigRunIOManager(home=str(home)).read_private(rules_policy_path(home))
            except FileNotFoundError:
                pass
            else:
                rules_learning = RegisteredRules.model_validate_json(payload)
        if config.expected_rules_digest and (
            rules_learning is None or rules_learning.digest != config.expected_rules_digest
        ):
            raise IntegrityError("Rules policy changed before refreshing global configuration")
        registered = RegisteredConfig.from_spec(
            spec, directory=approved if directory is None else directory, rules_learning=rules_learning,
        )
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
        project: Callable[..., Output[Contract]],
        inputs: dict[str, AssetIn],
        input_description: str,
        *,
        group_name: str | None = None,
    ) -> AssetsDefinition:
        return asset(
            key=mapping_key(scope),
            partitions_def=DATASET_PARTITIONS,
            group_name=FINALIZATION_GROUPS[scope] if group_name is None else group_name,
            io_manager_key="config_run_io_manager",
            dagster_type=mapping_type(contract),
            ins=inputs,
            description=(
                f"**How it works:** Project {input_description} into "
                f"`{contract.__name__}`. Its fields are "
                + ", ".join(f"`{name}`" for name in contract.model_fields)
                + ". The data version identifies the effective settings."
            ),
        )(project)

    def scoped_output[Contract: ConfigArtifact](value: Contract) -> Output[Contract]:
        effective = RootModel[JsonValue].model_validate_json(value.model_dump_json(by_alias=True))
        return Output(value, data_version=DataVersion(fingerprint(effective.root)))

    def global_mapping[Contract: ConfigArtifact](
        scope: str,
        contract: type[Contract],
        projection: Callable[[RegisteredConfig], Contract],
        *,
        group_name: str | None = None,
    ) -> AssetsDefinition:
        def project(registered: RegisteredConfig) -> Output[Contract]:
            return scoped_output(projection(registered))

        return mapping(
            scope,
            contract,
            project,
            {"registered": registered_input()},
            f"the registered settings for {scope}",
            group_name=group_name,
        )

    def taxonomy_mapping[Contract: ConfigArtifact](
        scope: str,
        contract: type[Contract],
        projection: Callable[[CategoryRules], Contract],
    ) -> AssetsDefinition:
        def project(category_rules: CategoryRules) -> Output[Contract]:
            return scoped_output(projection(category_rules))

        return mapping(scope, contract, project, {"category_rules": rules_input()}, "the category rules")

    def mixed_mapping[Contract: ConfigArtifact](
        scope: str,
        contract: type[Contract],
        projection: Callable[[RegisteredConfig, CategoryRules], Contract],
        *,
        group_name: str | None = None,
    ) -> AssetsDefinition:
        def project(registered: RegisteredConfig, category_rules: CategoryRules) -> Output[Contract]:
            return scoped_output(projection(registered, category_rules))

        return mapping(
            scope,
            contract,
            project,
            {"registered": registered_input(), "category_rules": rules_input()},
            f"the registered settings for {scope} and the category rules",
            group_name=group_name,
        )

    def declared_branch(operation: CollectionOperation) -> AssetsDefinition:
        if operation.branch == "keenable":
            return mixed_mapping(
                operation.mapping_scope,
                operation.contract,
                RegisteredConfig.project_keenable_candidates,
                group_name=COLLECTION_SCOPES[operation.branch],
            )
        return global_mapping(
            operation.mapping_scope,
            operation.contract,
            lambda registered: registered.collection_operation_settings(operation),
            group_name=COLLECTION_SCOPES[operation.branch],
        )

    return [
        config_directory_asset(home, authoring_root),
        global_config,
        *(declared_branch(operation) for operation in COLLECTION_OPERATIONS),
        global_mapping(
            "merged_command_candidates",
            CollectionMergeConfig,
            lambda registered: registered.scopes.merged_command_candidates.merged_command_candidates,
        ),
        mixed_mapping(
            "labeling_attempts",
            GuardedLabelingConfig,
            lambda registered, rules: GuardedLabelingConfig.project(
                registered.scopes.labeling_attempts.labeling_attempts, rules
            ),
        ),
        mixed_mapping(
            "filtered_commands",
            FilterConfig,
            FilterConfig.project,
        ),
        taxonomy_mapping(
            "proposed_annotations",
            ProposalRules,
            ProposalRules.from_rules,
        ),
        global_mapping(
            "deduplicated_annotations",
            DeduplicationPolicy,
            lambda registered: registered.scopes.deduplicated_annotations.deduplication,
        ),
        taxonomy_mapping(
            "category_eligible_annotations",
            CategoryEligibilityConfig,
            category_eligibility_settings,
        ),
        mixed_mapping(
            "human_review_queue",
            ReviewQueueConfig,
            RegisteredConfig.project_review_queue,
        ),
        mixed_mapping(
            "dataset_release",
            DatasetReleaseConfig,
            dataset_release_settings,
        ),
    ]
