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
    InputContext,
    OutputContext,
    PythonObjectDagsterType,
    asset,
)
from pydantic import BaseModel, TypeAdapter

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataset_asset_types import DATASET_PARTITIONS, rules_input
from codebench_pipeline.dataset_branches import (
    COLLECTION_BRANCHES,
    FINALIZATION_GROUPS,
    FINALIZATION_STAGES,
    Branch,
    collection_branches,
    stage_branch,
)
from codebench_pipeline.dataset_config import DatasetBuildSpec
from codebench_pipeline.dataset_registry import DatasetRegistry, read_registered_spec
from codebench_pipeline.keenable_policy import MAX_BYTES
from codebench_pipeline.stage_configuration import (
    ConfigArtifact,
    DeclaredBranchConfig,
    DeclaredGlobalConfig,
    DeclaredMergeConfig,
    DeclaredScopes,
    DocumentationCrawlConfig,
    ExistingDatasetsConfig,
    FilterConfig,
    GuardedLabelingConfig,
    KeenableGrantPreview,
    LabelingDocumentationConfig,
    PreparedLabelingConfig,
    ReviewQueueConfig,
    prepared_source_settings,
)
from codebench_pipeline.storage import IntegrityError, fingerprint

GLOBAL_CONFIG: Final = AssetKey(["datasets", "global_config"])
COLLECTION_SCOPES: Final = {branch.namespace: branch.group for branch in COLLECTION_BRANCHES}
type RegisteredConfig = DatasetBuildSpec | DeclaredGlobalConfig
REGISTERED_CONFIG_ADAPTER: Final = TypeAdapter(RegisteredConfig)
REGISTERED_CONFIG_TYPE: Final = PythonObjectDagsterType(
    name="RegisteredConfig", python_type=(DatasetBuildSpec, DeclaredGlobalConfig)
)
DIRECT_POLICY_STAGES: Final = frozenset(
    {"category_rules", "proposed_annotations", "category_eligible_annotations", "dataset_release"}
)


class GlobalConfig(Config):
    snapshot: str = ""
    expected_revision: str = ""

    @classmethod
    def for_spec(cls, spec: DatasetBuildSpec) -> "GlobalConfig":
        if spec.has_declared_collection:
            return cls(expected_revision=fingerprint(spec.model_dump_json()))
        return cls(snapshot=spec.model_dump_json())


def mapping_key(scope: str) -> AssetKey:
    return AssetKey(["datasets", f"map_{scope}_config"])


def registered_input() -> AssetIn:
    return AssetIn(key=GLOBAL_CONFIG, dagster_type=REGISTERED_CONFIG_TYPE)


def require_registered_rules(registered: RegisteredConfig, category_rules: CategoryRules) -> None:
    if category_rules != registered.rules:
        raise IntegrityError("Category rules differ from the run configuration; replan the build")


def configuration_key(stage: str, *, declared: bool) -> AssetKey | None:
    if stage in DIRECT_POLICY_STAGES:
        return GLOBAL_CONFIG
    if stage not in FINALIZATION_STAGES:
        return mapping_key(COLLECTION_SCOPES[stage_branch(stage)])
    if (
        stage in {"filtered_commands", "labeling_attempts", "human_review_queue"}
        or (declared and stage == "merged_command_candidates")
        or (not declared and stage == "labeling_documentation")
    ):
        return mapping_key(stage)
    return None


def configured_selection(stages: Iterable[AssetKey], *, declared: bool = False) -> list[AssetKey]:
    selection = list(stages)
    configs = [
        config
        for key in selection
        if (config := configuration_key("/".join(key.path[1:]), declared=declared)) is not None
    ]
    return list(dict.fromkeys([*selection, *configs, *([GLOBAL_CONFIG] if configs else [])]))


@cache
def _contract_type(contract: type[ConfigArtifact]) -> PythonObjectDagsterType:
    return PythonObjectDagsterType(python_type=contract, name=contract.__name__)


def mapped_input(scope: str, contract: type[ConfigArtifact]) -> AssetIn:
    return AssetIn(key=mapping_key(scope), dagster_type=_contract_type(contract))


class ConfigRunIOManager(ConfigurableIOManager):
    """Store typed configuration as inspectable JSON, with no cross-run fallback."""

    home: str

    def handle_output(self, context: OutputContext, obj: BaseModel) -> None:
        path = Path(self.home, "configuration-runs", context.run_id, *context.asset_key.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(obj.model_dump_json(by_alias=True), encoding="utf-8")

    def load_input(self, context: InputContext) -> BaseModel:
        path = Path(self.home, "configuration-runs", context.step_context.run_id, *context.asset_key.path)
        try:
            payload = path.read_bytes()
        except FileNotFoundError as error:
            raise IntegrityError(
                f"Missing configuration {context.asset_key.to_user_string()} in this run; "
                "include configuration assets when selecting processing stages"
            ) from error
        if context.asset_key == GLOBAL_CONFIG:
            return REGISTERED_CONFIG_ADAPTER.validate_json(payload, strict=True)
        contract = context.dagster_type.typing_type
        if not isinstance(contract, type) or not issubclass(contract, ConfigArtifact):
            raise IntegrityError(f"No scoped configuration contract for {context.asset_key.to_user_string()}")
        return contract.model_validate_json(payload, strict=True)


def configuration_assets(home: Path, *, declared: bool = False) -> list[AssetsDefinition]:
    @asset(
        key=GLOBAL_CONFIG,
        partitions_def=DATASET_PARTITIONS,
        group_name="dataset_configuration",
        io_manager_key="config_run_io_manager",
        dagster_type=REGISTERED_CONFIG_TYPE,
        description="**How it works:** Read the validated configuration for this dataset partition. "
        "Planned runs check their pinned specification. Declared runs publish only path-free settings "
        "for each active scope; historical prepared-source runs keep the registered build specification.",
    )
    def global_config(context: AssetExecutionContext, config: GlobalConfig) -> RegisteredConfig:
        registry = DatasetRegistry(home)
        if declared and config.snapshot:
            raise IntegrityError("Declared runs accept a revision, not an inline build specification")
        spec = (
            read_registered_spec(config.snapshot.encode())
            if config.snapshot
            else registry.get(context.partition_key)
        )
        if spec.dataset_id != context.partition_key:
            raise IntegrityError("Configuration snapshot belongs to another dataset partition")
        if spec.has_declared_collection != declared:
            raise IntegrityError("Dataset registration changed configured source inputs; reload definitions")
        if config.expected_revision and config.expected_revision != fingerprint(spec.model_dump_json()):
            raise IntegrityError("Run configuration no longer matches the registered specification")
        return DeclaredGlobalConfig.from_spec(spec) if declared else spec

    def mapping[Contract: ConfigArtifact](
        scope: str,
        contract: type[Contract],
        projection: Callable[[RegisteredConfig], Contract] | None = None,
        *,
        policy: Callable[[RegisteredConfig, CategoryRules], Contract] | None = None,
    ) -> AssetsDefinition:
        def project(registered: RegisteredConfig, category_rules: CategoryRules) -> Contract:
            require_registered_rules(registered, category_rules)
            assert policy is not None
            return policy(registered, category_rules)

        def settings_only(registered: RegisteredConfig) -> Contract:
            assert projection is not None
            return projection(registered)

        return asset(
            key=mapping_key(scope),
            partitions_def=DATASET_PARTITIONS,
            group_name=scope if scope in COLLECTION_SCOPES.values() else FINALIZATION_GROUPS[scope],
            io_manager_key="config_run_io_manager",
            dagster_type=_contract_type(contract),
            ins={
                "registered": registered_input(),
                **({"category_rules": rules_input()} if policy is not None else {}),
            },
            description=f"**How it works:** Take only the registered settings for {scope} and "
            "publish them as a typed same-run configuration. Policy mappings also check and carry category rules.",
        )(project if policy is not None else settings_only)

    def declared_scopes(registered: RegisteredConfig) -> DeclaredScopes:
        if not isinstance(registered, DeclaredGlobalConfig):
            raise IntegrityError("Expected declared collection configuration")
        return registered.scopes

    def prepared_spec(registered: RegisteredConfig) -> DatasetBuildSpec:
        if not isinstance(registered, DatasetBuildSpec):
            raise IntegrityError("Expected prepared-source configuration")
        return registered

    def prepared_documentation(
        registered: RegisteredConfig, rules: CategoryRules
    ) -> LabelingDocumentationConfig:
        docs = prepared_spec(registered).documentation
        assert docs is not None
        return LabelingDocumentationConfig(category_rules=rules, labeling_documentation=docs)

    def existing_datasets(registered: RegisteredConfig, rules: CategoryRules) -> ExistingDatasetsConfig:
        settings = prepared_source_settings(prepared_spec(registered), "dataset")
        return ExistingDatasetsConfig(
            category_rules=rules,
            source_catalog=settings.source_catalog,
            source_documents=settings.source_documents,
            document_adapters=settings.document_adapters,
            command_candidates=settings.command_candidates,
        )

    def documentation_crawl(registered: RegisteredConfig, rules: CategoryRules) -> DocumentationCrawlConfig:
        settings = prepared_source_settings(prepared_spec(registered), "documentation")
        return DocumentationCrawlConfig(
            category_rules=rules,
            **{
                "documentation/source_catalog": settings.source_catalog,
                "documentation/source_documents": settings.source_documents,
                "documentation/document_adapters": settings.document_adapters,
                "documentation/command_candidates": settings.command_candidates,
            },
        )

    def declared_branch(scope: str, branch: Branch) -> AssetsDefinition:
        def project(registered: RegisteredConfig, rules: CategoryRules) -> DeclaredBranchConfig:
            scopes = declared_scopes(registered)
            collection = {
                "dataset": scopes.existing_datasets,
                "documentation": scopes.documentation_crawl,
                "keenable": scopes.keenable_use_cases,
            }[branch]
            config = collection.collection
            preview = None
            if branch == "keenable":
                declaration = config.source.keenable_config()
                preview = KeenableGrantPreview(
                    effective_cases=declaration.search_cases,
                    effective_byte_capacity=declaration.budget.total_bytes
                    if declaration.budget is not None
                    else MAX_BYTES,
                )
            return DeclaredBranchConfig(category_rules=rules, collection=config, keenable_preview=preview)

        return mapping(scope, DeclaredBranchConfig, policy=project)

    return [
        global_config,
        *(
            [
                *(
                    declared_branch(branch.group, branch.namespace)
                    for branch in collection_branches(declared=True)
                ),
                mapping(
                    "merged_command_candidates",
                    DeclaredMergeConfig,
                    lambda registered: declared_scopes(registered).merged_command_candidates,
                ),
                mapping(
                    "labeling_attempts",
                    GuardedLabelingConfig,
                    policy=lambda registered, rules: GuardedLabelingConfig(
                        category_rules=rules,
                        labeling_attempts=declared_scopes(registered).labeling_attempts.labeling_attempts,
                    ),
                ),
            ]
            if declared
            else [
                mapping("existing_datasets", ExistingDatasetsConfig, policy=existing_datasets),
                mapping("documentation_crawl", DocumentationCrawlConfig, policy=documentation_crawl),
                mapping("labeling_documentation", LabelingDocumentationConfig, policy=prepared_documentation),
                mapping(
                    "labeling_attempts",
                    PreparedLabelingConfig,
                    policy=lambda registered, rules: PreparedLabelingConfig(
                        category_rules=rules, labeling_attempts=prepared_spec(registered).annotation
                    ),
                ),
            ]
        ),
        mapping(
            "filtered_commands",
            FilterConfig,
            policy=lambda registered, rules: FilterConfig(
                category_rules=rules,
                filtered_commands=registered.scopes.filtered_commands.filtered_commands
                if isinstance(registered, DeclaredGlobalConfig)
                else registered.filtering,
            ),
        ),
        mapping(
            "human_review_queue",
            ReviewQueueConfig,
            policy=lambda registered, rules: ReviewQueueConfig(
                category_rules=rules,
                human_review_queue=registered.scopes.human_review_queue.human_review_queue
                if isinstance(registered, DeclaredGlobalConfig)
                else registered.publication,
            ),
        ),
    ]
