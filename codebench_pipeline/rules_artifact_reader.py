from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypedDict, assert_never

import pandas as pd
from pydantic import ConfigDict, Field, JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.artifact_schemas import ArtifactSchema
from codebench_pipeline.config_assets import mapping_key
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import sha256
from codebench_pipeline.rules_artifacts import FinalizedRules, RulesDedupArtifact
from codebench_pipeline.rules_contracts import Category, NativeRule
from codebench_pipeline.rules_learning import CaptureResult
from codebench_pipeline.rules_registration_config import HistoricalRegisteredRules, RulesRuntime, SplitPin
from codebench_pipeline.rules_settings import (
    RULES_REGISTRATION,
    AgentSettings,
    LearningPins,
    LearningPolicy,
)
from codebench_pipeline.storage import IntegrityError, open_directory, open_regular_file
from codebench_pipeline.table_query import QueryModel, TablePage, TableQuery, query_table
from codebench_pipeline.train_config import MergeSettings


class CaptureSettingsProjection(QueryModel):
    """Read both merged-train and category-source capture settings."""

    model_config = ConfigDict(extra="ignore")
    category: Category
    runtime: RulesRuntime
    agent: AgentSettings
    pins: LearningPins | None = None
    guide_sha256: Digest | None = None
    sources_digest: Digest | None = None


class LearningSettingsProjection(QueryModel):
    model_config = ConfigDict(extra="ignore")
    pins: LearningPins | None = None
    policy: LearningPolicy | None = None
    test: SplitPin | None = None
    runtime: RulesRuntime
    agent: AgentSettings | None = None
    category_sources_digests: dict[Category, Digest] | None = None
    train_sources: MergeSettings | None = None
    test_sources: MergeSettings | None = None


class CaptureProjection(QueryModel):
    model_config = ConfigDict(extra="ignore")
    result: CaptureResult


type RulesOutput = (
    HistoricalRegisteredRules | CaptureSettingsProjection | LearningSettingsProjection
    | CaptureProjection | RulesDedupArtifact | FinalizedRules
)

RULES_RUN_CONTRACTS: Final[dict[tuple[str, ...], type[RulesOutput]]] = {
    tuple(RULES_REGISTRATION.path): HistoricalRegisteredRules,
    tuple(mapping_key("rules_capture").path): CaptureSettingsProjection,
    tuple(mapping_key("rules_dedup").path): LearningSettingsProjection,
    tuple(mapping_key("rules_finalize").path): LearningSettingsProjection,
    tuple(mapping_key("rules_eval").path): LearningSettingsProjection,
    ("codebench", "rules_capture"): CaptureProjection,
    ("codebench", "rules_dedup"): RulesDedupArtifact,
    ("codebench", "rules_finalize"): FinalizedRules,
}

RULES_CAPTURE_KEYS: Final = frozenset(
    key for key, contract in RULES_RUN_CONTRACTS.items()
    if contract in (CaptureProjection, CaptureSettingsProjection)
)


@dataclass(frozen=True, slots=True)
class RulesRunBinding:
    path: Path
    digest: Digest
    contract: type[RulesOutput]


class RulesConfigurationRow(QueryModel):
    category: Category
    train_digest: Digest | None = None
    train_rows: int | None = None
    test_digest: Digest | None = None
    test_rows: int | None = None
    runtime_version: str
    guide_sha256: Digest | None = None
    sources_digest: Digest | None = None
    registration_digest: Digest | None = None
    inference_digest: Digest | None = None


class RuleFields(TypedDict):
    rule_id: str
    scope: str
    category: Category
    risk: str
    severity: str
    action: str


def _rule_fields(rule: NativeRule) -> RuleFields:
    labels = rule.labels
    return RuleFields(
        rule_id=rule.id, scope=rule.scope or "default", category=labels.category, risk=labels.risk,
        severity=labels.severity, action=labels.action,
    )


class RuleRow(QueryModel):
    rule_id: str
    scope: str = Field(
        title="Native evaluation scope",
        description="Bashguard evaluation mode, not the training dataset. "
        "Simple applies to eligible simple commands; ast uses parsed shell structure. "
        "Legacy empty scope is shown as default and behaves like simple.",
    )
    category: Category
    risk: str
    severity: str
    action: str


class PackagedRuleRow(RuleRow):
    fixture_count: int


class CapturedRuleRow(PackagedRuleRow):
    batch_id: str


class FinalizedRuleRow(RuleRow):
    when: str
    bundle_sha256: Digest


def read_rules_table(
    binding: RulesRunBinding, snapshot: Digest | None, query: TableQuery
) -> tuple[TablePage, ArtifactSchema]:
    """Verify the private run output and paginate only its public projection."""
    if snapshot is not None and snapshot != binding.digest:
        raise HumanReviewError("stale", "The artifact changed; refresh before continuing")
    try:
        with (
            open_directory(binding.path.parent) as directory,
            open_regular_file(directory, binding.path.name) as stream,
        ):
            payload = stream.read()
        if sha256(payload) != binding.digest:
            raise IntegrityError("Rules output differs from its materialization")
        value = binding.contract.model_validate_json(payload, strict=True)
    except (OSError, IntegrityError, ValidationError):
        raise DatasetViewError("not_materialized", "This rules output cannot be verified") from None
    row_type, rows = _projection(value)
    frame = pd.DataFrame(
        [row.model_dump(mode="json") for row in rows], columns=list(row_type.model_fields), dtype=object
    )
    schema = TypeAdapter(dict[str, JsonValue]).validate_python(row_type.model_json_schema())
    return query_table(frame, query), ArtifactSchema(source="declared", schema=schema)


def _projection(value: RulesOutput) -> tuple[type[QueryModel], Iterable[QueryModel]]:
    match value:
        case HistoricalRegisteredRules(registration=settings, digest=digest):
            return RulesConfigurationRow, (
                RulesConfigurationRow(
                    category=category,
                    train_digest=settings.train.digest if settings.train is not None else None,
                    train_rows=settings.train.row_count if settings.train is not None else None,
                    test_digest=settings.test.digest if settings.test is not None else None,
                    test_rows=settings.test.row_count if settings.test is not None else None,
                    runtime_version=settings.runtime.version, guide_sha256=settings.prompt_guide.sha256,
                    registration_digest=digest,
                )
                for category in settings.categories
            )
        case CaptureSettingsProjection():
            train = value.pins.train if value.pins is not None else None
            guide = value.pins.guide_sha256 if value.pins is not None else value.guide_sha256
            return RulesConfigurationRow, (RulesConfigurationRow(
                category=value.category, runtime_version=value.runtime.version, guide_sha256=guide,
                sources_digest=value.sources_digest, inference_digest=value.agent.inference_digest,
                train_digest=train.digest if train is not None else None,
                train_rows=train.row_count if train is not None else None,
            ),)
        case LearningSettingsProjection():
            if (policy := value.policy or value.pins) is None:
                raise DatasetViewError("not_materialized", "Rules settings have no learning policy")
            train = value.pins.train if value.pins else None
            manifest = value.train_sources or value.test_sources
            sources = manifest.category_digests if manifest else value.category_sources_digests
            return RulesConfigurationRow, (
                RulesConfigurationRow(
                    category=category, train_digest=train.digest if train else None,
                    train_rows=train.row_count if train else None,
                    test_digest=value.test.digest if value.test else None,
                    test_rows=value.test.row_count if value.test else None,
                    runtime_version=value.runtime.version, guide_sha256=policy.guide_sha256,
                    sources_digest=sources.get(category) if sources else None,
                    inference_digest=value.agent.inference_digest if value.agent else None,
                )
                for category in policy.categories
            )
        case CaptureProjection(result=result):
            return CapturedRuleRow, (
                CapturedRuleRow(
                    **_rule_fields(entry.package.rule),
                    batch_id=batch.batch_id, fixture_count=len(entry.package.tests),
                )
                for batch in result.batches
                for entry in batch.rules
            )
        case RulesDedupArtifact(result=result):
            return PackagedRuleRow, (
                PackagedRuleRow(**_rule_fields(package.rule), fixture_count=len(package.tests))
                for package in result.packages
            )
        case FinalizedRules(bundle=bundle, bundle_sha256=digest):
            return FinalizedRuleRow, (
                FinalizedRuleRow(**_rule_fields(rule), when=rule.when, bundle_sha256=digest) for rule in bundle.rules
            )
        case _:
            assert_never(value)
