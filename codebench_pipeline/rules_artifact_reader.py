from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypedDict, assert_never

import pandas as pd
from pydantic import ConfigDict, JsonValue, TypeAdapter, ValidationError

from codebench_pipeline.artifact_schemas import ArtifactSchema
from codebench_pipeline.config_assets import mapping_key
from codebench_pipeline.data_view_models import DatasetViewError
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.models import Digest
from codebench_pipeline.object_store import sha256
from codebench_pipeline.rules_artifacts import FinalizedRules, RulesDedupArtifact
from codebench_pipeline.rules_contracts import Category, NativeRule
from codebench_pipeline.rules_learning import CaptureResult
from codebench_pipeline.rules_settings import (
    RULES_REGISTRATION,
    AgentSettings,
    LearningPins,
    RegisteredRules,
    RulesEvalSettings,
    RulesFinalizeSettings,
    RulesRuntime,
)
from codebench_pipeline.storage import IntegrityError, open_directory, open_regular_file
from codebench_pipeline.table_query import QueryModel, TablePage, TableQuery, query_table


class CaptureSettingsProjection(QueryModel):
    """Read both merged-train and category-source capture settings."""

    model_config = ConfigDict(extra="ignore")
    category: Category
    runtime: RulesRuntime
    agent: AgentSettings
    pins: LearningPins | None = None
    guide_sha256: Digest | None = None
    sources_digest: Digest | None = None


class DedupSettingsProjection(QueryModel):
    model_config = ConfigDict(extra="ignore")
    pins: LearningPins
    runtime: RulesRuntime
    agent: AgentSettings
    category_sources_digests: dict[Category, Digest] | None = None


class CaptureProjection(QueryModel):
    model_config = ConfigDict(extra="ignore")
    result: CaptureResult


type RulesOutput = (
    RegisteredRules | CaptureSettingsProjection | DedupSettingsProjection | RulesFinalizeSettings | RulesEvalSettings
    | CaptureProjection | RulesDedupArtifact | FinalizedRules
)

RULES_RUN_CONTRACTS: Final[dict[tuple[str, ...], type[RulesOutput]]] = {
    tuple(RULES_REGISTRATION.path): RegisteredRules,
    tuple(mapping_key("rules_capture").path): CaptureSettingsProjection,
    tuple(mapping_key("rules_dedup").path): DedupSettingsProjection,
    tuple(mapping_key("rules_finalize").path): RulesFinalizeSettings,
    tuple(mapping_key("rules_eval").path): RulesEvalSettings,
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
        rule_id=rule.id, scope=rule.scope, category=labels.category, risk=labels.risk,
        severity=labels.severity, action=labels.action,
    )


class RuleRow(QueryModel):
    rule_id: str
    scope: str
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
        case RegisteredRules(registration=settings, digest=digest):
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
        case DedupSettingsProjection() | RulesFinalizeSettings() | RulesEvalSettings():
            test = value.test if isinstance(value, RulesEvalSettings) else None
            dedup = value if isinstance(value, DedupSettingsProjection) else None
            return RulesConfigurationRow, (
                RulesConfigurationRow(
                    category=category, train_digest=value.pins.train.digest, train_rows=value.pins.train.row_count,
                    test_digest=test.digest if test is not None else None,
                    test_rows=test.row_count if test is not None else None,
                    runtime_version=value.runtime.version, guide_sha256=value.pins.guide_sha256,
                    sources_digest=dedup.category_sources_digests.get(category)
                    if dedup is not None and dedup.category_sources_digests is not None else None,
                    inference_digest=dedup.agent.inference_digest if dedup is not None else None,
                )
                for category in value.pins.categories
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
