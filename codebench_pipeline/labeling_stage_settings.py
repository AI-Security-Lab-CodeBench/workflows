from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Annotated, Self

from pydantic import Field

from codebench_pipeline.category_rules import (
    CategoryRules,
    ClassName,
    ClassRule,
    CompiledExamples,
    RiskRule,
    SeverityName,
    SeverityRule,
    StrictModel,
    Text,
)
from codebench_pipeline.collection_config import DeclaredCollection, KeenableSnippetDeclaration
from codebench_pipeline.keenable_policy import KeenableSnippetSettings, KeenableSourceAccess
from codebench_pipeline.models import Digest
from codebench_pipeline.repository_branch_config import RepositoryAcquisitionConfig
from codebench_pipeline.repository_config import (
    Commit,
    MaximumFileBytes,
    RepositoryBranch,
    RepositoryId,
    RepositoryLicense,
    RepositoryName,
    RepositoryOrigin,
    RightsFile,
)
from codebench_pipeline.storage import fingerprint
from codebench_pipeline.typed_config import ConfigArtifact


class RiskRoute(StrictModel):
    id: Annotated[str, Field(pattern=r"^CAT-[1-9]\d*\.[1-9]\d*$")]
    command_patterns: Annotated[tuple[Text, ...], Field(min_length=1)]

    @classmethod
    def from_rule(cls, rule: RiskRule) -> Self:
        return cls(id=rule.id, command_patterns=tuple(rule.command_patterns))


class ProposalRules(ConfigArtifact):
    category_id: Annotated[str, Field(pattern=r"^CAT-[1-9]\d*$")]
    risks: Annotated[tuple[RiskRoute, ...], Field(min_length=1)]
    classes: Annotated[tuple[ClassName, ...], Field(min_length=1)]
    severities: Annotated[tuple[SeverityName, ...], Field(min_length=1)]

    @classmethod
    def from_rules(cls, rules: CategoryRules) -> Self:
        return cls(
            category_id=rules.category_id,
            risks=tuple(RiskRoute.from_rule(risk) for risk in rules.risks),
            classes=tuple(rule.name for rule in rules.classes),
            severities=tuple(rule.name for rule in rules.severities),
        )


class LabelingRisk(RiskRoute):
    name: Text
    definition: Text
    guidance: Text

    @classmethod
    def from_rule(cls, rule: RiskRule) -> Self:
        return cls(
            id=rule.id,
            command_patterns=tuple(rule.command_patterns),
            name=rule.name,
            definition=rule.definition,
            guidance=rule.guidance,
        )


class LabelingRules(StrictModel):
    category_id: Annotated[str, Field(pattern=r"^CAT-[1-9]\d*$")]
    category_name: Text
    version: Text
    risks: Annotated[tuple[LabelingRisk, ...], Field(min_length=1)]
    classes: Annotated[tuple[ClassRule, ...], Field(min_length=1)]
    severities: Annotated[tuple[SeverityRule, ...], Field(min_length=1)]
    description_language: Text
    policy: Text
    base_prompt: str
    examples: tuple[CompiledExamples, ...]

    @classmethod
    def from_rules(cls, rules: CategoryRules) -> Self:
        return cls(
            category_id=rules.category_id,
            category_name=rules.category_name,
            version=rules.version,
            risks=tuple(LabelingRisk.from_rule(risk) for risk in rules.risks),
            classes=tuple(rules.classes),
            severities=tuple(rules.severities),
            description_language=rules.description_language,
            policy=rules.policy,
            base_prompt=rules.base_prompt,
            examples=tuple(rules.examples),
        )


class RepositoryEvidenceOrigin(StrictModel):
    origin_id: RepositoryId
    repository: RepositoryName
    commit: Commit
    tree_digest: Digest
    license_id: RepositoryLicense
    rights: Annotated[tuple[RightsFile, ...], Field(min_length=1)]
    attribution: Text
    origin_sha256: Digest

    @classmethod
    def from_origin(cls, origin: RepositoryOrigin) -> Self:
        return cls(
            origin_id=origin.origin_id,
            repository=origin.repository,
            commit=origin.commit,
            tree_digest=origin.tree_digest,
            license_id=origin.license_id,
            rights=(origin.license, *origin.notices),
            attribution=origin.attribution,
            origin_sha256=fingerprint(origin.model_dump(mode="json")),
        )


class RepositoryEvidenceSettings(StrictModel):
    receipt_config_digest: Digest
    maximum_file_bytes: MaximumFileBytes
    origins: Annotated[tuple[RepositoryEvidenceOrigin, ...], Field(min_length=1)]

    @classmethod
    def project(cls, config: RepositoryAcquisitionConfig, branch: RepositoryBranch) -> Self:
        return cls(
            receipt_config_digest=config.branch_digest(branch),
            maximum_file_bytes=config.maximum_file_bytes,
            origins=tuple(RepositoryEvidenceOrigin.from_origin(origin) for origin in config.origins(branch)),
        )


class LabelingSources(ConfigArtifact):
    existing_datasets: RepositoryEvidenceSettings
    documentation_crawl: RepositoryEvidenceSettings
    keenable_use_cases: KeenableSourceAccess | KeenableSnippetSettings

    @classmethod
    def from_collection(cls, collection: DeclaredCollection) -> Self:
        collection.require_current_declarations()
        declaration = collection.keenable_declaration
        return cls(
            existing_datasets=RepositoryEvidenceSettings.project(
                collection.repository_config("dataset"), "dataset"
            ),
            documentation_crawl=RepositoryEvidenceSettings.project(
                collection.repository_config("documentation"), "documentation"
            ),
            keenable_use_cases=(
                declaration.search
                if isinstance(declaration, KeenableSnippetDeclaration)
                else KeenableSourceAccess.project(declaration)
            ),
        )


def matching_risks(command: str, risks: Sequence[RiskRule | RiskRoute]) -> list[str]:
    return [
        risk.id for risk in risks if any(re.search(pattern, command) for pattern in risk.command_patterns)
    ]
