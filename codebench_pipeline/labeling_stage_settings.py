from __future__ import annotations

import re
from collections.abc import Sequence
from hashlib import sha256
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, TypeAdapter

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
from codebench_pipeline.keenable_policy import KeenableSourceAccess
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


class DemonstrationCommand(StrictModel):
    command: Text
    util: Text
    context: str


class DemonstrationPassage(StrictModel):
    utility: Text
    subcommands: tuple[str, ...]
    url: Text
    snippet: Text
    version_limits: Text


type DemonstrationConfidence = Literal["low", "medium", "high"]


class DemonstrationJudgment(StrictModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    decision: Literal["accept", "needs_review", "reject"]
    risk_id: Text
    severity: SeverityName
    class_: ClassName = Field(alias="class")
    description: Text
    category_confidence: DemonstrationConfidence
    risk_confidence: DemonstrationConfidence
    severity_confidence: DemonstrationConfidence
    class_confidence: DemonstrationConfidence
    rationale: Text
    documentation_urls: tuple[str, ...]


class LabelingDemonstration(StrictModel):
    candidate: DemonstrationCommand
    documentation: tuple[DemonstrationPassage, ...]
    expected: DemonstrationJudgment


class LabelingExampleFamily(StrictModel):
    risk_id: Text
    operation_family: Text
    examples: Annotated[tuple[LabelingDemonstration, ...], Field(min_length=1)]

    @classmethod
    def from_compiled(cls, family: CompiledExamples) -> Self:
        from codebench_pipeline.business_config import LabelingExample

        authored = TypeAdapter(tuple[LabelingExample, ...]).validate_json(family.examples_json)
        return cls(
            risk_id=family.risk_id,
            operation_family=family.operation_family,
            examples=tuple(
                LabelingDemonstration(
                    candidate=DemonstrationCommand(
                        command=example.candidate.command,
                        util=example.candidate.util,
                        context=example.candidate.context,
                    ),
                    documentation=tuple(
                        DemonstrationPassage(
                            utility=doc.utility,
                            subcommands=tuple(doc.subcommands),
                            url=doc.url,
                            snippet=doc.snippet,
                            version_limits=doc.version_limits,
                        )
                        for doc in example.documentation
                    ),
                    expected=DemonstrationJudgment.model_validate(
                        {
                            "decision": example.expected.decision,
                            "risk_id": example.expected.risk_id,
                            "severity": example.expected.severity,
                            "class": example.expected.class_,
                            "description": example.expected.description,
                            "category_confidence": example.expected.category_confidence,
                            "risk_confidence": example.expected.risk_confidence,
                            "severity_confidence": example.expected.severity_confidence,
                            "class_confidence": example.expected.class_confidence,
                            "rationale": example.expected.rationale,
                            "documentation_urls": tuple(example.expected.documentation_urls),
                        }
                    ),
                )
                for example in authored
            ),
        )

    @property
    def examples_json(self) -> str:
        return TypeAdapter(tuple[LabelingDemonstration, ...]).dump_json(self.examples, by_alias=True).decode()


class LabelingRules(StrictModel):
    category_id: Annotated[str, Field(pattern=r"^CAT-[1-9]\d*$")]
    category_name: Text
    risks: Annotated[tuple[LabelingRisk, ...], Field(min_length=1)]
    classes: Annotated[tuple[ClassRule, ...], Field(min_length=1)]
    severities: Annotated[tuple[SeverityRule, ...], Field(min_length=1)]
    description_language: Text
    policy: Text
    base_prompt: str
    examples: tuple[LabelingExampleFamily, ...]

    @classmethod
    def from_rules(cls, rules: CategoryRules) -> Self:
        return cls(
            category_id=rules.category_id,
            category_name=rules.category_name,
            risks=tuple(LabelingRisk.from_rule(risk) for risk in rules.risks),
            classes=tuple(rules.classes),
            severities=tuple(rules.severities),
            description_language=rules.description_language,
            policy=rules.policy,
            base_prompt=rules.base_prompt,
            examples=tuple(LabelingExampleFamily.from_compiled(family) for family in rules.examples),
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


class KeenableSnippetEvidence(ConfigArtifact):
    settings_sha256: Digest


class LabelingSources(ConfigArtifact):
    existing_datasets: RepositoryEvidenceSettings
    documentation_crawl: RepositoryEvidenceSettings
    keenable_use_cases: KeenableSourceAccess | KeenableSnippetEvidence

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
                KeenableSnippetEvidence(
                    settings_sha256=sha256(declaration.search.model_dump_json().encode()).hexdigest()
                )
                if isinstance(declaration, KeenableSnippetDeclaration)
                else KeenableSourceAccess.project(declaration)
            ),
        )


def matching_risks(command: str, risks: Sequence[RiskRule | RiskRoute]) -> list[str]:
    return [
        risk.id for risk in risks if any(re.search(pattern, command) for pattern in risk.command_patterns)
    ]
