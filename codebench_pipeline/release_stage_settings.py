from __future__ import annotations

from typing import Annotated, Self

from pydantic import Field

from codebench_pipeline.category_rules import CategoryRules, ClassName, SeverityName, StrictModel, Text
from codebench_pipeline.dataset_config import ReleaseCap
from codebench_pipeline.grounding_tools import GroundingOrigins
from codebench_pipeline.labeling_stage_settings import LabelingSources, ProposalRules, RiskRoute
from codebench_pipeline.models import Digest
from codebench_pipeline.storage import fingerprint
from codebench_pipeline.typed_config import ConfigArtifact, TypedConfig

CategoryId = Annotated[str, Field(pattern=r"^CAT-[1-9]\d*$")]


class PublicationRisk(RiskRoute):
    name: Text

    @property
    def canonical(self) -> str:
        return f"{self.id} {self.name}"


class PublicationTaxonomy(StrictModel):
    category_id: CategoryId
    category_name: Text
    version: Text
    description_language: Text
    risks: Annotated[tuple[PublicationRisk, ...], Field(min_length=1)]
    classes: Annotated[tuple[ClassName, ...], Field(min_length=1)]
    severities: Annotated[tuple[SeverityName, ...], Field(min_length=1)]

    @classmethod
    def from_rules(cls, rules: CategoryRules) -> Self:
        return cls(
            category_id=rules.category_id,
            category_name=rules.category_name,
            version=rules.version,
            description_language=rules.description_language,
            risks=tuple(
                PublicationRisk(id=risk.id, name=risk.name, command_patterns=tuple(risk.command_patterns))
                for risk in rules.risks
            ),
            classes=tuple(rule.name for rule in rules.classes),
            severities=tuple(rule.name for rule in rules.severities),
        )

    @property
    def canonical(self) -> str:
        return f"{self.category_id} {self.category_name}"

    def label_validation(self) -> ProposalRules:
        return ProposalRules(
            category_id=self.category_id,
            risks=self.risks,
            classes=self.classes,
            severities=self.severities,
        )


class CategoryEligibilityConfig(TypedConfig, ConfigArtifact):
    category_id: CategoryId


class ReviewQueueConfig(ConfigArtifact):
    taxonomy: ProposalRules
    review_columns: Annotated[tuple[Text, ...], Field(min_length=1)]
    limit_per_risk: ReleaseCap

    @classmethod
    def from_rules(cls, rules: CategoryRules, review_columns: tuple[str, ...], limit_per_risk: int) -> Self:
        return cls(
            taxonomy=ProposalRules.from_rules(rules),
            review_columns=review_columns,
            limit_per_risk=limit_per_risk,
        )


class ReleaseGroundingConfig(ConfigArtifact):
    allowed_domains: tuple[str, ...]
    trusted_url_prefixes: tuple[str, ...]

    @classmethod
    def from_grounding(cls, config: GroundingOrigins) -> Self:
        return cls(
            allowed_domains=tuple(config.allowed_domains),
            trusted_url_prefixes=tuple(config.trusted_url_prefixes),
        )

    def verifier_config(self) -> GroundingOrigins:
        return GroundingOrigins(
            allowed_domains=list(self.allowed_domains),
            trusted_url_prefixes=list(self.trusted_url_prefixes),
        )


class DatasetReleaseConfig(ConfigArtifact):
    taxonomy: PublicationTaxonomy
    limit_per_risk: ReleaseCap
    collection: LabelingSources
    grounding: ReleaseGroundingConfig

    @property
    def digest(self) -> Digest:
        return fingerprint(self.model_dump(mode="json", by_alias=True))
