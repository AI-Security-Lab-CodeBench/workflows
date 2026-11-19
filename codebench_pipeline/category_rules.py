from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    SerializerFunctionWrapHandler,
    StringConstraints,
    model_serializer,
    model_validator,
)

from codebench_pipeline.models import Digest
from codebench_pipeline.typed_config import TypedConfig

Text = Annotated[str, StringConstraints(min_length=1, pattern=r"\S")]
ClassName = Literal["allow", "deny", "ask"]
SeverityName = Literal["low", "medium", "high", "critical"]
type SerializedRules = dict[str, JsonValue]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class RuleConfig(TypedConfig):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class RiskRule(RuleConfig):
    id: Annotated[str, Field(pattern=r"^CAT-[1-9]\d*\.[1-9]\d*$")]
    name: Text
    definition: Text
    command_patterns: Annotated[list[Text], Field(min_length=1)]
    shell_routes: dict[str, str] = Field(default_factory=dict)
    guidance: Text

    @model_validator(mode="after")
    def valid_patterns(self) -> RiskRule:
        if any(not tool.strip() or not pattern.strip() for tool, pattern in self.shell_routes.items()):
            raise ValueError("Shell routing utilities and patterns must be nonempty")
        if len(set(self.command_patterns)) != len(self.command_patterns):
            raise ValueError("Duplicate command patterns")
        for pattern in (*self.command_patterns, *self.shell_routes.values()):
            try:
                re.compile(pattern)
            except re.error as error:
                raise ValueError(f"Invalid command pattern: {pattern}") from error
        return self

    @property
    def canonical(self) -> str:
        return f"{self.id} {self.name}"


class ClassRule(RuleConfig):
    name: ClassName
    definition: Text


class SeverityRule(RuleConfig):
    name: SeverityName
    definition: Text


class CompiledExamples(RuleConfig):
    risk_id: Text
    operation_family: Text
    examples_json: Text


class SharedLabelingPolicy(StrictModel):
    classes: Annotated[list[ClassRule], Field(min_length=3, max_length=3)]
    severities: Annotated[list[SeverityRule], Field(min_length=4, max_length=4)]
    description_language: Text

    @model_validator(mode="after")
    def valid_policy(self) -> SharedLabelingPolicy:
        if len({rule.name for rule in self.classes}) != len(self.classes):
            raise ValueError("Shared policy must define every class exactly once")
        if len({rule.name for rule in self.severities}) != len(self.severities):
            raise ValueError("Shared policy must define every severity exactly once")
        return self


class CategoryDefinition(StrictModel):
    category_id: Annotated[str, Field(pattern=r"^CAT-[1-9]\d*$")]
    category_name: Text
    version: Text
    risks: Annotated[list[RiskRule], Field(min_length=1)]

    @model_validator(mode="after")
    def valid_risks(self) -> CategoryDefinition:
        if len({risk.id for risk in self.risks}) != len(self.risks) or len(
            {risk.name for risk in self.risks}
        ) != len(self.risks):
            raise ValueError("Duplicate taxonomy identifiers or names")
        if any(not risk.id.startswith(f"{self.category_id}.") for risk in self.risks):
            raise ValueError("Risk IDs must use the category prefix")
        return self

    @property
    def canonical(self) -> str:
        return f"{self.category_id} {self.category_name}"

    def matching_risks(self, command: str) -> list[str]:
        return [
            risk.id
            for risk in self.risks
            if any(re.search(pattern, command) for pattern in risk.command_patterns)
        ]


class AuthoredCategoryRules(CategoryDefinition):
    allowed_severities: Annotated[list[SeverityName], Field(min_length=1)] = Field(default_factory=list)

    @model_validator(mode="after")
    def valid_allowed_severities(self) -> AuthoredCategoryRules:
        if len(set(self.allowed_severities)) != len(self.allowed_severities):
            raise ValueError("Duplicate allowed severities")
        return self


class CategoryTaxonomy(CategoryDefinition):
    classes: Annotated[list[ClassRule], Field(min_length=1)]
    severities: Annotated[list[SeverityRule], Field(min_length=1)]
    description_language: Text

    @model_validator(mode="after")
    def valid_taxonomy(self) -> CategoryTaxonomy:
        for values in ([rule.name for rule in self.classes], [rule.name for rule in self.severities]):
            if len(values) != len(set(values)):
                raise ValueError("Duplicate taxonomy identifiers or names")
        if {rule.name for rule in self.classes} != {"allow", "deny", "ask"}:
            raise ValueError("All v0 classes must be supported")
        return self


class CategoryRules(CategoryTaxonomy):
    policy: Text
    base_prompt: str = ""
    examples: list[CompiledExamples] = Field(default_factory=list)
    shared_policy_digest: Digest | None = None
    category_policy_digest: Digest | None = None

    @model_validator(mode="after")
    def paired_policy_digests(self) -> CategoryRules:
        if (self.shared_policy_digest is None) != (self.category_policy_digest is None):
            raise ValueError("Shared and category policy digests must be supplied together")
        return self

    @model_serializer(mode="wrap")
    def serialize_rules(self, handler: SerializerFunctionWrapHandler) -> SerializedRules:
        """Keep the historical serialized shape when no separate identities were recorded."""
        data: SerializedRules = handler(self)
        if self.shared_policy_digest is None:
            data.pop("shared_policy_digest", None)
            data.pop("category_policy_digest", None)
        return data


class CategoryRulesConfig(CategoryRules, TypedConfig):
    """Dagster launch configuration; materializes a plain CategoryRules value."""
