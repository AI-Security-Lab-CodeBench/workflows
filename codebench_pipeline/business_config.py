from __future__ import annotations

from pathlib import Path
from typing import Annotated, Final

from pydantic import Field, TypeAdapter

from codebench_pipeline.category_rules import (
    CategoryRules,
    CategoryTaxonomy,
    CompiledExamples,
    StrictModel,
    Text,
)
from codebench_pipeline.dataset_documentation_models import command_operations
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, routing, validate_label
from codebench_pipeline.models import SemanticSnippet

DEFAULT_BUSINESS_CONFIG: Final = (
    Path(__file__).resolve().parents[1] / "business-configs/categories/cat3/category.json"
)
DEFAULT_BASE_PROMPT: Final = Path(__file__).resolve().parents[1] / "business-configs/labeling-base.md"


class BusinessCategoryConfig(StrictModel):
    rules: CategoryTaxonomy
    base_prompt_file: Text
    instructions_file: Text
    examples_file: Text


class LabelingExample(StrictModel):
    candidate: CandidateRecord
    documentation: Annotated[list[SemanticSnippet], Field(min_length=1)]
    expected: LabelDecision


class ExampleFamily(StrictModel):
    risk_id: Text
    operation_family: Text
    examples: Annotated[list[LabelingExample], Field(min_length=1)]


def read_prompt(path: Path) -> str:
    content = path.read_text(encoding="utf-8")
    if not content.strip():
        raise ValueError(f"Empty labeling instructions: {path}")
    return content


def load_business_rules(path: Path, category_id: str | None = None) -> CategoryRules:
    """Compile only labeling business files, independently of collection manifests."""
    path = path.resolve()
    supplied = BusinessCategoryConfig.model_validate_json(path.read_bytes())
    if category_id is not None and supplied.rules.category_id != category_id:
        raise ValueError("Business configuration cannot change the registered category")
    parent = path.parent
    rules = CategoryRules(
        **supplied.rules.model_dump(),
        policy=read_prompt(parent / supplied.instructions_file),
        base_prompt=read_prompt(parent / supplied.base_prompt_file),
    )
    families = TypeAdapter(list[ExampleFamily]).validate_json(
        (parent / supplied.examples_file).read_bytes()
    )
    known = {risk.id for risk in rules.risks}
    groups: set[tuple[str, str]] = set()
    identities: set[str] = set()
    compiled: list[CompiledExamples] = []
    for family in families:
        key = (family.risk_id, family.operation_family)
        if family.risk_id not in known or key in groups:
            raise ValueError("Foreign risk or duplicate example operation family")
        groups.add(key)
        for example in family.examples:
            candidate = example.candidate
            if candidate.candidate_id in identities:
                raise ValueError("Duplicate example candidate identity")
            identities.add(candidate.candidate_id)
            if family.risk_id not in routing(candidate, rules):
                raise ValueError("Example family risk is outside its candidate routing")
            if example.expected.risk_id != family.risk_id:
                raise ValueError("Expected label differs from its example family risk")
            operations = command_operations(candidate.candidate_id, candidate.util, candidate.command)
            relevant = [
                doc for doc in example.documentation
                if any(
                    operation.matches(doc.utility, subcommand)
                    for operation in operations for subcommand in doc.subcommands
                )
            ]
            if not relevant:
                raise ValueError("Example requires matching command documentation")
            validate_label(candidate, example.expected, rules, {doc.url for doc in relevant})
        compiled.append(
            CompiledExamples(
                risk_id=family.risk_id,
                operation_family=family.operation_family,
                examples_json=TypeAdapter(list[LabelingExample]).dump_json(
                    family.examples, by_alias=True
                ).decode("utf-8"),
            )
        )
    if {family.risk_id for family in families} != known:
        raise ValueError("Examples must cover every category risk with operation families")
    return rules.model_copy(update={"examples": compiled})
