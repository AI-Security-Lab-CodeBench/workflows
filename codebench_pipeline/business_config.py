from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Final

from pydantic import Field, RootModel, TypeAdapter

from codebench_pipeline.category_rules import (
    AuthoredCategoryRules,
    CategoryRules,
    CategoryTaxonomy,
    CompiledExamples,
    SharedLabelingPolicy,
    StrictModel,
    Text,
)
from codebench_pipeline.dataset_documentation_models import command_operations
from codebench_pipeline.dataset_records import OTHER_RISK_ID, CandidateRecord, LabelDecision, validate_label
from codebench_pipeline.models import SemanticSnippet
from codebench_pipeline.storage import fingerprint

DEFAULT_BUSINESS_CONFIG: Final = (
    Path(__file__).resolve().parents[1] / "business-configs/categories/cat3/category.json"
)
DEFAULT_BASE_PROMPT: Final = DEFAULT_BUSINESS_CONFIG.parents[2] / "labeling/instructions.md"


class BusinessCategoryConfig(StrictModel):
    rules: CategoryTaxonomy
    base_prompt_file: Text
    instructions_file: Text
    examples_file: Text


class SplitBusinessCategoryConfig(StrictModel):
    rules: AuthoredCategoryRules


class BusinessCategoryDeclaration(RootModel[BusinessCategoryConfig | SplitBusinessCategoryConfig]):
    pass


class LabelingExample(StrictModel):
    candidate: CandidateRecord
    documentation: Annotated[list[SemanticSnippet], Field(min_length=1)]
    expected: LabelDecision


class ExampleFamily(StrictModel):
    risk_id: Text
    operation_family: Text
    examples: Annotated[list[LabelingExample], Field(min_length=1)]


def read_prompt(path: Path, read_bytes: Callable[[Path], bytes]) -> str:
    content = read_bytes(path).decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    if not content.strip():
        raise ValueError(f"Empty labeling instructions: {path}")
    return content


def load_business_rules(
    path: Path,
    category_id: str | None = None,
    *,
    read_bytes: Callable[[Path], bytes] = Path.read_bytes,
) -> CategoryRules:
    """Compile only labeling business files, independently of collection manifests."""
    return compile_business_rules(path.resolve(), category_id, read_bytes)


def compile_business_rules(
    path: Path, category_id: str | None, read_bytes: Callable[[Path], bytes]
) -> CategoryRules:
    supplied = BusinessCategoryDeclaration.model_validate_json(read_bytes(path)).root
    if category_id is not None and supplied.rules.category_id != category_id:
        raise ValueError("Business configuration cannot change the registered category")
    parent = path.parent
    split_policy: tuple[SharedLabelingPolicy, AuthoredCategoryRules] | None = None
    match supplied:
        case BusinessCategoryConfig():
            rules = CategoryRules(
                category_id=supplied.rules.category_id,
                category_name=supplied.rules.category_name,
                version=supplied.rules.version,
                risks=supplied.rules.risks,
                classes=supplied.rules.classes,
                severities=supplied.rules.severities,
                description_language=supplied.rules.description_language,
                policy=read_prompt(parent / supplied.instructions_file, read_bytes),
                base_prompt=read_prompt(parent / supplied.base_prompt_file, read_bytes),
            )
            examples_path = parent / supplied.examples_file
        case SplitBusinessCategoryConfig():
            labeling = path.parents[2] / "labeling"
            shared = SharedLabelingPolicy.model_validate_json(read_bytes(labeling / "policy.json"))
            split_policy = shared, supplied.rules
            allowed = set(supplied.rules.allowed_severities)
            rules = CategoryRules(
                category_id=supplied.rules.category_id,
                category_name=supplied.rules.category_name,
                version=supplied.rules.version,
                risks=supplied.rules.risks,
                classes=shared.classes,
                severities=[rule for rule in shared.severities if not allowed or rule.name in allowed],
                description_language=shared.description_language,
                policy=read_prompt(parent / "instructions.md", read_bytes),
                base_prompt=read_prompt(labeling / "instructions.md", read_bytes),
            )
            examples_path = parent / "examples.json"
    families = TypeAdapter(list[ExampleFamily]).validate_json(read_bytes(examples_path))
    known = {risk.id for risk in rules.risks}
    example_risks = known | {OTHER_RISK_ID}
    groups: set[tuple[str, str]] = set()
    identities: set[str] = set()
    covered: set[str] = set()
    compiled: list[CompiledExamples] = []
    for family in families:
        key = (family.risk_id, family.operation_family)
        if family.risk_id not in example_risks or key in groups:
            raise ValueError("Foreign risk or duplicate example operation family")
        groups.add(key)
        for example in family.examples:
            candidate = example.candidate
            if candidate.candidate_id in identities:
                raise ValueError("Duplicate example candidate identity")
            identities.add(candidate.candidate_id)
            if example.expected.risk_id not in {family.risk_id, OTHER_RISK_ID}:
                raise ValueError("Expected label differs from its example family risk")
            operations = command_operations(candidate.candidate_id, candidate.util, candidate.command)
            relevant = [
                doc
                for doc in example.documentation
                if any(
                    operation.matches(doc.utility, subcommand)
                    for operation in operations
                    for subcommand in doc.subcommands
                )
            ]
            if not relevant:
                raise ValueError("Example requires matching command documentation")
            validate_label(candidate, example.expected, rules, {doc.url for doc in relevant})
            covered.add(example.expected.risk_id)
        compiled.append(
            CompiledExamples(
                risk_id=family.risk_id,
                operation_family=family.operation_family,
                examples_json=TypeAdapter(list[LabelingExample])
                .dump_json(family.examples, by_alias=True)
                .decode("utf-8"),
            )
        )
    if not known <= covered:
        raise ValueError("Examples must cover every category risk with operation families")
    if split_policy is None:
        return rules.model_copy(update={"examples": compiled})
    shared, authored = split_policy
    return rules.model_copy(
        update={
            "examples": compiled,
            "shared_policy_digest": fingerprint(
                {"policy": shared.model_dump(mode="json"), "instructions": rules.base_prompt}
            ),
            "category_policy_digest": fingerprint(
                {
                    "rules": authored.model_dump(mode="json", exclude_unset=True),
                    "instructions": rules.policy,
                    "examples": TypeAdapter(list[CompiledExamples]).dump_python(compiled, mode="json"),
                }
            ),
        }
    )
