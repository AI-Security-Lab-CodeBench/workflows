"""Standard-library taxonomy reader for inert standalone tooling."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypeAlias

DEFAULT_CATEGORY_CONFIG: Final = (
    Path(__file__).resolve().parents[1] / "business-configs/categories/cat3/category.json"
)
JsonValue: TypeAlias = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True, slots=True)
class Risk:
    id: str
    name: str
    patterns: tuple[str, ...]
    shell_routes: tuple[tuple[str, str], ...]

    @property
    def canonical(self) -> str:
        return f"{self.id} {self.name}"


@dataclass(frozen=True, slots=True)
class Taxonomy:
    category_id: str
    category_name: str
    risks: tuple[Risk, ...]
    description_language: str

    @property
    def canonical(self) -> str:
        return f"{self.category_id} {self.category_name}"

    @property
    def risk_names(self) -> tuple[str, ...]:
        return tuple(risk.canonical for risk in self.risks)

    @property
    def risk_ids(self) -> tuple[str, ...]:
        return tuple(risk.id for risk in self.risks)

    def matching_shell(self, utility: str, arguments: list[str]) -> tuple[str, ...]:
        normalized = " ".join([utility, *arguments])
        return tuple(
            risk.id
            for risk in self.risks
            if (
                any(
                    tool == utility and re.search(pattern, " ".join(arguments))
                    for tool, pattern in risk.shell_routes
                )
                if risk.shell_routes
                else any(re.search(pattern, normalized) for pattern in risk.patterns)
            )
        )


class CategoryConfigError(ValueError):
    pass


def category_document(path: Path) -> tuple[Path, dict[str, JsonValue]]:
    document: JsonValue = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise CategoryConfigError("Expected an initial category configuration")
    reference = document.get("business_config")
    if reference is not None:
        if document.get("rules") is not None or not isinstance(reference, str) or not reference.strip():
            raise CategoryConfigError("Choose exactly one of rules or business_config")
        return category_document((path.parent / reference).resolve())
    return path, document


def business_paths(path: Path = DEFAULT_CATEGORY_CONFIG) -> tuple[Path, ...]:
    path, document = category_document(path)
    paths = [path]
    for field in ("base_prompt_file", "instructions_file", "examples_file"):
        reference = document.get(field)
        if not isinstance(reference, str) or not reference.strip():
            raise CategoryConfigError(f"Missing business file reference: {field}")
        paths.append((path.parent / reference).resolve())
    return tuple(paths)


def load_labeling_prompt(commands: list[str], path: Path = DEFAULT_CATEGORY_CONFIG) -> str:
    path, document = category_document(path)
    _, base, instructions, examples_path = business_paths(path)
    texts = [file.read_text(encoding="utf-8") for file in (base, instructions)]
    if any(not text.strip() for text in texts):
        raise CategoryConfigError("Empty labeling instructions")
    taxonomy = load_taxonomy(path)
    allowed = {
        risk.id for risk in taxonomy.risks
        if any(re.search(pattern, command) for pattern in risk.patterns for command in commands)
    }
    examples: JsonValue = json.loads(examples_path.read_text(encoding="utf-8"))
    if not isinstance(examples, list):
        raise CategoryConfigError("Expected example families")
    selected = []
    for family in examples:
        if not isinstance(family, dict) or not isinstance(family.get("risk_id"), str):
            raise CategoryConfigError("Invalid example family")
        if family["risk_id"] in allowed:
            selected.append(family)
    return "\n\n".join([
        *texts,
        json.dumps(document["rules"], ensure_ascii=False, allow_nan=False),
        json.dumps({"examples": selected}, ensure_ascii=False, allow_nan=False),
    ])


def load_taxonomy(path: Path = DEFAULT_CATEGORY_CONFIG) -> Taxonomy:
    """Read category-owned taxonomy without opening collection source manifests."""
    _, document = category_document(path)
    rules = document.get("rules")
    if not isinstance(rules, dict):
        raise CategoryConfigError("Missing category rules")
    category_id, category_name = rules.get("category_id"), rules.get("category_name")
    if not isinstance(category_id, str) or not re.fullmatch(r"CAT-[1-9]\d*", category_id):
        raise CategoryConfigError("Invalid category identifier")
    if not isinstance(category_name, str) or not category_name.strip():
        raise CategoryConfigError("Missing category name")
    language = rules.get("description_language")
    if not isinstance(language, str) or not language.strip():
        raise CategoryConfigError("Missing description language")
    raw_risks = rules.get("risks")
    if not isinstance(raw_risks, list):
        raise CategoryConfigError("Missing risk taxonomy")
    risks: list[Risk] = []
    for item in raw_risks:
        if not isinstance(item, dict):
            raise CategoryConfigError("Expected a risk configuration")
        identifier, name = item.get("id"), item.get("name")
        if not isinstance(identifier, str) or not re.fullmatch(
            re.escape(category_id) + r"\.[1-9]\d*", identifier
        ):
            raise CategoryConfigError("Risk IDs must use the category prefix")
        if not isinstance(name, str) or not name.strip():
            raise CategoryConfigError("Missing risk name")
        raw_patterns = item.get("command_patterns")
        routes = item.get("shell_routes", {})
        if not isinstance(raw_patterns, list) or not raw_patterns or not isinstance(routes, dict):
            raise CategoryConfigError("Invalid command routing patterns")
        patterns: list[str] = []
        for pattern in raw_patterns:
            if not isinstance(pattern, str) or not pattern.strip():
                raise CategoryConfigError("Invalid command routing pattern")
            re.compile(pattern)
            patterns.append(pattern)
        shell_routes: list[tuple[str, str]] = []
        for tool, pattern in routes.items():
            if not tool.strip() or not isinstance(pattern, str) or not pattern.strip():
                raise CategoryConfigError("Invalid shell routing pattern")
            re.compile(pattern)
            shell_routes.append((tool, pattern))
        risks.append(Risk(identifier, name, tuple(patterns), tuple(shell_routes)))
    if (
        not risks
        or len({risk.id for risk in risks}) != len(risks)
        or len({risk.name for risk in risks}) != len(risks)
    ):
        raise CategoryConfigError("Missing or duplicate taxonomy identifiers or names")
    return Taxonomy(category_id, category_name, tuple(risks), language)
