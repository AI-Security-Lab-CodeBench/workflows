"""Standard-library taxonomy reader for inert standalone tooling."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypeAlias

DEFAULT_CATEGORY_CONFIG: Final = Path(__file__).resolve().parents[1] / "configs/datasets/cat3.json"
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


def load_taxonomy(path: Path = DEFAULT_CATEGORY_CONFIG) -> Taxonomy:
    """Read category-owned rules from the same initial JSON used by dataset builds."""
    document: JsonValue = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise CategoryConfigError("Expected an initial category configuration")
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
