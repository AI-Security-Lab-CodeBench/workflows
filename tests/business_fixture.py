from __future__ import annotations

import json
from pathlib import Path

from pydantic import JsonValue
from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.category_rules import CategoryRules, SeverityName


def write_business(
    root: Path,
    rules: CategoryRules | None = None,
    *,
    allowed_severities: list[SeverityName] | None = None,
) -> Path:
    rules = rules or make_rules()
    shared = root / "labeling"
    shared.mkdir(parents=True, exist_ok=True)
    shared_fields = {"classes", "severities", "description_language"}
    policy_path = shared / "policy.json"
    if not policy_path.exists():
        policy_path.write_text(rules.model_dump_json(include=shared_fields), encoding="utf-8")
        (shared / "instructions.md").write_text("Generic fixture instructions.", encoding="utf-8")

    category = root / "categories" / rules.category_id.lower().replace("-", "")
    category.mkdir(parents=True, exist_ok=True)
    (category / "instructions.md").write_text(rules.policy, encoding="utf-8")
    families: list[JsonValue] = []
    for index, risk in enumerate(rules.risks):
        command = "probe inspect example" if index == 0 else "probe publish example"
        candidate = make_candidate(f"example-{index}", risk.id).model_copy(update={"command": command})
        families.append(
            {
                "risk_id": risk.id,
                "operation_family": f"family-{index}",
                "examples": [
                    {
                        "candidate": candidate.model_dump(mode="json"),
                        "documentation": [doc.model_dump(mode="json") for doc in documentation()],
                        "expected": make_label(candidate, rules).model_dump(mode="json", by_alias=True),
                    }
                ],
            }
        )
    (category / "examples.json").write_text(json.dumps(families), encoding="utf-8")
    category_rules = rules.model_dump(
        mode="json", exclude=shared_fields | {"policy", "base_prompt", "examples"}
    )
    if allowed_severities is not None:
        category_rules["allowed_severities"] = allowed_severities
    path = category / "category.json"
    path.write_text(json.dumps({"rules": category_rules}), encoding="utf-8")
    return path
