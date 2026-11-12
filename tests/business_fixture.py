from __future__ import annotations

import json
from pathlib import Path

from test_dataset_annotation import documentation
from test_dataset_rules import make_candidate, make_label, make_rules

from codebench_pipeline.category_rules import CategoryRules


def write_business(root: Path, rules: CategoryRules | None = None) -> Path:
    rules = rules or make_rules()
    root.mkdir(parents=True, exist_ok=True)
    (root / "base.md").write_text("Generic fixture instructions.", encoding="utf-8")
    (root / "instructions.md").write_text(rules.policy, encoding="utf-8")
    families = []
    for index, risk in enumerate(rules.risks):
        command = "probe inspect example" if index == 0 else "probe publish example"
        candidate = make_candidate(f"example-{index}", risk.id).model_copy(update={"command": command})
        families.append({
            "risk_id": risk.id,
            "operation_family": f"family-{index}",
            "examples": [{
                "candidate": candidate.model_dump(mode="json"),
                "documentation": [doc.model_dump(mode="json") for doc in documentation()],
                "expected": make_label(candidate, rules).model_dump(mode="json", by_alias=True),
            }],
        })
    (root / "examples.json").write_text(json.dumps(families), encoding="utf-8")
    path = root / "category.json"
    path.write_text(
        json.dumps({
            "rules": rules.model_dump(mode="json", exclude={"policy", "base_prompt", "examples"}),
            "base_prompt_file": "base.md",
            "instructions_file": "instructions.md",
            "examples_file": "examples.json",
        }),
        encoding="utf-8",
    )
    return path
