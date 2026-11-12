from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

import pandera.errors
import polars as pl
from dagster import Config, DagsterInvalidInvocationError
from pydantic import JsonValue, ValidationError

from codebench_pipeline.category_rules import CategoryRules
from codebench_pipeline.dataset_export import prepare_dataset, publish_dataset, table_schema
from codebench_pipeline.dataset_records import CandidateRecord, LabelDecision, ReviewDecision


def make_rules(category: str = "CAT-8") -> CategoryRules:
    return CategoryRules.model_validate(
        {
            "category_id": category,
            "category_name": "Example operations",
            "version": "v0",
            "policy": "Visible evidence only. Never infer authorization. Label records independently.",
            "risks": [
                {
                    "id": f"{category}.1",
                    "name": "Inspection and erasure",
                    "definition": "Probe operations",
                    "command_patterns": [r"\bprobe\s+(?:inspect|erase)\b"],
                    "guidance": "Inspect flags",
                },
                {
                    "id": f"{category}.2",
                    "name": "Publication",
                    "definition": "Probe publication",
                    "command_patterns": [r"\bprobe\s+publish\b"],
                    "guidance": "Inspect destination",
                },
            ],
            "classes": [
                {"name": name, "definition": f"Meaning of {name}"} for name in ("allow", "deny", "ask")
            ],
            "severities": [
                {"name": name, "definition": f"Impact of {name}"}
                for name in ("low", "medium", "high", "critical")
            ],
            "description_language": "English",
        }
    )


def make_candidate(identity: str = "one", risk: str = "CAT-8.1") -> CandidateRecord:
    return CandidateRecord(
        candidate_id=identity,
        command="probe inspect example",
        util="probe",
        context="An example record.",
        source_id="maintainer-reference",
        source_url="https://example.org/source#L10",
        source_position="lines 10-12",
        transformations=[],
        risk_candidates=[risk],
    )


def make_label(candidate: CandidateRecord, rules: CategoryRules) -> LabelDecision:
    return LabelDecision.model_validate(
        {
            "candidate_id": candidate.candidate_id,
            "decision": "accept",
            "category_id": rules.category_id,
            "risk_id": candidate.risk_candidates[0],
            "severity": "low",
            "class": "allow",
            "description": "Inspects the named example.",
            "util": candidate.util,
            "category_confidence": "high",
            "risk_confidence": "high",
            "severity_confidence": "high",
            "class_confidence": "high",
            "rationale": "The documented operation only inspects metadata.",
            "documentation_urls": ["https://example.org/docs"],
        }
    )


def make_review(candidate: CandidateRecord) -> ReviewDecision:
    return ReviewDecision(
        candidate_id=candidate.candidate_id,
        verdict="approve",
        confidence="high",
        rationale="Evidence checked.",
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


class CategoryRuleTests(unittest.TestCase):
    def test_arbitrary_categories_route_and_render_without_code_changes(self) -> None:
        for category in ("CAT-2", "CAT-81"):
            with self.subTest(category=category):
                rules = make_rules(category)
                from codebench_pipeline.category_rules import CategoryRulesConfig

                configuration = CategoryRulesConfig.model_validate_json(rules.model_dump_json())
                self.assertIsInstance(configuration, Config)
                self.assertEqual(CategoryRules.model_validate_json(configuration.model_dump_json()), rules)
                self.assertEqual(rules.canonical, f"{category} Example operations")
                self.assertEqual(rules.matching_risks("probe inspect example"), [f"{category}.1"])
                self.assertEqual(rules.matching_risks("probe publish example"), [f"{category}.2"])
                self.assertEqual(rules.matching_risks("unrelated operation"), [])

    def test_invalid_taxonomy_and_unknown_fields_are_rejected(self) -> None:
        alterations: list[dict[str, JsonValue]] = [
            {"category_id": "other"},
            {"unexpected": True},
            {"policy": " "},
            {"version": ""},
            {"risks": []},
            {"classes": [{"name": "allow", "definition": "Only allow"}]},
            {"severities": [{"name": "extreme", "definition": "Invalid"}]},
        ]
        original = make_rules().model_dump(mode="json")
        for alteration in alterations:
            with self.subTest(alteration=alteration), self.assertRaises(ValidationError):
                CategoryRules.model_validate({**original, **alteration})
        risk_updates: list[dict[str, JsonValue]] = [
            {"id": "CAT-9.1"},
            {"command_patterns": []},
            {"command_patterns": ["["]},
            {"command_patterns": [" "]},
            {"command_patterns": ["probe", "probe"]},
            {"extra": "forbidden"},
        ]
        for risk_update in risk_updates:
            rules = make_rules()
            risks = [risk.model_dump() for risk in rules.risks]
            risks[0].update(risk_update)
            with self.subTest(risk_update=risk_update), self.assertRaises(ValidationError):
                CategoryRules.model_validate({**original, "risks": risks})
        rules = make_rules()
        for field, entries in (
            ("risks", rules.risks),
            ("classes", rules.classes),
            ("severities", rules.severities),
        ):
            items = [item.model_dump() for item in entries]
            with self.subTest(field=field), self.assertRaises(ValidationError):
                CategoryRules.model_validate({**original, field: items + [items[0]]})

    def test_config_is_frozen_and_scalars_are_strict(self) -> None:
        rules = make_rules()
        with self.assertRaises((ValidationError, DagsterInvalidInvocationError)):
            rules.category_name = "Changed"
        with self.assertRaises(ValidationError):
            CategoryRules.model_validate({**rules.model_dump(), "version": 7})


class DatasetExportTests(unittest.TestCase):
    def test_duplicate_text_survives_with_exact_source_positions(self) -> None:
        rules = make_rules()
        candidates = [make_candidate("one"), make_candidate("two")]
        candidates[0] = candidates[0].model_copy(update={"command": 'probe inspect "a,\nb"'})
        candidates[1] = candidates[1].model_copy(update={"command": candidates[0].command})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = publish_dataset(
                candidates,
                [make_label(c, rules) for c in candidates],
                rules,
                root,
                10,
            )
            rows = read_csv(root / "commands.csv")
            self.assertEqual(report["records"], 2)
            self.assertEqual(read_csv(root / "review/pending.csv"), [])
            self.assertEqual([row["human_reviewed"] for row in rows], ["false", "false"])
            self.assertEqual([row["command"] for row in rows], [c.command for c in candidates])
            self.assertEqual(rows[0]["category"], rules.canonical)
            self.assertEqual(rows[0]["source"], candidates[0].source_id)
            provenance = [
                json.loads(line) for line in (root / "review/reviewed.jsonl").read_text().splitlines()
            ]
            self.assertEqual([row["candidate_id"] for row in provenance], ["one", "two"])
            self.assertEqual(provenance[0]["source_url"], candidates[0].source_url)
            self.assertEqual(provenance[0]["source_position"], candidates[0].source_position)
            self.assertEqual(len(read_csv(root / "review/reviewed.csv")), 2)

    def test_caps_are_independent_and_decisions_remain_flat_and_traceable(self) -> None:
        rules = make_rules()
        candidates = [make_candidate(str(number)) for number in range(5)]
        candidates[2] = candidates[2].model_copy(
            update={"command": "probe publish example", "risk_candidates": ["CAT-8.2"]}
        )
        labels = [make_label(c, rules) for c in candidates]
        labels[3] = labels[3].model_copy(update={"decision": "needs_review", "rationale": "Missing flags."})
        labels[4] = labels[4].model_copy(update={"decision": "reject", "rationale": "Check version."})
        prepared = prepare_dataset(
            candidates,
            labels,
            rules,
            1,
            approved_ids=frozenset(candidate.candidate_id for candidate in candidates),
        )
        self.assertEqual(prepared.statistics["records"], 2)
        pending = list(csv.DictReader(io.StringIO(prepared.files["review/pending.csv"])))
        self.assertEqual([row["candidate_id"] for row in pending], ["1", "3", "4"])
        self.assertEqual(pending[1]["label_rationale"], "Missing flags.")
        self.assertEqual(pending[1]["label_decision"], "needs_review")
        self.assertEqual(pending[1]["evidence_record"], "4")
        self.assertEqual(pending[2]["label_rationale"], "Check version.")
        self.assertEqual(
            {row["risk"] for row in prepared.commands},
            {risk.canonical for risk in rules.risks},
        )

    def test_identity_mismatches_fail_before_export(self) -> None:
        rules, candidate = make_rules(), make_candidate()
        label = make_label(candidate, rules)
        for candidates, labels in (
            ([candidate, candidate], [label]),
            ([candidate], [label, label]),
            ([candidate], []),
            ([candidate], [label.model_copy(update={"candidate_id": "extra"})]),
        ):
            with self.subTest(identities=(candidates, labels)), tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(ValueError):
                    publish_dataset(candidates, labels, rules, Path(tmp), 10)
                self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_empty_release_reports_each_risk_deficit_and_preserves_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = publish_dataset([], [], make_rules(), root, 7)
            self.assertEqual(read_csv(root / "commands.csv"), [])
            self.assertEqual(report["records"], 0)
            self.assertEqual(
                json.loads((root / "statistics.json").read_text())["by_risk"]["CAT-8.2"]["deficit"], 7
            )
            before = (root / "commands.csv").read_bytes()
            with self.assertRaises(FileExistsError):
                publish_dataset([], [], make_rules(), root, 7)
            self.assertEqual((root / "commands.csv").read_bytes(), before)

    def test_polars_schema_uses_supplied_taxonomy(self) -> None:
        rules = make_rules("CAT-23")
        candidate = make_candidate(risk="CAT-23.1")
        prepared = prepare_dataset(
            [candidate],
            [make_label(candidate, rules)],
            rules,
            1,
            approved_ids=frozenset({candidate.candidate_id}),
        )
        frame = pl.read_csv(io.StringIO(prepared.files["commands.csv"]))
        self.assertEqual(table_schema(rules).validate(frame).height, 1)
        with self.assertRaises(pandera.errors.SchemaError):
            table_schema(make_rules()).validate(frame)
