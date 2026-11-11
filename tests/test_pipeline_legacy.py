from __future__ import annotations

import csv
import json
import unittest

from pandera.errors import SchemaError
from pipeline_fixture import LegacyFixture
from pydantic import ValidationError

from codebench_pipeline.models import AdapterPlan, AdapterSpec, SourceDocument
from codebench_pipeline.replay import LegacyCommandError
from codebench_pipeline.schemas import (
    CATEGORY,
    FIELDS,
    JSON_OBJECT,
    RISKS,
    BridgeError,
    Candidate,
    RawCommand,
    read_jsonl,
    validate_commands,
    validate_export,
)


class LegacyBridgeTests(LegacyFixture):
    def test_source_specific_adapters_preserve_mixed_json_and_markdown(self) -> None:
        snapshot = self.roots["source_snapshot"]
        original = SourceDocument.model_validate_json((snapshot / "documents.jsonl").read_bytes())
        structured = original.model_copy(
            update={
                "source_id": "json-source",
                "local_path": original.local_path.parent / "second.json",
                "format": "json",
                "text_field": "payload",
            }
        )
        (snapshot / structured.local_path).write_text('{"payload":"terraform validate"}', encoding="utf-8")
        (snapshot / "documents.jsonl").write_text(
            original.model_dump_json() + "\n" + structured.model_dump_json() + "\n", encoding="utf-8"
        )
        plan = AdapterPlan(
            documents={
                original.source_id: AdapterSpec(format="markdown", rationale="Markdown source"),
                structured.source_id: AdapterSpec(
                    format="json", text_field="payload", rationale="Structured source"
                ),
            }
        )
        (self.roots["adapter"] / "adapter.json").write_text(plan.model_dump_json(), encoding="utf-8")
        self.execute("extraction")
        self.execute("filtering")
        records = validate_commands(self.roots["filtering"] / "filtered/eligible.jsonl", eligible=True)
        self.assertEqual(len(records), 4)
        (structured_record,) = [record for record in records if record.source_id == "json-source"]
        self.assertEqual(structured_record.command, "terraform validate")
        self.assertIn("record 1; field payload", structured_record.doc_pos)

    def test_real_cli_stages_keep_distinct_records_and_reject_templates(self) -> None:
        self.prepare_candidates()
        selected = read_jsonl(self.roots["candidates"] / "annotation/candidates.jsonl", Candidate)
        self.assertEqual(
            sorted(row.row.command for row in selected),
            ["terraform plan -destroy", "terraform validate", "terraform validate"],
        )
        rejected = (self.roots["filtering"] / "filtered/rejected.jsonl").read_text(encoding="utf-8")
        self.assertIn("unresolved_template_or_truncated_example", rejected)
        metadata = self.execute("label_packets")
        self.assertEqual(metadata.statistics["candidates"], 3)

    def test_empty_eligible_path_exports_header_and_filtering_evidence_without_inference(self) -> None:
        self.document.write_text("`terraform plan -var '{{name}}={{value}}'`\n", encoding="utf-8")
        for stage in (
            "extraction",
            "filtering",
            "candidates",
            "label_packets",
            "labels",
            "review_packets",
            "reviews",
            "export",
        ):
            metadata = self.execute(stage)
        bundle = self.roots["export"] / "bundle"
        self.assertEqual(validate_export(bundle / "commands.csv").height, 0)
        self.assertEqual(metadata.status, "empty")
        self.assertEqual(metadata.annotation_origin, "not_invoked")
        self.assertTrue((bundle / "data/review/filtering-rejections.jsonl").read_bytes())
        self.assertEqual(metadata.statistics["pending"], 0)

    def test_replay_preserves_original_bytes_names_and_duplicate_commands_with_extra_eligible_records(
        self,
    ) -> None:
        replay = self.record_history()
        self.spec = self.spec.model_copy(update={"replay": replay})
        for stage in ("candidates", "label_packets", "labels", "review_packets", "reviews", "export"):
            metadata = self.execute(stage)
        for original, stage, relative in (
            (replay.candidates, "candidates", "annotation/candidates.jsonl"),
            (replay.sources, "candidates", "annotation/sources.jsonl"),
            (replay.packets / "original-name.json", "label_packets", "exchange/packets/original-name.json"),
            (replay.responses / "original-name.json", "labels", "responses/original-name.json"),
            (
                replay.review_packets / "original-name.json",
                "review_packets",
                "exchange/packets/original-name.json",
            ),
            (replay.reviews / "original-name.json", "reviews", "responses/original-name.json"),
        ):
            self.assertEqual((self.roots[stage] / relative).read_bytes(), original.read_bytes())
        self.assertEqual(metadata.statistics["records"], 2)
        self.assertEqual(metadata.annotation_origin, "historical_replay")
        self.assertEqual(
            len(validate_commands(self.roots["filtering"] / "filtered/eligible.jsonl", eligible=True)), 3
        )

    def test_replay_rejects_commands_outside_current_filter_and_changed_snapshot_bytes(self) -> None:
        replay = self.record_history()
        self.spec = self.spec.model_copy(update={"replay": replay})
        eligible = self.roots["filtering"] / "filtered/eligible.jsonl"
        original = eligible.read_bytes()
        eligible.write_bytes(b"")
        with self.assertRaisesRegex(BridgeError, "absent from current eligible"):
            self.execute("candidates")
        eligible.write_bytes(original)
        self.document.write_text(self.document.read_text(encoding="utf-8") + "changed", encoding="utf-8")
        with self.assertRaisesRegex(BridgeError, "source bytes changed"):
            self.execute("candidates")

    def test_replay_rejects_response_hash_not_bound_to_original_packet(self) -> None:
        replay = self.record_history()
        response = replay.responses / "original-name.json"
        envelope = JSON_OBJECT.validate_json(response.read_bytes())
        envelope["packet_sha256"] = "0" * 64
        response.write_text(json.dumps(envelope), encoding="utf-8")
        self.spec = self.spec.model_copy(update={"replay": replay})
        self.execute("candidates")
        self.execute("label_packets")
        with self.assertRaises(LegacyCommandError):
            self.execute("labels")

    def test_export_schema_rejects_noncanonical_fields_enums_and_seed(self) -> None:
        row = [
            "terraform validate",
            "Проверяет конфигурацию.",
            "terraform",
            "low",
            CATEGORY,
            RISKS[0],
            "real",
            "-",
            "allow",
        ]
        path = self.root / "commands.csv"
        for index, value in ((3, "extreme"), (4, "CAT-3"), (5, "CAT-3.1"), (7, "prompt"), (8, "modify")):
            malformed = [*row]
            malformed[index] = value
            with path.open("w", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerows([FIELDS, malformed])
            with self.assertRaises(SchemaError):
                validate_export(path)
        path.write_text(",".join((*FIELDS, "extra")) + "\n", encoding="utf-8")
        with self.assertRaises(BridgeError):
            validate_export(path)

    def test_raw_boundary_rejects_malformed_and_duplicate_record_ids(self) -> None:
        self.execute("extraction")
        raw = self.roots["extraction"] / "extracted/raw_commands.jsonl"
        rows = read_jsonl(raw, RawCommand)
        malformed = rows[0].model_dump(mode="json")
        malformed["raw_id"] = "not-a-digest"
        raw.write_text(json.dumps(malformed) + "\n", encoding="utf-8")
        with self.assertRaises(ValidationError):
            validate_commands(raw)
        raw.write_text((rows[0].model_dump_json() + "\n") * 2, encoding="utf-8")
        with self.assertRaises(SchemaError):
            validate_commands(raw)


if __name__ == "__main__":
    unittest.main()
