from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pydantic import JsonValue

from codebench_pipeline.engine import DEPENDENCIES
from codebench_pipeline.legacy import run_stage
from codebench_pipeline.models import AdapterSpec, DatasetSpec, ReplaySpec, SourceDocument, Stage
from codebench_pipeline.replay import invoke
from codebench_pipeline.schemas import (
    CATEGORY,
    CONFIDENCES,
    JSON_OBJECT,
    RISKS,
    Candidate,
    Semantics,
    StageMetadata,
    read_jsonl,
    write_json,
)


def record_response(packet: Path, response: Path, result: dict[str, JsonValue]) -> None:
    original = JSON_OBJECT.validate_json(packet.read_bytes())
    write_json(
        response,
        {
            "packet_sha256": original["packet_sha256"],
            "model": "inert-test-fixture",
            "responded_at": "2026-11-04T00:00:00Z",
            "result": result,
        },
    )


class LegacyFixture(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="codebench-legacy-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.roots: dict[str, Path] = {}
        for stage in ("source_snapshot", "adapter", "semantic_evidence"):
            self.roots[stage] = self.root / stage
            self.roots[stage].mkdir()
        snapshot = self.roots["source_snapshot"]
        (snapshot / "documents").mkdir()
        self.document = snapshot / "documents/00000.txt"
        self.document.write_text(
            "# Commands\n\n`terraform validate`\n\n`terraform validate`\n\n"
            "`terraform plan -destroy`\n\n`terraform plan -var '{{name}}={{value}}'`\n",
            encoding="utf-8",
        )
        document = SourceDocument(
            source_id="fixture-source",
            url="https://example.org/source",
            local_path=Path("documents/00000.txt"),
            license="CC0-1.0",
            license_url="https://example.org/license",
            source_type="official_documentation",
            accessibility="accessible",
            reuse_decision="permitted",
            commands_present=True,
            revision="fixture-v1",
            retrieved_at="2026-11-04",
            format="markdown",
        )
        (snapshot / "documents.jsonl").write_text(document.model_dump_json() + "\n", encoding="utf-8")
        adapter = AdapterSpec(format="markdown", rationale="Explicit inert Markdown fixture")
        (self.roots["adapter"] / "adapter.json").write_text(adapter.model_dump_json(), encoding="utf-8")
        self.semantics = self.roots["semantic_evidence"] / "semantics.jsonl"
        semantics = Semantics(
            utility="terraform",
            subcommands=["validate", "plan"],
            url="https://example.org/docs",
            retrieved_at="2026-11-04",
            snippet="Inert fixture documentation.",
            version_limits="fixture-v1",
        )
        self.semantics.write_text(semantics.model_dump_json() + "\n", encoding="utf-8")
        self.spec = DatasetSpec(
            dataset_id="legacy-fixture",
            revision="v1",
            documents=snapshot / "documents.jsonl",
            semantics=self.semantics,
            adapter=adapter,
        )

    def execute(self, stage: Stage) -> StageMetadata:
        out = self.root / f"{stage}-{len(list(self.root.iterdir()))}"
        out.mkdir()
        inputs = {key: self.roots[key] for key in DEPENDENCIES[stage]}
        metadata = StageMetadata.model_validate(run_stage(stage, self.spec, inputs, out))
        self.roots[stage] = out
        return metadata

    def prepare_candidates(self) -> None:
        for stage in ("extraction", "filtering", "candidates"):
            self.execute(stage)

    def record_history(self) -> ReplaySpec:
        self.prepare_candidates()
        annotation = self.roots["candidates"] / "annotation"
        history = self.root / "history"
        history.mkdir()
        candidates = [
            item
            for item in read_jsonl(annotation / "candidates.jsonl", Candidate)
            if item.row.command == "terraform validate"
        ]
        (history / "candidates.jsonl").write_text(
            "".join(item.model_dump_json(by_alias=True) + "\n" for item in candidates),
            encoding="utf-8",
        )
        (history / "sources.jsonl").write_bytes((annotation / "sources.jsonl").read_bytes())
        invoke(
            "label_pipeline",
            "prepare",
            candidates=history / "candidates.jsonl",
            sources=history / "sources.jsonl",
            semantics=self.semantics,
            out=history / "label",
        )
        packets = history / "label/packets"
        (packets / "label-00001.json").rename(packets / "original-name.json")
        labels: list[JsonValue] = [
            {
                "candidate_id": candidate.candidate_id,
                "decision": "accept",
                "category": CATEGORY,
                "risk": RISKS[0],
                "severity": "low",
                "class": "allow",
                "description": "Проверяет конфигурацию.",
                "util": "terraform",
                **{field: 0.8 for field in CONFIDENCES},
                "rationale": "Inert fixture label",
                "documentation_urls": ["https://example.org/docs"],
            }
            for candidate in candidates
        ]
        responses = history / "responses"
        record_response(packets / "original-name.json", responses / "original-name.json", {"labels": labels})
        invoke(
            "label_pipeline", "review-prepare", packets=packets, responses=responses, out=history / "review"
        )
        review_packets = history / "review/packets"
        reviews = history / "reviews"
        verdicts: list[JsonValue] = [
            {
                "candidate_id": candidate.candidate_id,
                "verdict": "approve",
                "rationale": "Inert independent review",
            }
            for candidate in candidates
        ]
        record_response(
            review_packets / "original-name.json", reviews / "original-name.json", {"reviews": verdicts}
        )
        return ReplaySpec(
            candidates=history / "candidates.jsonl",
            sources=history / "sources.jsonl",
            semantics=self.semantics,
            packets=packets,
            responses=responses,
            review_packets=review_packets,
            reviews=reviews,
        )
