from __future__ import annotations

import csv
import hashlib
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Final, Literal

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.models import DatasetSpec, Digest, FrozenModel, ReplaySpec, SourceDocument
from codebench_pipeline.schemas import (
    ALL_REVIEW_FIELDS,
    FIELDS,
    JSON_OBJECT,
    PENDING_FIELDS,
    REVIEW_FIELDS,
    RISKS,
    BridgeError,
    Candidate,
    EligibleCommand,
    SourceEvidence,
    read_jsonl,
    write_json,
)

Script = Literal["prefilter", "postfilter", "prepare_review_candidates", "label_pipeline", "validate_dataset"]
SCRIPTS: Final = Path(__file__).resolve().parents[1] / "scripts"


class LegacyCommandError(BridgeError):
    def __init__(self, script: Script, returncode: int, output: str) -> None:
        self.script = script
        self.returncode = returncode
        self.output = output
        super().__init__(f"{script} exited {returncode}: {output}")


class SourceHash(FrozenModel):
    source_id: str
    path: Path
    sha256: Digest


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_cli(script: Script, arguments: Sequence[str | Path]) -> dict[str, JsonValue]:
    result = subprocess.run(
        [sys.executable, "-B", str(SCRIPTS / f"{script}.py"), *map(str, arguments)],
        cwd=SCRIPTS.parent,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if result.returncode:
        raise LegacyCommandError(script, result.returncode, result.stdout + result.stderr)
    return JSON_OBJECT.validate_json(result.stdout)


def invoke(script: Script, action: str = "", **options: str | Path) -> dict[str, JsonValue]:
    arguments: list[str | Path] = [action] if action else []
    for key, value in options.items():
        arguments.extend(("--" + key.replace("_", "-"), value))
    return run_cli(script, arguments)


def empty_bundle(out: Path, inputs: dict[str, Path], limit: int) -> dict[str, JsonValue]:
    bundle = out / "bundle"
    bundle.mkdir()
    tables = {
        "commands.csv": FIELDS,
        "review/commands.csv": REVIEW_FIELDS,
        "review/pending.csv": PENDING_FIELDS,
        "review/labeled_all.csv": ALL_REVIEW_FIELDS,
    }
    for risk in RISKS:
        code = risk.split()[0]
        tables[f"review/{code}.csv"] = REVIEW_FIELDS
        tables[f"review/labeled-{code}.csv"] = ALL_REVIEW_FIELDS
        path = bundle / f"data/candidates/{code}/candidates.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    for name, fields in tables.items():
        path = bundle / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as stream:
            csv.writer(stream).writerow(fields)
    for name in (
        "data/processed/provenance.jsonl",
        "data/review/pending.jsonl",
        "data/review/utility-normalization.jsonl",
    ):
        path = bundle / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    preserve(inputs["candidates"] / "annotation/sources.jsonl", bundle / "research/sources.jsonl")
    preserve(
        inputs["candidates"] / "annotation/filtering-rejections.jsonl",
        bundle / "data/review/filtering-rejections.jsonl",
    )
    report = run_cli("validate_dataset", [bundle])
    report.pop("target", None)
    report.pop("deficit", None)
    report.update(
        target_per_risk=250,
        limit_per_risk=limit,
        pending=0,
        pairing="deferred",
        deduplication="deferred",
        empty_reason="No eligible candidates reached labeling.",
    )
    report["by_risk"] = {
        risk: {"total": 0, "class": {}, "severity": {}, "utilities": {}, "deficit": 250} for risk in RISKS
    }
    write_json(bundle / "statistics.json", report)
    return report


def export_arguments(inputs: dict[str, Path], bundle: Path, limit: int) -> list[str | Path]:
    annotation = inputs["candidates"] / "annotation"
    return [
        "export",
        "--candidates",
        annotation / "candidates.jsonl",
        "--sources",
        annotation / "sources.jsonl",
        "--packets",
        inputs["label_packets"] / "exchange/packets",
        "--responses",
        inputs["labels"] / "responses",
        "--review-packets",
        inputs["review_packets"] / "exchange/packets",
        "--reviews",
        inputs["reviews"] / "responses",
        "--out",
        bundle,
        "--limit-per-risk",
        str(limit),
    ]


def preserve(source: Path, destination: Path) -> dict[str, str]:
    if source.is_symlink():
        raise BridgeError("Replay symlinks are not immutable evidence", source)
    paths = sorted(source.rglob("*")) if source.is_dir() else [source]
    hashes: dict[str, str] = {}
    for path in paths:
        if path.is_symlink():
            raise BridgeError("Replay symlinks are not immutable evidence", path)
        if path.is_file():
            relative = path.relative_to(source).as_posix() if source.is_dir() else source.name
            hashes[relative] = digest(path)
    if source.is_dir():
        shutil.copytree(source, destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    for name, expected in hashes.items():
        target = destination / name if source.is_dir() else destination
        if digest(target) != expected:
            raise BridgeError("Replay bytes changed during preservation", source)
    return hashes


def verify_replay(spec: DatasetSpec, inputs: dict[str, Path]) -> dict[str, str]:
    replay = spec.replay
    if replay is None:
        raise BridgeError("Historical evidence is required for replay")
    snapshot = inputs["source_snapshot"]
    documents = read_jsonl(snapshot / "documents.jsonl", SourceDocument)
    sources = read_jsonl(replay.sources, SourceEvidence)
    source_map = {source.source_id: source for source in sources}
    document_map = {document.source_id: document for document in documents}
    if len(source_map) != len(sources) or len(document_map) != len(documents):
        raise BridgeError("Duplicate replay source identities")
    eligible = read_jsonl(inputs["filtering"] / "filtered/eligible.jsonl", EligibleCommand)
    current_commands = {(row.source_id, row.command, row.doc_pos): row for row in eligible}
    candidates = read_jsonl(replay.candidates, Candidate)
    if len({candidate.candidate_id for candidate in candidates}) != len(candidates):
        raise BridgeError("Duplicate replay candidate identities")
    for candidate in candidates:
        for source_id in candidate.source_ids:
            current_record = current_commands.get(
                (source_id, candidate.row.command, candidate.source_locator)
            )
            if current_record is None:
                raise BridgeError(
                    f"Replay command is absent from current eligible records: {candidate.candidate_id}"
                )
            if current_record.context != candidate.evidence_context:
                raise BridgeError(f"Replay command context changed: {candidate.candidate_id}")
            if source_id not in source_map or source_id not in document_map:
                raise BridgeError(f"Replay source is absent from current snapshot: {source_id}")
            previous, current = source_map[source_id], document_map[source_id]
            for field in ("url", "revision", "source_type", "license", "license_url"):
                if getattr(previous, field) != getattr(current, field):
                    raise BridgeError(f"Replay source metadata changed: {source_id}/{field}")
    audit = JSON_OBJECT.validate_json((inputs["extraction"] / "extracted/audit.json").read_bytes())
    source_hashes = TypeAdapter(list[SourceHash]).validate_python(audit["inputs"])
    recorded = {item.source_id: item.sha256 for item in source_hashes}
    if len(recorded) != len(documents) or set(recorded) != set(document_map):
        raise BridgeError("Current extraction does not cover snapshot source bytes")
    hashes = {
        "candidates": digest(replay.candidates),
        "sources": digest(replay.sources),
    }
    for document in documents:
        path = document.local_path
        source_path = path if path.is_absolute() else snapshot / path
        if digest(source_path) != recorded[document.source_id]:
            raise BridgeError("Current source bytes changed after extraction", source_path)
        hashes[f"snapshot/{document.source_id}"] = recorded[document.source_id]
    return hashes


def validate_historical_exchange(replay: ReplaySpec, exchange: tuple[Path, Path], limit: int) -> None:
    with TemporaryDirectory(prefix="codebench-replay-") as temporary:
        invoke(
            "label_pipeline",
            "export",
            candidates=replay.candidates,
            sources=replay.sources,
            packets=replay.packets,
            responses=replay.responses,
            review_packets=exchange[0],
            reviews=exchange[1],
            out=Path(temporary) / "bundle",
            limit_per_risk=str(limit),
        )
