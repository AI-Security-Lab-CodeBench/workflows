from __future__ import annotations

import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.replay import empty_bundle, export_arguments, run_cli
from codebench_pipeline.reporting import write_dataset_card
from codebench_pipeline.schemas import Candidate, SourceEvidence, read_jsonl, validate_export, write_json
from codebench_pipeline.storage import (
    IntegrityError,
    digest,
    exclusive,
    fingerprint,
    inventory,
    load_artifact,
)


def publish(root: Path, exports: dict[str, str]) -> Path:
    artifacts = {key: load_artifact(Path(value) / "_artifact.json") for key, value in exports.items()}
    version = fingerprint(
        {
            "exports": {key: artifact.fingerprint for key, artifact in artifacts.items()},
            "publisher": digest(Path(__file__)),
        }
    )
    destination = root / "releases" / version
    if destination.exists():
        receipt = TypeAdapter(dict[str, JsonValue]).validate_json((destination / "release.json").read_bytes())
        expected = TypeAdapter(dict[str, str]).validate_python(receipt["files"])
        actual = inventory(destination)
        actual.pop("release.json")
        if actual != expected:
            raise IntegrityError("Published release content changed")
        validate_export(destination / "bundle/commands.csv")
        return destination
    with exclusive(destination.with_suffix(".lock")):
        with TemporaryDirectory(prefix=".release-", dir=destination.parent) as temporary:
            staging = Path(temporary) / "release"
            staging.mkdir()
            inputs = {
                stage: staging / "inputs" / stage
                for stage in ("candidates", "label_packets", "labels", "review_packets", "reviews")
            }
            candidates: dict[str, Candidate] = {}
            sources: dict[str, SourceEvidence] = {}
            rejections: list[str] = []
            for key, artifact in artifacts.items():
                upstream = {
                    stage: artifact.path.parents[1] / stage / artifact.inputs[stage] for stage in inputs
                }
                for path in upstream.values():
                    load_artifact(path / "_artifact.json")
                rejections.append(
                    (upstream["candidates"] / "annotation/filtering-rejections.jsonl").read_text(
                        encoding="utf-8"
                    )
                )
                for candidate in read_jsonl(
                    upstream["candidates"] / "annotation/candidates.jsonl", Candidate
                ):
                    if candidate.candidate_id in candidates:
                        raise IntegrityError(
                            f"Duplicate source record across datasets: {candidate.candidate_id}"
                        )
                    candidates[candidate.candidate_id] = candidate
                for source in read_jsonl(upstream["candidates"] / "annotation/sources.jsonl", SourceEvidence):
                    if source.source_id in sources and sources[source.source_id] != source:
                        raise IntegrityError(
                            f"Conflicting source evidence across datasets: {source.source_id}"
                        )
                    sources[source.source_id] = source
                for stage in ("label_packets", "labels", "review_packets", "reviews"):
                    relative = "exchange/packets" if stage.endswith("packets") else "responses"
                    target = inputs[stage] / relative
                    target.mkdir(parents=True, exist_ok=True)
                    for path in (upstream[stage] / relative).glob("*.json"):
                        shutil.copyfile(path, target / f"{key}--{path.name}")
            annotation = inputs["candidates"] / "annotation"
            annotation.mkdir(parents=True)
            (annotation / "candidates.jsonl").write_text(
                "".join(item.model_dump_json(by_alias=True) + "\n" for item in candidates.values()),
                encoding="utf-8",
            )
            (annotation / "sources.jsonl").write_text(
                "".join(item.model_dump_json() + "\n" for item in sources.values()), encoding="utf-8"
            )
            (annotation / "filtering-rejections.jsonl").write_text("".join(rejections), encoding="utf-8")
            statistics = (
                run_cli("label_pipeline", export_arguments(inputs, staging / "bundle", 250))
                if candidates
                else empty_bundle(staging, inputs, 250)
            )
            validate_export(staging / "bundle/commands.csv")
            write_dataset_card(
                staging / "bundle",
                any(artifact.metadata.get("replay") is True for artifact in artifacts.values()),
            )
            receipt: dict[str, JsonValue] = {
                "dataset_exports": {key: str(artifact.path) for key, artifact in artifacts.items()},
                "statistics": statistics,
                "limit_per_risk": 250,
                "files": {key: value for key, value in inventory(staging).items()},
            }
            write_json(staging / "release.json", receipt)
            os.rename(staging, destination)
    return destination
