from __future__ import annotations

from pathlib import Path
from typing import Final

from pydantic import JsonValue

from codebench_pipeline.models import (
    AdapterPlan,
    AdapterSpec,
    Artifact,
    DatasetSpec,
    SemanticSnippet,
    SourceDocument,
    Stage,
)
from codebench_pipeline.snapshot import build_snapshot, source_inputs
from codebench_pipeline.storage import (
    ArtifactRequest,
    ArtifactStore,
    IntegrityError,
    digest,
    fingerprint,
    load_artifact,
)

DEPENDENCIES: Final[dict[Stage, tuple[Stage, ...]]] = {
    "source_snapshot": (),
    "semantic_evidence": (),
    "adapter": ("source_snapshot",),
    "extraction": ("source_snapshot", "adapter"),
    "filtering": ("extraction",),
    "candidates": ("source_snapshot", "extraction", "filtering"),
    "label_packets": ("source_snapshot", "candidates", "semantic_evidence"),
    "labels": ("label_packets",),
    "review_packets": ("label_packets", "labels"),
    "reviews": ("review_packets",),
    "export": ("candidates", "label_packets", "labels", "review_packets", "reviews"),
}
SCRIPTS: Final[dict[Stage, tuple[str, ...]]] = {
    "source_snapshot": (),
    "semantic_evidence": ("label_inputs",),
    "adapter": (),
    "extraction": ("prefilter", "source_pipeline", "command_filter", "command_families"),
    "filtering": ("postfilter", "command_filter", "command_families"),
    "candidates": ("prepare_review_candidates", "source_pipeline", "dataset_schema"),
    "label_packets": ("label_pipeline", "label_contract", "label_inputs", "label_io"),
    "labels": ("run_label_agents", "label_contract", "label_io"),
    "review_packets": ("label_pipeline", "label_contract", "label_inputs", "label_io"),
    "reviews": ("run_label_agents", "label_contract", "label_io"),
    "export": ("pipeline_export", "integrate_dataset", "validate_dataset", "dataset_schema", "dataset_csv"),
}


def implementation_inputs(stage: Stage) -> dict[str, str]:
    package = Path(__file__).parent
    repo = package.parent
    paths = [package / name for name in ("models.py", "storage.py", "engine.py")]
    paths += [repo / "scripts" / f"{name}.py" for name in SCRIPTS[stage]]
    if stage == "source_snapshot":
        paths += [package / "snapshot.py", repo / "scripts/dataset_schema.py"]
    elif stage not in {"adapter", "semantic_evidence"}:
        paths += [package / name for name in ("legacy.py", "replay.py", "schemas.py", "reporting.py")]
        paths += [
            repo / "scripts" / f"{name}.py"
            for name in (
                "command_filter",
                "command_families",
                "dataset_schema",
                "dataset_csv",
                "validate_dataset",
                "source_pipeline",
                "label_io",
                "label_inputs",
                "label_contract",
                "label_pipeline",
                "integrate_dataset",
                "pipeline_export",
            )
        ]
    if stage in {"adapter", "labels", "reviews"}:
        paths += list(package.glob("*agent*.py"))
    if stage == "export":
        paths += [package / "schemas.py", package / "reporting.py"]
    if stage in {"label_packets", "review_packets"}:
        paths += [repo / "docs/labeling-policy.md"]
    if (repo / "uv.lock").exists():
        paths += [repo / "uv.lock", repo / "pyproject.toml"]
    return {str(path.relative_to(repo)): digest(path) for path in paths if path.exists()}


def stage_inputs(spec: DatasetSpec, stage: Stage, upstream: dict[str, Artifact]) -> dict[str, str]:
    inputs = implementation_inputs(stage)
    inputs.update({name: artifact.fingerprint for name, artifact in upstream.items()})
    if stage == "source_snapshot":
        inputs.update(source_inputs(spec))
        inputs["mode"] = fingerprint(bool(spec.replay))
    if stage == "semantic_evidence":
        inputs["semantics"] = digest(spec.semantics)
    if stage == "adapter":
        inputs["document_adapters"] = fingerprint(spec.use_document_adapters)
        inputs["adapter"] = fingerprint(
            spec.adapter.model_dump(mode="json") if spec.adapter else spec.label_model
        )
    if stage in {"label_packets", "review_packets"}:
        inputs["batch_size"] = fingerprint(spec.batch_size)
    if stage == "labels":
        inputs["model"] = fingerprint(spec.label_model)
    if stage == "reviews":
        inputs["model"] = fingerprint(spec.review_model)
    if stage == "export":
        inputs["limit"] = fingerprint(spec.limit_per_risk)
    if spec.replay:
        replay_paths: dict[Stage, tuple[Path, ...]] = {
            "candidates": (spec.replay.candidates, spec.replay.sources),
            "label_packets": (spec.replay.packets,),
            "labels": (spec.replay.responses,),
            "review_packets": (spec.replay.review_packets,),
            "reviews": (spec.replay.reviews,),
        }
        for path in replay_paths.get(stage, ()):
            files = [path] if path.is_file() else sorted(path.rglob("*.json"))
            for file in files:
                inputs[f"replay:{path.name}:{file.name}"] = digest(file)
    return inputs


def build_adapter(spec: DatasetSpec, source: Path, out: Path) -> dict[str, JsonValue]:
    if spec.use_document_adapters:
        mappings: dict[str, AdapterSpec] = {}
        for line in (source / "documents.jsonl").read_text(encoding="utf-8").splitlines():
            document = SourceDocument.model_validate_json(line)
            mappings[document.source_id] = AdapterSpec(
                format=document.format,
                text_field=document.text_field,
                context_field=document.context_field,
                rationale="Preserved source-specific adapter from acquisition.",
            )
        (out / "adapter.json").write_text(
            AdapterPlan(documents=mappings).model_dump_json(indent=2), encoding="utf-8"
        )
        return {"mappings": len(mappings), "execution": "declarative_mapping_only"}
    adapter = spec.adapter
    if adapter is None:
        from codebench_pipeline.agents import propose_adapter

        sample = "\n".join(
            path.read_text(encoding="utf-8")[:8000] for path in sorted((source / "documents").glob("*"))[:3]
        )
        adapter = propose_adapter(sample, out / "inference", spec.label_model)
    parsed = AdapterSpec.model_validate(adapter)
    (out / "adapter.json").write_text(parsed.model_dump_json(indent=2), encoding="utf-8")
    return {"format": parsed.format, "text_field": parsed.text_field, "execution": "declarative_mapping_only"}


def execute_stage(
    store: ArtifactStore,
    spec: DatasetSpec,
    stage: Stage,
    upstream_paths: dict[str, Path],
) -> tuple[Artifact, bool]:
    upstream = {key: load_artifact(path / "_artifact.json") for key, path in upstream_paths.items()}
    request = ArtifactRequest(spec, stage, stage_inputs(spec, stage, upstream))

    def compute(out: Path) -> dict[str, JsonValue]:
        if stage == "source_snapshot":
            return build_snapshot(spec, out)
        if stage == "semantic_evidence":
            content = spec.semantics.read_text(encoding="utf-8")
            rows = [
                SemanticSnippet.model_validate_json(line) for line in content.splitlines() if line.strip()
            ]
            (out / "semantics.jsonl").write_text(content, encoding="utf-8")
            return {"snippets": len(rows)}
        if stage == "adapter":
            return build_adapter(spec, upstream_paths["source_snapshot"], out)
        from codebench_pipeline.legacy import run_stage

        return run_stage(stage, spec, upstream_paths, out)

    def build(out: Path) -> dict[str, JsonValue]:
        metadata = compute(out)
        repo = Path(__file__).parent.parent
        for name, current_hash in implementation_inputs(stage).items():
            if request.inputs.get(name) != current_hash:
                raise IntegrityError(f"Implementation changed during materialization: {name}")
            target = out / "implementation" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((repo / name).read_bytes())
            if digest(target) != current_hash:
                raise IntegrityError(f"Implementation snapshot changed while copying: {name}")
        return metadata

    return store.materialize(request, build)
