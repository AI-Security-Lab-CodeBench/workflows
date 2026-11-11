from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import ConfigDict, JsonValue, TypeAdapter, validate_call

from codebench_pipeline.models import AdapterPlan, AdapterSpec, DatasetSpec, SourceDocument, Stage
from codebench_pipeline.replay import (
    digest,
    empty_bundle,
    export_arguments,
    invoke,
    preserve,
    run_cli,
    validate_historical_exchange,
    verify_replay,
)
from codebench_pipeline.reporting import write_dataset_card
from codebench_pipeline.schemas import (
    BridgeError,
    Candidate,
    Semantics,
    SourceEvidence,
    StageMetadata,
    read_jsonl,
    validate_commands,
    validate_export,
    write_json,
)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def run_stage(stage: Stage, spec: DatasetSpec, inputs: dict[str, Path], out: Path) -> dict[str, JsonValue]:
    """Build a legacy stage inside an existing empty root; inputs are completed artifact roots."""
    if not out.is_dir() or any(out.iterdir()):
        raise BridgeError("Stage output must be an existing empty directory", out)
    replay = spec.replay
    hashes: dict[str, str] = {}
    statistics: dict[str, JsonValue]
    empty = False
    match stage:
        case "source_snapshot" | "adapter" | "semantic_evidence":
            raise BridgeError("Source snapshots and adapter selection belong to the orchestration layer")
        case "extraction":
            snapshot = inputs["source_snapshot"]
            plan = TypeAdapter(AdapterSpec | AdapterPlan).validate_json(
                (inputs["adapter"] / "adapter.json").read_bytes()
            )
            documents = read_jsonl(snapshot / "documents.jsonl", SourceDocument)
            manifest = out / "documents.jsonl"
            rows: list[str] = []
            for document in documents:
                adapter = plan.documents[document.source_id] if isinstance(plan, AdapterPlan) else plan
                local = document.local_path
                changes = {
                    "local_path": local if local.is_absolute() else snapshot / local,
                    "format": adapter.format,
                    "text_field": adapter.text_field,
                    "context_field": adapter.context_field,
                }
                rows.append(document.model_copy(update=changes).model_dump_json())
            manifest.write_text("".join(row + "\n" for row in rows), encoding="utf-8")
            statistics = invoke("prefilter", manifest=manifest, out=out / "extracted")
            validate_commands(out / "extracted/raw_commands.jsonl")
            empty = statistics["raw_candidates"] == 0
        case "filtering":
            raw = inputs["extraction"] / "extracted/raw_commands.jsonl"
            validate_commands(raw)
            statistics = invoke("postfilter", input=raw, out=out / "filtered")
            validate_commands(out / "filtered/eligible.jsonl", eligible=True)
            empty = statistics["eligible"] == 0
        case "candidates":
            if replay is not None:
                hashes.update(verify_replay(spec, inputs))
                preserve(replay.candidates, out / "annotation/candidates.jsonl")
                preserve(replay.sources, out / "annotation/sources.jsonl")
                count = len(read_jsonl(out / "annotation/candidates.jsonl", Candidate))
                eligible_count = len(
                    validate_commands(inputs["filtering"] / "filtered/eligible.jsonl", eligible=True)
                )
                statistics = {
                    "candidates": count,
                    "eligible_records": eligible_count,
                    "fresh_annotations": 0,
                    "historical_evidence_preserved": True,
                }
            else:
                initial = out / "initial"
                initial.mkdir()
                (initial / "candidates.jsonl").touch()
                (initial / "sources.jsonl").touch()
                documents = read_jsonl(inputs["source_snapshot"] / "documents.jsonl", SourceDocument)
                candidate_manifest = out / "candidate-documents.jsonl"
                candidate_manifest.write_text(
                    "".join(
                        document.model_copy(
                            update={
                                "source_locator": document.source_locator or document.local_path.as_posix(),
                            }
                        ).model_dump_json()
                        + "\n"
                        for document in documents
                    ),
                    encoding="utf-8",
                )
                statistics = invoke(
                    "prepare_review_candidates",
                    inputs=initial,
                    eligible=inputs["filtering"] / "filtered/eligible.jsonl",
                    documents=candidate_manifest,
                    out=out / "annotation",
                )
            preserve(
                inputs["filtering"] / "filtered/rejected.jsonl", out / "annotation/filtering-rejections.jsonl"
            )
            candidates = read_jsonl(out / "annotation/candidates.jsonl", Candidate)
            read_jsonl(out / "annotation/sources.jsonl", SourceEvidence)
            empty = not candidates
        case "label_packets":
            candidates = inputs["candidates"] / "annotation"
            empty = not read_jsonl(candidates / "candidates.jsonl", Candidate)
            semantics = inputs["semantic_evidence"] / "semantics.jsonl"
            read_jsonl(semantics, Semantics)
            if replay is not None:
                if digest(replay.semantics) != digest(semantics):
                    raise BridgeError("Replay semantics differ from semantic evidence")
                hashes["semantics"] = digest(semantics)
                hashes.update(
                    {
                        f"packets/{key}": value
                        for key, value in preserve(replay.packets, out / "exchange/packets").items()
                    }
                )
                statistics = {"packets": len(list((out / "exchange/packets").glob("*.json")))}
            elif empty:
                (out / "exchange/packets").mkdir(parents=True)
                statistics = {"packets": 0, "candidates": 0, "pending_reason": "No eligible candidates"}
            else:
                statistics = invoke(
                    "label_pipeline",
                    "prepare",
                    candidates=candidates / "candidates.jsonl",
                    sources=candidates / "sources.jsonl",
                    semantics=semantics,
                    out=out / "exchange",
                    batch_size=str(spec.batch_size),
                )
        case "labels" | "reviews":
            packet_stage = "label_packets" if stage == "labels" else "review_packets"
            packets = inputs[packet_stage] / "exchange/packets"
            empty = not any(packets.glob("*.json"))
            if replay is not None:
                original = replay.responses if stage == "labels" else replay.reviews
                hashes.update(
                    {
                        f"responses/{key}": value
                        for key, value in preserve(original, out / "responses").items()
                    }
                )
                statistics = {"responses": len(list((out / "responses").glob("*.json")))}
            elif empty:
                (out / "responses").mkdir()
                statistics = {"responses": 0, "pending_reason": "No annotation packets"}
            else:
                from codebench_pipeline.agents import run_packets

                model = spec.label_model if stage == "labels" else spec.review_model
                run_packets(packets=packets, out=out / "responses", model=model)
                statistics = {"responses": len(list((out / "responses").glob("*.json"))), "model": model}
            if not empty and stage == "reviews" and replay is not None:
                validate_historical_exchange(replay, (packets, out / "responses"), spec.limit_per_risk)
            if not empty and stage == "labels":
                with TemporaryDirectory(prefix="codebench-label-check-") as temporary:
                    invoke(
                        "label_pipeline",
                        "review-prepare",
                        packets=packets,
                        responses=out / "responses",
                        out=Path(temporary) / "validated",
                    )
        case "review_packets":
            packets = inputs["label_packets"] / "exchange/packets"
            empty = not any(packets.glob("*.json"))
            if replay is not None:
                hashes.update(
                    {
                        f"packets/{key}": value
                        for key, value in preserve(replay.review_packets, out / "exchange/packets").items()
                    }
                )
                statistics = {"packets": len(list((out / "exchange/packets").glob("*.json")))}
            elif empty:
                (out / "exchange/packets").mkdir(parents=True)
                statistics = {"packets": 0, "pending_reason": "No labels to review"}
            else:
                statistics = invoke(
                    "label_pipeline",
                    "review-prepare",
                    packets=packets,
                    responses=inputs["labels"] / "responses",
                    out=out / "exchange",
                )
        case "export":
            empty = not read_jsonl(inputs["candidates"] / "annotation/candidates.jsonl", Candidate)
            statistics = (
                empty_bundle(out, inputs, spec.limit_per_risk)
                if empty
                else run_cli(
                    "label_pipeline",
                    export_arguments(inputs, out / "bundle", spec.limit_per_risk),
                )
            )
            validate_export(out / "bundle/commands.csv")
            validation_report = run_cli("validate_dataset", [out / "bundle"])
            write_json(out / "validation.json", validation_report)
            write_dataset_card(out / "bundle", replay is not None)
    metadata = StageMetadata(
        stage=stage,
        status="empty" if empty else "completed",
        replay=replay is not None,
        annotation_origin="historical_replay"
        if replay is not None
        else ("fresh_invocations" if stage in {"labels", "reviews"} and not empty else "not_invoked"),
        statistics=statistics,
        evidence_sha256=hashes,
    )
    result = metadata.model_dump(mode="json")
    write_json(out / "stage.json", result)
    return result
