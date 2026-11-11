from __future__ import annotations

import argparse
import json
from pathlib import Path

from dagster import AssetSelection, DagsterInstance, materialize

from codebench_pipeline.demo import prepare_demo
from codebench_pipeline.engine import DEPENDENCIES
from codebench_pipeline.legacy_definitions import DEFAULT_ROOT, build_definitions
from codebench_pipeline.models import DatasetSpec, FrozenModel
from codebench_pipeline.storage import Registry


def instance_directory(root: Path) -> Path:
    path = root / "dagster"
    path.mkdir(parents=True, exist_ok=True)
    return path


class RunReceipt(FrozenModel):
    dataset: str
    run_id: str
    cache_hits: dict[str, bool]


def process(root: Path, key: str) -> RunReceipt:
    definitions = build_definitions(root)
    with DagsterInstance.local_temp(str(instance_directory(root))) as instance:
        instance.add_dynamic_partitions("datasets", [key])
        result = materialize(
            assets=list(definitions.get_repository_def().asset_graph.assets_defs),
            instance=instance,
            partition_key=key,
            resources=definitions.resources,
            selection=AssetSelection.assets(*DEPENDENCIES),
        )
        if not result.success:
            raise RuntimeError(f"Dataset materialization failed: {result.run_id}")
        cache_hits = {
            stage: result.asset_materializations_for_node(stage)[0].metadata["cache_hit"].value is True
            for stage in DEPENDENCIES
        }
        receipt = RunReceipt(dataset=key, run_id=result.run_id, cache_hits=cache_hits)
        print(receipt.model_dump_json())
        return receipt


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reproducible CAT-3 asset pipeline. Source commands stay inert."
    )
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("register", "run", "demo", "status", "discover", "verify-incremental", "publish"):
        sub = commands.add_parser(action)
        sub.add_argument("--root", type=Path, default=DEFAULT_ROOT)
        if action in {"register", "discover"}:
            sub.add_argument("config", type=Path)
        if action == "run":
            sub.add_argument("dataset")
        if action == "demo":
            sub.add_argument("--packet", default="main-label-00003.json")
            sub.add_argument("--fresh", action="store_true")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    registry = Registry(root)
    match args.action:
        case "register":
            spec = DatasetSpec.model_validate_json(Path(args.config).read_text(encoding="utf-8"))
            key = registry.register(spec)
            with DagsterInstance.local_temp(str(instance_directory(root))) as instance:
                instance.add_dynamic_partitions("datasets", [key])
            print(json.dumps({"registered": key}))
        case "run":
            process(root, str(args.dataset))
        case "demo":
            packet = str(args.packet)
            if Path(packet).name != packet:
                raise ValueError("Packet must be a filename")
            repo = Path(__file__).resolve().parent.parent
            spec = prepare_demo(repo, root / "demo-inputs" / Path(packet).stem, packet)
            if args.fresh:
                spec = spec.model_copy(
                    update={
                        "dataset_id": "fresh-" + spec.dataset_id,
                        "replay": None,
                        "adapter": None,
                        "use_document_adapters": False,
                    }
                )
            registry.register(spec)
            process(root, spec.dataset_id)
        case "status":
            print(json.dumps({"root": str(root), "datasets": registry.keys(), "stages": list(DEPENDENCIES)}))
        case "discover":
            from codebench_pipeline.discovery import discover

            discover(Path(args.config), root / "discovery")
        case "verify-incremental":
            from codebench_pipeline.qa import verify_incremental

            verify_incremental(root)
        case "publish":
            definitions = build_definitions(root)
            with DagsterInstance.local_temp(str(instance_directory(root))) as instance:
                result = materialize(
                    assets=list(definitions.get_repository_def().asset_graph.assets_defs),
                    instance=instance,
                    resources=definitions.resources,
                    selection=AssetSelection.assets("published_release"),
                )
                if not result.success:
                    raise RuntimeError(f"Publication failed: {result.run_id}")
                print(json.dumps({"run_id": result.run_id, "published": True}))
        case _:
            raise ValueError("Unknown action")


if __name__ == "__main__":
    main()
