import argparse
import json
from pathlib import Path

from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_plan import INPUT_STAGES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.definitions import DEFAULT_HOME
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.viewer import launch_ui, preview_asset


def execute_build(config: Path, home: Path) -> dict[str, bool]:
    spec = load_build_config(config, home)
    DatasetRegistry(home).register(spec)
    return execute_registered_build(spec.dataset_id, home)


def execute_registered_build(dataset_id: str, home: Path) -> dict[str, bool]:
    outcome = run_registered(dataset_id, home)
    print(outcome.model_dump_json())
    return outcome.cache_hits


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build category-configured datasets as typed DataFrame assets."
    )
    subcommands = parser.add_subparsers(dest="action", required=True)
    for action in ("build", "register", "validate"):
        sub = subcommands.add_parser(action)
        if action == "build":
            source = sub.add_mutually_exclusive_group(required=True)
            source.add_argument("--config", type=Path)
            source.add_argument(
                "--dataset", help="Rebuild the registered configuration without replacing it."
            )
        else:
            sub.add_argument("--config", type=Path, required=True)
        sub.add_argument("--home", type=Path, default=DEFAULT_HOME)
    ui = subcommands.add_parser("ui", help="Open the UI using the same home as dataset builds.")
    ui.add_argument("--home", type=Path, default=DEFAULT_HOME)
    ui.add_argument("--port", type=int, default=3000)
    show = subcommands.add_parser("show", help="Read a stored asset without running pipeline stages.")
    show.add_argument("--home", type=Path, default=DEFAULT_HOME)
    show.add_argument("--dataset", required=True)
    show.add_argument("--stage", choices=tuple(INPUT_STAGES), default="dataset_release")
    show.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    home = Path(args.home).resolve()
    if args.action == "ui":
        launch_ui(home, int(args.port))
        return
    if args.action == "show":
        print(
            json.dumps(
                preview_asset(home, str(args.dataset), str(args.stage), int(args.limit)),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.action == "build" and args.dataset is not None:
        execute_registered_build(str(args.dataset), home)
        return
    config = Path(args.config).resolve()
    if args.action == "build":
        execute_build(config, home)
    else:
        spec = load_build_config(config, home)
        if args.action == "register":
            DatasetRegistry(home).register(spec)
            with dataset_instance(home) as instance:
                instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        print(json.dumps({"valid": True, "dataset": spec.dataset_id, "category": spec.rules.category_id}))


if __name__ == "__main__":
    main()
