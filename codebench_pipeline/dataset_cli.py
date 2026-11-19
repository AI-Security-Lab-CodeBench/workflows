import argparse
import json
from pathlib import Path

from pydantic import TypeAdapter

from codebench_pipeline.dataset_branches import HISTORICAL_PROCESSING_STAGES
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_plan import KNOWN_STAGES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import run_registered
from codebench_pipeline.definitions import DEFAULT_HOME
from codebench_pipeline.guarded_publication_controls import Action
from codebench_pipeline.guarded_publication_runtime import preview_proposal, update_publication_control
from codebench_pipeline.models import Digest
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.storage import IntegrityError
from codebench_pipeline.viewer import launch_ui, preview_asset


def execute_build(config: Path, home: Path, *, prepare_only: bool = False) -> dict[str, bool]:
    spec = load_build_config(config, home)
    if prepare_only and not spec.has_declared_collection:
        raise IntegrityError("Preparation-only builds require a declared collection")
    DatasetRegistry(home).register(spec)
    return execute_registered_build(spec.dataset_id, home, prepare_only=prepare_only)


def execute_registered_build(dataset_id: str, home: Path, *, prepare_only: bool = False) -> dict[str, bool]:
    outcome = run_registered(dataset_id, home, prepare_only=prepare_only)
    print(outcome.model_dump_json())
    return outcome.cache_hits


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build category-configured datasets as typed DataFrame assets."
    )
    subcommands = parser.add_subparsers(dest="action", required=True)
    for action in ("build", "register", "validate", "inspect-config"):
        sub = subcommands.add_parser(action)
        if action == "build":
            sub.add_argument(
                "--prepare-only",
                action="store_true",
                help="Materialize collection and filtering without labeling, review, or release.",
            )
            source = sub.add_mutually_exclusive_group(required=True)
            source.add_argument("--config", type=Path)
            source.add_argument(
                "--dataset", help="Rebuild the registered configuration without replacing it."
            )
        else:
            sub.add_argument("--config", type=Path, required=True)
        sub.add_argument("--home", type=Path, default=DEFAULT_HOME)
        if action == "inspect-config":
            sub.add_argument("--stage", required=True, choices=sorted(KNOWN_STAGES))
    bind = subcommands.add_parser("bind-business", help="Attach labeling policy to a registered dataset.")
    bind.add_argument("--dataset", required=True)
    bind.add_argument("--business-config", type=Path, required=True)
    bind.add_argument("--home", type=Path, default=DEFAULT_HOME)
    ui = subcommands.add_parser("ui", help="Open the UI using the same home as dataset builds.")
    ui.add_argument("--home", type=Path, default=DEFAULT_HOME)
    ui.add_argument("--port", type=int, default=3000)
    show = subcommands.add_parser("show", help="Read a stored asset without running pipeline stages.")
    show.add_argument("--home", type=Path, default=DEFAULT_HOME)
    show.add_argument("--dataset", required=True)
    show.add_argument(
        "--stage", choices=(*sorted(KNOWN_STAGES), *HISTORICAL_PROCESSING_STAGES), default="dataset_release"
    )
    show.add_argument("--limit", type=int, default=5)
    for name in ("preview-release", "hold-release", "clear-release-hold", "attest-release", "revoke-release"):
        control = subcommands.add_parser(name)
        control.add_argument("--dataset", required=True)
        control.add_argument("--home", type=Path, default=DEFAULT_HOME)
        if name in {"attest-release", "revoke-release"}:
            control.add_argument("--proposal", required=True)
    args = parser.parse_args()
    home = Path(args.home).resolve()
    if args.action == "preview-release":
        with dataset_instance(home) as instance:
            proposal, path = preview_proposal(home, instance, args.dataset)
        print(json.dumps({"proposal": proposal, "preview": str(path)}))
        return
    controls: dict[str, Action] = {
        "hold-release": "hold",
        "clear-release-hold": "clear",
        "attest-release": "attest",
        "revoke-release": "revoke",
    }
    if args.action in controls:
        action = controls[args.action]
        proposal = (
            TypeAdapter(Digest).validate_python(args.proposal) if action in {"attest", "revoke"} else ""
        )
        with dataset_instance(home) as instance:
            update_publication_control(home, instance, args.dataset, action, proposal)
        print(json.dumps({"dataset": args.dataset, "action": action, "proposal": proposal}))
        return
    if args.action == "bind-business":
        spec = DatasetRegistry(home).bind_business(str(args.dataset), Path(args.business_config))
        print(json.dumps({"dataset": spec.dataset_id, "business_config": spec.business_config}))
        return
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
        execute_registered_build(str(args.dataset), home, prepare_only=args.prepare_only)
        return
    config = Path(args.config).resolve()
    if args.action == "build":
        execute_build(config, home, prepare_only=args.prepare_only)
    else:
        spec = load_build_config(config, home)
        if args.action == "inspect-config":
            configs = stage_configs(spec)
            stage = str(args.stage)
            view = {"stage": stage, "settings": configs[stage].model_dump(mode="json")}
            if stage == "labeling_attempts":
                view["policy"] = spec.rules.model_dump(mode="json")
            print(json.dumps(view, ensure_ascii=False, indent=2))
            return
        if args.action == "register":
            DatasetRegistry(home).register(spec)
            with dataset_instance(home) as instance:
                instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        print(json.dumps({"valid": True, "dataset": spec.dataset_id, "category": spec.rules.category_id}))


if __name__ == "__main__":
    main()
