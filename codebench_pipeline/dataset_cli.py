import argparse
import json
from pathlib import Path
from typing import Literal, TypedDict, overload

from dagster import AssetKey, DagsterRunStatus, RunsFilter
from pydantic import TypeAdapter

from codebench_pipeline.dataset_branches import HISTORICAL_PROCESSING_STAGES
from codebench_pipeline.dataset_config import load_build_config
from codebench_pipeline.dataset_dependencies import KNOWN_STAGES
from codebench_pipeline.dataset_registry import DatasetRegistry
from codebench_pipeline.dataset_run import refresh_registered_configuration, run_registered
from codebench_pipeline.definitions import DEFAULT_HOME, build_definitions, dataset_run_config
from codebench_pipeline.guarded_publication_controls import Action
from codebench_pipeline.guarded_publication_runtime import preview_proposal, update_publication_control
from codebench_pipeline.models import Digest
from codebench_pipeline.runtime_instance import dataset_instance
from codebench_pipeline.stage_configuration import stage_configs
from codebench_pipeline.viewer import launch_ui, preview_asset


class ConfigurationRefresh(TypedDict):
    dataset: str
    configuration_run_id: str
    status: Literal["configuration_refreshed"]
    next: str


def execute_build(
    config: Path, home: Path, *, prepare_only: bool = False, force: bool = False
) -> ConfigurationRefresh | dict[str, bool]:
    spec = load_build_config(config, home)
    DatasetRegistry(home).register(spec)
    return execute_registered_build(spec.dataset_id, home, prepare_only=prepare_only, force=force)


@overload
def execute_registered_build(
    dataset_id: str, home: Path, *, prepare_only: bool = False, force: Literal[False] = False
) -> ConfigurationRefresh: ...


@overload
def execute_registered_build(
    dataset_id: str, home: Path, *, prepare_only: bool = False, force: Literal[True]
) -> dict[str, bool]: ...


@overload
def execute_registered_build(
    dataset_id: str, home: Path, *, prepare_only: bool = False, force: bool
) -> ConfigurationRefresh | dict[str, bool]: ...


def execute_registered_build(
    dataset_id: str, home: Path, *, prepare_only: bool = False, force: bool = False
) -> ConfigurationRefresh | dict[str, bool]:
    if not force:
        receipt: ConfigurationRefresh = {
            "dataset": dataset_id,
            "configuration_run_id": refresh_registered_configuration(dataset_id, home),
            "status": "configuration_refreshed",
            "next": "Check the partition's assets and dataset_build_requests sensor in Dagster.",
        }
        print(json.dumps(receipt))
        return receipt
    outcome = run_registered(dataset_id, home, prepare_only=prepare_only)
    print(outcome.model_dump_json())
    return outcome.cache_hits


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build category-configured datasets as typed DataFrame assets."
    )
    subcommands = parser.add_subparsers(dest="action", required=True)
    for name in ("stages", "through", "check-idle"):
        command = subcommands.add_parser(name)
        command.add_argument("--home", type=Path, default=DEFAULT_HOME)
        if name == "through":
            command.add_argument("--dataset", required=True)
            command.add_argument("--stage", required=True)
    for action in ("build", "register", "validate", "inspect-config"):
        sub = subcommands.add_parser(
            action,
            help="Record the partition configuration for native Dagster automation."
            if action == "build"
            else None,
        )
        if action == "build":
            sub.add_argument(
                "--force",
                action="store_true",
                help="Run the full named Dagster job synchronously, bypassing incremental automation.",
            )
            sub.add_argument(
                "--prepare-only",
                action="store_true",
                help="With --force, materialize collection and filtering without labeling or review.",
            )
            source = sub.add_mutually_exclusive_group(required=True)
            source.add_argument("--config", type=Path)
            source.add_argument("--dataset", help="Use the registered configuration without replacing it.")
        else:
            sub.add_argument("--config", type=Path, required=True)
        sub.add_argument("--home", type=Path, default=DEFAULT_HOME)
        if action == "inspect-config":
            sub.add_argument("--stage", required=True, choices=sorted(KNOWN_STAGES))
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
    if args.action == "check-idle":
        finished = {DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED}
        with dataset_instance(args.home) as instance:
            if instance.get_runs_count(
                filters=RunsFilter(statuses=[s for s in DagsterRunStatus if s not in finished])
            ):
                raise SystemExit("Drain active and queued Dagster runs before replacing the application.")
        return
    if args.action in {"stages", "through"}:
        home = args.home
        definitions = build_definitions(home)
        job = definitions.resolve_job_def("build_dataset")
        available = set(job.asset_layer.executable_asset_keys)
        if args.action == "stages":
            for key in sorted(key.to_user_string() for key in available):
                print(key)
            return
        target = AssetKey.from_user_string(args.stage)
        if target not in available:
            parser.error("Unknown build stage; use a full key from python3 scripts/stack.py stages")
        selected = (
            {target} | set(definitions.resolve_asset_graph().get_ancestor_asset_keys(target))
        ) & available
        spec = DatasetRegistry(home).get(args.dataset)
        with dataset_instance(home) as instance:
            result = job.execute_in_process(
                instance=instance,
                partition_key=args.dataset,
                asset_selection=list(selected),
                run_config=dataset_run_config(spec),
            )
        print(json.dumps({"run_id": result.run_id, "through": args.stage}))
        return
    if args.action == "build" and args.prepare_only and not args.force:
        parser.error("--prepare-only requires --force")
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
        execute_registered_build(str(args.dataset), home, prepare_only=args.prepare_only, force=args.force)
        return
    config = Path(args.config).resolve()
    if args.action == "build":
        execute_build(config, home, prepare_only=args.prepare_only, force=args.force)
    else:
        spec = load_build_config(config, home)
        if args.action == "inspect-config":
            configs = stage_configs(spec)
            stage = str(args.stage)
            view = {"stage": stage, "settings": configs[stage].model_dump(mode="json")}
            print(json.dumps(view, ensure_ascii=False, indent=2))
            return
        if args.action == "register":
            DatasetRegistry(home).register(spec)
            with dataset_instance(home) as instance:
                instance.add_dynamic_partitions("dataset_builds", [spec.dataset_id])
        print(json.dumps({"valid": True, "dataset": spec.dataset_id, "category": spec.rules.category_id}))


if __name__ == "__main__":
    main()
