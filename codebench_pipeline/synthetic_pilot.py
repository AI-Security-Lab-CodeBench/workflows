import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp
from typing import Final, Literal

from dagster import DagsterInstance, MaterializeResult, Nothing, ResourceParam, asset, materialize, resource
from dagster._core.definitions.utils import NoValueSentinel
from dagster._core.types.dagster_type import resolve_dagster_type
from pydantic import ConfigDict, Field

from codebench_pipeline.business_config import DEFAULT_BUSINESS_CONFIG, load_business_rules
from codebench_pipeline.category_rules import StrictModel
from codebench_pipeline.synthetic_pilot_policy import OPERATIONS, BackendMode, PilotError, Seed, VendorConfig
from codebench_pipeline.synthetic_pilot_vendor import Runtime

_ROOT: Final = Path(__file__).resolve().parents[1]
_CONFIG: Final = _ROOT / "business-configs" / "synthetic-pilot"
_CHILD: Final = (
    "import sys; from pathlib import Path; sys.path.insert(0, sys.argv.pop(1)); "
    "from codebench_pipeline.synthetic_pilot_vendor import run_vendor; run_vendor(Path(sys.argv[1]))"
)


class VendorReport(StrictModel):
    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    calls: dict[Literal["ok"], int]
    final_rows: int = Field(gt=0, le=len(OPERATIONS))


@dataclass(frozen=True, slots=True)
class PilotResult:
    run_dir: Path
    vendor_report: VendorReport


@dataclass(frozen=True, slots=True)
class PilotConfiguration:
    vendor: VendorConfig
    seeds: tuple[Seed, ...]
    prompt: bytes
    taxonomy: tuple[tuple[str, str], ...]


def load_pilot_configuration(config_dir: Path) -> PilotConfiguration:
    config = VendorConfig.model_validate_json((config_dir / "vendor.json").read_bytes())
    taxonomy = load_business_rules(DEFAULT_BUSINESS_CONFIG, category_id="CAT-3")
    operations_risks = set(OPERATIONS.values())
    if (
        taxonomy.category_id != "CAT-3"
        or not operations_risks <= {risk.id for risk in taxonomy.risks}
        or "allow" not in {label.name for label in taxonomy.classes}
        or "low" not in {label.name for label in taxonomy.severities}
    ):
        raise PilotError("CAT-3 taxonomy does not support the pilot inspection policy")

    seeds = tuple(
        Seed.model_validate_json(line)
        for line in (config_dir / "seeds.jsonl").read_bytes().splitlines()
        if line.strip()
    )
    if (
        not 1 <= len(seeds) <= len(OPERATIONS)
        or len({seed.seed_id for seed in seeds}) != len(seeds)
        or any(seed.split != config.split or seed.risk not in operations_risks for seed in seeds)
    ):
        raise PilotError("Pilot seeds must be distinct, match the split, and use a pilot inspection risk")
    return PilotConfiguration(
        vendor=config,
        seeds=seeds,
        prompt=(config_dir / "prompt.md").read_bytes(),
        taxonomy=(
            (taxonomy.category_id, taxonomy.canonical),
            *((risk.id, risk.canonical) for risk in taxonomy.risks),
        ),
    )


@asset(
    group_name="configuration",
    description=(
        "How it works: Validates the authored vendor settings and inspection seeds against the CAT-3 "
        "taxonomy, then persists the typed settings, seeds, prompt and taxonomy names for the pilot."
    ),
)
def cat3_synthetic_pilot_config() -> PilotConfiguration:
    return load_pilot_configuration(_CONFIG)


@resource
def pilot_runtime() -> Callable[[BackendMode], Runtime]:
    return Runtime.from_environment


def run_pilot(config_dir: Path = _CONFIG) -> PilotResult:
    """Run the isolated CAT-3 pilot from a standalone authored configuration directory."""
    configuration = load_pilot_configuration(config_dir)
    return execute_pilot(configuration, Runtime.from_environment(configuration.vendor.backend.mode))


def execute_pilot(configuration: PilotConfiguration, runtime: Runtime) -> PilotResult:
    config, seeds = configuration.vendor, configuration.seeds

    runtime.output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    work = Path(mkdtemp(prefix="cat3-pilot-", dir=runtime.output_dir))
    seeds_path = work / "seeds.jsonl"
    seeds_path.write_text("".join(seed.model_dump_json() + "\n" for seed in seeds), encoding="utf-8")
    (work / "prompt.md").write_bytes(configuration.prompt)
    taxonomy_path = work / "taxonomy.json"
    taxonomy_path.write_text(json.dumps(dict(configuration.taxonomy), ensure_ascii=False), encoding="utf-8")
    config_path = work / "vendor.json"
    vendor_config = config.model_dump(mode="json")
    vendor_config["backend"].update(base_url=runtime.endpoint, model=runtime.model)
    vendor_config.update(
        output_dir=str(work),
        taxonomy_path=str(taxonomy_path),
        labels={"classes": ["allow"], "severities": ["low"]},
        dedup={"against": []},
        validate={"bash_syntax_check": True, "flag_check": "reject", "label_anchors": True},
        export={"seed_column": "seed_id"},
    )
    config_path.write_text(json.dumps(vendor_config), encoding="utf-8")
    with (work / "vendor.log").open("wb") as log:
        process = subprocess.run(
            [sys.executable, "-I", "-B", "-c", _CHILD, str(_ROOT), str(config_path)],
            cwd=work,
            env=runtime.child_environment(),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if process.returncode != 0:
        raise PilotError(f"Vendor pilot exited with status {process.returncode}; see {work / 'vendor.log'}")
    run_dir = work / config.run_name
    report_path = (run_dir / "report.json").resolve()
    if not report_path.is_relative_to(work.resolve()):
        raise PilotError(f"Vendor pilot report escapes its work directory: {run_dir / 'report.json'}")
    report = VendorReport.model_validate_json(report_path.read_bytes())
    if report.calls.get("ok") != len(seeds) or not (run_dir / "dataset.csv").is_file():
        raise PilotError(f"Vendor pilot did not complete every seed or export the validation result: {work}")
    return PilotResult(run_dir, report)


@asset(
    dagster_type=resolve_dagster_type(Nothing),
    group_name="synthetic_pilot",
    description=(
        "How it works: The pilot renders authored CAT-3 inspection seeds with the host taxonomy, "
        "then runs the vendor CLI. Guarded SGLang calls use the shared capacity ledger; "
        "plain calls use the configured OpenAI-compatible endpoint without querying metrics. "
        "Only approved read-only command forms reach vendor validation; the asset exposes paths "
        "to the local artifacts and row counts after success. The pilot does not publish a dataset release."
    ),
)
def cat3_synthetic_pilot(
    cat3_synthetic_pilot_config: PilotConfiguration,
    pilot_runtime: ResourceParam[Callable[[BackendMode], Runtime]],
) -> MaterializeResult[type[NoValueSentinel]]:
    mode = cat3_synthetic_pilot_config.vendor.backend.mode
    result = execute_pilot(cat3_synthetic_pilot_config, pilot_runtime(mode))
    return MaterializeResult(
        metadata={
            "run_dir": str(result.run_dir),
            "rows": result.vendor_report.final_rows,
            "backend_mode": mode,
            "model_calls": result.vendor_report.calls["ok"],
        }
    )


if __name__ == "__main__":
    with DagsterInstance.get() as instance:
        result = materialize(
            [cat3_synthetic_pilot_config, cat3_synthetic_pilot],
            resources={"pilot_runtime": pilot_runtime},
            instance=instance,
        )
        print(result.run_id)
