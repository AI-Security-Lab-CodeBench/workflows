import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp
from typing import Final, Literal

from dagster import DagsterInstance, MaterializeResult, Nothing, asset, materialize
from dagster._core.definitions.utils import NoValueSentinel
from dagster._core.types.dagster_type import resolve_dagster_type
from pydantic import ConfigDict, Field

from codebench_pipeline.business_config import DEFAULT_BUSINESS_CONFIG, BusinessCategoryConfig
from codebench_pipeline.category_rules import StrictModel
from codebench_pipeline.synthetic_pilot_policy import OPERATIONS, PilotError, Seed, VendorConfig
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


def run_pilot(config_dir: Path = _CONFIG) -> PilotResult:
    """Run the isolated CAT-3 pilot and return a receipt for successful vendor validation."""
    runtime = Runtime.from_environment()
    config = VendorConfig.model_validate_json((config_dir / "vendor.json").read_bytes())
    taxonomy = BusinessCategoryConfig.model_validate_json(DEFAULT_BUSINESS_CONFIG.read_bytes()).rules
    operations_risks = set(OPERATIONS.values())
    if (
        taxonomy.category_id != "CAT-3"
        or not operations_risks <= {risk.id for risk in taxonomy.risks}
        or "allow" not in {label.name for label in taxonomy.classes}
        or "low" not in {label.name for label in taxonomy.severities}
    ):
        raise PilotError("CAT-3 taxonomy does not support the pilot inspection policy")

    seeds = [
        Seed.model_validate_json(line)
        for line in (config_dir / "seeds.jsonl").read_bytes().splitlines()
        if line.strip()
    ]
    if (
        not 1 <= len(seeds) <= len(OPERATIONS)
        or len({seed.seed_id for seed in seeds}) != len(seeds)
        or any(seed.split != config.split or seed.risk not in operations_risks for seed in seeds)
    ):
        raise PilotError("Pilot seeds must be distinct, match the split, and use a pilot inspection risk")
    prompt = (config_dir / "prompt.md").read_bytes()

    runtime.output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    work = Path(mkdtemp(prefix="cat3-pilot-", dir=runtime.output_dir))
    seeds_path = work / "seeds.jsonl"
    seeds_path.write_text("".join(seed.model_dump_json() + "\n" for seed in seeds), encoding="utf-8")
    (work / "prompt.md").write_bytes(prompt)
    taxonomy_path = work / "taxonomy.json"
    names = {taxonomy.category_id: taxonomy.canonical, **{risk.id: risk.canonical for risk in taxonomy.risks}}
    taxonomy_path.write_text(json.dumps(names, ensure_ascii=False), encoding="utf-8")
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
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if process.returncode != 0:
        raise PilotError(f"Vendor pilot exited with status {process.returncode}; see {work / 'vendor.log'}")
    run_dir = work / config.run_name
    report = VendorReport.model_validate_json((run_dir / "report.json").read_bytes())
    if report.calls.get("ok") != len(seeds) or not (run_dir / "dataset.csv").is_file():
        raise PilotError(f"Vendor pilot did not complete every seed or export the validation result: {work}")
    return PilotResult(run_dir, report)


@asset(
    dagster_type=resolve_dagster_type(Nothing),
    group_name="synthetic_pilot",
    description=(
        "How it works: The pilot renders authored CAT-3 inspection seeds with the host taxonomy, "
        "then runs the vendor CLI. Every model call uses the shared capacity ledger. "
        "Only approved read-only command forms reach vendor validation; the asset exposes paths "
        "to the local artifacts and row counts after success. The pilot does not publish a dataset release."
    ),
)
def cat3_synthetic_pilot() -> MaterializeResult[type[NoValueSentinel]]:
    result = run_pilot()
    return MaterializeResult(
        metadata={
            "run_dir": str(result.run_dir),
            "rows": result.vendor_report.final_rows,
            "guarded_calls": result.vendor_report.calls["ok"],
        }
    )


if __name__ == "__main__":
    with DagsterInstance.get() as instance:
        result = materialize([cat3_synthetic_pilot], instance=instance)
        print(result.run_id)
