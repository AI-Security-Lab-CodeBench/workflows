from __future__ import annotations

from pathlib import Path

from codebench_pipeline.cli import process
from codebench_pipeline.demo import prepare_demo
from codebench_pipeline.models import FrozenModel
from codebench_pipeline.storage import IntegrityError, Registry, digest


class IncrementalEvidence(FrozenModel):
    first_dataset: str
    second_dataset: str
    baseline_run: str
    new_dataset_run: str
    replay_run: str
    immutable_files: int
    unchanged: bool
    replay_cache_hits: dict[str, bool]


def verify_incremental(root: Path) -> IncrementalEvidence:
    repo = Path(__file__).resolve().parent.parent
    first = prepare_demo(repo, root / "demo-inputs/main-label-00003", "main-label-00003.json")
    registry = Registry(root)
    registry.register(first)
    baseline = process(root, first.dataset_id)
    first_root = root / "artifacts" / first.dataset_id
    before = {str(path): digest(path) for path in first_root.rglob("*") if path.is_file()}
    second = prepare_demo(repo, root / "demo-inputs/main-label-00004", "main-label-00004.json")
    registry.register(second)
    addition = process(root, second.dataset_id)
    replay = process(root, first.dataset_id)
    after = {str(path): digest(path) for path in first_root.rglob("*") if path.is_file()}
    if before != after or not all(replay.cache_hits.values()):
        raise IntegrityError("Incremental replay changed completed work or recomputed a stage")
    evidence = IncrementalEvidence(
        first_dataset=first.dataset_id,
        second_dataset=second.dataset_id,
        baseline_run=baseline.run_id,
        new_dataset_run=addition.run_id,
        replay_run=replay.run_id,
        immutable_files=len(before),
        unchanged=True,
        replay_cache_hits=replay.cache_hits,
    )
    destination = root / f"incremental-{replay.run_id}.json"
    destination.write_text(evidence.model_dump_json(indent=2), encoding="utf-8")
    print(evidence.model_dump_json())
    return evidence
