from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Literal

Branch = Literal["dataset", "documentation", "keenable"]


@dataclass(frozen=True, slots=True)
class CollectionBranch:
    namespace: Branch
    group: str
    job: str
    capture: str
    index: str | None = None

    @property
    def terminal(self) -> str:
        return stage_name("command_candidates", self.namespace)

    @property
    def declared_stages(self) -> tuple[str, ...]:
        names = (self.capture, "command_candidates")
        return (self.index, *names) if self.index is not None else names


COLLECTION_BRANCHES: Final = (
    CollectionBranch(
        "dataset",
        "existing_datasets",
        "build_existing_datasets",
        "capture_corpus_files",
        "enumerate_corpus_files",
    ),
    CollectionBranch(
        "documentation",
        "documentation_crawl",
        "build_documentation",
        "capture_documentation",
    ),
    CollectionBranch(
        "keenable",
        "keenable_use_cases",
        "build_keenable",
        "capture_origin_pages",
        "discover_use_cases",
    ),
)

BRANCH_BY_NAMESPACE: Final = {branch.namespace: branch for branch in COLLECTION_BRANCHES}
DECLARED_SOURCE_STAGES: Final = frozenset(
    name for branch in COLLECTION_BRANCHES for name in branch.declared_stages
)


def collection_branches(*, declared: bool) -> tuple[CollectionBranch, ...]:
    return COLLECTION_BRANCHES if declared else COLLECTION_BRANCHES[:2]


HISTORICAL_PROCESSING_STAGES = (
    "semantic_evidence",
    "human_review_snapshot",
    "merged_proposed_annotations",
    "documentation/filtered_commands",
    "documentation/labeling_documentation",
    "documentation/proposed_annotations",
)
FINALIZATION_GROUPS: Final = {
    "merged_command_candidates": "filtering",
    "filtered_commands": "filtering",
    "labeling_documentation": "labeling",
    "labeling_attempts": "labeling",
    "proposed_annotations": "labeling",
    "labeling_failures": "dataset_audit",
    "deduplicated_annotations": "deduplication",
    "category_eligible_annotations": "dataset_release",
    "human_review_queue": "dataset_release",
    "human_review_decisions": "dataset_release",
    "dataset_release": "dataset_release",
}
FINALIZATION_STAGES = tuple(FINALIZATION_GROUPS)


def stage_name(stage: str, branch: Branch = "dataset") -> str:
    if stage == "category_rules" or branch == "dataset":
        return stage
    return f"{branch}/{stage}"


def asset_path(stage: str, branch: Branch = "dataset") -> list[str]:
    return ["datasets", *stage_name(stage, branch).split("/")]


def stage_branch(stage: str) -> Branch:
    return asset_branch(("datasets", *stage.split("/")))


def asset_branch(path: Sequence[str]) -> Branch:
    if len(path) == 3 and path[0] == "datasets":
        for branch in COLLECTION_BRANCHES[1:]:
            if path[1] == branch.namespace:
                return branch.namespace
    if len(path) == 2 and path[0] == "datasets":
        return "dataset"
    raise ValueError("Unknown dataset branch asset")


def annotation_stage_id(branch: Branch) -> str:
    return "documentation_annotations" if branch == "documentation" else "annotations"


def automatic_stage_id(branch: Branch) -> str:
    return "documentation_automatic" if branch == "documentation" else "automatic"
