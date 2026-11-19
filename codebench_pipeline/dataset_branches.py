from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Literal

Branch = Literal["dataset", "documentation", "keenable"]


@dataclass(frozen=True, slots=True)
class CollectionBranch:
    namespace: Branch
    group: str
    job: str

    @property
    def terminal(self) -> str:
        return stage_name("command_candidates", self.namespace)


COLLECTION_BRANCHES: Final = (
    CollectionBranch("dataset", "existing_datasets", "build_existing_datasets"),
    CollectionBranch("documentation", "documentation_crawl", "build_documentation"),
    CollectionBranch("keenable", "keenable_use_cases", "build_keenable"),
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
FINALIZATION_STAGES = (
    "merged_command_candidates",
    "filtered_commands",
    "labeling_documentation",
    "labeling_attempts",
    "proposed_annotations",
    "labeling_failures",
    "deduplicated_annotations",
    "category_eligible_annotations",
    "human_review_queue",
    "human_review_decisions",
    "dataset_release",
)


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
