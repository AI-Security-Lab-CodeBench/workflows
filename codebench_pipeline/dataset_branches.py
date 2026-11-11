from collections.abc import Sequence
from typing import Literal

Branch = Literal["dataset", "documentation"]
FINALIZATION_STAGES = (
    "merged_proposed_annotations",
    "reviewed_annotations",
    "deduplicated_annotations",
    "human_review_queue",
    "human_review_decisions",
    "dataset_release",
)


def stage_name(stage: str, branch: Branch = "dataset") -> str:
    if stage == "category_rules" or branch == "dataset":
        return stage
    return f"documentation/{stage}"


def asset_path(stage: str, branch: Branch = "dataset") -> list[str]:
    return ["datasets", *stage_name(stage, branch).split("/")]


def stage_branch(stage: str) -> Branch:
    return "documentation" if stage.startswith("documentation/") else "dataset"


def asset_branch(path: Sequence[str]) -> Branch:
    if len(path) == 3 and tuple(path[:2]) == ("datasets", "documentation"):
        return "documentation"
    if len(path) == 2 and path[0] == "datasets":
        return "dataset"
    raise ValueError("Unknown dataset branch asset")


def annotation_stage_id(branch: Branch) -> str:
    return "documentation_annotations" if branch == "documentation" else "annotations"


def automatic_stage_id(branch: Branch) -> str:
    return "documentation_automatic" if branch == "documentation" else "automatic"
