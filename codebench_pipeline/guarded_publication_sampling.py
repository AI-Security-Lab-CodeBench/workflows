import re
from dataclasses import dataclass
from typing import Final, Literal

from codebench_pipeline.dataset_branches import COLLECTION_BRANCHES
from codebench_pipeline.grounding_policy import command_tokens
from codebench_pipeline.guarded_publication_selection import REQUIRED_CONTRIBUTIONS, Survivor
from codebench_pipeline.labeling_outcomes import LabelingAttempt

SAMPLE_MINIMUM: Final = 10
INSPECTION: Final = re.compile(
    r"^(?:(?:kubectl|oc) (?:get|describe|logs|api-resources|diff|rollout status|config (?:view|current-context))"
    r"|helm (?:list|ls|status|history|template)"
    r"|(?:terraform|tofu) (?:plan|validate|show|state (?:list|show))"
    r"|pulumi preview"
    r"|(?:docker|podman|nerdctl) (?:(?:(?:compose|container|image|volume|network) )?"
    r"(?:ps|ls|list|inspect|images|stats|logs)|system df|compose config)"
    r"|(?:docker-compose|podman-compose) (?:ps|logs|config)"
    r"|aws (?:s3 ls|s3api (?:list|head)-[\w-]+))(?= |$)"
)
STATE_CHANGING: Final = re.compile(
    r"^(?:(?:kubectl|oc) (?:apply|create|delete|edit|replace|patch|scale|set|expose|"
    r"rollout (?:restart|undo|pause|resume))"
    r"|helm (?:install|upgrade|uninstall|delete|rollback)"
    r"|(?:terraform|tofu) (?:apply|destroy|state (?:mv|rm|push))|pulumi (?:up|destroy)"
    r"|(?:docker|podman|nerdctl) (?:compose (?:up|down|start|stop|restart|run|rm|pull|build)"
    r"|stack deploy|(?:(?:system|container|volume|image|builder|network) )?(?:prune|rm|create)"
    r"|run|start|stop|restart|pull|build)"
    r"|(?:docker-compose|podman-compose) (?:up|down|start|stop|restart|run|rm|pull|build)"
    r"|aws (?:s3 (?:rm|rb)|s3api (?:delete|put)-[\w-]+)"
    r"|(?:gcloud storage|gsutil|mc) (?:rm|rb)|rclone (?:delete|purge)|dropdb)(?= |$)"
)
LABEL_CHANGE: Final = re.compile(r"[a-zA-Z0-9][\w.-]*(?:/[\w.-]+)?(?:=[\w.-]*|-)", re.ASCII)
RESOURCE_REF: Final = re.compile(r"[a-zA-Z0-9][\w.-]*/[a-zA-Z0-9][\w.-]*", re.ASCII)
Semantics = Literal["inspection", "state_changing", "unclassified"]
CoverageStatus = Literal["sampled", "present_but_unsampled", "absent", "unclassified"]


def _semantics(attempt: LabelingAttempt) -> Semantics:
    """Classify the verified literal invocation without treating labels as operation effects."""
    tokens = command_tokens(attempt)
    operation = " ".join(token if not any(char.isspace() for char in token) else "\0" for token in tokens)
    if any(flag in tokens for flag in ("--help", "-h")):
        return "unclassified"
    if tokens[:2] in (["kubectl", "label"], ["oc", "label"]):
        if len(tokens) < 4 or not RESOURCE_REF.fullmatch(tokens[2]):
            return "unclassified"
        arguments = tokens[3:]
        if arguments in (["--list"], ["--list=true"]):
            return "inspection"
        changes = [token for token in arguments if token not in {"--overwrite", "--overwrite=true"}]
        if changes and all(LABEL_CHANGE.fullmatch(token) for token in changes):
            return "state_changing"
        return "unclassified"
    if attempt.command in {r"\dt", r"\dn"} or INSPECTION.match(operation):
        return "inspection"
    if STATE_CHANGING.match(operation):
        if any(token.startswith("--dry-run") and token != "--dry-run=false" for token in tokens):
            return "unclassified"
        return "state_changing"
    return "unclassified"


@dataclass(frozen=True, slots=True)
class StratumCoverage:
    population: int
    sampled_row_ids: list[str]
    status: CoverageStatus

    @classmethod
    def from_rows(cls, rows: list[str], chosen: set[str], *, unclassified: bool) -> "StratumCoverage":
        sampled = [identity for identity in rows if identity in chosen]
        status: CoverageStatus = "absent"
        if unclassified:
            status = "unclassified"
        elif rows:
            status = "sampled" if sampled else "present_but_unsampled"
        return cls(len(rows), sampled, status)


@dataclass(frozen=True, slots=True)
class BranchSample:
    population: int
    sampled_row_ids: list[str]
    strata: dict[str, StratumCoverage]
    unclassified_row_ids: list[str]


def sample_survivors(rows: list[Survivor], attempts: dict[str, LabelingAttempt]) -> dict[str, BranchSample]:
    """Choose a stable witness for each populated stratum, then fill the per-branch minimum."""
    ordered = sorted(rows, key=lambda row: row.stable_key)
    sampling: dict[str, BranchSample] = {}
    for branch in COLLECTION_BRANCHES:
        population = [row for row in ordered if row.branch == branch.namespace]
        origins = {row.origin for row in population}
        if branch.namespace == "dataset":
            origins.update(set(REQUIRED_CONTRIBUTIONS) - {"keenable"})
        strata: dict[str, list[str]] = {
            **{f"origin:{origin}": [] for origin in sorted(origins)},
            "semantics:inspection": [],
            "semantics:state_changing": [],
            "semantics:unclassified": [],
            "route:automatic": [],
            "route:human_reviewed": [],
        }
        for row in population:
            strata[f"origin:{row.origin}"].append(row.identity)
            strata[f"semantics:{_semantics(attempts[row.identity])}"].append(row.identity)
            strata[f"route:{'human_reviewed' if row.uncertain else 'automatic'}"].append(row.identity)
        chosen = {ids[0] for ids in strata.values() if ids}
        for row in population:
            if len(chosen) >= SAMPLE_MINIMUM:
                break
            chosen.add(row.identity)
        unclassified = strata["semantics:unclassified"]
        sampling[branch.namespace] = BranchSample(
            len(population),
            [row.identity for row in population if row.identity in chosen],
            {
                name: StratumCoverage.from_rows(
                    ids,
                    chosen,
                    unclassified=bool(unclassified)
                    and (name == "semantics:unclassified" or (name.startswith("semantics:") and not ids)),
                )
                for name, ids in strata.items()
            },
            unclassified,
        )
    return sampling
