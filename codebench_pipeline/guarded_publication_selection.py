from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from math import isfinite
from typing import Final

from pydantic import JsonValue

from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.storage import fingerprint

SELECTION_REVISION: Final = "cat3-fair-cap-1"
REQUIRED_CONTRIBUTIONS: Final = ("docker-awesome-compose", "keenable", "kubernetes-examples")


@dataclass(frozen=True, slots=True)
class Survivor:
    identity: str
    branch: str
    origin: str
    risk: str
    operation: str
    classification: str
    uncertain: bool
    contribution: str = ""

    @property
    def stable_key(self) -> tuple[str, str]:
        return fingerprint([SELECTION_REVISION, self.identity]), self.identity


@dataclass(frozen=True, slots=True)
class Selection:
    ordered: tuple[str, ...]
    reservations: dict[str, str]
    coverage: dict[str, JsonValue]


def select_survivors(
    rows: list[Survivor], caps: dict[str, int], source_weights: dict[str, float] | None = None
) -> Selection:
    """Prefer verified source trust; reproduce source reservations for historical unweighted inputs."""
    if source_weights is not None and (
        source_weights.keys() != {row.identity for row in rows}
        or any(not isfinite(value) or not 0 <= value <= 1 for value in source_weights.values())
    ):
        raise HumanReviewError("stale", "Release selection requires a valid source weight for every survivor")
    weights = source_weights or {}
    ordered = sorted(rows, key=lambda row: (-weights.get(row.identity, 0.0), row.stable_key))
    if len({row.identity for row in ordered}) != len(ordered):
        raise HumanReviewError("conflict", "Release survivor identities are not unique")
    options: dict[str, dict[str, Survivor]] = {name: {} for name in REQUIRED_CONTRIBUTIONS}
    for row in ordered:
        if row.contribution in options and caps.get(row.risk, 0) > 0:
            options[row.contribution].setdefault(row.risk, row)

    def reserve(index: int, remaining: dict[str, int]) -> tuple[Survivor, ...] | None:
        if index == len(REQUIRED_CONTRIBUTIONS):
            return ()
        name = REQUIRED_CONTRIBUTIONS[index]
        for risk, row in sorted(options[name].items()):
            if remaining[risk] > 0:
                tail = reserve(index + 1, {**remaining, risk: remaining[risk] - 1})
                if tail is not None:
                    return (row, *tail)
        return None

    reserved = reserve(0, caps) if source_weights is None else ()
    if reserved is None:
        possibilities = {name: sorted(risks) for name, risks in options.items()}
        raise HumanReviewError(
            "pending", f"Mandatory source contributions cannot fit the risk caps: {possibilities}"
        )
    selected = list(reserved)
    used = {row.identity for row in reserved}
    risk_counts = Counter(row.risk for row in reserved)
    branch_counts = Counter((row.risk, row.branch) for row in reserved)
    origin_counts = Counter((row.risk, row.branch, row.origin) for row in reserved)
    buckets: dict[str, dict[str, dict[str, deque[Survivor]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(deque))
    )
    for row in ordered:
        if row.identity not in used and caps.get(row.risk, 0) > 0:
            buckets[row.risk][row.branch][row.origin].append(row)
    for risk, cap in sorted(caps.items()):
        branches = buckets[risk]
        while branches and risk_counts[risk] < cap:
            branch, origin = min(
                ((name, source) for name, origins in branches.items() for source in origins),
                key=lambda pair: (
                    -weights.get(branches[pair[0]][pair[1]][0].identity, 0.0),
                    branch_counts[risk, pair[0]],
                    pair[0],
                    origin_counts[risk, *pair],
                    pair[1],
                ),
            )
            origins = branches[branch]
            selected.append(origins[origin].popleft())
            risk_counts[risk] += 1
            branch_counts[risk, branch] += 1
            origin_counts[risk, branch, origin] += 1
            if not origins[origin]:
                del origins[origin]
            if not origins:
                del branches[branch]
    reservations = {row.contribution: row.identity for row in reserved}

    def frequencies(values: list[Survivor]) -> dict[str, JsonValue]:
        return {
            "branch": dict(Counter(row.branch for row in values)),
            "origin": dict(Counter(row.origin for row in values)),
            "risk": dict(Counter(row.risk for row in values)),
            "operation": dict(Counter(row.operation for row in values)),
            "class": dict(Counter(row.classification for row in values)),
            "uncertainty": dict(Counter("reviewed" if row.uncertain else "low" for row in values)),
        }

    coverage: dict[str, JsonValue] = {
        "before": frequencies(rows),
        "released": frequencies(selected),
        "risk_deficits": {risk: cap - risk_counts[risk] for risk, cap in caps.items()},
        "reservations": {name: identity for name, identity in reservations.items()},
        "excluded_by_cap": len(rows) - len(selected),
    }
    return Selection(tuple(row.identity for row in selected), reservations, coverage)
