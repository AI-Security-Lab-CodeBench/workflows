from typing import Final, Literal

from pydantic import Field

from codebench_pipeline.rules_capture_facts import DECIMAL_ROLE, AnalysisRow, CaptureRow, allow_shape
from codebench_pipeline.rules_contracts import Fixture, Labels, Other
from codebench_pipeline.typed_config import ConfigArtifact

_MAX_CANDIDATES: Final = 8
_ALLOW_PROGRAMS: Final = frozenset({"base32", "base64", "date", "pwd"})


class CaptureCandidate(ConfigArtifact):
    id: str
    labels: Labels
    scope: Literal["simple", "ast"]
    when: str
    supporting_rows: tuple[str, ...]
    fixtures: tuple[Fixture, ...]


class CandidateProposal(ConfigArtifact):
    candidate_id: str = Field(pattern=r"^candidate-[1-8]$")
    source: str
    rationale: str = Field(min_length=16, max_length=500)


def capture_candidates(
    rows: tuple[CaptureRow, ...], analyses: dict[str, AnalysisRow],
) -> tuple[CaptureCandidate, ...]:
    """Build optional drafts only from reviewed labels and pinned native facts."""
    by_shape: dict[tuple[Labels, tuple[str, ...]], list[str]] = {}
    by_redirect: dict[tuple[Labels, str], list[str]] = {}
    for row in rows:
        if not isinstance(row.gold, Labels) or row.truncated:
            continue
        if row.gold.action == "allow":
            shape = allow_shape(analyses[row.id])
            if shape is not None and shape[0] in _ALLOW_PROGRAMS:
                by_shape.setdefault((row.gold, shape), []).append(row.id)
        else:
            for operator in (">", ">>"):
                if any(node.kind == "Redirect" and node.operator == operator for node in row.ast_nodes):
                    by_redirect.setdefault((row.gold, operator), []).append(row.id)
    candidates: list[CaptureCandidate] = []
    for (labels, shape), ids in by_shape.items():
        when = " && ".join((
            f"argv.size() == {len(shape)}",
            *(f"argv[{index}] == '{token}'" for index, token in enumerate(shape)
              if token and token != DECIMAL_ROLE),
        ))
        sample = " ".join("12" if part == DECIMAL_ROLE else part or "rehearsal.txt" for part in shape)
        varied = "  ".join("76" if part == DECIMAL_ROLE else part or "alternate.csv" for part in shape)
        candidates.append(CaptureCandidate(
            id=f"candidate-{len(candidates) + 1}", labels=labels, scope="simple", when=when,
            supporting_rows=tuple(ids), fixtures=(
                Fixture(command=sample, expected=labels), Fixture(command="  " + varied, expected=labels),
                Fixture(command=sample + " > capture-output.txt", expected=Other()),
            ),
        ))
        if len(candidates) == _MAX_CANDIDATES:
            return tuple(candidates)
    for (labels, operator), ids in by_redirect.items():
        if len(ids) < 2:
            continue
        candidates.append(CaptureCandidate(
            id=f"candidate-{len(candidates) + 1}", labels=labels, scope="ast",
            when=f"nodes.exists(n, n.Kind == 'Redirect' && n.Operator == '{operator}')",
            supporting_rows=tuple(ids), fixtures=(
                Fixture(command=f": {operator} rehearsal.txt", expected=labels),
                Fixture(command=f": {operator} alternate.csv", expected=labels),
                Fixture(command=": < rehearsal.txt", expected=Other()),
            ),
        ))
        if len(candidates) == _MAX_CANDIDATES:
            break
    return tuple(candidates)
