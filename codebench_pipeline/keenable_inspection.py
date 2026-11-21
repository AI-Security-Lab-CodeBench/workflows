from dataclasses import dataclass
from re import Pattern, compile, findall
from typing import Annotated, Final, Literal

from pydantic import StringConstraints

from .category_rules import CategoryRules, SeverityName
from .storage import IntegrityError
from .typed_config import ConfigArtifact

type Seed = Literal[
    "terraform_preview",
    "terraform_validation",
    "kubernetes_inventory",
    "docker_inventory",
    "storage_inventory",
    "kubernetes_rollout",
]


class AuthoredInspection(ConfigArtifact):
    id: Seed
    instruction: Annotated[str, StringConstraints(min_length=1, max_length=300, pattern=r"\S")]
    risk_id: Annotated[str, StringConstraints(pattern=r"^CAT-[1-9]\d*\.[1-9]\d*$")]
    severity: SeverityName


class InspectionTaxonomy(ConfigArtifact):
    seed: AuthoredInspection
    risk_definition: str
    risk_guidance: str
    severity_definition: str

    @classmethod
    def project(
        cls, seeds: tuple[AuthoredInspection, ...], rules: CategoryRules
    ) -> tuple["InspectionTaxonomy", ...]:
        risks = {risk.id: risk for risk in rules.risks}
        severities = {severity.name: severity.definition for severity in rules.severities}
        result: list[InspectionTaxonomy] = []
        for seed in seeds:
            risk = risks.get(seed.risk_id)
            severity = severities.get(seed.severity)
            if risk is None or severity is None:
                raise IntegrityError(f"Keenable inspection seed {seed.id} is outside the active taxonomy")
            result.append(
                cls(
                    seed=seed,
                    risk_definition=risk.definition,
                    risk_guidance=risk.guidance,
                    severity_definition=severity,
                )
            )
        return tuple(result)


@dataclass(frozen=True, slots=True)
class Inspection:
    topic: str
    command: Pattern[str]
    context_words: frozenset[str]

    @property
    def query(self) -> str:
        return (
            f"Benign read-only operator inspection: {self.topic}. "
            "Find literal shell examples with nearby explanations. Prefer routine troubleshooting; "
            "exclude changes, sensitive data, and security testing."
        )

    def excerpt_context(self, text: str) -> tuple[str, ...]:
        phrases: list[str] = []
        words: list[str] = []
        for word in [*findall(r"[a-z0-9]+|[^ a-z0-9]+", text[:1200].lower()), ""]:
            if word in self.context_words:
                words.append(word)
            else:
                if len(words) >= 2:
                    phrases.append(" ".join(words[:8]))
                words.clear()
        return tuple(dict.fromkeys(phrases))[:3]


INSPECTIONS: Final[dict[Seed, Inspection]] = {
    "terraform_preview": Inspection(
        "Terraform change previews with a bare terraform plan command",
        compile(r"terraform[ \t]+plan"),
        frozenset(
            "terraform configuration module provider plan preview proposed changes scope resources".split()
        ),
    ),
    "terraform_validation": Inspection(
        "Terraform configuration validation with terraform validate",
        compile(r"terraform[ \t]+validate"),
        frozenset(
            "terraform configuration module modules provider providers schema syntax validation validate diagnostics variable variables".split()
        ),
    ),
    "kubernetes_inventory": Inspection(
        "Kubernetes workload or namespace inventory with kubectl get",
        compile(r"kubectl[ \t]+get[ \t]+(?:pods?|deployments?|services?|namespaces?)"),
        frozenset(
            "kubernetes kubectl cluster workload workloads pods deployments namespaces services inventory readiness status selector labels".split()
        ),
    ),
    "docker_inventory": Inspection(
        "Docker container, image or volume inventory and disk usage",
        compile(
            r"docker[ \t]+(?:(?:ps|container[ \t]+ls)(?:[ \t]+(?:--all|-a))?"
            r"|(?:image|volume)[ \t]+ls|images|system[ \t]+df)"
        ),
        frozenset(
            "docker container containers image images volume volumes disk usage storage inventory stopped running sizes space cache".split()
        ),
    ),
    "storage_inventory": Inspection(
        "AWS S3 bucket inventory with aws s3api list-buckets",
        compile(r"aws[ \t]+s3api[ \t]+list-buckets"),
        frozenset(
            "aws s3 bucket buckets cloud storage inventory listing region regions pagination names".split()
        ),
    ),
    "kubernetes_rollout": Inspection(
        "Kubernetes deployment inventory with kubectl rollout status --watch=false",
        compile(
            r"kubectl[ \t]+rollout[ \t]+status[ \t]+deployment/[a-z0-9][a-z0-9-]{0,62}"
            r"[ \t]+--watch=false"
        ),
        frozenset(
            "kubernetes kubectl deployment deployments rollout status readiness revision progress replicas conditions".split()
        ),
    ),
}

_FENCE: Final = compile(rb"^ {0,3}(`{3,}|~{3,})([^\r\n]*)")
_SHELL: Final = frozenset({b"bash", b"shell", b"sh", b"zsh"})
_CONSOLE: Final = frozenset({b"console", b"shell-session", b"terminal"})
_WORD: Final = compile(rb"[a-zA-Z0-9_./+-]+(?=[ \t]|$)")


@dataclass(frozen=True, slots=True)
class ShellSpan:
    start: int
    end: int
    description: str


def shell_spans(body: bytes) -> tuple[ShellSpan, ...]:
    """Extract complete shell fences while preserving command bytes and offsets."""
    spans: list[ShellSpan] = []
    pending: list[ShellSpan] = []
    fence = b""
    language = b""
    description = ""
    preceding_prose = ""
    offset = 0
    continued = False
    for line in body.splitlines(keepends=True):
        start, offset = offset, offset + len(line)
        if delimiter := _FENCE.fullmatch(line.rstrip(b"\r\n")):
            marker, info = delimiter.groups()
            if not fence:
                fence, language = marker, info.strip().split(b" ", 1)[0].lower()
                description = preceding_prose
                preceding_prose = ""
                continued = False
            elif marker.startswith(fence) and not info.strip():
                spans.extend(pending)
                pending.clear()
                fence = b""
            continue
        if not fence:
            if line.strip():
                preceding_prose = line.strip().decode()
            continue
        if language not in _SHELL | _CONSOLE | {b""}:
            continue
        raw = line.strip(b" \t\r\n")
        if continued or raw.endswith(b"\\"):
            continued = raw.endswith(b"\\")
            continue
        if raw.startswith(b"# "):
            description = raw[2:].decode()
            continue
        command = raw.removeprefix(b"$ ")
        if command == raw and language in _CONSOLE:
            continue
        utility = _WORD.match(command)
        if utility is None or utility[0].endswith(b"/"):
            continue
        command_start = start + line.index(command)
        pending.append(ShellSpan(command_start, command_start + len(command), description))
    return tuple(spans)
