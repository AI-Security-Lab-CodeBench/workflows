from datetime import datetime
from hashlib import sha256
from pathlib import Path
from re import fullmatch
from typing import Annotated, ClassVar, Final, Literal, Self, assert_never
from urllib.parse import urlsplit, urlunsplit

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StringConstraints,
    model_validator,
)

from codebench_pipeline.typed_config import ConfigArtifact

type Case = Literal["plan", "workloads", "containers", "storage_sql", "rollout"]
type Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
type Identity = Annotated[str, StringConstraints(pattern=r"^[a-zA-Z0-9_-]{1,128}$")]
type Media = Literal["text/html", "text/plain", "text/markdown"]

CASES: Final[tuple[Case, ...]] = ("plan", "workloads", "containers", "storage_sql", "rollout")
RESPONSE_BYTES: Final = 512 * 1024
MAX_SEARCH: Final = 5
MAX_ORIGINS: Final = 15
MAX_BYTES: Final = 10 * 1024 * 1024
MAX_RESULTS: Final = 5
PROVIDER: Final = "https://api.keenable.ai/v1/search"

QUERIES: Final[dict[Case, str]] = {
    "plan": "Official Terraform CLI documentation for terraform plan and validate: preview scope and validation",
    "workloads": "Official Kubernetes kubectl get documentation for workload inventory and status scope",
    "containers": "Official Docker CLI documentation for container ls and system df: container and storage usage",
    "storage_sql": "Official AWS CLI S3 list-buckets or PostgreSQL psql documentation: inventory and metadata scope",
    "rollout": "Official Kubernetes kubectl rollout status documentation for deployment health and status scope",
}
_KUBE = "https://kubernetes.io/docs/reference/kubectl/generated/"
_DOCKER = "https://docs.docker.com/reference/cli/docker/"
_TF = "https://developer.hashicorp.com/terraform/cli/commands/"
_AWS = "https://docs.aws.amazon.com/cli/latest/reference/"
_PSQL = "https://www.postgresql.org/docs/current/"
PAGE_CASES: Final[dict[str, Case]] = {
    _TF + "plan": "plan",
    _TF + "validate": "plan",
    _KUBE + "kubectl_get/": "workloads",
    _KUBE + "kubectl_get": "workloads",
    _KUBE + "kubectl_describe/": "workloads",
    _DOCKER + "container/ls/": "containers",
    _DOCKER + "system/df/": "containers",
    _DOCKER + "container/stats/": "containers",
    _DOCKER + "volume/ls/": "containers",
    _AWS + "s3api/list-buckets.html": "storage_sql",
    _AWS + "s3api/list-objects-v2.html": "storage_sql",
    _PSQL + "app-psql.html": "storage_sql",
    _PSQL + "infoschema-tables.html": "storage_sql",
    _PSQL + "infoschema-columns.html": "storage_sql",
    _KUBE + "kubectl_rollout/kubectl_rollout_status/": "rollout",
    _KUBE + "kubectl_rollout/kubectl_rollout_status": "rollout",
}
COMMANDS: Final[dict[Case, frozenset[str]]] = {
    "plan": frozenset({"terraform plan", "terraform validate"}),
    "workloads": frozenset(
        {
            "kubectl get pods",
            "kubectl get deployments",
            "kubectl get services",
            "kubectl get namespaces",
            "kubectl get namespace",
        }
    ),
    "containers": frozenset(
        {
            "docker container ls",
            "docker system df",
            "docker container stats --no-stream",
            "docker image prune",
            "docker container prune",
            "docker volume prune",
            "docker network prune",
            "docker system prune",
        }
    ),
    "storage_sql": frozenset({"aws s3api list-buckets", r"\dt", r"\dn"}),
    "rollout": frozenset({"kubectl rollout status deployment/nginx-deployment"}),
}


class Record(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True, strict=True)


class KeenableError(RuntimeError):
    def __init__(self, code: str, status: int | None = None) -> None:
        self.code = code
        self.status = status
        super().__init__(code if status is None else f"{code} ({status})")


def digest(body: bytes) -> str:
    return sha256(body).hexdigest()


def record_digest(record: Record) -> str:
    return digest(record.model_dump_json().encode())


def canonical_url(value: str) -> str | None:
    """Return a canonical HTTPS destination without ambiguous syntax."""
    if not value.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        return None
    target = value.split("#", 1)[0]
    if any(c in target for c in "\\%?"):
        return None
    try:
        url = urlsplit(target)
        host = url.hostname
        if (
            url.scheme != "https"
            or host is None
            or not fullmatch(r"[a-z0-9.-]+", host)
            or url.netloc.lower() not in (host, host + ":443")
            or "//" in url.path
            or any(segment in (".", "..") for segment in url.path.split("/"))
        ):
            return None
    except ValueError:
        return None
    return urlunsplit(("https", host, url.path or "/", "", ""))


class ReviewedSpan(Record):
    start: int = Field(ge=0, lt=RESPONSE_BYTES)
    end: int = Field(gt=0, le=RESPONSE_BYTES)

    @model_validator(mode="after")
    def ordered(self) -> "ReviewedSpan":
        if self.start >= self.end or self.end - self.start > 256:
            raise ValueError("Expected a bounded, nonempty byte span")
        return self


class PagePermission(Record):
    url: str
    origin_id: Identity
    content_sha256: Digest
    media_type: Media
    rights_evidence_sha256: Digest
    steward_decision: Identity
    valid_until: AwareDatetime
    attribution: str = Field(min_length=1, max_length=256)
    permitted_use: Literal["fetch_retain_extract"]
    spans: tuple[ReviewedSpan, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def approved_page(self) -> "PagePermission":
        if self.url not in PAGE_CASES or not self.attribution.strip():
            raise ValueError("Expected an exact allowlisted page and attribution")
        if tuple(sorted(set((s.start, s.end) for s in self.spans))) != tuple(
            (s.start, s.end) for s in self.spans
        ) or any(a.end > b.start for a, b in zip(self.spans, self.spans[1:])):
            raise ValueError("Reviewed spans must be ordered, unique and disjoint")
        return self


class LicenseReference(Record):
    identifier: Literal["CC-BY-4.0", "Apache-2.0", "PostgreSQL"]
    evidence_url: str
    license_url: str

    @model_validator(mode="after")
    def evidence_urls(self) -> Self:
        if any(canonical_url(url) != url for url in (self.evidence_url, self.license_url)):
            raise ValueError("Public license requires canonical HTTPS evidence and license URLs")
        return self


class PublicLicense(LicenseReference):
    notice: str | None = Field(default=None, min_length=1, max_length=256)


class PinnedLicenseFile(LicenseReference):
    kind: Literal["pinned_license_file"]
    evidence_sha256: Digest


class PageNotice(LicenseReference):
    kind: Literal["page_notice"]
    declared_evidence_sha256: Digest
    required_page_notice: str = Field(min_length=1, max_length=256)


class StewardGrant(Record):
    kind: Literal["steward_decision"]
    steward_decision: Identity
    declared_evidence_sha256: Digest


type SourceRights = Annotated[PinnedLicenseFile | PageNotice | StewardGrant, Field(discriminator="kind")]


class SourcePermission(Record):
    origin_id: Identity
    url_prefix: str | None = Field(default=None, exclude_if=lambda prefix: prefix is None)
    exact_urls: tuple[str, ...] = Field(default=(), max_length=64, exclude_if=lambda urls: not urls)
    discover: bool = Field(default=False, exclude_if=lambda discover: not discover)
    cases: tuple[Case, ...] = Field(min_length=1, max_length=len(CASES))
    media_type: Media
    rights_evidence_sha256: Digest
    steward_decision: Identity | None = Field(default=None, exclude_if=lambda decision: decision is None)
    public_license: PublicLicense | None = Field(default=None, exclude_if=lambda license_: license_ is None)
    valid_until: AwareDatetime
    attribution: str = Field(min_length=1, max_length=256)
    permitted_use: Literal["fetch_retain_quote", "fetch_retain_extract_quote"]

    @model_validator(mode="after")
    def approved_scope(self) -> "SourcePermission":
        if (self.steward_decision is None) == (self.public_license is None):
            raise ValueError("Expected one HTTPS source scope and one rights authority")
        _validate_source(self)
        return self

    @property
    def allow_discovery(self) -> bool:
        return self.discover or not self.exact_urls

    @property
    def page_notice(self) -> str | None:
        return self.public_license.notice if self.public_license is not None else None

    def covers(self, url: str, case: Case) -> bool:
        return case in self.cases and _covers_url(self, url)


class CurrentSourcePermission(Record):
    origin_id: Identity
    url_prefix: str | None = Field(default=None, exclude_if=lambda prefix: prefix is None)
    exact_urls: tuple[str, ...] = Field(default=(), max_length=64, exclude_if=lambda urls: not urls)
    cases: tuple[Case, ...] = Field(min_length=1, max_length=len(CASES))
    media_type: Media
    valid_until: AwareDatetime
    attribution: str = Field(min_length=1, max_length=256)
    permitted_use: Literal["fetch_retain_quote", "fetch_retain_extract_quote"]
    allow_discovery: bool
    rights: SourceRights

    @model_validator(mode="after")
    def approved_scope(self) -> Self:
        if self.exact_urls and len(self.cases) != 1:
            raise ValueError("Exact seed references require a single case")
        _validate_source(self)
        return self

    @property
    def public_license(self) -> PinnedLicenseFile | PageNotice | None:
        match self.rights:
            case PinnedLicenseFile() | PageNotice() as license_:
                return license_
            case StewardGrant():
                return None
        assert_never(self.rights)

    @property
    def rights_evidence_sha256(self) -> str:
        match self.rights:
            case PinnedLicenseFile(evidence_sha256=value):
                return value
            case PageNotice(declared_evidence_sha256=value) | StewardGrant(declared_evidence_sha256=value):
                return value
        assert_never(self.rights)

    @property
    def page_notice(self) -> str | None:
        match self.rights:
            case PageNotice(required_page_notice=notice):
                return notice
            case PinnedLicenseFile() | StewardGrant():
                return None
        assert_never(self.rights)

    def covers(self, url: str, case: Case) -> bool:
        return case in self.cases and _covers_url(self, url)


type SourceGrant = SourcePermission | CurrentSourcePermission
type Permission = PagePermission | SourceGrant


def _validate_source(source: SourceGrant) -> None:
    prefix = source.url_prefix
    if (
        (prefix is None and not source.exact_urls)
        or not source.attribution.strip()
        or len(set(source.cases)) != len(source.cases)
    ):
        raise ValueError("Expected one HTTPS source scope and one rights authority")
    if prefix is not None and (
        canonical_url(prefix) != prefix or urlsplit(prefix).path == "/" or not prefix.endswith("/")
    ):
        raise ValueError("Expected a canonical bounded HTTPS path prefix")
    if source.allow_discovery and prefix is None:
        raise ValueError("Discovery requires a bounded HTTPS path prefix")
    if len(set(source.exact_urls)) != len(source.exact_urls) or any(
        canonical_url(url) != url or (prefix is not None and not url.startswith(prefix))
        for url in source.exact_urls
    ):
        raise ValueError("Expected unique canonical HTTPS reference URLs")
    if (license_ := source.public_license) is None:
        return
    if source.page_notice is None:
        repository = (prefix or "").split("/")[:6]
        if (
            source.media_type != "text/plain"
            or len(repository) != 6
            or repository[2] != "raw.githubusercontent.com"
            or not fullmatch(r"[0-9a-f]{40}", repository[5])
            or prefix == "/".join(repository) + "/"
            or not license_.evidence_url.startswith("/".join(repository) + "/")
            or license_.evidence_url in source.exact_urls
        ):
            raise ValueError("Raw references and rights must be at the same immutable repository commit")
    elif (
        source.media_type != "text/html"
        or license_.identifier != "CC-BY-4.0"
        or source.permitted_use != "fetch_retain_quote"
        or source.allow_discovery
        or not source.exact_urls
    ):
        raise ValueError("An HTML notice must identify the declared documentation license")


def _covers_url(source: SourceGrant, url: str) -> bool:
    return url in source.exact_urls or _covers_discovery_url(source, url)


def _covers_discovery_url(source: SourceGrant, url: str) -> bool:
    return (
        source.allow_discovery
        and source.url_prefix is not None
        and url.startswith(source.url_prefix)
        and (source.public_license is None or urlsplit(url).path.endswith((".md", ".mdx", ".sgml")))
    )


class CallBudget(Record):
    origin_calls: int = Field(default=MAX_ORIGINS, ge=1, le=64)
    search_calls: int = Field(default=MAX_SEARCH, ge=0, le=MAX_SEARCH)

    @property
    def total_bytes(self) -> int:
        return (self.origin_calls + self.search_calls) * RESPONSE_BYTES


class AcquisitionBudget(Record):
    origin_calls: int = Field(default=MAX_ORIGINS, ge=1, le=64)
    search_calls: int = Field(default=MAX_SEARCH, ge=0, le=MAX_SEARCH)
    total_bytes: int = Field(default=MAX_BYTES, ge=RESPONSE_BYTES, le=69 * RESPONSE_BYTES)

    @model_validator(mode="after")
    def covers_reservations(self) -> "AcquisitionBudget":
        if self.total_bytes < (self.origin_calls + self.search_calls) * RESPONSE_BYTES:
            raise ValueError("The byte budget must cover every configured call reservation")
        return self


class AdministrativeScope(Record):
    case: Case
    source: Identity
    reviewed_urls: tuple[str, ...] = Field(min_length=1, max_length=8)

    @property
    def origin_id(self) -> str:
        return self.source


class CurrentAdministrativeScope(Record):
    case: Case
    origin_id: Identity
    reviewed_urls: tuple[str, ...] = Field(min_length=1, max_length=8)


class AdministrativeSearch(Record):
    mode: Literal["fixed", "adaptive"]
    scopes: tuple[AdministrativeScope, ...] = Field(min_length=1, max_length=len(CASES))


class CurrentAdministrativeSearch(Record):
    mode: Literal["fixed", "adaptive"]
    max_queries: int | None = Field(default=None, ge=1, le=MAX_SEARCH, exclude_if=lambda n: n is None)
    scopes: tuple[CurrentAdministrativeScope, ...] = Field(min_length=1, max_length=len(CASES))


class KeenableDeclaration(Record):
    dataset: Identity
    generation: Identity
    generation_review: Identity
    cases: tuple[Case, ...]
    permissions: tuple[PagePermission, ...] = Field(default=(), max_length=len(PAGE_CASES))
    sources: tuple[SourcePermission, ...] = Field(
        default=(), max_length=20, exclude_if=lambda sources: not sources
    )
    revision: Literal["1", "2"] = "1"
    budget: AcquisitionBudget | None = Field(default=None, exclude_if=lambda budget: budget is None)
    search_plan: AdministrativeSearch | None = Field(default=None, exclude_if=lambda plan: plan is None)

    @property
    def source_scoped(self) -> bool:
        return self.revision == "2"

    @model_validator(mode="after")
    def approved_scope(self) -> "KeenableDeclaration":
        if self.cases != CASES:
            raise ValueError("Expected all five administrative cases in their stable order")
        if len({page.url for page in self.permissions}) != len(self.permissions):
            raise ValueError("Expected at most one current permission per URL")
        if (self.revision == "1" and (self.sources or self.budget or self.search_plan)) or (
            self.revision == "2" and self.permissions
        ):
            raise ValueError("Declaration revision 2 uses source scope instead of page permissions")
        if self.revision == "2":
            _validate_source_declaration(self)
        return self

    @property
    def raw_licenses(self) -> tuple[tuple[str, str], ...]:
        return _raw_licenses(self.sources)

    @property
    def search_cases(self) -> tuple[Case, ...]:
        if self.revision == "1":
            return self.cases
        return _search_cases(self.sources)

    @property
    def declaration_sha256(self) -> str:
        return _declaration_digest(self)

    def check_current(self, now: datetime) -> None:
        _check_current(self, now)

    def authority(self, url: str, case: Case) -> PagePermission | SourcePermission | None:
        return _source_authority(self.sources, self.permissions, url, case)


class CurrentKeenableDeclaration(Record):
    schema_version: Literal["3"]
    dataset_id: Identity
    generation: Identity
    budget: CallBudget = CallBudget()
    search_plan: CurrentAdministrativeSearch | None = Field(
        default=None, exclude_if=lambda plan: plan is None
    )
    sources: tuple[CurrentSourcePermission, ...] = Field(max_length=20)

    cases: ClassVar[tuple[Case, ...]] = CASES
    permissions: ClassVar[tuple[PagePermission, ...]] = ()
    source_scoped: ClassVar[bool] = True

    @model_validator(mode="after")
    def approved_scope(self) -> Self:
        _validate_source_declaration(self)
        return self

    @property
    def dataset(self) -> str:
        return self.dataset_id

    @property
    def raw_licenses(self) -> tuple[tuple[str, str], ...]:
        return _raw_licenses(self.sources)

    @property
    def search_cases(self) -> tuple[Case, ...]:
        return _search_cases(self.sources)

    @property
    def declaration_sha256(self) -> str:
        return _declaration_digest(self)

    def check_current(self, now: datetime) -> None:
        _check_current(self, now)

    def authority(self, url: str, case: Case) -> CurrentSourcePermission | None:
        return next((source for source in self.sources if source.covers(url, case)), None)

    def admits_search_result(self, url: str, case: Case) -> bool:
        return any(case in source.cases and _covers_discovery_url(source, url) for source in self.sources)


type Declaration = KeenableDeclaration | CurrentKeenableDeclaration


class KeenableOriginBudget(Record):
    origin_calls: int = Field(ge=1, le=64)
    total_bytes: int = Field(ge=RESPONSE_BYTES, le=69 * RESPONSE_BYTES)


class KeenableSourceAccess(Record, ConfigArtifact):
    dataset: Identity
    generation: Identity
    declaration_sha256: Digest
    budget: KeenableOriginBudget
    permissions: tuple[PagePermission, ...]
    sources: tuple[SourceGrant, ...]
    legacy_reviewed_spans: bool

    @classmethod
    def project(cls, declaration: Declaration) -> Self:
        budget = declaration.budget
        return cls(
            dataset=declaration.dataset,
            generation=declaration.generation,
            declaration_sha256=declaration.declaration_sha256,
            budget=KeenableOriginBudget(
                origin_calls=MAX_ORIGINS if budget is None else budget.origin_calls,
                total_bytes=MAX_BYTES if budget is None else budget.total_bytes,
            ),
            permissions=declaration.permissions,
            sources=declaration.sources,
            legacy_reviewed_spans=isinstance(declaration, KeenableDeclaration)
            and declaration.revision == "1",
        )

    @property
    def raw_licenses(self) -> tuple[tuple[str, str], ...]:
        return _raw_licenses(self.sources)

    def check_current(self, now: datetime) -> None:
        _check_current(self, now)

    def authority(self, url: str, case: Case) -> Permission | None:
        return _source_authority(self.sources, self.permissions, url, case)


def _source_authority[Grant: SourceGrant](
    sources: tuple[Grant, ...], permissions: tuple[PagePermission, ...], url: str, case: Case
) -> Grant | PagePermission | None:
    for source in sources:
        if source.covers(url, case):
            return source
    return next((permission for permission in permissions if permission.url == url), None)


def _raw_licenses(sources: tuple[SourceGrant, ...]) -> tuple[tuple[str, str], ...]:
    return tuple(
        sorted(
            {
                (license_.evidence_url, source.rights_evidence_sha256)
                for source in sources
                if (license_ := source.public_license) is not None and source.page_notice is None
            }
        )
    )


def _search_cases(sources: tuple[SourceGrant, ...]) -> tuple[Case, ...]:
    return tuple(
        case for case in CASES if any(source.allow_discovery and case in source.cases for source in sources)
    )


def _declaration_digest(declaration: Declaration) -> str:
    ordered = {
        "sources": tuple(sorted(declaration.sources, key=lambda source: source.origin_id)),
    }
    match declaration:
        case KeenableDeclaration():
            return record_digest(
                declaration.model_copy(
                    update={
                        **ordered,
                        "permissions": tuple(sorted(declaration.permissions, key=lambda page: page.url)),
                    }
                )
            )
        case CurrentKeenableDeclaration():
            return record_digest(declaration.model_copy(update=ordered))
    assert_never(declaration)


def _check_current(declaration: Declaration | KeenableSourceAccess, now: datetime) -> None:
    if any(permission.valid_until <= now for permission in (*declaration.permissions, *declaration.sources)):
        raise KeenableError("authorization_expired")


def _validate_source_declaration(declaration: Declaration) -> None:
    sources = {source.origin_id: source for source in declaration.sources}
    if len(sources) != len(declaration.sources) or any(
        any(_covers_url(right, url) for url in left.exact_urls)
        or any(_covers_url(left, url) for url in right.exact_urls)
        or (
            left.url_prefix is not None
            and right.url_prefix is not None
            and left.allow_discovery
            and right.allow_discovery
            and (left.url_prefix.startswith(right.url_prefix) or right.url_prefix.startswith(left.url_prefix))
        )
        for index, left in enumerate(declaration.sources)
        for right in declaration.sources[index + 1 :]
    ):
        raise ValueError("Source identities and HTTPS scopes must not overlap")
    budget = declaration.budget or AcquisitionBudget()
    raw_licenses = declaration.raw_licenses
    if len({url for url, _ in raw_licenses}) != len(raw_licenses):
        raise ValueError("Raw rights URLs must bind to one verified digest")
    if (
        sum(len(source.exact_urls) for source in declaration.sources) + len(raw_licenses)
        > budget.origin_calls
    ):
        raise ValueError("Origin budget must cover all exact references and deduplicated rights files")
    if budget.search_calls < len(declaration.search_cases):
        raise ValueError("Search budget must cover each configured discovery case")
    if (plan := declaration.search_plan) is None:
        return
    if isinstance(plan, CurrentAdministrativeSearch) and plan.max_queries is not None:
        if not len(plan.scopes) <= plan.max_queries <= budget.search_calls:
            raise ValueError("Query limit must cover all search scopes and fit the search call budget")
    if tuple(scope.case for scope in plan.scopes) != declaration.search_cases:
        raise ValueError("Expected one reviewed search scope per discovery case in case order")
    for scope in plan.scopes:
        source = sources.get(scope.origin_id)
        if (
            source is None
            or source.url_prefix is None
            or not source.allow_discovery
            or len(set(scope.reviewed_urls)) != len(scope.reviewed_urls)
            or any(
                canonical_url(url) != url
                or not fullmatch(r"https://[a-z0-9.-]+/[A-Za-z0-9._/-]+", url)
                or not url.startswith(source.url_prefix)
                or not (
                    _covers_discovery_url(source, url) and scope.case in source.cases
                    if isinstance(declaration, CurrentKeenableDeclaration)
                    else source.covers(url, scope.case)
                )
                for url in scope.reviewed_urls
            )
        ):
            raise ValueError("Expected exact reviewed documentation URLs within the discovery scope")


def parse_declaration(raw: str | bytes) -> Declaration:
    return RootModel[Declaration].model_validate_json(raw).root


def load_declaration(path: Path) -> Declaration:
    """Load declaration-only config; no captures, credentials or network."""
    return parse_declaration(path.read_bytes())
