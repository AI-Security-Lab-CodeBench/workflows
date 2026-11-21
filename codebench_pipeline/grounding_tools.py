from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.message import Message
from http.client import HTTPException
from pathlib import Path
from threading import RLock
from typing import IO, Final, Literal, Protocol, assert_never
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError, model_validator

from scripts.dataset_schema import SECRET_PATTERNS

from .grounding_policy import OperationGrant
from .object_store import safe_key
from .repository_receipts import publish_bytes
from .storage import IntegrityError
from .typed_config import TypedConfig

MAXIMUM_BATCH_PROVIDER_CALLS: Final = 10


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


def _url(value: str) -> str:
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.port not in (None, 443)
        or "\\" in value
        or any(ord(char) <= 32 for char in value)
        or any(part in {".", ".."} for part in parts.path.split("/"))
        or "%" in parts.netloc + parts.path
    ):
        raise ValueError("Expected an HTTPS URL without credentials or ambiguous path")
    return urlunsplit(("https", parts.hostname.lower(), parts.path or "/", parts.query, ""))


class GroundingOrigins(TypedConfig):
    allowed_domains: list[str] = Field(default_factory=list)
    trusted_url_prefixes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def explicit_trust(self) -> GroundingOrigins:
        if not self.allowed_domains and not self.trusted_url_prefixes:
            raise ValueError("Configure explicit trusted domains or URL prefixes")
        for domain in self.allowed_domains:
            if _url("https://" + domain) != "https://" + domain.lower() + "/":
                raise ValueError("Allowed domains must be host names")
        for prefix in self.trusted_url_prefixes:
            if urlsplit(_url(prefix)).query or urlsplit(prefix).fragment:
                raise ValueError("Trusted prefixes cannot contain queries or fragments")
        return self


class LiveGroundingConfig(GroundingOrigins):
    keenable_api_key_env: str = Field(default="KEENABLE_API_KEY", min_length=1)
    maximum_response_bytes: int = Field(default=262_144, ge=1)
    maximum_total_response_bytes: int = Field(default=2_097_152, ge=1)
    timeout: float = Field(default=30, gt=0, allow_inf_nan=False)


class GroundingConfig(GroundingOrigins):
    keenable_api_key_env: str = Field(default="KEENABLE_API_KEY", min_length=1)
    context7_api_key_env: str = Field(default="CONTEXT7_API_KEY", min_length=1)
    maximum_response_bytes: int = Field(default=262_144, ge=1)
    maximum_total_response_bytes: int = Field(default=2_097_152, ge=1)
    maximum_evidence_bytes: int = Field(default=65_536, ge=1)
    timeout: float = Field(default=30, gt=0, allow_inf_nan=False)
    maximum_provider_calls: int = Field(default=8, ge=0)
    revision: str = Field(default="1", min_length=1)
    pinned_library_ids: list[str] = Field(default_factory=list)
    operation_grants_json: str = Field(default="", exclude_if=lambda grants: not grants)

    @property
    def provider_call_limit(self) -> int:
        return min(self.maximum_provider_calls, MAXIMUM_BATCH_PROVIDER_CALLS)

    @property
    def operation_grants(self) -> tuple[OperationGrant, ...]:
        if not self.operation_grants_json:
            return ()
        return TypeAdapter(tuple[OperationGrant, ...]).validate_json(self.operation_grants_json)

    @model_validator(mode="after")
    def valid_grants(self) -> GroundingConfig:
        for grant in self.operation_grants:
            if grant.library_id is not None and grant.library_id not in self.pinned_library_ids:
                raise ValueError("An operation grant must name a pinned Context7 library")
        return self


Action = Literal["context7_search", "context7_context", "keenable_search", "keenable_fetch"]
Provider = Literal["context7", "keenable"]


@dataclass(frozen=True, slots=True)
class GroundingCredentials:
    keenable: str | None = field(default=None, repr=False)
    context7: str | None = field(default=None, repr=False)

    @classmethod
    def bind(
        cls, config: GroundingConfig | LiveGroundingConfig, lookup: Callable[[str], str | None]
    ) -> GroundingCredentials:
        context7 = lookup(config.context7_api_key_env) if isinstance(config, GroundingConfig) else None
        return cls(lookup(config.keenable_api_key_env), context7)

    @classmethod
    def from_environment(cls, config: GroundingConfig | LiveGroundingConfig) -> GroundingCredentials:
        return cls.bind(config, os.environ.get)

    def contains(self, text: str) -> bool:
        return any(key is not None and bool(key) and key in text for key in (self.keenable, self.context7))


class DocumentationQuery(_Frozen):
    """An operation question, never raw private source context."""

    action: Action
    query: str = Field(min_length=1, max_length=500)
    candidate_ids: list[str] = Field(min_length=1, max_length=50)
    library_name: str | None = Field(default=None, min_length=1, max_length=200)
    library_id: str | None = Field(default=None, min_length=1, max_length=300)
    url: str | None = Field(default=None, max_length=2048)
    requested_version: str | None = Field(default=None, min_length=1, max_length=100)

    @model_validator(mode="after")
    def action_fields(self) -> DocumentationQuery:
        if not self.query.strip() or any(
            not value.strip() or len(value) > 200 for value in self.candidate_ids
        ):
            raise ValueError("Nonempty operation questions and bounded candidate IDs required")
        match self.action:
            case "context7_search":
                valid = self.library_name is not None and self.library_id is None and self.url is None
            case "context7_context":
                valid = self.library_id is not None and self.library_name is None and self.url is None
            case "keenable_search":
                valid = self.library_name is None and self.library_id is None and self.url is None
            case "keenable_fetch":
                valid = self.url is not None and self.library_name is None and self.library_id is None
            case _:
                assert_never(self.action)
        if not valid:
            raise ValueError("Fields do not match the documentation action")
        return self


class GroundingMetadata(_Frozen):
    kind: Literal["info", "code", "page"]
    requested_library_id: str | None = None
    requested_version: str | None = None
    returned_version: str | None = None
    provider_summary: str | None = None
    provider_metadata_json: str = "{}"
    truncated: bool = False


class GroundingDocument(_Frozen):
    evidence_id: str
    url: str
    text: str
    content_sha256: str
    retrieved_at: str
    provider: Provider
    metadata: GroundingMetadata


class DiscoveryResult(_Frozen):
    """Discovery only; neither snippets nor library matches authorize grounding."""

    library_id: str | None = None
    url: str | None = None
    title: str = ""
    snippet: str = ""
    metadata_json: str = "{}"


class GroundingError(_Frozen):
    code: str
    status: int | None = None
    retry_after: str | None = None


class GroundingResult(_Frozen):
    documents: list[GroundingDocument] = Field(default_factory=list)
    discovery_results: list[DiscoveryResult] = Field(default_factory=list)
    error: GroundingError | None = None
    provider_calls: int = 0
    cache_hit: bool = False


class GroundingReceipt(_Frozen):
    index: int
    query: DocumentationQuery
    request_sha256: str | None
    response_sha256: str | None
    evidence_ids: tuple[str, ...]
    error: GroundingError | None
    provider_calls: int
    cache_hit: bool
    total_provider_calls: int
    response_bytes: int
    evidence_bytes: int


class _ProviderModel(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)


class _Hit(_ProviderModel):
    id: str | None = None
    url: str | None = None
    title: str = ""
    snippet: str = ""
    version: str | None = None
    versions: list[str] = Field(default_factory=list)


class _Search(_ProviderModel):
    results: list[_Hit]


class _Code(_ProviderModel):
    language: str
    code: str


class _CodeSnippet(_ProviderModel):
    codeTitle: str
    codeDescription: str
    codeId: str
    codeList: list[_Code]
    generationDate: str | None = None
    rank: int | float | None = None


class _InfoSnippet(_ProviderModel):
    pageId: str
    content: str
    breadcrumb: str = ""
    rank: int | float | None = None


class _Context(_ProviderModel):
    codeSnippets: list[_CodeSnippet]
    infoSnippets: list[_InfoSnippet]
    version: str | None = None


class _Page(_ProviderModel):
    url: str
    content: str
    truncated: bool = False
    version: str | None = None


class _Cached(_Frozen):
    request_sha256: str
    response_sha256: str
    retrieved_at: str
    body: str


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self, req: Request, fp: IO[bytes], code: int, msg: str, headers: Message, newurl: str
    ) -> Request | None:
        return None


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _json(value: JsonValue) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


class ResponseCache(Protocol):
    @property
    def root(self) -> Path: ...

    def read(self, key: str, maximum: int) -> bytes | None: ...

    def write(self, key: str, body: bytes) -> None: ...


@dataclass(frozen=True, slots=True)
class GroundingFiles:
    root: Path

    def _address(self, key: str) -> Path:
        root = self.root.resolve()
        path = root / safe_key(key)
        if not path.is_relative_to(root) or path.resolve() != path:
            raise IntegrityError("Grounding artifact is outside its bound directory or traverses a link")
        return path

    def read(self, key: str, maximum: int) -> bytes | None:
        try:
            with self._address(key).open("rb") as stream:
                return stream.read(maximum + 1)
        except FileNotFoundError:
            return None

    def write(self, key: str, body: bytes) -> None:
        publish_bytes(self._address(key), body)


class GroundingBroker:
    """One batch's serialized call budget, discovery permissions and bounded evidence."""

    def __init__(
        self,
        config: GroundingConfig,
        credentials: GroundingCredentials,
        artifacts: GroundingFiles,
        *,
        response_cache: ResponseCache | None = None,
        retain_responses: bool = True,
    ) -> None:
        self.config = config.model_copy(deep=True)
        self.credentials = credentials
        self.artifacts = artifacts
        self.response_cache = response_cache
        self.retain_responses = retain_responses
        self._provider_calls = 0
        self._response_bytes = 0
        self.evidence_bytes = 0
        self._lock = RLock()
        self._results: dict[str, GroundingResult] = {}
        self._seen_urls: set[str] = set()
        self._library_ids: set[str] = set(config.pinned_library_ids)
        self._evidence_ids: set[str] = set()
        self._response_hashes: dict[str, str] = {}
        self._history: list[GroundingReceipt] = []

    @classmethod
    def from_environment(
        cls, config: GroundingConfig, out: Path, cache: Path | None = None
    ) -> GroundingBroker:
        return cls(
            config,
            GroundingCredentials.from_environment(config),
            GroundingFiles(out),
            response_cache=GroundingFiles(cache) if cache is not None else None,
        )

    @property
    def out(self) -> Path:
        return self.artifacts.root

    @property
    def cache(self) -> Path | None:
        return self.response_cache.root if self.response_cache is not None else None

    @property
    def provider_calls(self) -> int:
        with self._lock:
            return self._provider_calls

    @property
    def response_bytes(self) -> int:
        with self._lock:
            return self._response_bytes

    def reserve_origin_request(self, maximum_bytes: int) -> bool:
        """Charge a direct origin attempt and its maximum response before transport."""
        with self._lock:
            if maximum_bytes < 1 or self._response_bytes + maximum_bytes > (
                self.config.maximum_total_response_bytes
            ):
                return False
            if not self._reserve_provider_call():
                return False
            self._response_bytes += maximum_bytes
            return True

    def _reserve_provider_call(self) -> bool:
        if self._provider_calls >= self.config.provider_call_limit:
            return False
        self._provider_calls += 1
        return True

    @property
    def query_history(self) -> tuple[GroundingReceipt, ...]:
        """Ordered receipts for per-candidate retrieval replay, including cache hits."""
        with self._lock:
            return tuple(receipt.model_copy(deep=True) for receipt in self._history)

    def _context_library_trusted(self, library_id: str | None) -> bool:
        return library_id is not None and library_id in self._library_ids

    def _trusted(self, value: str) -> bool:
        try:
            canonical = _url(value)
        except ValueError:
            return False
        if urlsplit(canonical).hostname in {domain.lower() for domain in self.config.allowed_domains}:
            return True
        for prefix in self.config.trusted_url_prefixes:
            base = _url(prefix).rstrip("/")
            if canonical == base or canonical.startswith(base + "/") or canonical.startswith(base + "?"):
                return True
        return False

    def _sensitive(self, text: str) -> bool:
        return self.credentials.contains(text) or any(pattern.search(text) for pattern in SECRET_PATTERNS)

    def execute(self, query: DocumentationQuery) -> GroundingResult:
        """Return bounded evidence or a structured error; calls are per execution."""
        with self._lock:
            if self._sensitive(query.model_dump_json()):
                return GroundingResult(error=GroundingError(code="sensitive_query"))
            canonical = query.model_dump(mode="json", exclude={"candidate_ids"})
            canonical["query"] = query.query.strip()
            if query.url is not None:
                try:
                    canonical["url"] = _url(query.url)
                except ValueError:
                    return self._receipt(
                        query, None, GroundingResult(error=GroundingError(code="invalid_url"))
                    )
            request_hash = _digest(
                _json({"config": self.config.model_dump(mode="json"), "request": canonical}).encode()
            )
            match query.action:
                case "keenable_fetch":
                    if canonical["url"] not in self._seen_urls or not self._trusted(str(canonical["url"])):
                        return self._receipt(
                            query,
                            request_hash,
                            GroundingResult(error=GroundingError(code="unseen_or_untrusted_url")),
                        )
                case "context7_context":
                    if not self._context_library_trusted(query.library_id):
                        return self._receipt(
                            query,
                            request_hash,
                            GroundingResult(error=GroundingError(code="unresolved_library")),
                        )
                case "context7_search" | "keenable_search":
                    pass
                case _:
                    assert_never(query.action)
            if request_hash in self._results:
                result = self._results[request_hash].model_copy(
                    deep=True, update={"provider_calls": 0, "cache_hit": True}
                )
                return self._receipt(query, request_hash, result)
            cached = self._load_cache(request_hash)
            if isinstance(cached, GroundingError):
                return self._receipt(query, request_hash, GroundingResult(error=cached))
            if cached is not None:
                result = self._parse(query, cached.body, cached.retrieved_at)
                result = result.model_copy(update={"cache_hit": True})
            else:
                result, cached = self._retrieve(query, request_hash)
            if result.error is None:
                self._authorize(result)
                if cached is not None:
                    self._response_hashes[request_hash] = cached.response_sha256
                    if self.retain_responses:
                        self.artifacts.write(f"responses/{cached.response_sha256}.json", cached.body.encode())
                    if self.retain_responses and self.response_cache is not None:
                        self.response_cache.write(f"{request_hash}.json", cached.model_dump_json().encode())
            self._results[request_hash] = result.model_copy(deep=True)
            return self._receipt(query, request_hash, result)

    def _authorize(self, result: GroundingResult) -> None:
        for hit in result.discovery_results:
            if hit.library_id is not None:
                self._library_ids.add(hit.library_id)
            if hit.url is not None and self._trusted(hit.url):
                self._seen_urls.add(_url(hit.url))

    def _load_cache(self, key: str) -> _Cached | GroundingError | None:
        if self.response_cache is None:
            return None
        maximum = self.config.maximum_response_bytes
        cache_limit = maximum * 6 + 2048
        payload = self.response_cache.read(f"{key}.json", cache_limit)
        if payload is None:
            return None
        if len(payload) > cache_limit:
            return GroundingError(code="invalid_cache")
        try:
            cached = _Cached.model_validate_json(payload)
            decoded = TypeAdapter(JsonValue).validate_json(cached.body, strict=True)
        except ValidationError:
            return GroundingError(code="invalid_cache")
        body = cached.body.encode()
        if (
            cached.request_sha256 != key
            or len(body) > maximum
            or _digest(body) != cached.response_sha256
            or self._sensitive(cached.body)
            or self._sensitive(_json(decoded))
        ):
            return GroundingError(code="invalid_cache")
        return cached

    def _retrieve(self, query: DocumentationQuery, key: str) -> tuple[GroundingResult, _Cached | None]:
        provider: Provider = "context7" if query.action.startswith("context7_") else "keenable"
        api_key = self.credentials.context7 if provider == "context7" else self.credentials.keenable
        if not api_key:
            return GroundingResult(error=GroundingError(code="missing_api_key")), None
        if self.provider_calls >= self.config.provider_call_limit:
            return GroundingResult(error=GroundingError(code="provider_budget_exhausted")), None
        remaining = self.config.maximum_total_response_bytes - self.response_bytes
        if remaining <= 0:
            return GroundingResult(error=GroundingError(code="response_budget_exhausted")), None
        headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
        body: bytes | None = None
        match query.action:
            case "context7_search":
                endpoint = "https://context7.com/api/v2/libs/search"
                params = {"libraryName": query.library_name, "query": query.query.strip(), "fast": "true"}
            case "context7_context":
                endpoint = "https://context7.com/api/v2/context"
                params = {
                    "libraryId": query.library_id,
                    "query": query.query.strip(),
                    "type": "json",
                    "fast": "true",
                }
            case "keenable_search":
                endpoint = "https://api.keenable.ai/v1/search"
                params = {}
                payload: dict[str, JsonValue] = {
                    "query": query.query.strip(),
                    "max_results": 3,
                    "snippet_max_length": 1500,
                }
                body = _json(payload).encode()
                headers["Content-Type"] = "application/json"
            case "keenable_fetch":
                endpoint = "https://api.keenable.ai/v1/fetch"
                params = {"url": _url(query.url or ""), "max_chars": "12000", "live": "true"}
            case _:
                assert_never(query.action)
        headers["Authorization" if provider == "context7" else "X-API-Key"] = (
            "Bearer " + api_key if provider == "context7" else api_key
        )
        request = Request(endpoint + ("?" + urlencode(params) if params else ""), data=body, headers=headers)
        maximum = min(self.config.maximum_response_bytes, remaining)
        if not self._reserve_provider_call():
            return GroundingResult(error=GroundingError(code="provider_budget_exhausted")), None
        try:
            with build_opener(_NoRedirect()).open(request, timeout=self.config.timeout) as response:
                raw: bytes = response.read(maximum + 1)
                final_url: str = response.geturl()
            self._response_bytes += len(raw)
            if final_url != request.full_url:
                return GroundingResult(error=GroundingError(code="redirect_denied"), provider_calls=1), None
            if len(raw) > maximum:
                return GroundingResult(
                    error=GroundingError(code="response_too_large"), provider_calls=1
                ), None
            text = raw.decode("utf-8")
            decoded = TypeAdapter(JsonValue).validate_json(text, strict=True)
            if self._sensitive(text) or self._sensitive(_json(decoded)):
                return GroundingResult(
                    error=GroundingError(code="sensitive_response"), provider_calls=1
                ), None
            retrieved_at = datetime.now(UTC).isoformat()
            result = self._parse(query, text, retrieved_at).model_copy(update={"provider_calls": 1})
            if result.error is not None:
                return result, None
            cached = _Cached(
                request_sha256=key, response_sha256=_digest(raw), retrieved_at=retrieved_at, body=text
            )
            return result, cached
        except HTTPError as error:
            retry = error.headers.get("Retry-After")
            if retry is not None and (len(retry) > 100 or self._sensitive(retry)):
                retry = None
            error.close()
            return GroundingResult(
                error=GroundingError(code="http_error", status=error.code, retry_after=retry),
                provider_calls=1,
            ), None
        except (URLError, TimeoutError, HTTPException, OSError):
            return GroundingResult(error=GroundingError(code="transport_error"), provider_calls=1), None
        except (UnicodeDecodeError, ValidationError):
            return GroundingResult(error=GroundingError(code="invalid_response"), provider_calls=1), None

    def _parse(self, query: DocumentationQuery, body: str, retrieved_at: str) -> GroundingResult:
        documents: list[GroundingDocument] = []
        hits: list[DiscoveryResult] = []
        provider: Provider = "context7" if query.action.startswith("context7_") else "keenable"
        try:
            match query.action:
                case "context7_search" | "keenable_search":
                    response = _Search.model_validate_json(body)
                    for hit in response.results[:3] if provider == "keenable" else response.results:
                        if provider == "context7" and hit.id:
                            hits.append(
                                DiscoveryResult(
                                    library_id=hit.id, title=hit.title, metadata_json=hit.model_dump_json()
                                )
                            )
                        if provider == "keenable" and hit.url is not None and self._trusted(hit.url):
                            hits.append(
                                DiscoveryResult(
                                    url=_url(hit.url),
                                    title=hit.title,
                                    snippet=hit.snippet,
                                    metadata_json=hit.model_dump_json(),
                                )
                            )
                case "context7_context":
                    context = _Context.model_validate_json(body)
                    for info in context.infoSnippets:
                        self._document(
                            documents,
                            query,
                            info.pageId,
                            info.content,
                            provider,
                            retrieved_at,
                            GroundingMetadata(
                                kind="info",
                                returned_version=context.version,
                                provider_metadata_json=info.model_dump_json(exclude={"content"}),
                            ),
                        )
                    for code in context.codeSnippets:
                        self._document(
                            documents,
                            query,
                            code.codeId,
                            "\n\n".join(item.code for item in code.codeList),
                            provider,
                            retrieved_at,
                            GroundingMetadata(
                                kind="code",
                                returned_version=context.version,
                                provider_summary=code.codeDescription,
                                provider_metadata_json=code.model_dump_json(
                                    exclude={"codeList", "codeDescription"}
                                ),
                            ),
                        )
                case "keenable_fetch":
                    page = _Page.model_validate_json(body)
                    if not self._trusted(page.url):
                        return GroundingResult(error=GroundingError(code="untrusted_final_url"))
                    self._document(
                        documents,
                        query,
                        page.url,
                        page.content,
                        provider,
                        retrieved_at,
                        GroundingMetadata(
                            kind="page",
                            returned_version=page.version,
                            truncated=page.truncated,
                            provider_metadata_json=page.model_dump_json(exclude={"content"}),
                        ),
                    )
                case _:
                    assert_never(query.action)
        except (ValidationError, ValueError):
            return GroundingResult(error=GroundingError(code="invalid_response"))
        new_documents = {
            doc.evidence_id: doc for doc in documents if doc.evidence_id not in self._evidence_ids
        }
        retained = sum(len(doc.model_dump_json().encode()) for doc in new_documents.values())
        if self.evidence_bytes + retained > self.config.maximum_evidence_bytes:
            return GroundingResult(error=GroundingError(code="evidence_budget_exhausted"))
        self.evidence_bytes += retained
        self._evidence_ids.update(new_documents)
        if self.retain_responses:
            for doc in documents:
                artifact = doc.model_dump_json().encode()
                self.artifacts.write(f"documents/{_digest(artifact)}.json", artifact)
        return GroundingResult(documents=documents, discovery_results=hits)

    def _document(
        self,
        documents: list[GroundingDocument],
        query: DocumentationQuery,
        url: str,
        text: str,
        provider: Provider,
        retrieved_at: str,
        metadata: GroundingMetadata,
    ) -> None:
        if not self._trusted(url) or not text.strip():
            return
        canonical = _url(url)
        content_hash = _digest(text.encode())
        metadata = metadata.model_copy(
            update={"requested_library_id": query.library_id, "requested_version": query.requested_version}
        )
        evidence_id = _digest(
            _json(
                {
                    "provider": provider,
                    "url": canonical,
                    "content_sha256": content_hash,
                    "applicability": metadata.model_dump(
                        mode="json", exclude={"provider_summary", "provider_metadata_json"}
                    ),
                }
            ).encode()
        )
        documents.append(
            GroundingDocument(
                evidence_id=evidence_id,
                url=canonical,
                text=text,
                content_sha256=content_hash,
                retrieved_at=retrieved_at,
                provider=provider,
                metadata=metadata,
            )
        )

    def _receipt(
        self, query: DocumentationQuery, request_hash: str | None, result: GroundingResult
    ) -> GroundingResult:
        receipt = GroundingReceipt(
            index=len(self._history) + 1,
            query=query.model_copy(deep=True),
            request_sha256=request_hash,
            response_sha256=self._response_hashes.get(request_hash) if request_hash else None,
            evidence_ids=tuple(doc.evidence_id for doc in result.documents),
            error=result.error,
            provider_calls=result.provider_calls,
            cache_hit=result.cache_hit,
            total_provider_calls=self.provider_calls,
            response_bytes=self.response_bytes,
            evidence_bytes=self.evidence_bytes,
        )
        body = receipt.model_dump_json().encode()
        self.artifacts.write(f"receipts/{_digest(body)}.json", body)
        self._history.append(receipt)
        return result
