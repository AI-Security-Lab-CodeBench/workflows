from hashlib import sha256
from typing import Final

from pandera.typing import DataFrame
from pydantic import TypeAdapter

from codebench_pipeline.dataframe_schemas import (
    CommandCandidatesTable,
    DocumentAdaptersTable,
    SourceCatalogTable,
    SourceDocumentsTable,
    validated_frame,
)
from codebench_pipeline.keenable import KeenableCollection
from codebench_pipeline.keenable_policy import (
    PAGE_CASES,
    CurrentSourcePermission,
    KeenableError,
    PagePermission,
    Permission,
)
from codebench_pipeline.keenable_receipts import (
    AdapterReceipt,
    CandidateReceipt,
    CaptureLock,
    CatalogReceipt,
)

PROVIDER_RECEIPT: Final = "keenable_receipt_json"
ADAPTER_RECEIPTS: Final = TypeAdapter(tuple[AdapterReceipt, ...])
CANDIDATE_RECEIPTS: Final = TypeAdapter(tuple[CandidateReceipt, ...])


def provider_source_id(url: str) -> str:
    return "keenable-" + sha256(url.encode()).hexdigest()


def provider_catalog(collection: KeenableCollection, api_key: str) -> DataFrame[SourceCatalogTable]:
    catalog = collection.discover_with_credential(api_key)
    permissions = {page.lead.url: page.permission for page in catalog.selected}
    inspections = {page.lead.url for page in catalog.selected if page.state == "inspect"}
    frame = validated_frame(
        [
            {
                "source_id": f"{lead.search_slot or lead.case}-{lead.rank}-{lead.url_sha256}",
                "source_url": lead.url or "",
                "license": license_.identifier if license_ else "",
                "license_url": license_.license_url if license_ else "",
                "attribution": permission.attribution if permission else "",
                "revision": collection.declaration.generation,
                "retrieved_at": "",
                "source_type": "web_page" if unlisted else "official_documentation",
                "provenance_type": "web_search" if unlisted else "vendor_documentation",
                "source_locator": lead.search_slot or lead.case,
                "sha256": permission.content_sha256 if isinstance(permission, PagePermission) else "",
                "format": "text",
                "text_field": "",
                "context_field": "",
                "sample": "",
                "captured_content": "",
                "decision": (
                    "selected"
                    if lead.excluded is None and (permission is not None or lead.url in inspections)
                    else "exclude"
                ),
                "rationale": lead.excluded
                or (
                    "permission"
                    if permission is not None
                    else "inspect_page_license"
                    if lead.url in inspections
                    else "withheld"
                ),
                "analysis": (
                    _rights_description(permission) if isinstance(permission, CurrentSourcePermission) else ""
                ),
                "confidence": "not_applicable",
            }
            for lead in catalog.leads
            for permission in (permissions.get(lead.url),)
            for unlisted in (
                collection.declaration.inspect_unlisted and lead.origin == "search" and permission is None,
            )
            for license_ in (
                permission.public_license
                if permission is not None and not isinstance(permission, PagePermission)
                else None,
            )
        ],
        SourceCatalogTable,
    )
    frame.attrs[PROVIDER_RECEIPT] = catalog.model_dump_json()
    return frame


def provider_documents(
    collection: KeenableCollection, catalog: DataFrame[SourceCatalogTable], api_key: str
) -> DataFrame[SourceDocumentsTable]:
    lock = collection.capture_with_credential(
        CatalogReceipt.model_validate_json(catalog.attrs[PROVIDER_RECEIPT]), api_key
    )
    bodies = collection.read_bodies(lock)
    permissions: dict[str, Permission] = {}
    for page in lock.pages:
        if page.observed_license is not None:
            continue
        permission = collection.declaration.authority(page.url, page.case or PAGE_CASES[page.url])
        if permission is None:
            raise KeenableError("capture_authorization_mismatch")
        permissions[page.url] = permission
    frame = validated_frame(
        [
            {
                "source_id": provider_source_id(page.url),
                "source_url": page.url,
                "license": "CC-BY-4.0" if observed else license_.identifier if license_ else "",
                "license_url": observed.license_url if observed else license_.license_url if license_ else "",
                "attribution": observed.attribution
                if observed
                else permission.attribution
                if permission
                else "",
                "revision": collection.declaration.generation,
                "retrieved_at": page.call.observed_at or "",
                "source_type": "web_page" if observed else "official_documentation",
                "provenance_type": "web_search" if observed else "vendor_documentation",
                "source_locator": page.call.slot,
                "sha256": page.body_sha256,
                "format": "text",
                "text_field": "",
                "context_field": "",
                "content": bodies[page.url].decode("utf-8"),
            }
            for page in lock.pages
            for observed in (page.observed_license,)
            for permission in (permissions.get(page.url),)
            for license_ in (
                permission.public_license
                if permission and not isinstance(permission, PagePermission)
                else None,
            )
        ],
        SourceDocumentsTable,
    )
    frame.attrs[PROVIDER_RECEIPT] = lock.model_dump_json()
    return frame


def provider_adapters(
    collection: KeenableCollection, documents: DataFrame[SourceDocumentsTable]
) -> DataFrame[DocumentAdaptersTable]:
    lock = CaptureLock.model_validate_json(documents.attrs[PROVIDER_RECEIPT])
    adapters = collection.adapters(lock)
    frame = validated_frame(
        [
            {
                "source_id": provider_source_id(adapter.url),
                "format": "text",
                "text_field": "",
                "context_field": "",
                "rationale": adapter.parser,
            }
            for adapter in adapters
        ],
        DocumentAdaptersTable,
    )
    frame.attrs[PROVIDER_RECEIPT] = ADAPTER_RECEIPTS.dump_json(adapters).decode()
    return frame


def provider_candidates(
    collection: KeenableCollection,
    documents: DataFrame[SourceDocumentsTable],
    adapters: DataFrame[DocumentAdaptersTable],
) -> DataFrame[CommandCandidatesTable]:
    lock = CaptureLock.model_validate_json(documents.attrs[PROVIDER_RECEIPT])
    receipts = ADAPTER_RECEIPTS.validate_json(adapters.attrs[PROVIDER_RECEIPT])
    candidates = collection.candidates(lock, receipts)
    parsers = {adapter.url: adapter.parser for adapter in receipts}
    frame = validated_frame(
        [
            {
                "candidate_id": candidate.candidate_id,
                "command": candidate.command,
                "util": candidate.command.split()[0],
                "context": "",
                "source_id": provider_source_id(candidate.url),
                "source_url": candidate.url,
                "source_position": f"bytes:{candidate.start}:{candidate.end}",
                "transformations": [parsers[candidate.url]],
                "risk_candidates": [],
            }
            for candidate in candidates
        ],
        CommandCandidatesTable,
    )
    frame.attrs[PROVIDER_RECEIPT] = CANDIDATE_RECEIPTS.dump_json(candidates).decode()
    frame.attrs["keenable_capture_lock"] = lock.model_dump_json()
    return frame


def _rights_description(permission: CurrentSourcePermission) -> str:
    match permission.rights.kind:
        case "pinned_license_file":
            return "Pinned license file; capture verifies the evidence bytes"
        case "page_notice":
            return "Declared evidence metadata; capture checks the notice in the page"
        case "steward_decision":
            return "Declared steward decision and evidence metadata; no runtime lookup"
