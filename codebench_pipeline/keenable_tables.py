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
    KeenableDeclaration,
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


def provider_catalog(collection: KeenableCollection) -> DataFrame[SourceCatalogTable]:
    catalog = collection.discover()
    permissions = {page.lead.url: page.permission for page in catalog.selected}
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
                "source_type": "official_documentation",
                "provenance_type": "vendor_documentation",
                "source_locator": lead.search_slot or lead.case,
                "sha256": permission.content_sha256 if isinstance(permission, PagePermission) else "",
                "format": "text",
                "text_field": "",
                "context_field": "",
                "sample": "",
                "captured_content": "",
                "decision": "selected" if permission is not None and lead.excluded is None else "exclude",
                "rationale": lead.excluded or ("permission" if permission is not None else "withheld"),
                "analysis": (
                    _rights_description(permission) if isinstance(permission, CurrentSourcePermission) else ""
                ),
                "confidence": "not_applicable",
            }
            for lead in catalog.leads
            for permission in (permissions.get(lead.url),)
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
    collection: KeenableCollection, catalog: DataFrame[SourceCatalogTable]
) -> DataFrame[SourceDocumentsTable]:
    lock = collection.capture(CatalogReceipt.model_validate_json(catalog.attrs[PROVIDER_RECEIPT]))
    bodies = collection.read_bodies(lock)
    permissions: dict[str, Permission] = {}
    for page in lock.pages:
        permission = collection.declaration.authority(page.url, page.case or PAGE_CASES[page.url])
        if permission is None:
            raise KeenableError("capture_authorization_mismatch")
        permissions[page.url] = permission
    frame = validated_frame(
        [
            {
                "source_id": provider_source_id(page.url),
                "source_url": page.url,
                "license": license_.identifier if license_ else "",
                "license_url": license_.license_url if license_ else "",
                "attribution": permission.attribution,
                "revision": collection.declaration.generation,
                "retrieved_at": page.call.observed_at or "",
                "source_type": "official_documentation",
                "provenance_type": "vendor_documentation",
                "source_locator": page.call.slot,
                "sha256": page.body_sha256,
                "format": "text",
                "text_field": "",
                "context_field": "",
                "content": bodies[page.url].decode("utf-8"),
            }
            for page in lock.pages
            for permission in (permissions[page.url],)
            for license_ in (None if isinstance(permission, PagePermission) else permission.public_license,)
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
    candidates = collection.candidates(lock, ADAPTER_RECEIPTS.validate_json(adapters.attrs[PROVIDER_RECEIPT]))
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
                "transformations": [
                    "reviewed-byte-spans-v1"
                    if isinstance(collection.declaration, KeenableDeclaration)
                    and collection.declaration.revision == "1"
                    else "literal-code-blocks-v2"
                ],
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
