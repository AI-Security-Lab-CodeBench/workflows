from pydantic import TypeAdapter

from .keenable_policy import (
    MAX_ORIGINS,
    MAX_RESULTS,
    PAGE_CASES,
    Case,
    CurrentKeenableDeclaration,
    Declaration,
    KeenableDeclaration,
    canonical_url,
    digest,
)
from .keenable_receipts import (
    CallReceipt,
    CatalogReceipt,
    Lead,
    SearchEnvelope,
    SearchPlanReceipt,
    SelectedPage,
)

LEADS: TypeAdapter[tuple[Lead, ...]] = TypeAdapter(tuple[Lead, ...])


def search_leads(
    declaration: Declaration,
    case: Case,
    envelope: SearchEnvelope,
    *,
    reviewed: frozenset[str] | None = None,
) -> tuple[Lead, ...]:
    leads: list[Lead] = []
    for rank, hit in enumerate(envelope.results[:MAX_RESULTS], 1):
        canonical = canonical_url(hit.url)
        if canonical is None:
            leads.append(
                Lead(
                    case=case,
                    rank=rank,
                    url_sha256=digest(hit.url.encode()),
                    url=None,
                    excluded="invalid_url",
                )
            )
        elif (reviewed is not None and canonical not in reviewed) or (
            not declaration.inspect_unlisted and not _admits_search_result(declaration, canonical, case)
        ):
            leads.append(
                Lead(
                    case=case,
                    rank=rank,
                    url_sha256=digest(hit.url.encode()),
                    url=None,
                    excluded="outside_case",
                )
            )
        else:
            leads.append(Lead(case=case, rank=rank, url_sha256=digest(hit.url.encode()), url=canonical))
    return tuple(leads)


def _admits_search_result(declaration: Declaration, url: str, case: Case) -> bool:
    if isinstance(declaration, CurrentKeenableDeclaration):
        return declaration.admits_search_result(url, case)
    return (
        PAGE_CASES.get(url) == case
        if declaration.revision == "1"
        else declaration.authority(url, case) is not None
    )


def freeze_catalog(
    declaration: Declaration,
    leads: list[Lead],
    searches: list[CallReceipt],
    plan: SearchPlanReceipt | None = None,
) -> CatalogReceipt:
    case_order = {case: index for index, case in enumerate(declaration.cases)}
    maximum = (declaration.budget.origin_calls if declaration.budget else MAX_ORIGINS) - len(
        declaration.raw_licenses
    )
    ordered = sorted(
        leads,
        key=lambda lead: (
            lead.origin != "declared",
            not (isinstance(declaration, KeenableDeclaration) and declaration.revision == "1")
            and (lead.url is None or declaration.authority(lead.url, lead.case) is None),
            lead.rank,
            case_order[lead.case],
        ),
    )
    ranked: list[Lead] = []
    selected: dict[str, SelectedPage] = {}
    for lead in ordered:
        url = lead.url
        if url is not None:
            if url in selected:
                lead = lead.model_copy(update={"excluded": "duplicate"})
            elif len(selected) >= maximum:
                lead = lead.model_copy(update={"excluded": "selection_budget"})
            else:
                permission = declaration.authority(url, lead.case)
                selected[url] = SelectedPage(
                    lead=lead,
                    permission=permission,
                    state="pending"
                    if permission
                    else "inspect"
                    if declaration.inspect_unlisted
                    else "withheld",
                )
        ranked.append(lead)
    return CatalogReceipt(
        declaration_sha256=declaration.declaration_sha256,
        leads=tuple(ranked),
        selected=tuple(selected.values()),
        searches=tuple(searches),
        plan=plan,
    )


def declared_leads(declaration: Declaration) -> list[Lead]:
    """Resolve preverified reference targets without asking search to find them again."""
    return [
        Lead(case=source.cases[0], rank=rank, url_sha256=digest(url.encode()), url=url, origin="declared")
        for source in sorted(declaration.sources, key=lambda source: source.origin_id)
        for rank, url in enumerate(source.exact_urls, 1)
    ]
