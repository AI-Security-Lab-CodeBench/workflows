from __future__ import annotations

from collections.abc import Iterator

from .discovery_models import DiscoveryError, InspectedPage, InspectedSelection, SourceProposal


def _source_pages(
    selection: InspectedSelection, pages: list[InspectedPage]
) -> Iterator[tuple[SourceProposal, InspectedPage]]:
    inspected = {(page.risk, page.url): page for page in pages}
    ids: set[str] = set()
    for proposal in selection.sources:
        if proposal.source_id in ids:
            raise DiscoveryError("Duplicate proposed source ID")
        ids.add(proposal.source_id)
        page = inspected.get((proposal.risk, proposal.url))
        if page is None:
            raise DiscoveryError("Model selected a source that was not inspected for this risk")
        yield proposal, page


def gate_documentation(selection: InspectedSelection, pages: list[InspectedPage]) -> InspectedSelection:
    """Admit semantic references independently of unresolved corpus reuse terms."""
    sources: list[SourceProposal] = []
    for proposal, page in _source_pages(selection, pages):
        excluded = proposal.origin in {"agent", "sft", "synthetic"} or any(
            token in proposal.source_type.lower() for token in ("agent", "sft", "synthetic", "generated")
        )
        if excluded or proposal.reuse_decision == "prohibited":
            proposal = proposal.model_copy(
                update={"decision": "exclude", "rationale": "Excluded origin or explicit reuse prohibition."}
            )
        elif proposal.decision == "proposed" and (
            proposal.origin == "unknown"
            or not proposal.revision.strip()
            or any(
                not excerpt.strip() or excerpt not in page.text
                for excerpt in (proposal.commands_evidence, proposal.origin_evidence)
            )
        ):
            proposal = proposal.model_copy(
                update={
                    "decision": "needs_review",
                    "rationale": "Semantic reference lacks grounded command, provenance or revision evidence.",
                }
            )
        sources.append(proposal.model_copy(update={"adapter_requested": False}))
    return InspectedSelection(sources=sources)


def gate_selection(selection: InspectedSelection, pages: list[InspectedPage]) -> InspectedSelection:
    by_url = {page.url: page for page in pages}
    sources: list[SourceProposal] = []
    for proposal, page in _source_pages(selection, pages):
        license_page = by_url.get(proposal.license_url)
        ungrounded = (
            any(
                excerpt and excerpt not in page.text
                for excerpt in (proposal.commands_evidence, proposal.origin_evidence)
            )
            or bool(proposal.license_url and proposal.license_url not in by_url)
            or bool(
                proposal.license_evidence
                and (license_page is None or proposal.license_evidence not in license_page.text)
            )
        )
        excluded = proposal.origin in {"agent", "sft", "synthetic"} or any(
            token in proposal.source_type.lower() for token in ("agent", "sft", "synthetic", "generated")
        )
        excluded = excluded or proposal.reuse_decision == "prohibited"
        unknown = (
            proposal.origin == "unknown"
            or not proposal.origin_evidence.strip()
            or not proposal.commands_evidence.strip()
            or proposal.license.strip().lower() in {"", "unknown", "unverified", "not specified", "n/a"}
            or not proposal.license_evidence.strip()
            or proposal.license.strip().lower() not in proposal.license_evidence.lower()
            or not proposal.license_url
            or proposal.reuse_decision != "permitted"
            or not proposal.revision.strip()
            or ungrounded
        )
        if excluded:
            proposal = proposal.model_copy(
                update={
                    "decision": "exclude",
                    "rationale": "Excluded origin or prohibited reuse terms.",
                    "adapter_requested": False,
                }
            )
        elif unknown and proposal.decision == "proposed":
            proposal = proposal.model_copy(
                update={
                    "decision": "needs_review",
                    "rationale": (
                        "Command, origin or license claims are not verbatim inspected evidence."
                        if ungrounded
                        else "Inspected license, command or human-origin evidence is incomplete."
                    ),
                    "adapter_requested": False,
                }
            )
        if proposal.decision != "proposed" and proposal.adapter_requested:
            proposal = proposal.model_copy(update={"adapter_requested": False})
        sources.append(proposal)
    return InspectedSelection(sources=sources)
