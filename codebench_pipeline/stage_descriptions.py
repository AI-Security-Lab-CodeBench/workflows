from typing import Protocol

from dagster import AssetKey, AssetSpec

DESCRIPTIONS: dict[str, str] = {
    "category_rules": """
**Input:** The category's inline taxonomy, version, policy, command families and language.

**Goal:** Preserve the exact rules used by selection, labeling, review and publication.

**Why:** Category policy must be explicit and inspectable, not hidden in Python or prompts.

**Output:** A validated `CategoryRules` value stored as JSON, with rule/version metadata.
This is the only non-table asset; inspect its `rules` metadata rather than a row sample.
Patterns route candidates but never assign safety labels.
""",
    "source_catalog": """
**Input:** Category scope and provided source seeds or bounded live searches, plus the selector model.

**Goal:** Propose which dataset/corpus sources are eligible for human selection.

**Why:** Provenance, command relevance and reuse eligibility must be considered before extraction.
Relevant benign counterexamples are included; danger is not an exclusion reason.

**Output:** `SourceCatalogTable` with source URLs, licensing, revisions, samples and decision reasons.
Model selection is not human approval. Source selection must be completed before acquisition proceeds.
Exclusions stay here. Search snippets are not inspected page evidence.
Inspect `sample` and the retained selection/search receipts.
""",
    "source_documents": """
**Input:** Source catalog, completed human source decisions and snapshot settings.

**Goal:** Capture the complete source text and its actual byte hash.

**Why:** Extraction must use full source bodies, not search snippets or bounded selection samples.

**Output:** `SourceDocumentsTable` with inert `content`, SHA-256 and source attribution.
Hash mismatches, oversized bodies, invalid UTF-8 or detected sensitive content fail acquisition.
Excluded sources remain in the catalog. UI samples truncate long cells; persisted text is complete.
Every output is bound to current source approval; retraction makes dependent materializations stale.
Original documentation has a separate acquisition branch, without this human-admission dependency.
""",
    "human_source_decisions": """
**Input:** The discovered/inspected source catalog and a completed Source selection review.

**Goal:** Materialize the exact source choices before any source is used downstream.

**Why:** An agent's relevance/license assessment does not mean a human picked that source.

**Output:** Version-bound `HumanReviewDecisionsTable` over source IDs. Only approved, eligible
catalog rows proceed to source_documents. Manual materialization uses the same ledger gate.
Retraction invalidates descendants; completing a revised selection continues the pipeline.
""",
    "document_adapters": """
**Input:** Full source documents and provided mappings or adapter-model settings.

**Goal:** Identify each document's format and command/context fields.

**Why:** Markdown, text and structured datasets require different decoding before extraction.

**Output:** `DocumentAdaptersTable`: source ID, format, text field, context field and rationale.
Mappings are declarative, never executable generated parsers. Extraction later checks actual field types.
Inspect each mapping and, for inferred mappings, its independent model receipt.
""",
    "command_candidates": """
**Input:** Source documents, per-document adapters and the surrounding-context window.

**Goal:** Extract broad command candidates while retaining their original evidence.

**Why:** Raw extraction must remain auditable before eligibility and safety judgments.

**Output:** `CommandCandidatesTable` with command, utility, context, source URL/position,
transformation markers and stable candidate identity. Distinct source records remain distinct.
Extraction is line-oriented, not a full shell parser; candidates may still contain messy or unrelated text.
""",
    "filtered_commands": """
**Input:** All raw candidates, category command-family patterns and filtering settings.

**Goal:** Mark candidates eligible for inference or quarantine them with a concrete reason.

**Why:** Unrelated text, unresolved templates and opaque shell forms must not become guessed labels.

**Output:** `FilteredCommandsTable`, retaining every candidate and adding risk routing,
`filter_decision` and `filter_reason`. Filtering does not assign safety labels.
Quarantined records remain here, not in the downstream human-review queue.
""",
    "labeling_documentation": """
**Input:** Eligible filtered commands, category rules and supplied or live documentation-search settings.

**Goal:** Expose the behavior, flags and version limits used to support judgments.

**Why:** Command names alone do not establish their effects or safety.

**Output:** `DocumentationTable` with utility, subcommands, source URL, retrieval time, snippet and limits.
Provided snippets stay offline. Live mode searches/fetches inspected documentation, preserves
provenance, and requires snippets to occur exactly in eligible source text. Unresolved licensing
does not block semantic references; separate reuse-review metadata does not authorize corpus reuse
or redistribution. Explicit prohibitions remain excluded. Receipts retain gaps and exclusions
rather than inventing evidence. Both labeling and review depend on this table.
""",
    "proposed_annotations": """
**Input:** Eligible filtered candidates, documentation, category rules and labeler settings.

**Goal:** Label each record independently from original evidence.

**Why:** Parse eligibility and pattern matching cannot determine semantic safety judgments.

**Output:** `ProposedAnnotationsTable` with candidate evidence, proposed category/risk/class/severity,
description, four confidences, rationale and citations. Each record uses a fresh isolated invocation.
Malformed responses fail with retained evidence; acceptance requires supported taxonomy and documentation.
Confidence is self-reported, not proof of safety.
""",
    "reviewed_annotations": """
**Input:** Unified proposed annotations plus each record's original evidence, documentation,
rules and reviewer settings.

**Goal:** Independently check the proposed judgments and preserve disagreements.

**Why:** A structurally valid label can still lack semantic support.

**Output:** `ReviewedAnnotationsTable`, preserving proposals and adding `review_verdict`,
`review_rationale` and low/medium/high `review_confidence`. Classifier confidences use the same
enum for category, risk, severity and class. Review uses separate isolated, bounded context
packets. Validated per-record results and receipts are reused when that record's relevant
inputs and review policy are unchanged; neighboring records do not invalidate them.
Agentic approval is not human authorization; invalid review output fails with retained evidence.
""",
    "merged_proposed_annotations": """
**Input:** Current labeled outputs from available authorized collection branches and category rules.

**Goal:** Combine authorized Existing datasets and Documentation crawl records into one category table.

**Why:** Independent collection branches must converge before shared agentic review,
deduplication, human review and release.

**Output:** `ProposedAnnotationsTable` retaining branch/source identities, original evidence
and each record's semantic documentation.
Unconfigured branches are distinct from configured branches with empty, waiting, failed or stale outputs.
Only current authorized inputs are accepted; historical or unauthorized outputs cannot substitute for them.
Available rows can receive agentic review while another branch waits. Final publication requires
all configured branches to be current and complete.
""",
    "deduplicated_annotations": """
**Input:** The unified merged reviewed annotations with original evidence and category rules.

**Goal:** Remove only proven exact normalized command duplicates before mandatory human review.

**Why:** One category release must avoid exact duplicates without erasing meaningful command or label differences.

**Output:** `ReviewedAnnotationsTable` of retained records with linked duplicate-to-survivor provenance
and exclusion reasons. Normalization preserves shell semantics, quoted text, flags, operands,
substitutions and meaningful context/label disagreements. Ambiguous similarities remain for review.
Fuzzy matching, resource renaming, safe-pair construction and model-conditioned deduplication are excluded.
""",
    "dataset_release": """
**Input:** Deduplicated annotations, category rules, completed human decisions and a version-bound authorization.

**Goal:** Produce the accepted canonical dataset table.

**Why:** Consumers need inclusion decisions and explicit per-row human-review provenance.

**Output:** `CommandsTable`: command, description, util, severity, category, risk, source, seed, class,
human_reviewed. Every retained record requires explicit human approval, regardless of confidence.
Human exclusions precede category-wide per-risk caps on the unified release. Snapshot, record-set
and frozen decision hashes are checked
again when output is committed; manual materialization cannot bypass this boundary.
Parquet and a ten-column CSV bundle are registered through an authoritative publication ledger.
Older published outputs remain historical. An empty release requires explicit completion.
Retracted or superseded approval marks old outputs stale; completing changed decisions rebuilds release.
""",
    "human_review_decisions": """
**Input:** The prepared review queue and an authorization issued by explicit human completion.

**Goal:** Materialize the exact completed batch as a typed decision table.

**Why:** Human decisions must be separate from machine labels and visibly upstream of publication.

**Output:** `HumanReviewDecisionsTable` with record identity, decision, notes, reviewer, revisions
and snapshot binding. Its Parquet metadata also binds empty tables to the completed batch.
Open reviews live in the durable ledger and Review tab; this asset materializes only at publication.
Unchanged row evidence and policy preserve saved decisions across changed table snapshots,
with original reviewer and carry-forward audit provenance. New snapshots require explicit completion;
changed or retracted row decisions are never resurrected from old approval.
Reviewer names are self-reported. Approving a record does not authorize executing its command.
""",
    "human_review_queue": """
**Input:** The unified deduplicated annotations and category rules.

**Goal:** Expose every retained release candidate for mandatory human inclusion decisions.

**Why:** Confidence informs human review; it never bypasses approval or creates an automatic release.

**Output:** `ReviewQueueTable` with original candidate/label/review fields and `exclusion_reason`.
Machine-eligible records have an empty reason; caps are applied only after human exclusions.
This queue precedes human_review_decisions and release. Source exclusions stay in source_catalog,
filtered candidates stay in filtered_commands, and inference failures stay in observations/receipts.
Machine-rejected or unsupported rows remain ineligible. No row here implies human sign-off.
""",
}


class DescribedAsset(Protocol):
    @property
    def key(self) -> AssetKey: ...

    def replace_attributes(self, *, description: str, group_name: str) -> AssetSpec: ...


def describe_stage(spec: DescribedAsset) -> AssetSpec:
    stage = spec.key.path[-1]
    description = DESCRIPTIONS[stage].strip()
    group_name = "existing_datasets"
    if stage == "category_rules":
        group_name = "dataset_configuration"
    elif len(spec.key.path) == 2 and stage in {
        "merged_proposed_annotations",
        "reviewed_annotations",
        "deduplicated_annotations",
        "human_review_queue",
        "human_review_decisions",
        "dataset_release",
    }:
        group_name = "dataset_finalization"
    if len(spec.key.path) == 3 and spec.key.path[1] == "documentation":
        group_name = "documentation_crawl"
        if stage == "source_catalog":
            description = """
**Input:** Explicit original-documentation sources or bounded documentation searches and category scope.
**Goal:** Select trustworthy, relevant documentation command sources independently of dataset admission.
**Why:** Documentation and corpus discovery are separate branches, not interchangeable approval paths.
**Output:** SourceCatalogTable with actual analysis, rationale, enum confidence, provenance and reuse evidence.
Only eligible original documentation proceeds. Missing license/trust evidence stays withheld; agent/SFT
corpora cannot enter this branch. No human source decision or synthetic approval is created.
""".strip()
        elif stage == "source_documents":
            description = """
**Input:** The selected documentation catalog and its own snapshot configuration.
**Goal:** Persist the exact inspected documentation bodies for command extraction.
**Why:** Documentation processing must proceed without reading unapproved dataset sources.
**Output:** SourceDocumentsTable bound to the documentation inventory and content hashes.
Selected, eligible documentation only; changed bytes, sensitive text and oversized sources fail.
This is a processing binding, not a human review token. Dataset approval/retraction cannot authorize
or invalidate these independent documentation inputs.
""".strip()
        else:
            description = (
                "Documentation branch: uses only its own inspected-source binding and preparation. "
                "Pending dataset admission is not an input.\n\n" + description
            )
    return spec.replace_attributes(description=description, group_name=group_name)
