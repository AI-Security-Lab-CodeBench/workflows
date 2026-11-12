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
**Input:** Category scope and the source manifest prepared by the local agent.

**Goal:** Validate the declared dataset/corpus inventory for acquisition.

**Why:** Provenance, command relevance and reuse eligibility must be considered before extraction.
Relevant benign counterexamples are included; danger is not an exclusion reason.

**Output:** `SourceCatalogTable` with source URLs, licensing, revisions, samples and decision reasons.
Only eligible declared sources proceed. Invalid provenance or manifest data fails validation.
Source discovery, README/license assessment and scope enumeration happen outside Dagster.
Inspect the retained manifest and local preparation inventory; no selector model is invoked.
""",
    "source_documents": """
**Input:** The eligible source catalog and snapshot settings.

**Goal:** Capture the complete source text and its actual byte hash.

**Why:** Extraction must use full source bodies, not search snippets or bounded selection samples.

**Output:** `SourceDocumentsTable` with inert `content`, SHA-256 and source attribution.
Hash mismatches, oversized bodies, invalid UTF-8 or detected sensitive content fail acquisition.
Excluded sources remain in the catalog. UI samples truncate long cells; persisted text is complete.
Every output is bound to its current inventory; changed evidence makes dependent materializations stale.
Original documentation has a separately bound acquisition branch. Neither branch waits for human admission.
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
**Input:** Full source documents and their manifest-declared format mappings.

**Goal:** Bind each document to its declared format and command/context fields.

**Why:** Markdown, text and structured datasets require different decoding before extraction.

**Output:** `DocumentAdaptersTable`: source ID, format, text field, context field and rationale.
Mappings are declarative; optional reviewed Python batch transformations are explicitly trusted
and pinned by module hash in the manifest. Extraction checks actual field and output types.
Invalid mappings fail; no adapter model is invoked.
""",
    "command_candidates": """
**Input:** Source documents, per-document adapters and the surrounding-context window.

**Goal:** Extract broad command candidates while retaining their original evidence.

**Why:** Raw extraction must remain auditable before eligibility and safety judgments.

**Output:** `CommandCandidatesTable` with command, utility, context, source URL/position,
transformation markers and stable candidate identity. Distinct source records remain distinct.
Declared decoders or trusted hash-pinned batch transformations preserve original positions.
Invalid transformation hashes or canonical outputs fail extraction. Collected strings remain inert.
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
**Input:** Eligible filtered commands, category rules and the prepared semantic-reference manifest.

**Goal:** Expose the behavior, flags and version limits used to support judgments.

**Why:** Command names alone do not establish their effects or safety.

**Output:** `DocumentationTable` with utility, subcommands, source URL, retrieval time, snippet and limits.
References are prepared outside Dagster and used offline. Invalid reference records or byte limits
fail validation. Receipts retain operation-level gaps rather than inventing evidence.
Semantic use does not authorize corpus redistribution. Only the single labeling pass uses this table.
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
    "merged_proposed_annotations": """
**Input:** Current labeled outputs from available authorized collection branches and category rules.

**Goal:** Combine authorized Existing datasets and Documentation crawl records into one category table.

**Why:** Independent collection branches must converge before deduplication,
uncertainty-based human review and release, without a second command-labeling pass.

**Output:** `ProposedAnnotationsTable` retaining branch/source identities, original evidence
and each record's semantic documentation.
Unconfigured branches are distinct from configured branches with empty, waiting, failed or stale outputs.
Only current authorized inputs are accepted; historical or unauthorized outputs cannot substitute for them.
Final publication requires all configured branches to be current and complete.
""",
    "deduplicated_annotations": """
**Input:** The unified merged labels with original evidence and category rules.

**Goal:** Remove only proven exact normalized command duplicates before uncertainty-based human review.

**Why:** One category release must avoid exact duplicates without erasing meaningful command or label differences.

**Output:** `ProposedAnnotationsTable` of retained records with linked duplicate-to-survivor provenance
and exclusion reasons. Normalization preserves shell semantics, quoted text, flags, operands,
substitutions and meaningful context/label disagreements. Ambiguous similarities remain for review.
Fuzzy matching, resource renaming, safe-pair construction and model-conditioned deduplication are excluded.
""",
    "dataset_release": """
**Input:** Deduplicated annotations, category rules, completed human decisions and a version-bound authorization.

**Goal:** Produce the accepted canonical dataset table.

**Why:** Consumers need inclusion decisions and explicit per-row human-review provenance.

**Output:** `CommandsTable`: command, description, util, severity, category, risk, source, seed, class,
human_reviewed. Medium or higher uncertainty requires explicit human approval; eligible rows whose
four label confidences are high are automatic and have human_reviewed=false.
Human exclusions precede category-wide per-risk caps on the unified release. Snapshot, record-set
and frozen decision hashes are checked
again when output is committed; manual materialization cannot bypass this boundary.
Parquet and a ten-column CSV bundle are registered through an authoritative publication ledger.
Older published outputs remain historical. An empty human queue completes automatically with no human decisions.
Retracted or superseded approval marks old outputs stale; completing changed decisions rebuilds release.
""",
    "human_review_decisions": """
**Input:** The uncertain-row queue and its snapshot-bound completion authorization.

**Goal:** Materialize the exact completed batch as a typed decision table.

**Why:** Human decisions must be separate from machine labels and visibly upstream of publication.

**Output:** `HumanReviewDecisionsTable` with record identity, decision, notes, reviewer, revisions
and snapshot binding. Its Parquet metadata also binds empty tables to the completed batch.
Open reviews live in the durable ledger and Review tab; this asset materializes only at publication.
Unchanged row evidence and policy preserve saved decisions across changed table snapshots,
with original reviewer and carry-forward audit provenance. Nonempty snapshots require explicit completion;
changed or retracted row decisions are never resurrected from old approval.
An empty queue is completed by the automatic policy, with zero human decisions.
Reviewer names are self-reported. Approving a record does not authorize executing its command.
""",
    "human_review_queue": """
**Input:** The unified deduplicated annotations and category rules.

**Goal:** Expose final rows with medium or higher uncertainty for human inclusion decisions.

**Why:** Any medium/low classifier confidence requires human review; all four high
confidences permit automatic inclusion only when machine eligibility also passes.

**Output:** `ReviewQueueTable` with original candidate/label fields and `exclusion_reason`.
Machine-eligible records have an empty reason; caps are applied only after human exclusions.
This queue precedes human_review_decisions and release. Low-uncertainty rows remain visible in
deduplicated_annotations and need no human decisions. Source exclusions stay in source_catalog,
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
**Input:** The original-documentation manifest prepared by the local agent and category scope.
**Goal:** Validate the declared documentation inventory independently of corpus collection.
**Why:** Documentation and corpus inputs remain separate and retain their original provenance.
**Output:** SourceCatalogTable with declared source identity, format, provenance and reuse evidence.
Only eligible original documentation proceeds; invalid metadata fails validation.
Agent/SFT corpora cannot enter this branch. Discovery and license assessment occur outside Dagster;
no selector model, human source decision or synthetic approval is created.
""".strip()
        elif stage == "source_documents":
            description = """
**Input:** The selected documentation catalog and its own snapshot configuration.
**Goal:** Persist the exact inspected documentation bodies for command extraction.
**Why:** Documentation processing must use its own inspected inventory rather than corpus sources.
**Output:** SourceDocumentsTable bound to the documentation inventory and content hashes.
Selected, eligible documentation only; changed bytes, sensitive text and oversized sources fail.
This is a processing binding, not a human review token. Dataset inventory changes cannot authorize
or invalidate these independent documentation inputs.
""".strip()
        else:
            description = (
                "Documentation branch: uses only its own inspected-source binding and preparation. "
                "The dataset inventory is not an input.\n\n" + description
            )
    return spec.replace_attributes(description=description, group_name=group_name)
