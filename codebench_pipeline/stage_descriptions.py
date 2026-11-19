from typing import Protocol

from dagster import AssetKey, AssetSpec

from codebench_pipeline.bashguard_audit import AUDIT_STAGE
from codebench_pipeline.collection_descriptions import (
    DECLARED_COLLECTION_DESCRIPTIONS,
    FINALIZATION_DESCRIPTIONS,
)
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, FINALIZATION_STAGES, asset_branch

DESCRIPTIONS: dict[str, str] = {
    AUDIT_STAGE: """
**How it works:** BashGuard parses the inert text in each saved merged candidate and
walks the syntax tree for features such as pipelines, redirects and substitutions.
The audit keeps the merged candidate ID and original source and candidate IDs so each
result can be traced to the original row. It counts candidates by parser outcome and
counts the number of candidates containing each flag.

**Input:** The previously materialized merged candidate table for this partition.
**Output:** A `BashGuardAuditTable` with one row per candidate and aggregate metrics.
Unparseable or oversized text remains in the table with its parser outcome and no
flags. An unavailable analyzer or invalid analyzer response fails only this audit.
The parser never executes Bash or decodes payloads. Flags do not assign labels or
change eligibility, review or publication.
""",
    "category_rules": """
**How it works:** Registration combines the shared labeling policy with the selected
category's taxonomy, instructions and examples. This asset reads that registered
snapshot for the dataset partition. Each consumer receives the same resolved rules.
Command patterns route records to risks; they do not assign labels.

**Input:** Registered shared instructions, class and severity definitions, output language,
and the selected category's membership rules, routing, defaults and examples.

**Goal:** Preserve the exact rules used by selection, labeling, review and publication.

**Why:** Category policy must be explicit and inspectable, not hidden in Python or prompts.

**Output:** A validated `CategoryRules` value stored as JSON, with rule/version metadata.
Inspect its `rules` metadata rather than a row sample.
Patterns route candidates but never assign safety labels.
""",
    "source_catalog": """
**How it works:** A source manifest prepared outside the pipeline becomes a structured
inventory of the declared corpus files, their formats and their provenance and reuse
evidence. The catalog preserves the source settings needed for retrieval, so acquisition
can follow the inspected inventory rather than rediscovering sources or asking a model
to choose them.

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
**How it works:** Each eligible catalog entry is loaded from its declared local or remote
location and checked against its pinned integrity and content limits. The complete text
is stored with a content hash and source attribution, giving extraction a reproducible
copy of the source rather than a search snippet. A failed check stops acquisition instead
of quietly substituting different bytes.

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
**How it works:** This historical stage converted a completed source-selection review
into a version-bound table of human decisions, which formerly controlled acquisition.
It is no longer part of the active pipeline: current collection follows eligible
manifests directly. Old decisions remain evidence of past reviews, not permission
to acquire or publish current data.

**Input:** The discovered/inspected source catalog and a completed Source selection review.

**Goal:** Materialize the exact source choices before any source is used downstream.

**Why:** An agent's relevance/license assessment does not mean a human picked that source.

**Output:** Version-bound `HumanReviewDecisionsTable` over source IDs. Only approved, eligible
catalog rows proceed to source_documents. Manual materialization uses the same ledger gate.
Retraction invalidates descendants; completing a revised selection continues the pipeline.
""",
    "document_adapters": """
**How it works:** Each captured document is paired with its declared format and the fields
that contain command text and context. This creates a decoding plan for extraction rather
than guessing the document's structure with a model. When a source needs custom conversion,
the manifest names a reviewed, hash-pinned Python transformation; downloaded content itself
is never treated as executable code.

**Input:** Full source documents and their manifest-declared format mappings.

**Goal:** Bind each document to its declared format and command/context fields.

**Why:** Markdown, text and structured datasets require different decoding before extraction.

**Output:** `DocumentAdaptersTable`: source ID, format, text field, context field and rationale.
Mappings are declarative; optional reviewed Python batch transformations are explicitly trusted
and pinned by module hash in the manifest. Extraction checks actual field and output types.
Invalid mappings fail; no adapter model is invoked.
""",
    "command_candidates": """
**How it works:** Extraction follows each document's decoding plan, using the declared
fields or a trusted batch transformation to collect commands with their surrounding
context. Each candidate gets an identity derived from its source, position and command,
while transformation markers retain how it was obtained. The result is a broad, traceable
candidate table; command text is not executed or assigned a safety label here.

**Input:** Source documents, per-document adapters and the surrounding-context window.

**Goal:** Extract broad command candidates while retaining their original evidence.

**Why:** Raw extraction must remain auditable before eligibility and safety judgments.

**Output:** `CommandCandidatesTable` with command, utility, context, source URL/position,
transformation markers and stable candidate identity. Distinct source records remain distinct.
Declared decoders or trusted hash-pinned batch transformations preserve original positions.
Invalid transformation hashes or canonical outputs fail extraction. Collected strings remain inert.
""",
    "filtered_commands": """
**How it works:** Each candidate is matched against the category's command-family patterns,
then checked for oversized text, unresolved placeholders, opaque shell constructs and
unbalanced quotes. Every row is retained with either an eligible marker or a concrete
quarantine reason. This deterministic pass decides what can proceed to labeling, not
whether an eligible command is safe.

**Input:** All raw candidates, category command-family patterns and filtering settings.

**Goal:** Mark candidates eligible for inference or quarantine them with a concrete reason.

**Why:** Unrelated text, unresolved templates and opaque shell forms must not become guessed labels.

**Output:** `FilteredCommandsTable`, retaining every candidate and adding risk routing,
`filter_decision` and `filter_reason`. Filtering does not assign safety labels.
Quarantined records remain here, not in the downstream human-review queue.
""",
    "labeling_documentation": """
**How it works:** Prepared semantic references are loaded and validated into a documentation
table, with a receipt comparing their coverage against the eligible command operations.
Missing coverage stays visible as a gap rather than being filled with invented evidence.
The labeler uses these offline references to reason about command behavior; this stage
does not search for sources or label commands.

**Input:** Eligible filtered commands, category rules and the prepared semantic-reference manifest.

**Goal:** Expose the behavior, flags and version limits used to support judgments.

**Why:** Command names alone do not establish their effects or safety.

**Output:** `DocumentationTable` with utility, subcommands, source URL, retrieval time, snippet and limits.
References are prepared outside Dagster and used offline. Invalid reference records or byte limits
fail validation. Receipts retain operation-level gaps rather than inventing evidence.
Semantic use does not authorize corpus redistribution. Only the single labeling pass uses this table.
""",
    "labeling_attempts": """
**How it works:** Eligible commands first reuse validated record-level results. Enhanced
labeling groups the remaining commands by vector similarity and lets a bounded model
conversation either submit independent labels or request shared Keenable/Context7
documentation. Each outcome is checked against its original command, routing and evidence.
Unsupported or invalid outcomes are retained as failures, never filled with guessed labels.
The native baseline mode retains its isolated single-command invocation behavior.

**Input:** Eligible filtered candidates, documentation, category rules and labeler settings.

**Goal:** Account for every eligible command with a supported label or inspectable failure.

**Why:** Similar commands can share retrieval without sharing judgments or losing provenance.

**Output:** `LabelingAttemptsTable`, with exactly one success or failure per eligible candidate,
original evidence, document hashes and invocation references. Enhanced remote calls require
fresh metrics and a shared 50% slot/token admission guard. Capacity waiting does not become a
labeling failure. Corrupt coverage or changed upstream bindings fail materialization.
""",
    "proposed_annotations": """
**How it works:** The current labeling-attempt table is validated, and only successful
outcomes are decoded into complete command labels. Each label retains its own documentation
and original inference reference. Grounded rejections and context-dependent needs_review
labels remain inspectable; failed attempts cannot enter this projection.

**Input:** Current labeling attempts and category rules.
**Goal:** Supply complete, validated proposals to exact deduplication and eligibility.
**Why:** Retrieval or protocol failures must not masquerade as accepted or rejected labels.
**Output:** `ProposedAnnotationsTable` with original evidence, category, risk, severity, class,
four confidences, rationale and citations. This projection runs no inference and creates
no human approval. Invalid identities, routing or evidence fail validation.
""",
    "labeling_failures": """
**How it works:** Failed outcomes are selected from the same current labeling-attempt
table that supplies successful annotations. Original commands, failure reasons and
evidence references are retained without manufacturing taxonomy or confidence values.
Inspect these rows to distinguish exhausted documentation grounding from invalid output.

**Input:** The current complete labeling-attempt table.
**Goal:** Make unsupported and failed labeling attempts visible for audit and targeted repair.
**Why:** A smaller review queue must not hide missing labels or unsupported confidence.
**Output:** `LabelingFailuresTable`; these rows are excluded from deduplication, review
admission and release. An empty failure table is valid. Malformed failure records fail
validation; capacity pauses remain execution state, not failed command judgments.
""",
    "merged_command_candidates": """
**How it works:** Current candidates from the two independent collection branches are
combined into one table. Candidate and source identifiers are prefixed with their branch,
and an origins map preserves the original identities, preventing collisions without
losing provenance. Branch-state bindings distinguish an unconfigured source from one
still waiting; shared processing requires every configured branch to be ready.

**Input:** Current raw command candidates from authorized collection branches.

**Goal:** Combine authorized Existing datasets and Documentation crawl records into one category table.

**Why:** Collection is independent, but filtering, semantic documentation and labeling
must run once on the combined candidate table before deduplication and release.

**Output:** `CommandCandidatesTable` with branch-qualified candidate and source IDs,
an origins map preserving original identities, branch metadata and current input bindings.
Unconfigured branches are distinct from configured branches with empty, waiting, failed or stale outputs.
Only current authorized inputs are accepted; historical or unauthorized outputs cannot substitute for them.
Only the merge may expose partial waiting inputs; shared processing and publication
require all configured branches to be current and complete.
""",
    "deduplicated_annotations": """
**How it works:** Commands containing only a restricted set of plain characters have
repeated spaces and tabs collapsed; commands with quoting or shell syntax are left
unchanged. A comparison key combines that command with its annotation, context and
documentation evidence. Only identical keys are collapsed, keeping the first row and
recording a duplicate-to-survivor link; similar commands or disagreeing labels remain.

**Input:** The unified merged labels with original evidence and category rules.

**Goal:** Remove only proven exact normalized command duplicates before uncertainty-based human review.

**Why:** One category release must avoid exact duplicates without erasing meaningful command or label differences.

**Output:** `ProposedAnnotationsTable` of retained records with linked duplicate-to-survivor provenance
and exclusion reasons. Normalization preserves shell semantics, quoted text, flags, operands,
substitutions and meaningful context/label disagreements. Ambiguous similarities remain for review.
Fuzzy matching, resource renaming, safe-pair construction and model-conditioned deduplication are excluded.
""",
    "category_eligible_annotations": """
**How it works:** The deduplicated labels are filtered by the configured category:
rejected labels, other-category assignments and low category confidence are excluded.
Target-category rows with medium or high category confidence are retained, along with
their other confidence values for the next review step. Excluded identities and reasons
are recorded separately, so an empty result is explainable rather than padded.

**Input:** Exact-deduplicated annotations and the configured category rules.

**Goal:** Keep relevant category labels with medium or high category confidence.

**Why:** Unrelated commands and low-confidence category assignments must not enter final review or release.

**Output:** `ProposedAnnotationsTable` retaining target-category, non-rejected rows with medium/high
category confidence. Excluded identities and reasons are preserved in `category_exclusions`
and the category-exclusions.json receipt; original rows remain in deduplicated_annotations.
An empty result is valid. Invalid input schemas fail; no inference or human decision runs here.
Other medium/low label confidences remain for human correction and review.
""",
    "dataset_release": """
**How it works:** Eligible labels are combined with the completed review snapshot:
low-uncertainty rows can enter automatically, while uncertain rows need real approval
and any validated human corrections. Human exclusions are applied before category-wide
caps, and authorization is checked again when the output is committed. The resulting
canonical table and export preserve which rows were actually human reviewed.

**Input:** Category-eligible annotations, category rules, completed human decisions and a version-bound authorization.

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
**How it works:** The completed review's authorization is checked against the current
prepared queue before saved decisions are turned into a typed table. The table preserves
reviewer identity, revisions and corrections separately from the model's original labels,
giving release an explicit decision input. An automatically completed empty queue produces
an empty decision table, not fabricated human approvals.

**Input:** The uncertain-row queue and its snapshot-bound completion authorization.

**Goal:** Materialize the exact completed batch as a typed decision table.

**Why:** Human decisions must be separate from machine labels and visibly upstream of publication.

**Output:** `HumanReviewDecisionsTable` with record identity, decision, notes, reviewer, revisions
and snapshot binding. Approved label corrections retain original model evidence and apply
validated human values at release. Its Parquet metadata also binds empty tables to the completed batch.
Open reviews live in the durable ledger and Review tab; this asset materializes only at publication.
Unchanged row evidence and policy preserve saved decisions across changed table snapshots,
with original reviewer and carry-forward audit provenance. Nonempty snapshots require explicit completion;
changed or retracted row decisions are never resurrected from old approval.
An empty queue is completed by the automatic policy, with zero human decisions.
Reviewer names are self-reported. Approving a record does not authorize executing its command.
""",
    "human_review_queue": """
**How it works:** Category-eligible labels are screened for machine eligibility and
uncertainty: any medium or low label confidence, or a supported needs_review label,
requires human attention. The resulting queue is prepared in the durable review store,
reusing saved decisions by the original values of the configured review columns.
Unresolved items leave a waiting batch instead of a sleeping worker; an empty queue
can complete automatically without human decisions.

**Input:** Category-eligible annotations and category rules.

**Goal:** Expose final rows with medium or higher uncertainty for human value corrections and inclusion decisions.

**Why:** Any medium/low classifier confidence requires human review; all four high
confidences permit automatic inclusion only when machine eligibility also passes.
A supported needs_review label always requires explicit human resolution.

**Output:** `ReviewQueueTable` with original candidate/label fields and `exclusion_reason`.
Machine-eligible records have an empty reason; caps are applied only after human exclusions.
This queue precedes human_review_decisions and release. Low-uncertainty rows remain visible in
category_eligible_annotations and need no human decisions. Source exclusions stay in source_catalog,
filtered candidates stay in filtered_commands, and inference failures stay in observations/receipts.
Machine-rejected or unsupported rows remain ineligible. No row here implies human sign-off.
After computing or loading the cached queue, this same asset prepares durable PostgreSQL review
items and reusable terminal decisions. The namespaced `codebench_review_preparation` attributes
bind batch_id and snapshot_id even when the queue is empty. Invalid inputs fail preparation;
unresolved or blocked items leave a durable waiting batch without occupying a worker.
Selected columns define reusable identity and editable values; duplicate occurrences retain provenance.
Review page reads never prepare snapshots. Cache hits still revalidate and prepare current authority.
""",
}


class DescribedAsset(Protocol):
    @property
    def key(self) -> AssetKey: ...

    def replace_attributes(self, *, description: str, group_name: str) -> AssetSpec: ...


def describe_stage(spec: DescribedAsset, *, declared: bool = False) -> AssetSpec:
    stage = spec.key.path[-1]
    if declared:
        qualified = "/".join(spec.key.path[1:])
        if qualified in DECLARED_COLLECTION_DESCRIPTIONS:
            group = BRANCH_BY_NAMESPACE[asset_branch(spec.key.path)].group
            return spec.replace_attributes(
                description=f"**How it works:** {DECLARED_COLLECTION_DESCRIPTIONS[qualified]}",
                group_name=group,
            )
        if stage in FINALIZATION_DESCRIPTIONS:
            return spec.replace_attributes(
                description=f"**How it works:** {FINALIZATION_DESCRIPTIONS[stage]}",
                group_name="dataset_finalization",
            )
    description = DESCRIPTIONS[stage].strip()
    group_name = "existing_datasets"
    if stage == "category_rules":
        group_name = "dataset_configuration"
    elif len(spec.key.path) == 2 and stage in FINALIZATION_STAGES:
        group_name = "dataset_finalization"
    if len(spec.key.path) == 3 and spec.key.path[1] == "documentation":
        group_name = "documentation_crawl"
        if stage == "source_catalog":
            description = """
**How it works:** The separately prepared documentation manifest becomes an inventory
of original documentation sources, retaining their declared formats, provenance and
reuse evidence. Source-kind checks reject dataset and agent-training corpora from this
branch. Its inventory is independent of the dataset catalog, so later acquisition uses
only the documentation sources actually declared here.

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
**How it works:** Each eligible entry in the documentation catalog is retrieved and
checked against its own pinned content and size limits. The complete documentation text
is saved with hashes and source attribution for extraction. Its input binding belongs
to this branch alone, so a change to the dataset inventory cannot authorize different
documentation or invalidate an otherwise unchanged documentation input.

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
                description + "\n\nDocumentation branch: uses only its own inspected-source binding "
                "and preparation. The dataset inventory is not an input."
            )
    return spec.replace_attributes(description=description, group_name=group_name)
