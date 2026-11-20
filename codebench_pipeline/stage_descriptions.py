from typing import Protocol

from dagster import AssetKey, AssetSpec

from codebench_pipeline.bashguard_audit import AUDIT_STAGE
from codebench_pipeline.collection_descriptions import (
    DECLARED_COLLECTION_DESCRIPTIONS,
    FINALIZATION_DESCRIPTIONS,
)
from codebench_pipeline.dataset_branches import BRANCH_BY_NAMESPACE, FINALIZATION_GROUPS, asset_branch

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
category_eligible_annotations and need no human decisions. Source exclusions stay in collection evidence,
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


def describe_stage(spec: DescribedAsset) -> AssetSpec:
    stage = spec.key.path[-1]
    qualified = "/".join(spec.key.path[1:])
    if qualified in DECLARED_COLLECTION_DESCRIPTIONS:
        return spec.replace_attributes(
            description=f"**How it works:** {DECLARED_COLLECTION_DESCRIPTIONS[qualified]}",
            group_name=BRANCH_BY_NAMESPACE[asset_branch(spec.key.path)].group,
        )
    if stage in FINALIZATION_DESCRIPTIONS:
        return spec.replace_attributes(
            description=f"**How it works:** {FINALIZATION_DESCRIPTIONS[stage]}",
            group_name=FINALIZATION_GROUPS[stage],
        )
    return spec.replace_attributes(
        description=DESCRIPTIONS[stage].strip(),
        group_name="configuration" if stage == "category_rules" else FINALIZATION_GROUPS[stage],
    )
