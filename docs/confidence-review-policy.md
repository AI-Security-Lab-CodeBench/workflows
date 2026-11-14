# Automatic collection and uncertainty-only human review

The current contract places source discovery and assessment in the local agent,
not Dagster. Each eligible command receives one isolated labeling invocation.
Historical CSVs, decisions and evidence remain unchanged.

## Manifest-driven source collection

The local agent follows `skills/codebench-source-preparation/SKILL.md`, reads
actual README/license/origin evidence and enumerates complete declared scopes.
It writes category-bound source manifests, semantic references, exclusions and
preparation evidence. Unresolved eligibility is withheld, never inferred.
Dagster retrieves every admitted file, verifies byte identity and validates
provenance/content before extraction. A failed file fails acquisition rather
than producing a silently partial successful table.

No source search, source-selector model or adapter model runs inside Dagster.
Manifest-backed catalog confidence is `not_applicable`, not a fabricated model
judgment. Optional reviewed local Python transformations require explicit trust
and pinned module hashes. Collected content remains inert.
`human_source_decisions` is historical, not an active source gate.

The branches remain independently executable through extraction. They converge
at `merged_command_candidates` before shared filtering, semantic documentation
and `proposed_annotations`, with one isolated labeling invocation
per eligible command, followed
by conservative exact deduplication, category eligibility filtering, the uncertain human queue and one
`dataset_release`. `reviewed_annotations` is removed from the active graph;
historical artifacts remain readable. Current rows must not receive manufactured
`review_verdict` or `review_confidence` values.
A configured blocked branch cannot be silently
omitted from the final release. See [independent branches](independent-branches.md).

## Command-row uncertainty

The classifier supplies four enum confidences: category, risk, severity and class.
These are qualitative judgments, not calibrated probabilities.

- The post-deduplication `category_eligible_annotations` stage excludes rejected
  labels, non-target categories and `low` category confidence. Excluded records
  remain inspectable in the original table with linked exclusion reasons.
- Among retained rows, any `medium` or `low` among these four confidences means medium-or-higher
  uncertainty and requires review in the shared `human_review_queue`.
- All four `high` means low uncertainty. Such a row needs no human decision and
  is automatically admitted only if machine eligible.
- Missing or invalid confidence is a validation failure, never an implicit `high`.
- A supported `needs_review` label always requires review, even if all four
  confidence fields say `high`. A human may confirm or correct it before approval.
  Missing supporting documentation, rejected labels and invalid taxonomy cannot
  be repaired by inclusion approval.

The Review inspector allows changes only to configured `publication.review_columns`,
which also define review deduplication identity. The default includes command,
category, risk, severity and class; selected high-confidence fields remain editable.
Corrections are recorded separately from immutable model labels
with the reviewer, notes, row revision and decision history. Only approved
corrections reach release. Corrected values must satisfy the configured taxonomy
and corrected command routing; a correction cannot move a command into another
dataset category or replace its source evidence. Original confidence values
remain evidence, not rewritten model judgments.

The queue follows category eligibility filtering after conservative exact normalized
deduplication of the unified labeled dataset. Full evidence, exclusions, branch/source identities and
duplicate-to-survivor mappings remain inspectable outside the queue. There is
one final `annotations` review stage, not branch-specific final reviews.

## Single release and human markers

`dataset_release` is the only terminal dataset asset. When the uncertainty queue
is empty, the build releases eligible rows automatically through this same asset,
without human decisions or a human reviewer. The ledger completes the empty
snapshot under the explicit `automatic low-uncertainty policy` actor and issues
the same snapshot-bound authorization used at publication. No row approval is created.
If uncertain rows exist, the single release waits for their real terminal
decisions and explicit completion of the current snapshot. Low-uncertainty rows
need no decision even in that mixed release. Waiting doesn't occupy a worker.

Admission combines machine-eligible all-high rows and machine-eligible uncertain
rows actually approved by a human. Rejected rows are excluded. Category-wide
per-risk caps apply after eligibility and required approval across both branches,
in input order. Caps don't reduce the uncertainty queue or bypass decisions.

New releases have these ten columns:

```text
command,description,util,severity,category,risk,source,seed,class,human_reviewed
```

`human_reviewed` is the literal `false` for automatically admitted low-uncertainty
rows and `true` for included uncertain rows with actual human approval, including
valid per-record carried decisions. Local source preparation, labeling and historical source
selection don't make a command human-reviewed.

## Snapshot validity and reuse

Both automatic and reviewed admission bind the exact current input, policy,
configuration and implementation snapshot. Recheck eligibility and queue emptiness
at commit for automatic release; recheck decisions and completion authorization
for a nonempty queue. Changed evidence, policy or retraction makes affected
publication stale. An empty queue doesn't authorize reuse of stale inputs.

Label reuse still binds relevant evidence, labels, semantic documentation and policy.
Human decisions instead reuse the hash of original selected review-column values.
Unselected evidence and policy changes do not erase decisions; current eligibility
and validation are checked separately, and unusable cached approvals are blocked.
Candidate renaming, packet order and neighboring changes do not invalidate identity.
Completion authorization doesn't carry to a new snapshot: a nonempty queue needs
explicit completion again; an empty queue is revalidated automatically.
Historical evidence and CSVs stay immutable, and no older publication silently
becomes current. Publication retries don't repeat valid inference.

## Source rows and semantic references

A source row isn't a command row. One supplied document can yield many commands;
source confidence doesn't determine command uncertainty. This code-only repository
doesn't import a historical collection or grant its rows current approval.

Semantic documentation selection remains separate from corpus licensing
eligibility. Grounded references may support labeling with unresolved licensing,
recorded as `use_scope=semantic_reference_only` and a separate `reuse_review`
decision. That doesn't permit redistribution of the evidence bundle or ingestion
as corpus examples. Explicit prohibitions and ungrounded excerpts remain excluded.
