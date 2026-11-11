# Automatic collection and uncertainty-only human review

The current November 11, 2026 user instruction supersedes mandatory corpus
admission and all-row final review. Historical CSVs, decisions and evidence remain
unchanged. This is the operational contract for the requested pipeline.

## Automatic source collection

Both Existing datasets and Documentation crawl automatically crawl or load every
eligible configured source. Provenance, license/reuse terms, content eligibility
and captured-byte integrity remain mandatory. Confidence isn't a substitute for
these checks. Machine-ineligible sources remain inspectable with exclusion
reasons, but human approval cannot make them eligible.

Source candidates retain agent `analysis`, `rationale` and `confidence`. Source
confidence is exactly `low`, `medium` or `high`, not a number or a missing value.
It is evidence, not a human-admission trigger. `human_source_decisions` and the
`sources` admission stage are removed from the active collection graph. Historical
source decisions remain evidence, not current authorization.

The branches remain independently executable through labeling and converge before
shared independent agentic review. A configured blocked branch cannot be silently
omitted from the final release. See [independent branches](independent-branches.md).

## Command-row uncertainty

The classifier supplies four enum confidences: category, risk, severity and class.
The independent reviewer supplies its own enum confidence in its judgment.
These are qualitative judgments, not calibrated probabilities.

- Any `medium` or `low` among these five confidences means medium-or-higher
  uncertainty and requires review in the shared `human_review_queue`.
- All five `high` means low uncertainty. Such a row needs no human decision and
  is automatically admitted only if machine eligible.
- Missing or invalid confidence is a validation failure, never an implicit `high`.
- Machine-ineligible rows are never admitted by inclusion approval. Uncertain
  ineligible rows remain in the queue but cannot be approved for release;
  all-high ineligible rows remain inspectable in the full deduplicated table.
  Supporting evidence must be resolved through a new valid label/review pass.

The queue follows conservative exact normalized deduplication of the unified
agent-reviewed dataset. Full evidence, exclusions, branch/source identities and
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
valid per-record carried decisions. Agentic approval and historical source
selection don't make a command human-reviewed.

## Snapshot validity and reuse

Both automatic and reviewed admission bind the exact current input, policy,
configuration and implementation snapshot. Recheck eligibility and queue emptiness
at commit for automatic release; recheck decisions and completion authorization
for a nonempty queue. Changed evidence, policy or retraction makes affected
publication stale. An empty queue doesn't authorize reuse of stale inputs.

Reuse model reviews and real human decisions per record only when all relevant
evidence, labels, semantic documentation, eligibility and policy still match.
Packet order and neighboring changes don't invalidate an unchanged record.
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
