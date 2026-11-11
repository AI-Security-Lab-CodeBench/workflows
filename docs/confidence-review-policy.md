# Confidence information and mandatory human gates

The November 11, 2026 user instruction changes the current dataset pipeline's
confidence and release contracts. Historical CSVs and evidence remain unchanged.

## Dataset/source selection

Every candidate source has an agent `analysis`, `rationale` and `confidence`.
Confidence is required and is exactly `low`, `medium` or `high`; numbers and
missing values are rejected. Supplied-source and live-discovery agents both
produce these fields. They are visible as full, selectable, scrollable table cells.

All dataset/corpus source candidates pass through the independent `sources` human-review stage.
High confidence never authorizes source use. Every source decision must be
terminal before completion; only human-approved machine-eligible sources flow
to acquisition. The source token is checked at computation and output commit.

Original documentation has a separate inventory and full processing branch,
as clarified by the user later on November 11. It does not require or receive
dataset source approval. Its own provenance/reuse/content binding is checked
at computation and commit. See [independent branches](independent-branches.md).

## Command-row review

The classifier supplies four enum confidences: category, risk, severity and class.
The independent reviewer supplies its own enum confidence in its judgment.
These are qualitative judgments, not calibrated probabilities or numeric thresholds.

- Every retained row enters the unified `human_review_queue` after exact
  normalized deduplication, including rows with all five confidences `high`.
- Confidence helps the reviewer inspect uncertainty; it never authorizes release.
- Machine-rejected or unsupported rows are not admitted. A rejected
  row remains inspectable but ineligible; inclusion approval cannot manufacture
  supporting evidence. Resolving such a judgment requires a new supported
  label/review pass.

Each branch's `reviewed_annotations` retains its classified rows. The shared
merged and deduplicated tables feed one `annotations` review stage. Dataset
source review remains a separate `sources` stage with its own decisions and
snapshot. There is no branch-specific final command review or automatic stage.

## Publication and retraction

The only terminal asset is `dataset_release`. Preparation stops at the unified
review queue. Explicit human completion signals downstream materialization after
every retained row has a terminal decision. No worker waits while a human reviews.
Per-risk caps apply to eligible approved records across both collection branches
in input order, so changed decisions can change which rows fit under a cap.

New releases have these ten columns:

```text
command,description,util,severity,category,risk,source,seed,class,human_reviewed
```

`human_reviewed` is the literal `true` for included rows approved in the human
command-review stage. There are no automatically admitted rows. Mandatory
human source selection alone does not make a command row human-reviewed.

The newest ledger publication is authoritative. Retraction or changed evidence
makes it stale; readers never silently fall back to an older automatic release.
Historical CSV downloads remain immutable. Terminal publication can be retried
without repeating inference.

## Source rows and command rows

A source row is not a command row. A configuration with one supplied document
initially has one source-selection record; extraction may produce many commands
from that document. This code-only repository does not import a historical
collection. Local historical acceptance is not current human source or command
approval.

## Documentation references

Semantic documentation selection is separate from corpus licensing admission.
Grounded references may support labeling with unresolved licensing, recorded
as `use_scope=semantic_reference_only` and a separate `reuse_review` decision.
This does not grant permission to redistribute the evidence bundle or ingest
its text as corpus examples. Explicit prohibitions and ungrounded excerpts
remain excluded.
