# Reusable human review

Human review accepts immutable rows and one configured list of `review_columns`.
Those columns are both the deduplication identity and the only values a human
may edit. PostgreSQL stores review inputs, reusable decisions, revisions and
publication authorization. Set `CODEBENCH_REVIEW_DATABASE_URL` for the build,
webserver, daemon and tests; there is no SQLite runtime fallback. Dataset homes
have separate database namespaces, including temporary test homes.

## Identity and edits

The key is SHA-256 of canonical JSON containing the selected column names and
their original typed values. Column order does not affect it. String whitespace,
quoted command text, JSON types and nulls remain meaningful. Missing columns,
duplicate column names and nonfinite numbers fail preparation.

Candidate IDs, neighboring rows, provenance, model identity, documentation and
implementation versions are not part of this key unless explicitly selected.
Lookup is scoped to dataset home, opaque partition (`dataset_id`), review stage,
`artifact_id` and `scope_id`. Empty artifact/scope strings preserve legacy
annotation callers. Generic sources and workflows never resolve a DatasetRegistry
or category configuration. Decisions survive
renamed candidate IDs, reordered inputs and disappearance followed by return.

For generic artifacts, use `review_artifact_id(tuple(asset_key))` to encode the
full path and `review_partition_id(partition_key)` to encode a nullable native
partition as an opaque transport key. Null and every literal string have distinct
keys. Set `ReviewTable.partition_key` to the native value. Legacy annotation
partitions keep their existing `dataset_id` with empty artifact/scope defaults.
`workflow.partitions(artifact_id=..., scope_id=...)` reads current prepared
descriptors directly from durable state, returning `dataset_id`, `artifact_id`,
`scope_id`, nullable `partition_key` and `invalidated`. Legacy empty-artifact
descriptors expose `partition_key = dataset_id`. The API can return these as
`{"partitions": [...]}`; the UI should reuse the returned `dataset_id` rather
than sending a nullable partition in that string field.

An approved `edits` object overlays a copy of each matching original row.
It does not change the lookup key: if original A is corrected to B, A returning
reuses that correction, but a new original B does not inherit A's approval.
Rejections are also reusable. Retraction persists a pending revision so an older
approval cannot return from history.

Duplicate occurrences share one review item and decision while retaining all
original occurrences and provenance. Selection bounds count occurrences. A stage
that needs to distinguish otherwise identical rows must include the distinguishing
column in `review_columns`.

The annotation default is `command`, `category_id`, `risk_id`, `severity`, `class`,
configurable under `publication.review_columns`. Confidence determines which
rows enter human review, not which selected fields can be edited. Approvals
validate the corrected command and labels statically against captured rules.
Commands are never executed. Human approval cannot override machine eligibility.
A cached approval that cannot be applied to current occurrences remains blocked
and inspectable rather than silently becoming a new review prompt.

## Prepare once, read persisted state

The `human_review_queue` Dagster asset consumes uncertain eligible annotations
and captured rules. It captures immutable inputs and bulk-matches decision
hashes in PostgreSQL. Its `ReviewSnapshotTable` output identifies the prepared
snapshot and batch. Unresolved decisions persist as waiting state; no worker
sleeps while humans review.

Review page, item and next-pending requests query prepared database state. They
do not invoke the planner, load live business configuration, decode upstream
Parquet tables or create snapshots. Pagination, filtering and sorting happen
in PostgreSQL. Responses contain page-local decisions and compact batch metadata,
not every candidate ID. Full provenance is available separately from immutable
evidence artifacts.

Missing preparation requires running the preparation stage; opening the UI
does not implicitly materialize it. Updating registered configuration or upstream
references invalidates prepared authority. A subsequent build prepares current
membership while reusing unchanged selected-column decisions.

## Integrate another stage

1. Define `ReviewStage` with `stage_id`, `title`, native `asset_keys`,
   `review_columns`, completion label and optional approval bounds.
2. Supply a reader returning `ReviewTable`: actual rows, string ID column,
   input revision, evidence and eligible IDs. Optional `artifact_id` and `scope_id`
   identify independent review surfaces within the same stage and partition.
   The reader must not run inference.
3. Construct `TableReviewSource(home, stage, reader)` and
   `ReviewWorkflow(home, stage, source)`.
4. Call `workflow.prepare(dataset_id)` explicitly at the upstream materialization
   boundary, including after input changes.
5. Register `ReviewRegistration(stage, workflow)` with `make_dataset_api`.
6. After real completion, a consumer calls
   `workflow.approved_records(dataset_id, token)` for corrected approved rows.

Generic stages have no command-specific field requirement. For model selection,
`review_columns=("model_id", "score")` makes both values reviewable and editable.
Supply `ReviewTable.column_schema` where additional field constraints apply.
Each entry is a standard JSON Schema field schema: for example,
`{"score": {"type": "number", "minimum": 0, "maximum": 1}}`.
Preparation captures a schema for every original column, inferring missing types
from the complete selected occurrence set. It marks exactly `review_columns`
with `editable: true` and `readOnly: false`; all other columns are readonly.
Contradictory editable declarations and unknown schema columns are rejected.
Declared enums, numeric ranges, patterns, nested shapes and nullability are
validated using JSON Schema Draft 2020-12. An approval validates all selected
values, not just changed fields.

`ReviewTable.validation["row_schema"]` optionally declares constraints spanning
the corrected row. `original_row_schema` checks immutable original values, so an
edit cannot turn an originally ineligible record into an eligible one.
Annotation-specific code compiles captured taxonomy, command
routing, documentation and candidate/label shape into this schema using
`annotation_review_validation(rules)`. Direct annotation preparation must use
that helper too; a `rules` evidence object alone is not executable validation.
Generic transactions do not import or interpret CategoryRules. Original machine
eligibility is still enforced independently from editable values.

`ReviewTable.membership` is a preparation-time `TableQuery`: search and filters
select original occurrences before grouping by selected-column hash. Its limit
does not truncate membership; offset and sort are rejected. Duplicate occurrences
that do not match are excluded even if a matching occurrence shares their hash.
The immutable manifest captures this query, selected rows and eligibility.
UI page filters are separate and never change prepared membership.

Carried approvals keep their original hash and edits but are revalidated against
every current occurrence's captured field and row schema. Invalid approvals stay
visible and blocked; they cannot complete or authorize downstream consumption.

## Completion and publication

A reusable decision is not a reusable release authorization. Completion binds
the exact prepared membership, eligibility, validation contract, bounds, decision
revisions and generation. A new nonempty snapshot requires explicit completion
even when all decisions are reused. An empty uncertainty queue completes under
the automatic-policy actor with zero human decisions.

Writers serialize per dataset, with optimistic row and batch revision checks.
Read-only repeatable-read transactions keep page, counts and status consistent.
Registry and asset-reference writers use the same dataset authority lock.
Current batches, decision heads and completion tokens bind artifact and named
scope as well as partition and stage. `invalidate(..., artifact_id=..., scope_id=...,
review_stage=...)` invalidates only that surface. Omitting `review_stage` retains
the legacy all-stages behavior within the specified artifact/scope, not other
artifacts/scopes. Changing membership in one named scope cannot supersede another.
The scope migration invalidates existing prepared authority, preserving historical
batches and reusable decisions until explicit preparation captures the new schema.
Read-only requests against an older storage schema report `stale`; preparation's
writer transaction applies the migration before capturing current inputs.
Release validates current inputs and authorization again at commit; it applies
approved edits to copies, then applies eligibility and category-wide caps.
Original annotations, decisions and historical downloads remain immutable.

The native completion action launches `release_dataset` asynchronously and
returns a run link. **Continue publication** reuses an active run or current
publication, or retries a failed continuation without repeating human review.
Historical source selection is not an acquisition prerequisite.

## HTTP

- `GET /codebench/api/review-stages` discovers stage descriptors.
- `POST /codebench/api/reviews/{stage_id}` queries a prepared page.
- `POST .../next` finds the next pending item under the current filter.
- `GET .../{batch_id}/item/{candidate_id}?dataset_id=...` loads one item.
- `PUT .../decision` saves `change.edits` and the revision-bound decision.
- `POST .../complete`, `.../reopen`, `.../retract`, `.../continue` manage lifecycle.
- `GET .../{batch_id}/evidence/{candidate_id}?dataset_id=...` reads original evidence.

Review query, decision, lifecycle and item/evidence requests carry `artifact_id`
and `scope_id` alongside `dataset_id`; both default to empty strings. These fields
must match the prepared snapshot and completion token. View/item responses expose
`column_schema` in addition to the existing `review_columns`, rows and decision
fields. The UI must use captured schema rather than live category configuration.
Only `change.edits` changes row values; readonly columns cannot be supplied there.

The `/review` aliases retain annotation meaning. URLs and drafts remain scoped
by partition, stage, artifact and named scope. Self-reported reviewer names are audit labels, not
authentication. Unsaved drafts stay in the browser; saved decisions live in
PostgreSQL.

Legacy SQLite ledgers require explicit verified import. Preserve the original
ledger and manifests; never use old completion tokens to authorize the new
column contract. Unverifiable or conflicting historical decisions remain
inspectable history instead of being guessed into reusable approvals.
