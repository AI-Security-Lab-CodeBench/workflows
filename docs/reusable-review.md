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
Lookup is scoped to dataset home, dataset and review stage. Decisions survive
renamed candidate IDs, reordered inputs and disappearance followed by return.

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

The `human_review_snapshot` Dagster asset consumes the uncertain queue, eligible
annotations and rules. It captures immutable inputs and bulk-matches decision
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
   input revision, evidence and eligible IDs. The reader must not run inference.
3. Construct `TableReviewSource(home, stage, reader)` and
   `ReviewWorkflow(home, stage, source)`.
4. Call `workflow.prepare(dataset_id)` explicitly at the upstream materialization
   boundary, including after input changes.
5. Register `ReviewRegistration(stage, workflow)` with `make_dataset_api`.
6. After real completion, a consumer calls
   `workflow.approved_records(dataset_id, token)` for corrected approved rows.

Generic stages have no command-specific field requirement. For model selection,
`review_columns=("model_id", "score")` makes both values reviewable and editable.
Use an application validation contract where additional domain constraints apply.
The native inspector renders editors from the prepared columns and original types.

## Completion and publication

A reusable decision is not a reusable release authorization. Completion binds
the exact prepared membership, eligibility, validation contract, bounds, decision
revisions and generation. A new nonempty snapshot requires explicit completion
even when all decisions are reused. An empty uncertainty queue completes under
the automatic-policy actor with zero human decisions.

Writers serialize per dataset, with optimistic row and batch revision checks.
Read-only repeatable-read transactions keep page, counts and status consistent.
Registry and asset-reference writers use the same dataset authority lock.
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

The `/review` aliases retain annotation meaning. URLs and drafts remain scoped
by dataset and stage. Self-reported reviewer names are audit labels, not
authentication. Unsaved drafts stay in the browser; saved decisions live in
PostgreSQL.

Legacy SQLite ledgers require explicit verified import. Preserve the original
ledger and manifests; never use old completion tokens to authorize the new
column contract. Unverifiable or conflicting historical decisions remain
inspectable history instead of being guessed into reusable approvals.
