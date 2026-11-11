# Native Dagster HITL integration contract

Implementation contract for the native Data and Review tabs, their loopback API,
and the independent Dagster publication gate.

## Integration boundary

Dagster is pinned to 1.13.29, source commit
`7dca97fabebf771d4ac0515d674e50e2a742446f`.
The project owns a frontend build and an ASGI adapter, not modified Dagster source.
An exact build alias replaces `@shared/assets/AssetsOverviewRoot` with our wrapper.
The wrapper imports the original root through
`@dagster-io/ui-core/shared/assets/AssetsOverviewRoot` and supplies an inner
`AssetFeatureContext.Provider`.

This is a private build-time interface, not a supported runtime plugin. The
upstream URL decoder currently passes custom view strings to the feature
renderer. Upgrades require build, route, context-identity and browser checks.

Build and installation instructions are in `dagster_ui/BUILD.md`. URL `partition`
values are dataset IDs, not taxonomy category IDs. The category selector preserves
that binding across the native Data and Review tabs.

## Shared transport

All paths start with `/codebench/api`. JSON requests and responses use snake_case.
Requests reject unknown fields. Bodies are bounded; commands and evidence are
literal data, never query expressions, HTML or executable code.

State-changing requests require the configured same origin and an allowed Host.
There is no cross-origin API. The server binds to loopback by default.
Reviewer identity is self-reported, not authenticated identity or a role system.

Errors have the shape:

```json
{"error":{"code":"stale","message":"Decision row revision changed"}}
```

Codes are `invalid_request`, `forbidden`, `internal_error`, `not_found`, `not_materialized`, `not_ready`,
`conflict`, `stale`, `pending`, `ineligible`, `completed`, and
`publication_failed`, `continuation_failed`, and `selection`. Invalid requests return 422, missing resources return 404,
origin failures return 403, and review conflicts/staleness/incomplete batches
return 409. Responses must not expose exception stacks, credentials or arbitrary
server configuration.

## Dataset and table queries

`GET /datasets` returns:

```json
{"datasets":[{"dataset_id":"example","category_id":"CAT-3","category":"CAT-3 Example"}]}
```

`POST /data/{stage}` accepts:

```json
{
  "dataset_id":"example",
  "snapshot_id":null,
  "query":{"offset":0,"limit":25,"search":"","filters":[],"sort":[]}
}
```

Documentation tables use `POST /data/documentation/{stage}` and return a
qualified `stage` value such as `documentation/proposed_annotations`. Collection
branches retain separate current references; finalization and publication are shared.

The query contract is implemented in `codebench_pipeline/table_query.py`.
Limits are 1-200 rows, up to 20 AND-combined filters and four sort columns.
Filters are literal `contains`, `equals`, or `is_null`.
Equality is exact text for strings and serialized JSON for other non-numeric
cells; numeric cells compare by numeric value, so `1`, `1.0`, and `1e0` agree.
Contains and global search are case-insensitive. Sorts are stable, with original
row positions as tie order and nulls last. Unknown columns are errors even for
empty tables. No SQL, regex or expression execution is exposed.

A table response is:

```json
{
  "kind":"table",
  "dataset_id":"example",
  "stage":"reviewed_annotations",
  "snapshot_id":"64-lowercase-hex-digest",
  "page":{
    "columns":["candidate_id","command"],
    "total_rows":1,
    "matched_rows":1,
    "offset":0,
    "limit":25,
    "rows":[{"row_index":0,"values":{"candidate_id":"record-1","command":"inert text"}}]
  }
}
```

`row_index` is the zero-based original position, not the filtered page position.
Values are complete JSON values; server pagination never truncates cells.
The first request selects the current immutable snapshot. Subsequent requests
send its digest; a changed current snapshot returns 409 rather than mixing pages.
`category_rules` instead returns `kind: "rules"` and `rules` containing the typed
taxonomy JSON, alongside the same dataset/stage/snapshot fields.
Table responses also expose `publication_state`. Stale and legacy-unreviewed tables
remain readable for inspection but are rejected as downstream materialization inputs.

## Review view

Review is now stage-scoped and reusable; see `reusable-review.md`.
`GET /review-stages` returns descriptors with stage ID, title, asset keys,
completion label, minimum/maximum approvals, `can_continue`, `continuation_label`
and `additional_publication_stages`.
`/reviews/{stage_id}` and its decision/complete/reopen/retract/continue/evidence
subroutes use the same contracts below. There is no active `sources` ingestion
gate or `human_source_decisions` asset. Both branches automatically acquire
eligible sources under provenance, license and content checks. The `annotations`
stage gates publication only when its uncertainty queue is nonempty.

The shared `annotations` stage reviews uncertain deduplicated records from both
collection branches. All five `high` confidences means low uncertainty; any
`medium` or `low` among category, risk, severity, class and independent reviewer
confidence requires human review. There is no active `documentation_annotations`
or separate automatic release asset.
Generic clients still reject undeclared cross-stage responses.
Legacy `/review` paths select the `annotations` stage.

`POST /review` accepts the same dataset/snapshot/query body, without a stage path.
It returns:

- `batch`: `ReviewBatch` from `codebench_pipeline/human_review.py`.
- `counts`: `ReviewCounts`, with `pending`, `approve`, and `reject`.
- `decisions`: the batch's `ReviewDecision` records.
- `page`: the table page over reviewed annotations joined with display-only
  `human_decision`, `human_reviewer`, and `human_notes` columns.
- `publication`: the current authorized `PublicationReference`, or null.

The view must not mutate original annotation columns or persisted source tables.
Every review row includes a top-level `review_record_id`; another stage may use
an original ID column such as `model_id`, without a `candidate_id` table column.
Snapshots and completion tokens carry `review_stage`; snapshots also bind
`min_approvals` and `max_approvals`.
The annotation batch snapshot includes only low/medium-confidence candidate IDs
and their machine-eligible subset, using all five confidence dimensions.
Historical source batches remain evidence, not active ingestion authorization.
All queued candidates require an explicit terminal
decision, including ineligible rows.
Human approval cannot override existing eligibility checks.

`GET /review/{batch_id}/evidence/{candidate_id}?dataset_id=...` returns
`{"fields":{...}}`: full candidate/label/review evidence, source context and
documentation resolved from that batch's immutable manifest. The UI renders
these values as text. Source links permit only HTTP(S).

## Decision and completion writes

`PUT /review/decision` accepts:

```json
{
  "dataset_id":"example",
  "snapshot_id":"64-lowercase-hex-digest",
  "change":{
    "batch_id":"server-issued-id",
    "generation":1,
    "candidate_id":"record-1",
    "expected_row_revision":0,
    "decision":"approve",
    "notes":"",
    "reviewer":"QA reviewer"
  }
}
```

It returns `batch`, `decision`, and `counts`. Decisions are `pending`, `approve`,
or `reject`. A stale row revision fails; it never overwrites another decision.
The UI keeps unsaved input on failure and offers explicit refresh/recovery.

`POST /review/complete` and `POST /review/reopen` accept:

```json
{
  "dataset_id":"example",
  "snapshot_id":"64-lowercase-hex-digest",
  "expected":{"batch_id":"server-issued-id","generation":1,"batch_revision":2},
  "reviewer":"QA reviewer"
}
```

Completion freezes the current fully decided batch and launches the actual
Dagster release job. Success returns `batch`, `counts`, `publication`, and
`run_id`. A failed publication is not reported as success.
The button is explicit: **Complete review and publish**.

That publication behavior belongs to the annotation adapter. Generic stages
return `completion` with a durable token and nullable `publication`/`run_id`.
Their button label and selection bounds come from the stage descriptor.
Out-of-bounds selection returns HTTP 409 with code `selection`.

Reopen returns `batch` and `counts`. It retains same-snapshot decisions, advances
the generation and invalidates prior completion tokens. A changed evidence
snapshot creates an open batch. Terminal decisions carry only for matching
record fingerprints, eligibility and bounds; changed/new records start pending.
The new batch has no inherited completion authorization.

`POST /reviews/{stage_id}/retract` accepts the completion payload plus
`candidate_id` and `expected_row_revision`. It atomically reopens a completed
batch if needed and returns only the selected saved row to pending. Other
decisions and the audit history are retained. Prior completion authorization
and dependent publications immediately become stale.

`POST /reviews/{stage_id}/continue` accepts the completion payload for a completed
stage whose descriptor permits continuation. Generic registered stages can use
this endpoint to resume a failed run or process subsequent changes. Historical
source-completion launches aren't part of automatic collection. The batch retains
`continuation_run_id`.
An unchanged plan creates no new run and preserves the existing run link.
Launch success is not build success: follow the native run for its final status.

`GET /publications/{publication_id}/commands.csv?dataset_id=...` downloads the
registered immutable CSV. New publications have the nine command columns followed
by `human_reviewed` (`true` or `false`); historical nine-column CSVs remain unchanged.
The native run link is `/runs/{run_id}`.

For a nonempty queue, the annotation Review view exposes a current publication
only after completion of its exact unified snapshot. If the queue is empty, the
build releases automatically through the same `dataset_release`, without a human
decision or human reviewer. The empty ledger batch receives a completion token
under the explicit `automatic low-uncertainty policy` actor; it contains no row
decisions. Automatically admitted machine-eligible
all-high rows have `human_reviewed=false`; actually approved uncertain rows have
`human_reviewed=true`. Historical publications remain history, not authorization.

## Mandatory publication boundary

Resolving the current snapshot, changing decisions and registering publication
use the same SQLite transaction protocol. Upstream current-reference/config
writers must participate so review authorization cannot race a snapshot change.
The publisher checks snapshot binding again when committing output. A nonempty
queue requires current completion authorization; an empty queue requires
revalidated emptiness and machine eligibility, not a fabricated human token.
A manual release-only materialization cannot bypass this boundary.

The service preserves immutable evidence manifests and prior decisions.
Publication includes machine-eligible low-uncertainty rows without decisions and
machine-eligible uncertain rows with actual human approval from the deduplicated
union. Approval cannot override machine ineligibility. Category-wide caps apply
after eligibility and required approval. The release
remains a Pandera-validated DataFrame persisted as Parquet, plus the ten-column
CSV; detailed audit evidence stays in sidecars. Retraction invalidates current
publication and never makes an older automatic publication silently current.
