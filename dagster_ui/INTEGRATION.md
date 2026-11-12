# Native panel development

All source in this directory is a native Dagster asset-view extension, not an
independent application. `src/pages/` re-exports the original OSS pages.
`AssetsOverviewRoot.tsx` supplies the inner feature provider and preserves
inherited fields. Data and eligible Review tabs follow Overview immediately.
Native and custom links preserve the category partition.

Use `python -m codebench_pipeline.ui_build` for a packaged production build;
see `BUILD.md`. The following setup is for direct frontend development.

## Development build inputs

Provision these ignored links before typechecking or building:

- `.upstream`: the unchanged pinned Dagster source root.
- `node_modules`: `.upstream/js_modules/node_modules`.
- `public`: `.upstream/js_modules/app-oss/public`.

Use the pinned upstream installation for React, React Router, Next, TypeScript,
and Dagster packages. The root package declares their inspected versions as
peers; do not install another root dependency tree or a second React.

Bootstrap `tooling/` separately with Yarn 4.12.0. It declares the exact
`@yarnpkg/cli-dist` and Zod versions. Its node-modules linker is explicit.
Use the committed tooling lock with immutable installation.
TypeScript paths and webpack map Zod only to this isolated tree.
The test command uses Bun's existing runtime and node:test compatibility.

After the lead has provisioned the links and isolated dependencies, run from the
repository root:

```sh
cd dagster_ui
./node_modules/.bin/tsc --noEmit --project tsconfig.json
bun test ./tests --timeout 30000
cd ..
uv run python -m codebench_pipeline.ui_build --source /path/to/pinned/dagster
```

Do not invoke upstream `build.sh`. The project builder packages the inherited
Next export with the original CSP, vendor assets and licenses.
PostCSS resolves plugins relative to original app-oss.

## Source organization

- `models.ts`, `api.ts`: v1 response parsing, literal cell serialization,
  same-origin HTTP with a 30 second deadline and no retries.
- `ReviewStageContext.tsx`, `reviewStages.ts`: shared native stage registry, exact
  asset-key matching, scoped routes, URL selection and component identity.
- `useResource.ts`: abort-on-obsolescence reads, response-key isolation,
  immutable snapshot binding, explicit recovery and bound reload after writes.
- `Scope.tsx`, `useNarrowNavigation.ts`: category URL selection and native
  navigation context; manual reopening is respected within a narrow interval.
- `QueryControls.tsx`, `query.ts`, `RowsTable.tsx`: shared global search,
  AND filters, up to four ordered sorts and full server pagination.
- `DataView.tsx`, `Fields.tsx`: table/rules rendering and complete inert text
  inspection, including nested evidence strings with original whitespace.
- `ReviewView.tsx`, `DecisionInspector.tsx`, `decisions.ts`, `Evidence.tsx`:
  per-record decisions, snapshot-scoped drafts, revision conflict recovery,
  immutable evidence, explicit completion and reopening.
- `PublicationLinks.tsx`, `Status.tsx`, `useReviewer.ts`: successful publication
  references, state feedback and convenience-only reviewer persistence.
- `tests/`: query/URL/cell behavior, request parsing/cancellation/no-retry errors,
  revision-bound writes, normalized record identity, next-pending traversal,
  stage isolation, approval limits and nullable publication completion.

Drafts survive failed writes, page/filter changes and explicit same-batch
refresh. A changed persisted row revision requires an explicit rebase or
discard. A changed evidence snapshot never inherits a draft approval.
Drafts are intentionally in memory, not a second persisted decision store;
browser reload restores server decisions and the saved reviewer.

## Reusable review stages

The active dataset workflow registers one final `annotations` uncertainty review,
not a human source-admission stage. An LLM assesses each source dataset from actual
README/card and license bodies and origin evidence, retains receipts and holds
unresolved eligibility. Both collection branches automatically enumerate and load
complete eligible configured corpora, not bounded inspected-page snapshots.
Branch extraction outputs converge at `merged_command_candidates` before shared
filtering and semantic docs. Each eligible command then gets one isolated
labeling invocation in `proposed_annotations`, followed by conservative exact
deduplication before the human queue. `reviewed_annotations` is removed from the active graph; historical
artifacts remain readable. Don't manufacture `review_verdict` or `review_confidence`
for current rows.
`human_source_decisions` is removed from the active graph. Generic stage support
below remains reusable; historical source-selection examples aren't active gates.

The final queue contains rows with any `medium` or `low` among category, risk,
severity and class confidence. All four `high` means low
uncertainty and automatic admission only if machine eligible, without a decision.
Ineligible evidence remains inspectable and approval cannot admit it. A nonempty
queue blocks the single `dataset_release` until real terminal decisions and
explicit completion. An empty queue lets the build release automatically through
that same asset, with no human decisions, human reviewer or human completion.
The empty batch is completed under an explicit automatic-policy actor and retains
a valid snapshot-bound publication token.
Automatically admitted rows have `human_reviewed=false`; actually approved
uncertain rows have `human_reviewed=true`. Caps apply after eligibility and approval.

`GET /codebench/api/review-stages` returns:

```json
{
  "stages": [{
    "stage_id": "annotations",
    "title": "Dataset inclusion",
    "asset_keys": [["datasets", "deduplicated_annotations"], ["datasets", "human_review_queue"]],
    "completion_label": "Complete review and publish",
    "min_approvals": 0,
    "max_approvals": null
  }]
}
```

Asset eligibility comes exclusively from exact full `asset_keys` matches,
not an annotation, queue or release asset allowlist. The native Review tab
appears for every matching asset, including assets outside `datasets`.
The Data tab keeps its existing dataset behavior. The original feature
provider, native pages and no-fork production build seam remain intact.

The Review stage selector uses descriptor titles and writes `review_stage`
while preserving `partition` and other native query parameters. A direct
Review URL without a stage selects the first matching descriptor and replaces
the URL with that explicit identity. An unknown stage never silently opens
another stage: it requires selecting a matching descriptor. Registry failures
offer a retry in the Review view; they do not hide or replace Data.

All UI review traffic uses these paths under `/codebench/api`:

- `POST /reviews/{stage_id}` with the unchanged `DatasetQueryRequest`.
- `PUT /reviews/{stage_id}/decision` with the unchanged decision CAS payload.
- `POST /reviews/{stage_id}/complete` and `/reviews/{stage_id}/reopen` with
  the unchanged batch CAS payload.
- `GET /reviews/{stage_id}/{batch_id}/evidence/{candidate_id}?dataset_id=...`
  returning the same generic `fields` dictionary.

Every path segment and dataset query value is URL-encoded. The UI does not
call legacy `/review` routes; their compatibility remains server-side.
Review page rows require top-level `review_record_id: string` alongside
`row_index` and `values`. The backend normalizes the configured source ID
column into this field. Original values remain available for inspection;
they need not contain `candidate_id`. Row selection and inspection use
`review_record_id`. Snapshot `candidate_ids`, eligibility and decision
`candidate_id` identify those same stable reviewed records; next-pending
traverses the full snapshot rather than only the filtered page.

Snapshots and completion tokens contain `review_stage`. Omission defaults
to `annotations` only for historical data. Response parsers bind the
snapshot, completion and publication authorization to the requested stage.
Dataset plus stage keys remount the table on scope changes; draft prefixes
also include dataset, stage, batch and snapshot. Stage changes cannot
reuse a selected record, an approval draft or a snapshot binding from
another stage. Returning to a stage reloads its saved decisions, not its
discarded in-memory state.

Completion returns `{batch, counts, completion, publication, run_id}`.
`completion` is required; `publication` and `run_id` are nullable.
This describes an explicit human completion request, not the automatic empty-queue
build path, which doesn't require that request or a human completion token.
A completed generic review is saved and read-only with Reopen even without
a publication. Publication links and the publication-registered notice
appear only when an actual publication reference exists. The completion
button uses `completion_label`; it requires an open batch, a reviewer,
no pending decisions, no unsaved drafts, and an inclusive approval count
between `min_approvals` and `max_approvals` (null means no maximum).
The view shows the limits and current approval count.

### Decision retraction and pipeline continuation

The descriptor also accepts `can_continue` (default `false`) and
`continuation_label` (default `Continue pipeline`). A completed stage that
advertises this capability exposes that label as an explicit action:
`POST /reviews/{stage_id}/continue` with the same batch CAS body as completion.
The response is `{batch, counts, completion, publication, run_id}` with nullable
completion, publication and run ID. An unchanged continuation can return no
new run. `continuation_failed` is surfaced as a server error, without retries
or an automatic save, approval or completion.

Generic review stages need no asset-specific frontend configuration: eligibility,
title, completion label and continuation come from the discovered descriptor.
A generic stage can return a pipeline run without producing a publication;
automatic source collection doesn't require such a review. Run navigation renders
independently of publication/download controls. `batch.continuation_run_id`
(nullable, default `null`) restores that link on a fresh page load.

Saved approve/reject decisions expose `Retract decision` in the row inspector,
including completed batches. It posts to `/reviews/{stage_id}/retract`:

```json
{
  "dataset_id": "dataset-id",
  "snapshot_id": "64-character digest",
  "expected": {"batch_id": "batch-id", "generation": 1, "batch_revision": 2},
  "candidate_id": "record-id",
  "expected_row_revision": 3,
  "reviewer": "reviewer"
}
```

The existing decision response supplies the new batch, pending decision and
counts. No global Reopen is needed. Selection stays on the same record,
persisted state reloads, and publication references are cleared locally.
All drafts in the active scope must be saved/discarded before retraction;
missing reviewer, row revision mismatch, stale/conflicting reads and in-flight
reads/writes also disable it. The server owns atomicity, reopening generation,
preservation of other saved rows, and dependent-result invalidation.

`publication_state: "stale"` renders a warning on any table, including human
decision materializations. Rows remain inspectable as historical data, never
as current approvals. A human-authorized publication whose authorization isn't the
current completed batch is explicitly labeled historical, including its download.
Automatic empty-queue publication instead binds the current machine-eligible
snapshot and must not be mislabeled stale merely because it has no human completion.
The context module remains `ReviewStageContext.tsx`; the renderer map retains
its string-keyed lookup around the upstream closed tab union.

Lead execution and browser QA scenarios:

1. Confirm both branches collect eligible sources without a source Review gate.
   Inspect provenance/license/content exclusions. With all four label confidences high,
   verify an empty final queue automatically produces the single release with
   `human_reviewed=false` and no fabricated human decisions. With any one of the
   four medium/low, verify that row enters the shared queue and blocks release
   until real decisions and completion. Mixed releases keep low-uncertainty rows
   decision-free and mark only actually approved uncertain rows `human_reviewed=true`.
   Confirm approval can't admit machine-ineligible rows and caps apply afterward.
2. In an open batch, retract both an approved and a rejected row. Confirm each
   becomes pending, retains selection and shows refreshed server counts and
   revision. Other saved rows keep their decisions and notes.
3. In a completed batch, retract a row without global Reopen. Verify generation
   advances, old completion/publication is unauthorized, downstream outputs
   show stale warnings (including decision tables), and historical downloads
   are never presented as currently approved. Decide again and explicitly
   complete before confirming downstream materializations become current.
4. Leave an unsaved draft on a different row, then inspect a saved decision.
   Retraction must be disabled until all scoped drafts are saved/discarded.
   Repeat with blank reviewer, an in-flight request, a stale row revision and
   a competing browser write. A 409 must preserve selection/drafts and require
   explicit refresh/rebase rather than retrying the mutation.
5. Reload a completed stage with `can_continue` and use its configured button.
   Check the batch CAS payload, actual run link, unchanged/null run response,
   and `continuation_failed`. Retry only explicitly after inspecting saved
   state; no automatic review completion or approvals are permitted.
6. Exercise an unrelated discovered stage, multiple stages on one asset,
   unknown stage URLs and category changes. Preserve partition parameters
   and stage/dataset isolation. Verify no source/model/annotation allowlist is
   involved. Existing annotation publication and global Reopen still work.
7. At desktop and mobile widths in both themes, inspect the wrapping toolbar,
   inspector and warning states. Check keyboard access, focus rings, disabled
   controls, long cell values and encoded native links. WCAG contrast, focus,
   target size and reflow checks remain unverified until this browser pass.

This change reuses `DESIGN.md`'s Dagster controls, themed CSS tokens, wrapping
toolbar and list/detail layout without new CSS or a second design system.
The stage selector follows the existing Category selector pattern.

## Verification

The stage-agnostic increment passes 21 frontend tests and the production build's
strict TypeScript check. The default builder was exercised from a fresh pinned
Dagster checkout. Regression coverage and manual verification include:

- Annotations retain dataset inclusion, publication links and reopening.
- A generic model-selection descriptor opens on its configured asset,
  resolves original rows without a `candidate_id` column, and completes
  without a publication or Dagster run.
- Multiple descriptors on one asset retain partition while changing stage;
  pending selection, drafts and late responses never cross stage identity.
- Min/max approval boundaries, pending decisions and drafts disable
  completion with an explicit requirement.
- Desktop/mobile, both themes and keyboard operation retain native layout,
  loading/error/empty states and accessible selector labels.

The reusable service's model-choice, stage-isolation and no-publication
completion contracts are exercised through real SQLite and HTTP in
`tests/test_review_workflow.py`. Keep publication-cycle evidence and ledgers
local rather than including them in this source repository.
The configured TypeScript, Biome and YAML language servers were unavailable;
CLI typechecking, not LSP, established TypeScript correctness.

The server resolves the numeric equality ambiguity by comparing numeric cells
by value: `1`, `1.0`, and `1e0` agree. Strings retain exact equality, and other
non-numeric cells use compact, sorted JSON. This needs no transport change.
