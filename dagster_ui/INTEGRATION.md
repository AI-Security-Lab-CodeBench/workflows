# Native panel development

All source in this directory is a native Dagster asset-view extension, not an
independent application. `src/pages/` re-exports the original OSS pages.
`AssetsOverviewRoot.tsx` supplies the inner feature provider and preserves
inherited fields. Data and eligible Review tabs follow Overview immediately.
Native and custom links preserve the artifact partition.

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
- `Scope.tsx`, `ReviewScope.tsx`: persisted artifact and prepared-review partition
  selection, without category discovery. Review targets retain their returned
  opaque storage ID, artifact identity and named scope.
- `QueryControls.tsx`, `query.ts`, `RowsTable.tsx`: shared global search,
  AND filters, up to four ordered sorts and full server pagination.
- `DataView.tsx`, `Fields.tsx`: table/JSON rendering and complete inert text
  inspection, including nested evidence strings with original whitespace.
- `ReviewView.tsx`, `DecisionInspector.tsx`, `decisions.ts`, `Evidence.tsx`:
  per-record decisions, snapshot-scoped drafts, revision conflict recovery,
  immutable evidence, explicit completion and reopening.
- `FullscreenInspector.tsx`: shared viewport-sized inspector, visible Close,
  topmost-dialog Escape handling and focus restoration. Closing retains drafts;
  a late Save-and-next response cannot reopen a dismissed inspector.
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

## Data artifact Inputs / Output

The native Data tab defaults to **Output**. The **Artifact** selector switches
between **Output** and **Inputs**. Inputs exposes an **Input asset** selector
with full direct upstream asset paths and an independent **Input partition**
selector. Partitioned inputs require an explicit choice; unpartitioned inputs
use `null`. The owner's `partition` and unrelated native query parameters are
preserved. No owner-to-input partition mapping is inferred.

These are **current upstream persisted artifacts**, not exact historical
versions consumed by the output, including when the native URL has `asOf`.
The view labels that distinction. Both modes reuse the same schema-backed artifact
viewer and **Row 1** / **Complete row values** inspector.
Changing mode, owner partition, input asset, or input partition remounts the
table, clearing its query, snapshot binding, and inspector. Aborted obsolete
reads and response-key isolation prevent late responses from replacing it.
Review is unchanged.

URL selectors are `data_view=output|inputs`, `data_input=<JSON asset path array>`,
and `input_partition=<partition key>`. Missing `data_view` means Output.
Changing mode or input clears the old input selectors. Unknown modes or input
identities show a recoverable selection state, never a fallback artifact.
For example, `/assets/qa/combined?view=data&partition=alpha&data_view=inputs&data_input=%5B%22qa%22%2C%22source_rows%22%5D&input_partition=beta`
reads the source's beta artifact while preserving owner alpha.

Dependency discovery uses the existing native Apollo provider and `gql` /
`useQuery` exports from `@dagster-io/ui-core/apollo-client`, with a network-only
`CodeBenchArtifactInputs` query:
`assetNodeOrError(assetKey: $assetKey) { ... on AssetNode { id dependencyKeys { path } } }`.
This is the definition's current direct dependency list, not transitive lineage
or materialization-used-data. The inspected pin is
`7dca97fabebf771d4ac0515d674e50e2a742446f`. Primary sources:

- https://github.com/dagster-io/dagster/blob/7dca97fabebf771d4ac0515d674e50e2a742446f/js_modules/ui-core/src/graphql/schema.graphql
- https://github.com/dagster-io/dagster/blob/7dca97fabebf771d4ac0515d674e50e2a742446f/js_modules/ui-core/src/apollo-client/index.tsx
- https://github.com/dagster-io/dagster/blob/7dca97fabebf771d4ac0515d674e50e2a742446f/js_modules/ui-core/src/assets/AssetMaterializationUpstreamData.tsx

`AssetViewFeatureInput.definition` is not assumed to contain dependency keys.
Discovery errors and non-AssetNode results offer **Retry input discovery**;
empty dependencies state that the asset has no direct upstream inputs.
Missing persisted input partitions offer **Refresh partitions**, and data-read
errors retain **Refresh snapshot**. Output remains selectable in every state,
even if the owner's output has never been persisted.

The existing `/artifacts/partitions` and `/artifacts/data` readers receive the
selected full asset key and its own partition. Their response schemas enforce
that binding.

### Schema-backed artifact viewing

Both artifact variants require `data_schema: {source: "declared" | "storage",
schema: <JSON Schema object>}`. JSON schemas describe the artifact root; table
schemas describe one row, not the page envelope. The boundary retains arbitrary
schema keywords, property order and nested definitions. The source is displayed
as supplied, without claiming that the UI validated persisted data.

The platform discovers declared schemas from the existing Dagster output
annotations at service initialization, including `Output[DataFrame[Model]]`
and Pydantic configuration models. Pipeline authors do not enable a viewer,
register schemas, or maintain an asset-name map. Browsing reads persisted
values; it does not invoke asset functions or reload business configuration.

Data is the default view. Its outline and detail pane show immediate properties,
with breadcrumbs, actual value types and bounded previews. Objects use schema
property order followed by undeclared keys in their original order. Array entries
retain their numeric indices and may show an existing name, title or ID.
The outline uses ordinary disclosure buttons and navigation, not ARIA tree roles.
On narrow screens an Outline button reveals navigation above the detail pane.
Properties are paged in groups of 50; outline children load in groups of 50.
Search covers keys and complete scalar values, including collapsed descendants;
results retain full JSON Pointer paths and show at most 100 matches.

Every property, container and root supports Copy and full-value inspection.
Copy preserves literal strings, including whitespace; other values use formatted
JSON. The full-value dialog offers Copy as JSON, Copy path (JSON Pointer), Download,
line wrapping and case-sensitive find. Empty strings, null, false and zero remain
distinct. Content is inert text, never interpreted as markup. Copy reads underlying
values, not truncated previews, and reports clipboard failures. Full-value dialogs
reuse the native focus-enforced inspector and return focus on close.
Arrays use tuple or item schema when supplied. The fullscreen row inspector uses
this renderer without changing Review's evidence renderer.

Schema shows field types (including null unions), required/optional fields,
descriptions, all supplied constraints and enum values, object properties, array
items, additional properties, composition branches and definitions. Local JSON
Pointer references resolve with sibling metadata; recursive, unavailable and
external references remain explicit and are never fetched. Full schema JSON is
always available. Schema browsing is not a JSON Schema validator.

JSON shows pretty-printed original values. For tables it contains only current
page row values and is explicitly labeled **current page**, never a complete
dataset export. Table search, filters, sorting and pagination remain available.
JSON artifacts have none of those controls; Refresh snapshot remains available
in loading, error and loaded states. Artifact selection, independent partition
scope, aborts and immutable snapshot binding remain owned by the existing code.

Focused parser/helper coverage is in `tests/artifactSchema.test.ts` and
`tests/structuredValue.test.ts`, with
transport binding fixtures in `tests/api.test.ts`. The integration lead owns
execution, native typecheck/build and browser QA. Exercise Data/Schema/JSON and
row inspection at 390/1440 in light/dark themes, long prompts, null/empty roots,
nested collections, nullable references, reference cycles, storage schema,
table page changes, refresh, Inputs selection and partition isolation. No test,
build or browser scenario should execute collected content.

Focused URL coverage lives in `tests/query.test.ts`; existing
`tests/api.test.ts` covers full-key/partition response binding and cancellation.
Lead invocation from `dagster_ui`:
`bun test ./tests/query.test.ts ./tests/api.test.ts --timeout 30000`.
Tests, native strict typecheck/build, and real-fixture browser QA are assigned
to the integration lead. Verify source values 2/3 against output scores 20/30,
alpha/beta isolation, unpartitioned lookup, absent persisted input, no dependencies,
unknown URL input, discovery retry, and existing output filtering.

## Reusable review stages

The active dataset workflow registers one final `annotations` uncertainty review,
not a human source-admission stage. An LLM assesses each source dataset from actual
README/card and license bodies and origin evidence, retains receipts and holds
unresolved eligibility. Both collection branches automatically enumerate and load
complete eligible configured corpora, not bounded inspected-page snapshots.
Branch extraction outputs converge at `merged_command_candidates` before shared
filtering and semantic docs. `labeling_attempts` accounts for each eligible command
independently, using native single-command inference or bounded similarity batches
with shared documentation retrieval. Supported labels feed `proposed_annotations`;
unsupported or invalid outcomes feed the inspectable `labeling_failures` asset and
cannot enter release. Conservative exact deduplication precedes the human queue.
`reviewed_annotations` is removed from the active graph; historical
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
