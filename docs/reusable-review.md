# Reusable human review

Human review is a stage-scoped workflow, independent of command annotation,
CSV export, or Dagster release jobs. Dataset inclusion is one adapter:
`ReviewService` supplies annotation evidence and `ReleaseRunner`; the lifecycle
lives in `ReviewWorkflow`.

## Integrate another stage

1. Define a `ReviewStage` with a stable `stage_id`, title, native asset keys,
   completion label, and approval bounds. A model-selection stage can require
   `min_approvals=1, max_approvals=1`. Every other candidate must be explicitly
   rejected before completion.
2. Provide an authoritative reader returning `ReviewTable`: the actual DataFrame,
   its stable string-ID column, evaluation/configuration revision, evidence, and
   eligible IDs. The table need not contain command, risk, or annotation fields.
   Do not generate data or run models inside this reader.
3. Wrap it in `TableReviewSource(home, stage, reader)`, then construct
   `ReviewWorkflow(home, stage, source)`.
4. Pass `ReviewRegistration(stage, workflow)` through the `stages` argument of
   `make_dataset_api`. The default annotation adapter remains registered.
   The native UI discovers the asset-to-review mapping from `/review-stages`;
   no frontend asset list, record-ID assumption, or publication button branch
   needs editing.
5. A downstream consumer accepts the returned completion token and calls
   `workflow.approved_records(dataset_id, token)` to obtain only currently
   authorized rows. A changed evaluation, selection policy, dataset, stage,
   generation, decision set, or snapshot rejects the old token.

The generic completion response contains the durable completion token and
`publication: null`, `run_id: null`. It does not run a dataset release or pretend
that a model was deployed. An optional completion publisher is used by the
dataset-inclusion adapter.

In the native UI, dataset completion launches `release_dataset` asynchronously
and returns its run ID with no publication yet. The publication link appears
after the run commits and the view refreshes. **Continue publication** reuses an
active run for the same completion or an already-current publication; a failed
run can be retried without repeating human decisions or inference. The synchronous
`ReleaseRunner` remains available for in-process callers and regression checks.

The command adapter supplies only uncertain records from the unified deduplicated
dataset to human review. Any `medium` or `low` among category, risk, severity,
class and independent reviewer confidence requires review. All five `high` means
low uncertainty: machine-eligible rows need no decision. Machine-ineligible rows
remain inspectable and cannot be admitted by approval.

Both branches crawl or load eligible sources automatically, with provenance,
license and content checks; `human_source_decisions` is removed from the active
graph. They feed one final queue and one `dataset_release`. If the queue is empty,
the build completes the empty batch under the explicit automatic-policy actor
and releases without human decisions or a human completion signal. Otherwise
the single release waits for real terminal queue decisions and explicit completion.
See [confidence policy](confidence-review-policy.md) for the
ten-column publication envelope and per-row `human_reviewed` marker.

Adapters may declare `additional_publication_stages` for an independently
authorized publication shown beside their own review batch. This is explicit
descriptor metadata, not a frontend assumption that all reviews are annotations.
The unified annotation adapter advertises no automatic companion.

For an operation that computes before committing an artifact, check authorization
again at the commit boundary. For a nonempty uncertainty queue, hold
`HumanReviewStore.transaction()` while resolving the current source and calling
`transaction.authorize_completion(snapshot, token)`, then commit the output.
For an empty queue, revalidate queue emptiness, machine eligibility and the current
snapshot at commit without inventing a human token. Automatically admitted rows
have `human_reviewed=false`; actually approved uncertain rows have
`human_reviewed=true`. Writers of underlying current references/configuration must participate in
that same transaction protocol. The workflow is a local authority, not a
distributed locking service.

## Snapshot and storage contracts

`TableReviewSource` fingerprints the exact table bytes, revision, stage,
eligibility, selection bounds, and evidence. It retains immutable evidence
manifests so historical decisions can still be inspected after input changes.
Its reader must supply sanitized evidence and authoritative source revisions.
Applications needing different evidence storage can implement the small
`ReviewSource` protocol directly.

`ReviewSnapshot.record_fingerprints` binds each record's relevant evidence and
policy separately from the whole snapshot. When supplied it must cover exactly
the snapshot's candidate IDs. An empty mapping is the historical default and
disables carry-forward. Producers must not use the complete surrounding table's
digest as each row's fingerprint.

When a snapshot changes, the ledger opens a new batch. A prior-current terminal
decision carries only when the record ID, nonempty fingerprint, eligibility and
selection bounds still match. The decision retains notes, reviewer, timestamps
and row revision; `carried_from_batch_id` and a `carried` audit event link its
origin. Pending/retracted decisions never revive older approvals. Completion
tokens and continuation runs do not carry; the new snapshot needs explicit
completion after its new or changed records are decided when its queue is nonempty.
The dataset adapter revalidates an empty queue automatically without human action.

Current batches are isolated by `(dataset_id, review_stage)`. Records keep their
stable IDs even through filtering and sorting. The transport field
`candidate_id` denotes that generic record ID; it is not restricted to command
candidates. Review-page rows expose a separate top-level `review_record_id`,
leaving original table columns intact.

Existing single-stage SQLite ledgers migrate transactionally to the
`annotations` scope. Prior decisions and history remain intact. Approval bounds
are enforced at completion and authorization, not only by disabled UI controls.
Reopening retains decisions but advances the generation and invalidates earlier
completion tokens.

Saved terminal decisions can be retracted individually. Retraction atomically
reopens a completed batch when needed, advances its generation, returns only
the chosen row to pending, and preserves neighboring decisions and history.
The old completion no longer authorizes consumers. Explicit completion after
editing materializes the revised decisions and corresponding dataset.

WAL-backed read snapshots allow inspection without reserving the writer lock.
Mutation paths still validate row/batch revisions; bounded lock waits do not
turn stale writes into overwrites.

## HTTP and native UI

- `GET /codebench/api/review-stages` discovers stage descriptors.
- `POST /codebench/api/reviews/{stage_id}` queries its current review table.
- `PUT .../decision` saves a revision-bound decision.
- `POST .../complete` freezes a fully decided selection.
- `POST .../reopen` explicitly reopens it.
- `POST .../retract` retracts one saved row with its row revision and expected batch version.
- `POST .../continue` retries a configured continuation for a completed stage.
- `GET .../{batch_id}/evidence/{candidate_id}?dataset_id=...` reads immutable evidence.

Existing `/review` routes retain their annotation meaning. Dataset-scoped
transport uses the registered dataset identity. Review URLs preserve both
`partition` and `review_stage`; drafts and responses cannot cross stages.

`CompletionEffect` may return a publication, a native run ID, or neither.
Descriptors advertise `can_continue` and a continuation label. Successful run
IDs persist on the batch; an unchanged/no-op continuation preserves the previous
link. Generic stages can launch continuations asynchronously through Dagster,
so the browser doesn't wait for long work. Historical source-selection
continuations aren't an active collection prerequisite.

Stale and legacy-unreviewed materializations remain inspectable but are rejected
as authorized pipeline inputs. Historical publication downloads remain
explicitly historical. Completing a revised review creates a new publication
without rewriting the previous CSV.

The native UI shows selection requirements, completion without publication,
stage-specific evidence, and explicit stale/conflict recovery. Self-reported
reviewer identity is not authentication. Unsaved drafts live in the browser;
saved decisions live in SQLite.

## Existing pipeline data

Rebuild a registered dataset without replacing its individual configuration:

```sh
uv run python -m codebench_pipeline.dataset_cli build \
  --dataset cloud_infrastructure_commands --home .codebench-selective
```

This uses the registered source paths, model roles, policy and settings rather
than registering an example config over them. Preparation rematerializes only
the changed dependency closure and stops at human review only when the final
uncertainty queue is nonempty. With an empty queue it automatically releases
machine-eligible rows through the same terminal asset. Existing published
artifacts remain historical evidence; they are not silently approved.

Subrisk IDs, names, definitions, guidance and routing belong to each category's
initial JSON instruction; see `category-configuration.md`.

## Verification

`tests/test_review_workflow.py` exercises model candidates with arbitrary
`model_id` and score columns through the real generic service and HTTP routes.
It checks one-choice completion without publication, downstream selected-row
consumption, stage isolation, changed-evidence invalidation, immutable old
evidence, and legacy ledger migration. Existing real-Dagster release tests
exercise the annotation adapter and the independent publication gate.

Per-row retraction, source selection, documentation integration and concurrency
contracts are exercised by the service, reader, source-review and real-Dagster
tests. Synthetic fixtures make these checks independent of excluded category
configs and local source captures. Keep live review ledgers and QA evidence local.
