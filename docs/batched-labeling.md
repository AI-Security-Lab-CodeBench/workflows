# Batched documentation-grounded labeling

The current declared pipeline requires a guarded OpenAI-compatible labeler.
Configure `labeling.batch` in the declared template or inherit the existing
labeling settings with `labeling.from_registration`. The batch settings pin
the backend revision and share a capacity state directory accessible by all
workers calling that endpoint. Batch size, context, model turns and evidence
requests are bounded.

`labeling_attempts` checks operation evidence against the verified captures
from collection. Missing evidence does not trigger another remote crawl.
Supported outcomes feed `proposed_annotations`; unsupported outcomes feed
`labeling_failures`; quota deferrals are reported separately. Batch membership
does not decide a peer's label or invalidate an otherwise reusable row.
See [the stage guide](pipeline-stages.md#datasetslabeling_attempts) for the
current admission and inspection contract.

## Historical enhanced labeling and evaluation

The configuration, provider retrieval and evaluator instructions below
describe the older prepared-input labeling workflow. Use them to interpret
historical receipts or reproduce the standalone evaluation context; they are
not settings or acquisition instructions for current CAT-3 registration.

## Configuration

`AnnotationConfig.batch` enables enhanced labeling. File-based dataset
configuration exposes the same value as `labeling_batch`. Omitting it retains
native single-command labeling. Enhanced mode requires an `openai_compatible`
labeler, an explicitly pinned `backend_revision`, trusted documentation sources,
and a shared capacity-guard directory.

Important settings:

- `batch_size`: 1-50, normally 25 or 50.
- `reasoning_effort`: optional `low`, `high`, or `max` template setting.
  Verify the deployed template before configuring it. GLM-5.3-Flash uses
  `chat_template_kwargs.reasoning_effort` and always opens a reasoning block;
  a `thinking=false` toggle does not disable that block. Evaluate quality at
  the selected effort rather than assuming a lower budget preserves judgments.
- `maximum_model_turns`: default 4; the final turn exposes only `submit_labels`.
- `maximum_grounding_rounds`: default 2.
- `grounding.maximum_provider_calls`: default 8, including failed attempts.
- `grounding.maximum_response_bytes`, `maximum_total_response_bytes`, and
  `maximum_evidence_bytes`: independent response and retained-evidence bounds.
- `grounding.allowed_domains` / `trusted_url_prefixes`: explicit source trust.
  A library search result or URL alone is not grounding evidence.
- `grounding.pinned_library_ids`: previously verified Context7 library IDs.
- `grounding.revision`: requests document refresh. Unchanged relevant document
  bytes preserve the original row inference.
- `retry_revision`: requests a new stage attempt without discarding successful
  record caches.
- `capacity.state_directory`: the same persistent path for every process calling
  the same model endpoint, including evaluation and grading.
- `capacity.expected_model_path` and `context_length`: verified serving settings,
  not an assumption based on the public model alias.
- `capacity.max_fraction`: at most 0.5.

Provider credentials are environment references, never configuration values.
Defaults are `KEENABLE_API_KEY` and `CONTEXT7_API_KEY`. The runtime needs these
variables, not merely the shell that registered the dataset.

Set a completion budget suitable for the batch. The old 4,096-token single-row
default is not automatically sufficient for 25 or 50 complete labels. Every turn
is checked through the serving tokenizer with messages and tool definitions;
the configured context limit takes precedence over a larger tokenizer limit.
Oversized batches split before generation. An individually oversized record
remains an explicit failure.

## Retrieval and evidence

The model either submits independent outcomes immediately or calls
`request_documentation` with related operation questions and affected candidate
IDs. The broker supports Context7 library search/context and Keenable search/fetch.
It coalesces equivalent requests, retains bounded original excerpts and receipts,
and counts actual provider attempts. It does not build a documentation index.
Policy, demonstrations and identical documents are transmitted once per batch;
row references select the applicable examples and grounding evidence.

Search results authorize discovery, not factual grounding. Keenable fetches must
follow observed, trusted URLs; arbitrary fetches and authentication redirects are
rejected. Context7 excerpts preserve their original page/code URLs. Generated
provider descriptions are marked separately from original content.

Each submitted label echoes the host's row fingerprint and identifies its assigned
evidence, with short verbatim supporting excerpts in `grounding_quotes`. The host
checks each quote against the selected source and persists it in the row's
`support.json`. Identity, routing, utility, confidence enums and citations are also
checked locally. An exact quotation can still be irrelevant: these checks do not
prove semantic correctness, so targeted evaluation must inspect applicability to
the operation and its significant flags.

Unknown production status, authorization, backups or unseen script contents are
not solved by repeated web searches. A documented command with consequential
source-context uncertainty remains `needs_review`. Missing operation grounding
after the tool budget becomes `labeling_failures`, without fabricated labels.

## Reuse and interruption

Checkpoints protect work lost to interruption. A retry of the same labeling
materialization can skip completed batches and must retry the failed batch as
a unit. If a change to the sources, inputs, or I/O contract requires a new
materialization, label the entire input again, including unchanged rows. Keep
the old receipts for inspection; they do not supply results for the new run.
Human review uses its own per-record decision identity.

The admission guard permits at most one owned in-flight request and conservatively
reserves a complete configured context window. It checks request slots, token
occupancy, queues, actual model identity and fresh per-rank scheduler timestamps.
Tensor-parallel replicas are not summed. Historical queue histograms are not
current queued requests.

Side load pauses new calls. Missing or stale telemetry denies admission.
Ambiguous requests retain a durable quarantine across process restarts; elapsed
time, an idle aggregate reading or worker death does not clear it. Inspect the
recorded request ID and establish request-specific termination before operational
reconciliation. The guard deliberately has no blind reset command.

This is a metrics-based slot/token admission limit, not GPU compute isolation.
Other clients can arrive after a snapshot, and already-running work cannot be
paused by a client-side admission controller.

## Targeted evaluation

Freeze at most 100 actual baseline rows with original labels, documentation,
selection reasons, development/heldout splits and immutable stage digests.
Include high-category-confidence rows uncertain in risk/action, explicit
documentation gaps, available risk/class strata and high-confidence controls.

Run the isolated evaluator against the frozen `cohort.json` and adjacent `spec.json`:

```sh
python3 -B -m codebench_pipeline.labeling_eval \
  --cohort /private/evidence/baseline/cohort.json \
  --annotation-config /private/evidence/enhanced-annotation.json \
  --split development --batch-size 25 \
  --out /private/evidence/batch25
```

Repeat with 50 using a separate output/cache directory. The optional
`--credentials-stdin` accepts exactly the two provider environment keys as JSON,
without putting them in process arguments. Output contains attempts, original
receipts, token/call accounting, active model time, capacity wait, confidence
transitions and potential false-allow changes. Its quality gate remains
`pending_grounded_adjudication`; metrics alone never grant rollout approval.

Compare grounded per-field judgments and unsupported/dangerous downgrades before
using a smaller review queue as an improvement claim. Model-assisted evaluation
does not constitute human review approval. Validate the chosen configuration on
heldout rows before backing up and relabeling the full eligible dataset.
