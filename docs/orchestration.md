# Dataset building with Dagster

The default job is **build_dataset**, in code location **Dataset building**.
Its assets are namespaced under **datasets**, with named native collection and
finalization groups.
Dataset/corpus and original-documentation branches share category rules and
processing code, with separate provenance and eligibility bindings. `build_dataset`
supports selective planning across both; neither waits for human source admission.
`build_documentation` runs only documentation collection through `command_candidates`.
Both branches converge at `merged_command_candidates`, followed by shared
filtering, semantic documentation, single per-command labeling,
conservative exact deduplication, uncertainty-only human review and one `release_dataset` job.
Configured branches must be current and authorized before final publication.
Model training is not part of this namespace or job.

## View existing data without rebuilding

Use the **same home that produced the materializations**. In this workspace the
previously verified category data is in `.codebench-selective`; `.codebench` contains
the historical pipeline and `.codebench-datasets` contains earlier dataset runs.
Opening the new graph against the historical instance displays "Never materialized"
even though the tables exist in another home.
Those historical releases predate the human gate. The new Data tab identifies
them as `legacy_unreviewed`; it does not silently grant human approval.

```sh
uv run python -m codebench_pipeline.ui_build
uv run python -m codebench_pipeline.dataset_cli ui --home .codebench-selective --port 3001
```

Build the UI once before launching it. This is a project-owned build-time
extension of pinned Dagster 1.13.29, not a source fork or supported runtime plugin.
See [build and upgrade instructions](../dagster_ui/BUILD.md).

The launcher prints the dataset home, Dagster history directory and viewing URL.
It sets both `CODEBENCH_HOME` and `DAGSTER_HOME` for the child process, overriding
conflicting inherited values. It does not build data, start inference or move
history. For future builds replace `.codebench-selective` with their actual `--home`.
An uninitialized home is rejected instead of silently opening an empty instance.
Local instances explicitly use Dagster's immediate run coordinator. The standalone
UI can launch preparation without a queue daemon; a successful launch is still
distinct from a successful run.

In the browser:

1. Open **Catalog**, then **datasets / dataset_release** for the combined dataset.
   Native lineage groups distinguish `existing_datasets`, `documentation_crawl`,
   `dataset_finalization` and `dataset_configuration`.
2. The Overview shows the latest materialization and its category partition.
   To choose a particular category, open **Partitions**, click the name
   `cloud_infrastructure_commands`, and open
   its materialization details. Until a name is clicked, the details pane says
   **No partition selected / None** even when the Materialized counter is 2;
   the partition-filter checkboxes do not select a details row.
3. In materialization **Metadata**, find `sample` to see rows. The Overview also
   shows row count and columns. Collapse Description/Columns or filter metadata
   for `sample` if the table is below the fold.
4. Open **Data** for complete persisted rows, bounded server-side filtering,
   sorting and pagination. Expand a row to inspect full text, not the 500-character
   metadata sample. A changed snapshot requires explicit refresh.
5. Both branches crawl or load eligible sources automatically. Inspect provenance,
   license/content eligibility and exclusions in `source_catalog`; there is no
   active source-admission Review stage. If the shared `human_review_queue`
   contains uncertain rows, open **Review**, select the category and enter a
   reviewer name. Save each record as approved, rejected or pending, with notes.
   Approvals concern dataset inclusion, never command execution. A row enters this
   queue when any of category, risk, severity or class confidence is `medium` or `low`. All four `high` means low uncertainty; eligible
   rows are admitted without a decision and marked `human_reviewed=false`.
   Every queued record needs a terminal
   decision, and human approval cannot bypass machine gates. Included uncertain
   rows with actual approval are marked `human_reviewed=true`. If the queue is
   empty, the build releases automatically through the same `dataset_release`,
   without a reviewer, invented decisions or human completion.
   **Complete review and publish** launches the native release job and promptly
   returns its run link. Refresh the snapshot after that run succeeds to see the
   CSV download. **Continue publication** resumes a failed launch or returns the
   same active/current result without starting a duplicate run. Use **Retract** on a saved row to return it to
   pending without clearing other decisions. Complete again after changing the
   decision to publish a replacement dataset. Prior outputs remain inspectable
   as stale history, but cannot feed downstream builds.
6. Inspect `human_review_queue` for unified human decisions,
   `merged_command_candidates` for combined extraction, `proposed_annotations` for unified model labels,
   and `deduplicated_annotations` for exact duplicate elimination,
   `filtered_commands` for quarantined extraction candidates, and `source_catalog`
   for source exclusions. These are different outputs, not missing release rows.

For full cell values and an explicit category, bypass the bounded UI sample:

```sh
uv run python -m codebench_pipeline.dataset_cli show \
  --home .codebench-selective --dataset cloud_infrastructure_commands \
  --stage dataset_release --limit 10
```

`show` reads persisted Parquet and prints JSON with total/shown row counts; it
does not create a run or materialization. `--stage` accepts the registered
stages below; `category_rules` prints its typed JSON instead of rows. `--limit`
bounds rows from 1 to 1000, not cell lengths. The UI's first-N sample limits
individual cells to 500 characters; this command preserves full cell contents.

If the UI says "Never materialized", check **Deployment > Configuration**:
its storage base directory must be `<chosen home>/dagster`. Do not click
**Materialize all** merely to compensate for viewing the wrong history.

## Run a complete dataset

```sh
uv sync --frozen --python 3.12
uv run python -m codebench_pipeline.dataset_cli build --config business-configs/datasets/cat3.json --home .codebench-datasets
uv run python -m codebench_pipeline.dataset_cli ui --home .codebench-datasets
```

The CLI initializes persistent Dagster metadata and registers the dataset partition.
Both branches automatically collect every eligible configured source, enforcing
provenance, license/reuse terms, content checks and captured-byte integrity.
`human_source_decisions` is removed from the active graph. Unconfigured branches
remain distinct from empty or blocked configured branches. Finalization releases
automatically if the uncertainty queue is empty; otherwise it waits for real
terminal queue decisions and explicit completion.
Receipts expose both branch outcomes and identify the
waiting stage and blocked descendants. Later builds select only changed preparation stages and
their affected descendants, loading unchanged upstream DataFrames from storage.
An unchanged configuration creates no run and emits no materializations.
Run receipts in `$CODEBENCH_HOME/build-<run-id>.json` record selected stages,
skipped stages, selection reasons and per-stage cache hits. Registration alone is available through
`dataset_cli register`; validation without registration through `dataset_cli validate`.
Both accept the same `--config` and `--home` arguments.

In the UI, open **Jobs → build_dataset → Materialize all**, select a dataset partition,
and inspect **Config → Add config** before launch. Open an asset's latest
materialization to inspect **sample**, **dagster/row_count**, column schema,
configuration and inference receipts. Samples are bounded by `storage.sample_rows`
and individual displayed cells by 500 characters; persisted tables are complete.
The `dataset_build_requests` sensor is stopped by default. It uses the same
selective planner as the CLI and emits no request for an unchanged partition.
The UI's explicit **Materialize all** action remains a manual full selection;
use the CLI or sensor for automatic incremental selection.

## Data contracts

The graph passes actual pandas DataFrames, annotated as `DataFrame[Schema]` and
validated by Pandera, rather than strings containing artifact paths:

Every asset's Description explains its inputs, goal, rationale and output.
The [stage guide](pipeline-stages.md) adds detailed gates and limitations;
maintaining these descriptions is mandatory in [the working rules](../AGENTS.md).

The following are dataset/common stage names. Documentation instantiates the
collection stages through `command_candidates` under `documentation/`, except
shared `category_rules`. `human_source_decisions` is historical, not an active stage.
Finalization is shared, not duplicated.
See [branch contracts and migration](independent-branches.md).

1. `category_rules`: typed, versioned taxonomy and policy (the only non-table asset).
2. `source_catalog`: selected and excluded sources with provenance and reasons.
3. `source_documents`: automatically acquired eligible sources' inert text and attribution.
4. `document_adapters`: per-source declarative parsing mappings.
5. `command_candidates`: extracted records, context and positions.
6. `merged_command_candidates`: current branch candidates, namespaced identities and original provenance.
7. `filtered_commands`: all merged candidates with eligibility or quarantine reasons.
8. `labeling_documentation`: prepared manifest semantics for eligible operations.
9. `proposed_annotations`: independent per-command labels, confidence and rationale.
10. `deduplicated_annotations`: conservative exact normalized survivors with an audit mapping.
11. `human_review_queue`: only medium-or-higher uncertainty annotations, with machine exclusion reasons.
12. `human_review_decisions`: real, version-bound terminal queue decisions; no invented rows for automatic admission.
13. `dataset_release`: the single current release, with per-row `human_reviewed` after the nine command columns.

The authoritative publication pointer is the SQLite review ledger, committed
after immutable Parquet, CSV and audit sidecars are written. A manual release
materialization uses the same admission gate. Decisions bind the exact evidence
snapshot and a digest of the completed decision set when the queue is nonempty;
automatic release revalidates the current empty queue and machine eligibility
instead of requiring human completion. Stale revisions, missing records and changed
decisions fail explicitly. Eligibility and required approval precede the per-risk cap.
Historical raw outputs remain inspectable but are not retroactively human-approved.

Labeling caches validated results and receipts by each record's relevant input,
independently of batch membership. Adding or changing a row labels only missing
or invalidated rows. Human batches similarly carry terminal decisions only when
the row evidence/policy fingerprint, eligibility and selection bounds match.
Carried decisions retain their reviewer and audit origin. A changed snapshot is
open until explicit completion if the uncertainty queue is nonempty. An empty
queue is revalidated automatically; old completion tokens never carry forward.

One local dataset home is the review authority, including when table objects are
in S3. Config/current-reference writers share its transaction protocol; separate
hosts with independent review databases are not a distributed review service.
Reviewer identity is self-reported. The UI/API binds to loopback and rejects
foreign-origin writes; it does not provide authenticated reviewer roles.

Human review is reusable for other stages, including model-candidate selection.
Stage registrations provide records, evidence, eligibility and completion rules;
the native Review tab discovers them rather than hardcoding annotation assets.
See [the integration contract](reusable-review.md). Rebuild existing registered
data with `dataset_cli build --dataset <dataset_id> --home <home>` to retain its
individual settings instead of replacing them with an example configuration.

Filtered-out records remain in `filtered_commands`; they are not relabeled or
silently discarded. Agentic approval is not human approval. No command text is
executed. Exact normalized deduplication precedes human review; fuzzy similarity
and safe-counterpart pairing remain deferred.

## Versioned configuration

Each `business-configs/datasets/*.json` supplies model settings, processing settings
and storage, plus inline sources or external manifests. The shipped configuration
uses `business_config` to reference category-owned files. Inline `rules` remain
available for self-contained configurations and synthetic fixtures; do not supply
both forms.
`seeds.manifest` and `documentation_sources.manifest` are category-bound
source lists. `documentation.manifest` supplies semantic snippets. Paths resolve
relative to the referencing file. Unknown fields and invalid scalar types fail.
Registered versions and immutable historical outputs remain retained.

The category file defines category ID/name/version, risk definitions and
command-family patterns, class definitions, severities and description language.
It references the generic base prompt, category instructions and family examples.
Patterns determine routing, never safety. The current v0
contract permits allow/deny/ask and low/medium/high/critical. Another category uses
another single configuration file without changing Python. CAT-2 remains a local
multicategory example and is excluded from this repository.

Use a distinct `dataset_id` for each category. Its registry, asset storage and
materialization history are partition-isolated. Top-level `revision` records
configuration history; changing it alone does not rerun acquisition. The separate
`seeds.revision` requests source collection refresh. Historical registered
specifications and materialized instruction snapshots remain immutable.

For an existing home, bind the category business configuration instead of
registering the shipped dataset template over its individual runtime settings:

```sh
uv run python -m codebench_pipeline.dataset_cli bind-business \
  --dataset cloud_infrastructure_commands \
  --business-config business-configs/categories/cat3/category.json \
  --home .codebench-datasets
uv run python -m codebench_pipeline.dataset_cli build \
  --dataset cloud_infrastructure_commands --home .codebench-datasets
```

Registered builds refresh the bound policy files while retaining model, worker,
storage and source settings. Editing the dataset template itself does not replace
an existing registration. Policy or applicable example changes invalidate their
inference inputs and stale review authorization; unchanged inputs reuse receipts.
Missing or malformed files fail rather than reusing stale policy.

Only `models.labeler` is required. Legacy selector, adapter and reviewer settings
are unused by the active graph. The labeler can use native authentication or an
OpenAI-compatible endpoint, for example:

```json
{
  "provider": "openai_compatible",
  "model": "your-model-id",
  "base_url": "https://your-provider.example/v1",
  "api_key_env": "DATASET_REVIEW_API_KEY",
  "timeout_seconds": 900.0,
  "max_output_tokens": 8192,
  "structured_output": "json_schema"
}
```

The transport appends `/chat/completions`. Use `json_object` for providers without
JSON-schema response format support. Local strict validation always applies.
Credentials are environment references, not stored values. There is no automatic
provider fallback or response repair. Native mode uses supported `omo` authentication
and a fresh no-tools/no-session invocation per record. Native sampling/token controls
use CLI defaults, recorded honestly in receipts; API generation controls are explicit.
The native child also sets `PI_OPENAI_WEB_SEARCH`, `PI_ANTHROPIC_WEB_SEARCH`,
`PI_OPENAI_IMAGE_GEN` and `PI_WEBFETCH` to `0`, overriding inherited provider-native
features without changing user settings or credentials. Receipts retain these
overrides; provider-native tool blocks remain rejected, not stripped from an answer.
Malformed JSON, unsupported taxonomy and overlong rationales fail closed with retained
evidence. Failed attempts are visible as Dagster asset observations.

## Sources and examples

Before building, use `skills/codebench-source-preparation/SKILL.md` to discover,
inspect and enumerate complete sources locally. The shipped CAT-3 config expects
`business-configs/inputs/cat3/datasets.json`, `documentation-sources.json` and
`semantics.json`.
These ignored inputs and their preparation inventory are not included in Git.

Both source manifests have `version: 1`, `category_id`, `revision`, `sources`
and `evidence`. The semantic manifest uses `snippets` instead of `sources`.
Source records include provenance, license, format and a local path or exact URL.
Remote URLs require SHA-256 or explicit Git blob SHA-1; full bodies are validated.
Semantic references use actual operation paths or option names. The explicit
`<default>` operation tag documents an exact no-argument invocation; it does not
match arbitrary subcommands or option-selected behavior. Missing candidate
documentation blocks inference before any labeling call.
The active modes are only `provided_sources`, `provided_snippets` and provided
adapters. Online search requests are rejected, not silently ignored.

Reviewed Python functions/classes may be referenced by `transform` with local
`module_path`, `sha256`, `entrypoint`, `kind`, `trusted: true`,
`options_json` and `batch_size`. They consume source batches and return canonical
rows with original positions. Only explicitly trusted, matching module bytes are
imported; collected strings remain inert. See `transforms/` for reusable examples.

Run validation, then build with `--config` into an empty home. Retain source counts,
exclusions, download receipts and label receipts. Repeat unchanged to prove reuse;
replay immutable inference receipts explicitly when comparing deterministic labeled
outputs. Fresh LLM calls are not promised to be byte-identical.

## Parquet storage, including MinIO

The IO manager owns persistence. Asset functions do not pass storage locations.
Default local storage is `$CODEBENCH_HOME/dataframes`; category rules use JSON,
all table values use Parquet. Both assets and cached stages have hash-checked
immutable objects. Changing current asset references preserves previous versions.
To use an existing MinIO/S3 bucket, add this top-level configuration:

```json
{
  "storage": {
    "backend": "s3",
    "bucket": "dataset-tables",
    "endpoint_url": "http://localhost:9000",
    "region": "us-east-1",
    "access_key_env": "DATASET_S3_ACCESS_KEY",
    "secret_key_env": "DATASET_S3_SECRET_KEY",
    "prefix": "datasets",
    "sample_rows": 10
  }
}
```

Create the bucket separately. AWS can omit endpoint and credential references to use
its normal SDK credential chain. Local and S3 use the same IO manager contract;
Dagster metadata itself remains in the persistent Dagster instance, not in S3.
Object hashes, conditional writes and immutable evidence reject silent overwrites.
The real MinIO verification can be repeated with an installed binary:

```sh
uv run python -m codebench_pipeline.minio_qa --binary /path/to/minio --report .codebench-datasets/minio-qa.json
```

This owns an ephemeral loopback-only server on ports 19000/19001, uses generated
credentials without logging them, checks actual Parquet, cache reuse and previews,
and stops/removes its server data afterward. Those ports must be available.

## Reuse and retained evidence

Stage signatures include effective configuration, input DataFrame contents,
relevant rule fields, source/documentation hashes, implementation bytes and lockfile.
The planner compares partition-specific materialization fingerprints before
starting a run. Cache reuse is a separate recovery mechanism within a selected
stage; a cache hit is not misreported as a skipped materialization.

| Changed configuration | Selected stages after an initial build |
| --- | --- |
| Nothing, or only top-level revision | None |
| Labeler settings | Affected labeling outputs and shared finalization |
| Policy, description language, or rule version | Rules, labeling and shared finalization |
| Documentation snippets | Documentation, labeling and shared finalization |
| Publication cap | Shared review preparation and release authorization |
| Extraction settings | Extraction, filtering, labeling and shared finalization |
| Source manifests | Source catalog and dependent acquisition/processing stages |

Rule changes use stage-specific projections: changing only labeling policy does
not invalidate source selection or filtering. Changing command-family patterns
can affect both. Unselected categories receive no materializations.
Implementation and dependency changes conservatively invalidate stage caches;
changing a category does not require changing either.

Validated registry and manifest parsing uses bounded, process-local caches keyed
by exact file bytes, not timestamps. Lookups still read current files, resolve
paths and verify trusted transformation hashes. Returned mutable collections are
isolated from cached values. Manifest-backed registrations hydrate their current
inputs instead of repeatedly copying superseded embedded source lists.

Review snapshot capture retains the complete reference and evidence inventory
from one validated preparation plan. It verifies each configured branch's current
source authorization and binds shared annotations to those branch candidate references.
It reads the annotation and taxonomy values by their validated references and
rejects references or materialization digests that change during capture. Unused
table bodies are not repeatedly loaded and authorized merely to recover references.
Annotation schema checks and publication preparation operate on table values
without the evidence metadata attached, avoiding repeated whole-evidence copies
during column validation. Snapshot fingerprints still include each record's
semantic evidence, and the returned review frame retains its original metadata.

Every successful stage retains effective config, exact code snapshots and inference
receipts with prompt/input hashes, response, observed model and timestamps. Failed
attempts also retain receipts. Storage corruption fails instead of being repaired.
The current table parser supports text, Markdown, JSON, JSONL and CSV; declarative
adapters are declarative. Reviewed hash-pinned Python batch transformations handle
source-specific formats; collected command strings remain inert.

The small examples verify orchestration; they do not meet the 250-record-per-risk
collection target. The current cap is per dataset partition, not a cross-category
combined release. Historical CAT-3 CSV bundle publication remains available below.

## Historical compatibility and checks

The old graph is in `codebench_pipeline.legacy_definitions`; the existing
`python -m codebench_pipeline.cli` commands continue to use it and `.codebench`.
Do not confuse historical replay or its optional discovery branch with this new
full configured job. Existing source data, root CSVs and `strategy/` are preserved.

```sh
uv run python -B -m unittest discover -s tests -v
uv run basedpyright
uv run ruff check codebench_pipeline tests/test_dataset*.py tests/test_dataframe_io.py
```

The locked dagster-pandera integration emits its upstream beta warning; validation
is still enforced. Dagster's own UI is desktop-oriented; its mobile layout clips
and overlaps content. Use a desktop viewport. No custom web application or
external spreadsheet is needed. Runtime evidence and collected datasets stay
local rather than being bundled with this code-only publication.
