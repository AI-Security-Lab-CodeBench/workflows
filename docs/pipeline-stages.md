# Dataset pipeline stages

This guide describes independent dataset and documentation collection branches
and one shared finalization path. Native lineage groups are `existing_datasets`,
`documentation_crawl`, `dataset_finalization` and `dataset_configuration`.
Each dataset has its own `dataset_builds` partition.
See [orchestration](orchestration.md) for configuration, launch and storage setup.
The implementation is in `dataset_assets_sources.py`,
`dataset_assets_annotations.py` and their helpers under `codebench_pipeline/`.

`category_rules` is a typed `CategoryRules` value persisted as JSON. The other
assets pass actual pandas DataFrames, not artifact paths. Their declared
Pandera models in `dataframe_schemas.py` also supply Dagster types. Table schemas
reject extra columns and coerce column types; record models and helper checks
add constraints that column schemas alone don't express.

All source commands, context, snippets and model responses are inert, untrusted
data. No stage executes a collected command. Source selection decides which
documents to process. Filtering decides which extracted records can enter
inference. One isolated labeling invocation assesses each eligible record.
Both branches automatically crawl or load eligible sources, enforcing provenance,
license/reuse terms, content eligibility and inspected-source bindings. Agent
proposals aren't human approval. Final human review covers only rows with any
`medium` or `low` among the four label confidences. All four `high` means low uncertainty and automatic admission only
if machine eligible. Conservative exact normalized
deduplication runs on the combined labeled dataset before human review.
Fuzzy linking and safe-counterpart pairing remain deferred.

The sections below describe common stage behavior and the dataset paths.
Documentation instantiates collection stages through `command_candidates` at
`datasets/documentation/<stage>`, except shared `category_rules`.
`human_source_decisions` is removed from the active graph; neither branch waits
on human source admission.
Both branches converge at `merged_command_candidates` before shared filtering,
semantic documentation, labeling, deduplication, final human review and release.
See [independent branch contracts](independent-branches.md) for configuration
and source-binding rules.

## `datasets/category_rules`

**Input:** Inline rules from the dataset configuration: category identity and
version, policy, risks and command patterns, class and severity definitions,
and description language.

**Goal:** Produce the exact taxonomy and policy snapshot used downstream.

**Why:** Selection, routing, labels and publication need a shared, inspectable
contract rather than hidden category-specific defaults.

**Output:** A `CategoryRules` value with a content-derived data version, immutable
rules evidence and `rules`, `category`, `version` and `cache_hit` metadata.
This asset has no DataFrame row count or table sample.

**Gate/inspection:** Strict model validation rejects unknown fields, duplicate
taxonomy identifiers or names, invalid regex patterns, risks outside the
category prefix, and a class set other than `allow`, `deny`, `ask`.
Severity names are limited to `low`, `medium`, `high`, `critical`.
Patterns support routing, never a safety verdict; policy text remains evidence
for model judgment, not a complete executable policy validator.

## `datasets/source_catalog`

**Input:** Category scope and a category-bound source manifest, or inline sources.

**Goal:** Validate the declared inventory for complete acquisition.

**Why:** Source discovery and README/license/origin assessment belong to the local
agent, outside Dagster. The preparation inventory retains scope and exclusions.

**Output:** `SourceCatalogTable` with source identity, URL, license, revision,
format and provenance. Manifest-backed confidence is `not_applicable`.
Complete source metadata is retained in the `source_inputs_json` table attribute;
the JSON string avoids repeated deep copies of a large nested inventory during
schema validation without dropping evidence.

**Gate/inspection:** Invalid category, duplicate identities, unsupported provenance,
prohibited reuse or agent/SFT corpus sources fail validation. No search or selector
model runs. Inspect the retained manifest and its referenced preparation evidence.

## Historical `datasets/human_source_decisions`

This source-admission stage is removed from the active graph. Its historical
decisions and snapshots remain inspectable evidence, not current authorization.
The active collection path goes from the catalog to automatic acquisition of
eligible sources without a human decision or completion token.

## `datasets/source_documents`

**Input:** The validated catalog and manifest sources, size limits and worker count.

**Goal:** Retrieve every admitted file in full and persist its actual byte hash.

**Why:** Bounded page samples cannot substitute for complete source bodies.

**Output:** `SourceDocumentsTable` with inert text, SHA-256 and attribution;
per-source receipts distinguish verified cache reuse from new downloads.
Source metadata and prior transformations remain in serialized JSON attributes,
validated and decoded by extraction after persisted-table reload.

**Gate/inspection:** Local or exact remote bytes must match declared SHA-256 or
Git blob SHA-1. Redirects, oversized, empty, non-UTF8 or sensitive bodies fail.
A failed file prevents a successful partial snapshot. Verified successful bodies
remain cached for recovery. Catalog IDs absent from the manifest are rejected.
Transport connection/DNS/timeout failures get at most three attempts per missing
file, with each failure retained. HTTP status failures and integrity/content
failures are not retried. Exhausted transport attempts still fail the stage.

## `datasets/document_adapters`

**Input:** Full documents and their manifest-declared format/field mappings.

**Goal:** Bind each document to its deterministic decoder.

**Why:** Source-specific formats must not be guessed by a model during a build.

**Output:** `DocumentAdaptersTable` with source ID, format and field mapping.

**Gate/inspection:** Supported formats are text, Markdown, JSON, JSONL and CSV.
Extraction validates actual values. Optional reviewed local Python batch modules
use `SourceRow`, `TransformContext` and `CanonicalSourceRow`; a function or
class entrypoint requires explicit trust and an exact module SHA-256. Hash failure
prevents import. Returned canonical rows are validated, with positions and
transformation hashes preserved. Downloaded commands are never executable code.

## `datasets/command_candidates`

**Input:** Full source documents, their adapters and the context-window setting
(100 characters on either side by default).

**Goal:** Extract broad candidate records with enough source location and context
to inspect them later.

**Why:** Extraction preserves evidence before eligibility or safety decisions.

**Output:** `CommandCandidatesTable`: candidate ID, command, utility, context,
source ID/URL/position, transformation markers and initially empty risk routing.
IDs hash source identity, position and command, so identical commands at distinct
source positions remain separate records.

**Gate/inspection:** Structured formats must decode and yield textual command
and context fields. Text extraction handles recognized shell Markdown fences,
inline backticks and eligible plain lines; it skips comments and some Markdown
lines and strips a leading `$ ` prompt. Positions refer to character offsets
within the decoded document or selected record field. Markers identify fence or
inline extraction, not a complete normalization audit.

Utility is the first shell token's basename, with a basic split fallback for
unbalanced quotes. It isn't a full compound-command utility analysis. Extraction
is line-oriented, not a general shell parser: prose or console output can become
candidates, and multiline shell constructs aren't reconstructed. Invalid
structured fields fail the stage instead of silently dropping the record.

## `datasets/merged_command_candidates`

**Input:** Current authorized `command_candidates` from both collection branches.

**Goal:** Merge extracted rows before any shared processing or labeling.

**Why:** Acquisition must remain independent without duplicating category processing.

**Output:** `CommandCandidatesTable` with branch-prefixed candidate/source IDs.
The `origins` attribute preserves original branch, candidate ID and source ID;
`branch_metadata` preserves input attributes. `codebench_finalization` binds each
configured branch's exact candidate digest, or an explicit unconfigured/waiting state.

**Gate/inspection:** A configured empty table is bound as configured, not omitted.
Missing, changed or unauthorized configured inputs become waiting placeholders,
never historical substitutes. Only the merge can materialize partial waiting
inputs for inspection; filtering and every later stage require all configured
branches to be current. `build_documentation` stops at its own command candidates;
the registered build performs this merge and the shared tail.

## `datasets/filtered_commands`

**Input:** Every merged command candidate, category command patterns and filtering
settings.

**Goal:** Mark records eligible for labeling or quarantine them with a concrete
reason, without assigning safety labels.

**Why:** Broad extraction can include unrelated text, unresolved templates and
shell forms that the current parser can't safely interpret.

**Output:** `FilteredCommandsTable`, retaining every candidate and adding matched
`risk_candidates`, `filter_decision` (`eligible` or `needs_review`) and
`filter_reason`. An eligible row has an empty reason.

**Gate/inspection:** Checks run in order: no configured family match; excessive
length or NUL; unresolved template/ellipsis; dynamic-shell markers (`$(`,
backticks or `<<`) when quarantine is enabled; then unbalanced shell quotes.
Only the first reason is recorded. Multiple matched risks remain available to
the labeler. Regex matching and quote checks aren't semantic validation.

Quarantined rows stay here and never enter labeling automatically. They also
don't enter `human_review_queue`, whose input is deduplicated labels, not this
table. Inspect this asset separately to account for extraction/filter losses.

## `datasets/labeling_documentation`

**Input:** Eligible operations, category rules and prepared semantic references.

**Goal:** Expose original behavior evidence for the single labeling invocation.

**Why:** Command names do not establish effects, flags or version applicability.

**Output:** `DocumentationTable` with utility, operation tags, URL, retrieval time,
full snippet and version limits, plus retained coverage and manifest evidence.

**Gate/inspection:** Only provided snippets or a semantic manifest are accepted.
Invalid category, record fields, URLs, sensitive indicators or byte limits fail.
No online discovery occurs. Missing operation coverage remains explicit; an
undocumented record cannot enter labeling inference. Operation-prefix and option
matching selects relevant references, not a claim of complete semantic proof.
Semantic use does not authorize corpus redistribution.

## `datasets/proposed_annotations`

**Input:** Only eligible filtered records, the documentation table, compiled
generic/category instructions, applicable operation-family examples, rules and
labeler model configuration.

**Goal:** Label each record independently from its original evidence.

**Why:** Filtering can establish routing and parse eligibility, but can't decide
category, risk, severity, class or factual description.

**Output:** `ProposedAnnotationsTable`, retaining candidate evidence plus
`accept`/`needs_review`/`reject`, category/risk IDs, severity, class, description,
four confidence values, rationale and cited documentation URLs. Each candidate
gets a fresh isolated no-tools invocation with retained request/response receipts.

**Gate/inspection:** Local validation requires matching identity and utility,
the configured category, a risk supported by both routing and current patterns,
allowed class/severity, low/medium/high confidences, a nonblank rationale of at
most 400 characters and unique citations drawn from supplied documentation.
An accepted label must cite documentation. Candidate identity, category and
source utility are bound in the per-record response schema; risk choices are
restricted to the supplied routing shortlist. Independent post-validation
still checks taxonomy and citations. Missing matching candidate documentation
fails preflight before any record in the invocation is sent to the model.
Example snippets cannot fill this gap or authorize candidate citations.
There is no confidence threshold at this stage.

The prompt requires one sentence in the configured language and forbids invented
approval, production, backups or hidden behavior; structural validation doesn't
prove those requirements or citation relevance. Confidence is self-reported,
not calibrated proof. Invalid responses fail the stage, not a fabricated
`needs_review` label; earlier invocation evidence remains available.

## Historical `datasets/reviewed_annotations`

This asset is absent from the active graph. Historical review artifacts remain
readable, but current rows receive no fabricated review verdict or confidence.
The single shared labeling pass flows directly into exact deduplication.

## Historical branch processing and annotation merge

`datasets/merged_proposed_annotations` and the documentation branch's old
`filtered_commands`, `labeling_documentation` and `proposed_annotations` are absent
from the active graph. Persisted tables and immutable review manifests remain
readable, including original inference receipts, without rematerialization.
They never authorize new shared processing or release.

## `datasets/deduplicated_annotations`

**Input:** The shared `proposed_annotations` table, including per-row semantic evidence.

**Goal:** Remove only proven exact normalized duplicates before human review.

**Why:** Humans should review one representative of an exact duplicate, with
all original evidence still traceable.

**Output:** A typed proposed-annotation table and duplicate-to-survivor evidence.

**Gate/inspection:** Preserve shell semantics, quoted payloads and meaningful
context/label differences. Ambiguous similarities remain separate. No fuzzy
matching, resource renaming, model-based equivalence or safe-pair gate applies.

## `datasets/dataset_release`

**Input:** Unified deduplicated annotations, real completed decisions when the
uncertainty queue is nonempty, category rules and the per-risk publication cap
(default and maximum 250, independently within each dataset partition).

**Goal:** Produce the accepted canonical release table.

**Why:** Consumers need command labels and an explicit indication of human row review.

**Output:** `CommandsTable`, in this exact order:

```text
command,description,util,severity,category,risk,source,seed,class,human_reviewed
```

Category and risk use canonical code-plus-name values. `source` is the candidate's
source ID and `seed` is `-`; the current candidate contract doesn't support
synthetic prompt/model provenance.

**Gate/inspection:** Publication checks complete matching candidate/label
identities and revalidates labels. This single asset publishes machine-eligible
all-high rows without human decisions, marked `human_reviewed=false`, plus
machine-eligible uncertain rows actually approved by a human, marked
`human_reviewed=true`. If the queue is nonempty, release waits for real terminal
decisions and explicit completion of the current snapshot. An empty queue lets
the build release automatically without a fabricated reviewer, decision or
human completion token. Accepted labels remain mandatory; human inclusion cannot repair an unsupported label.
Eligibility and required human approval precede the cap in input order.
Final validation checks nonblank fields,
column order and allowed taxonomy/class/severity values. No random sampling or
pairing is performed; exact deduplication is the preceding shared stage.
There is no fresh source licensing or
source-body audit here; consult the catalog and snapshots.

This asset persists Parquet and an immutable run-scoped CSV/JSONL/card bundle.
The IO boundary rechecks the snapshot, exact record set, current completion
generation and frozen decision digest while holding the shared review transaction.
For an empty queue, the automatic policy completes a verified empty decision set;
machine eligibility and the exact empty-queue snapshot remain bound to its token.
The committed SQLite publication reference is authoritative, not a mutable
object-store pointer. Root `commands.csv`/`README.md` are not replaced.
An empty release is not quota completion. Historical nine-column releases are
preserved; newly published releases use the ten-column contract.

## `datasets/human_review_queue`

**Input:** Unified deduplicated annotations, rules and the same publication cap as the release.
It precedes `human_review_decisions` and `dataset_release`.

**Goal:** Expose only medium-or-higher uncertainty rows for final inclusion review.

**Why:** Uncertain judgments need human decisions without adding all-high rows to
the queue. Full-table evidence and cap exclusions remain inspectable separately.

**Output:** `ReviewQueueTable`, preserving candidate and label columns
plus `exclusion_reason`. Machine-eligible rows have an empty reason; other rows
retain the actual label exclusion. Caps do not exclude rows from human review.

**Gate/inspection:** Any `medium` or `low` among category, risk, severity or class confidence puts a row in this queue. All four `high` removes
it from the queue, not from machine eligibility checks. Missing/invalid confidence
fails validation. Uncertain machine-ineligible rows cannot receive inclusion
approval; all-high ineligible rows remain inspectable in the full deduplicated
table and never enter release. Every queued row requires a real terminal decision.
This isn't an all-pipeline error inbox: source exclusions stay in
the catalog, quarantined candidates stay in `filtered_commands`, and inference
failures stay in observations/receipts. Nothing here records human sign-off or
automatically resolves a withheld row.

## `datasets/human_review_decisions`

**Input:** The prepared uncertainty queue and a completion
authorization bound to the current dataset/evidence snapshot, generation and exact
decision digest.

**Goal:** Materialize the frozen human decision set separately from original labels.

**Why:** Uncertain annotations need real human decisions separate from model judgments.

**Output:** `HumanReviewDecisionsTable` with candidate/batch/snapshot identities,
generation and revisions, approve/reject, notes, reviewer and decision timestamp.
An empty queue retains its automatic-policy completion binding in Parquet
attributes, with zero decision rows. The policy actor is not a human reviewer.
Automatic-admission evidence remains separate from real human decisions.

**Gate/inspection:** Open/pending reviews remain in the durable ledger and Review
tab; release materializes the decision output without requiring human action for
an empty queue. Every expected queued record
must be present and terminal. Approval cannot override machine eligibility.
The IO boundary revalidates authorization before advancing its current reference.
Reopening increments the generation. Changed evidence starts an open batch,
carrying terminal decisions only for rows with unchanged evidence/policy,
eligibility and selection bounds. Changed/new rows start pending; retracted rows
are never restored from older approval. Carried decisions retain reviewer,
notes, timestamps and their origin batch in the audit history. The new snapshot
still requires explicit completion if its queue is nonempty; an empty queue is
revalidated automatically. Legacy snapshots without row fingerprints
cannot carry decisions. Prior decisions and immutable evidence remain available.
Names are self-reported, not authenticated identities. Approval concerns record
inclusion, never execution of the stored command.

## Inspecting data, evidence and materializations

Open the relevant dataset partition and asset materialization in Dagster.
The IO manager adds `dagster/row_count`, `dagster/column_schema` and a `sample`
table to DataFrame outputs. Samples contain the first `storage.sample_rows`
records; displayed strings and serialized list/dictionary cells are truncated
to 500 characters. Persisted Parquet contains the full table. The preview schema
reports pandas dtypes; the declared Pandera model carries additional constraints.
Rules have JSON metadata instead of a table preview.

Table materializations also expose `schema`, `input_fingerprint`, `cache_hit`
and an `evidence` index. Successful computed stages retain effective config,
implementation snapshots and available inference receipts. Attempt observations
retain receipts even on failure; a failed attempt isn't a successful table
materialization. Inference receipts include hashes, requested/observed model,
timestamps and response evidence. No automatic response repair or provider
fallback supplies missing judgments. Storage integrity failures surface as errors.

CLI builds and the stopped-by-default sensor use selective planning. A first
build selects preparation stages only; later plans compare partition history and fingerprints,
select changed stages and affected descendants, and load unchanged dependencies
from storage. An unchanged CLI build reports `status: unchanged`, creates no run
and emits no new materializations. A partial selection isn't missing work by
itself; check selected/skipped stages and reasons in `build-<run-id>.json` for a
successful CLI run. Top-level revision alone doesn't request source refresh;
`seeds.revision` does.

A selected stage can reuse its cached table and still materialize with
`cache_hit: true`. That differs from a skipped stage, which has no new event.
An empty table is another distinct outcome: a successful materialization with
row count zero. The UI's explicit **Materialize all** selects the full graph;
it isn't the CLI/sensor no-op planner.

The UI and build must use the same persistent home. Use `dataset_cli ui --home`
to align the registry and Dagster history, and `dataset_cli show --home --dataset
--stage` to inspect full cell values without inference. See the exact viewing
commands in [orchestration](orchestration.md). Missing or stale display behavior
must be investigated against the actual partition, events and stored output.

## Mandatory working rule for stage changes

Every new or changed stage must maintain this guide's Input, Goal, Why, Output
and Gate/inspection description alongside its implementation. The stage must be
exposed in Dagster with an accurate asset description and dependencies, a declared
schema or typed non-table contract, inspectable data and retained evidence.
Make exclusions, quarantine, failures, empty outputs, cache reuse and selection
behavior visible rather than implying successful review or publication.

Keep previews bounded without replacing full inspectable data. Describe actual
limitations and acceptance gates, including where manual inspection is required.
The descriptions are wired into asset metadata through `stage_descriptions.py`
and `definitions.py`. A stage is not complete until its description and persisted
data have been inspected in the intended UI instance.
