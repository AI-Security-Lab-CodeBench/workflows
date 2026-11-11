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
inference. Labeling and separate agentic review assess individual records.
Agent proposals are not human approval: source use and final inclusion have
separate gates: dataset/corpus selection requires a human; original documentation
has an independent inspected-source binding. All retained final records require
human review, regardless of confidence. Conservative exact normalized
deduplication runs on the combined agent-reviewed dataset before human review.
Fuzzy linking and safe-counterpart pairing remain deferred.

The sections below describe common stage behavior and the dataset paths.
Documentation instantiates collection stages through `proposed_annotations` at
`datasets/documentation/<stage>`, except shared `category_rules` and dataset-only
`human_source_decisions`. Its collection does not wait on corpus admission.
Both branches converge before shared agentic review, deduplication, final human
review and release.
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

**Input:** Category scope and `CatalogConfig`, using either provided source
seeds plus local samples or bounded risk-specific Keenable searches and fetched
inspection text. The selector role makes structured source decisions.

**Goal:** Record which sources are selected, need review or are excluded.

**Why:** Document eligibility, origin and reuse must be considered before
extracting commands. A dangerous command isn't a reason to exclude its source;
relevant inspection commands and counterexamples also belong in scope.

**Output:** `SourceCatalogTable`, one row per returned source, with identity,
URL, licensing, attribution, revision, retrieval time, parsing hints, sample,
decision, analysis, rationale and low/medium/high confidence. Search/fetch and
selector evidence is retained separately. High source confidence never bypasses human selection.
The live catalog isn't an inventory of every search hit.

**Gate/inspection:** Provided seeds must declare accessible, permitted sources
with commands present; corpus metadata requires inspected human provenance.
Known agent, synthetic and SFT source types fail that boundary. Local hashes
are checked, samples are limited to 8,000 characters and checked for sensitive
patterns, and selector identities must match the complete supplied inventory.
These declarations don't constitute an independent license audit.

Live selection must refer to inspected pages and verbatim evidence. Its gate
excludes prohibited reuse and agent/SFT/synthetic origins; incomplete origin,
license, command or revision evidence withholds a proposal as `needs_review`.
`proposed` becomes `selected` in this asset. Search snippets aren't inspected
page evidence. Discovery failures are retained; failures with no inspected pages
fail the asset. Inspect catalog decisions and discovery receipts, not just counts.

## `datasets/human_source_decisions`

**Input:** The inspected source catalog and the completed `sources` review.

**Goal:** Materialize exactly which eligible sources the reviewer picked.

**Why:** Search results and model proposals are not permission to use a source.

**Output:** A version-bound `HumanReviewDecisionsTable` over stable source IDs.
Only approved, machine-eligible catalog rows reach acquisition. Rejected and
withheld sources remain in the catalog with their provenance and rationale.

**Gate/inspection:** Discovery stops at `awaiting_source_review`. Complete
Source selection in the native Review tab to launch the remaining preparation
assets as a native asynchronous Dagster run. Direct materialization uses the
same authorization check. Retraction makes source-dependent outputs stale.

## `datasets/source_documents`

This stage additionally depends on `human_source_decisions`. Live discovery
uses the exact retained inspected capture, not a later refetch that could differ
from what was reviewed. Provided files must still match their recorded hashes.

**Input:** The catalog and snapshot configuration, including local source seeds
when using provided sources, plus a maximum acquisition size.

**Goal:** Persist the approved source text and its actual byte hash.

**Why:** Extraction needs stable, reviewed source text rather than search
snippets. Live discovery captures are bounded by the configured fetch limit;
they are not a claim to contain the complete remote document.

**Output:** `SourceDocumentsTable`, containing selected sources only, with
`content`, SHA-256 and attribution/provenance columns. Excluded and
`needs_review` sources remain visible upstream in the catalog.

**Gate/inspection:** Local files and retained live captures must match their
catalog hashes. Live sources require nonempty captured text and its hash; this
stage does not refetch a possibly changed page. Oversized bodies, invalid UTF-8
and detected sensitive content fail acquisition rather than becoming
command-review rows. Attribution and retrieval time remain bound to the capture.

## `datasets/document_adapters`

**Input:** Source documents and adapter settings: explicit provided mappings or
the adapter model with a bounded content sample.

**Goal:** Choose how each document exposes command-bearing text and context.

**Why:** Markdown pages and structured dataset records require different
decoding before command extraction.

**Output:** `DocumentAdaptersTable`, one row per document: `source_id`, `format`,
`text_field`, `context_field` and rationale. Inferred mappings have model receipts;
provided mappings copy the document's declared format and fields.

**Gate/inspection:** Only `text`, `markdown`, `json`, `jsonl` and `csv` are
supported. Mappings are declarative, never generated executable parsers.
`text_field` supports a dot-separated dictionary path or `$` for the record
itself. `context_field` is a single dictionary key. A schema-valid mapping can
still be wrong for later records; extraction checks actual fields and types.

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

## `datasets/filtered_commands`

**Input:** Every command candidate, category command patterns and filtering
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
don't enter `human_review_queue`, whose input is reviewed annotations, not this
table. Inspect this asset separately to account for extraction/filter losses.

## `datasets/labeling_documentation`

**Input:** Eligible filtered commands, category rules, and documentation settings.

**Goal:** Collect utility/subcommand semantics and make them visible to both
inference stages.

**Why:** Labels need evidence about actual behavior, flags and version limits
rather than guesses based on command names.

**Output:** `DocumentationTable`: utility, subcommands, URL, retrieval time,
snippet and version limits.

**Gate/inspection:** Configuration and snippet models check required fields,
HTTP(S) URLs and sensitive patterns. `provided_snippets` stays offline.
`keenable` searches and inspects real pages, validates provenance and exact
excerpt grounding, and retains acquisition receipts and coverage gaps.
Semantic selection is independent of dataset-source licensing admission.
Unknown licensing or a mismatched license-evidence URL does not discard a
grounded behavior reference. Receipts mark `use_scope: semantic_reference_only`
and retain separate `reuse_review` decisions with original license metadata.
This does not authorize incorporation into a corpus or redistribution of the
evidence bundle. Explicit prohibited reuse and ungrounded/agent-generated
sources remain excluded. Dataset-source acquisition still applies its strict
license gate, including when given a documentation-discovery result.
`inspection_urls` can prioritize command, README and license references within
the same per-risk page budget; they are not uninspected evidence.

Missing or ungrounded evidence is withheld, not manufactured. Partial coverage
is recorded as partial. Coverage is conservative command-prefix matching, not a
proof of complete behavioral semantics. Both inference stages receive the table;
the final human review must still assess relevance and version limitations.

## `datasets/proposed_annotations`

**Input:** Only eligible filtered records, the documentation table, rules and
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
An accepted label must cite documentation. Exact candidate identity is also
bound in the per-record response schema. With no documentation, the schema
permits only `needs_review`/`reject` and an empty citation list; a model cannot
claim unsupported acceptance. There is no confidence threshold at this stage.

The prompt requires one sentence in the configured language and forbids invented
approval, production, backups or hidden behavior; structural validation doesn't
prove those requirements or citation relevance. Confidence is self-reported,
not calibrated proof. Invalid responses fail the stage, not a fabricated
`needs_review` label; earlier invocation evidence remains available.

## `datasets/reviewed_annotations`

**Input:** Unified proposed annotations, including rejected and withheld labels,
plus each row's original evidence, applicable documentation, rules and reviewer settings.

**Goal:** Independently check the proposed judgments while preserving disagreement.

**Why:** A valid label structure doesn't establish that its judgments are supported.

**Output:** `ReviewedAnnotationsTable`, preserving each proposal and adding
`review_verdict` (`approve`, `needs_review`, `reject`), `review_rationale` and
low/medium/high `review_confidence`. All four classifier confidence fields use
that same enum; numeric values are rejected, not converted.
Review uses separate isolated, bounded context packets; it doesn't rewrite labels.
Validated results and original receipts are cached per row's relevant review input.
New packet membership, row ordering or neighboring changes do not repeat an
unchanged row's inference. Oversized context must fail explicitly rather than
silently discarding evidence.

**Gate/inspection:** Labels are revalidated before review. Review identities must
match and rationales must be nonblank and at most 400 characters. The reviewer
sees the proposed label as well as evidence and is asked to check all choices
and confidences. Separate invocation doesn't require a different model ID or
prove unbiased review. Malformed output fails the stage with retained evidence.
Agentic approval isn't human authorization.

## `datasets/merged_proposed_annotations`

**Input:** Current authorized labeled outputs from available configured
collection branch and category configuration.

**Goal:** Form one traceable category dataset without conflating source identities.

**Why:** Collection independence must not produce separate final datasets.

**Output:** A typed proposed-annotation table retaining branch-qualified identities,
original evidence, row-specific semantic documentation and branch completeness.

**Gate/inspection:** Available authorized rows can advance through agentic review
while another branch waits; a blocked configured branch prevents final publication.
An unconfigured branch contributes no records. No prior publication is silently
substituted for current branch output.

## `datasets/deduplicated_annotations`

**Input:** The merged agent-reviewed table.

**Goal:** Remove only proven exact normalized duplicates before human review.

**Why:** Humans should review one representative of an exact duplicate, with
all original evidence still traceable.

**Output:** A typed reviewed-annotation table and duplicate-to-survivor evidence.

**Gate/inspection:** Preserve shell semantics, quoted payloads and meaningful
context/label differences. Ambiguous similarities remain separate. No fuzzy
matching, resource renaming, model-based equivalence or safe-pair gate applies.

## `datasets/dataset_release`

**Input:** Unified deduplicated annotations, completed human decisions, category rules and the per-risk publication cap
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

**Gate/inspection:** Publication checks complete matching candidate/label/review
identities and revalidates labels. This human-completion asset requires a current
completed unified review. It publishes only eligible human-approved rows.
`human_reviewed` is `true` for every released row. Accepted labels and approving independent
reviews remain mandatory; human inclusion cannot repair an unsupported label.
Human exclusions precede the cap in input order. Final validation checks nonblank fields,
column order and allowed taxonomy/class/severity values. No random sampling or
pairing is performed; exact deduplication is the preceding shared stage.
There is no fresh source licensing or
source-body audit here; consult the catalog and snapshots.

This asset persists Parquet and an immutable run-scoped CSV/JSONL/card bundle.
The IO boundary rechecks the snapshot, exact record set, completion generation
and frozen decision digest while holding the shared review transaction.
The committed SQLite publication reference is authoritative, not a mutable
object-store pointer. Root `commands.csv`/`README.md` are not replaced.
An empty release is not quota completion. Historical nine-column releases are
preserved; newly published releases use the ten-column contract.

## `datasets/human_review_queue`

**Input:** Unified deduplicated annotations, rules and the same publication cap as the release.
It precedes `human_review_decisions` and `dataset_release`.

**Goal:** Expose all retained records for mandatory human inclusion review.

**Why:** Disagreement, rejection and capacity limits must remain inspectable
instead of disappearing from the accepted-only release.

**Output:** `ReviewQueueTable`, preserving candidate, label and review columns
plus `exclusion_reason`. Machine-eligible rows have an empty reason; other rows
retain the actual label/review exclusion. Caps do not exclude rows from human review.

**Gate/inspection:** Every retained row remains visible regardless of confidence
or machine acceptance, with machine-ineligible rows unable to receive inclusion
approval. High confidence does not remove a record from this queue.
This isn't an all-pipeline error inbox: source exclusions stay in
the catalog, quarantined candidates stay in `filtered_commands`, and inference
failures stay in observations/receipts. Nothing here records human sign-off or
automatically resolves a withheld row.

## `datasets/human_review_decisions`

**Input:** The prepared queue and a completion authorization bound to the current
dataset/evidence snapshot, generation and exact decision digest.

**Goal:** Materialize the frozen human decision set separately from original labels.

**Why:** A saved or machine-approved annotation is not permission to publish.

**Output:** `HumanReviewDecisionsTable` with candidate/batch/snapshot identities,
generation and revisions, approve/reject, notes, reviewer and decision timestamp.
Parquet attributes carry completion binding even when there are no rows.

**Gate/inspection:** Open/pending reviews remain in the durable ledger and Review
tab; this asset materializes only in `release_dataset`. Every expected record
must be present and terminal. Approval cannot override machine eligibility.
The IO boundary revalidates authorization before advancing its current reference.
Reopening increments the generation. Changed evidence starts an open batch,
carrying terminal decisions only for rows with unchanged evidence/policy,
eligibility and selection bounds. Changed/new rows start pending; retracted rows
are never restored from older approval. Carried decisions retain reviewer,
notes, timestamps and their origin batch in the audit history. The new snapshot
still requires explicit completion. Legacy snapshots without row fingerprints
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
