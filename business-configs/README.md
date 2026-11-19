# Editing business configuration

This directory holds the human-editable labeling policy, runtime templates and
reference declarations. Edit the files below instead of copying policy into
Python, runtime snapshots or a second category configuration.

For CAT-3, start at `datasets/cat3.json`. This template selects the category by
ID. The loader resolves the category policy and each branch declaration by
convention. Branches create file inventories and captures at run time; source
bodies and documentation snippets do not belong in the active configuration.
`datasets/cat3-prepared.json` preserves the prepared manifest references used by
historical builds.

| File | What to edit |
| --- | --- |
| `labeling/policy.json` | Shared class and severity definitions and output description language. |
| `labeling/instructions.md` | Shared annotation method, evidence authority, output discipline and confidence semantics. Keep domain rules out. |
| `categories/cat3/category.json` | CAT-3 identity, risk taxonomy, routing patterns and concise risk guidance under `rules`. An optional `allowed_severities` list may restrict the shared severity names; omit it to allow all. |
| `categories/cat3/instructions.md` | CAT-3 membership, benign-family scope, risk boundaries and defaults for material operations. Change the general rule rather than adding one exception per tool or flag. |
| `categories/cat3/examples.json` | A small bank of documented demonstrations grouped by risk and operation family. Update expected labels when a business rule changes. |
| `datasets/cat3.json` | Dataset and category IDs, labeling settings, publication settings and storage. |
| `categories/cat3/collection/existing-datasets.json` | Pinned repository corpora and their acquisition settings. |
| `categories/cat3/collection/documentation-crawl.json` | Pinned original documentation repositories and their acquisition settings. |
| `categories/cat3/keenable/declaration.json` | Discovery generation, reviewed source scopes, rights evidence, reference URLs, cases and budgets. |
| `categories/cat3/reference-examples/sources.jsonl` | The relocated, pinned historical source declaration, kept only as a reference example for source preparation. |
| `categories/cat3/reference-examples/semantics.jsonl` | The relocated historical Terraform semantic snippets, kept as reference examples rather than a complete current semantic manifest. |

The logical `CAT-3` ID resolves to `categories/cat3/category.json` under this
directory. The loader finds `instructions.md` and `examples.json` next to the
category file and loads the two shared files from `labeling/` under the same
business configuration root. Category declarations carry no file references.

## Reading the dataset configuration

`datasets/cat3.json` is a starting template. Register the template to change a
dataset's active configuration. Its logical category selects the three
source-driven branches; there is no pipeline version to choose.

Read the dataset file from top to bottom:

| Field | Meaning |
| --- | --- |
| `dataset_id` | Dataset partition to configure. The Keenable declaration's `dataset` must match. |
| `category_id` | `CAT-3` selects the category policy and independently loads the corpus, documentation and Keenable declarations. |
| `revision` | Identifier for this dataset configuration. |
| `finalization.annotation` | Model, context limit, parallelism and guarded batch settings. `{"mode":"registered"}` reuses existing registration settings. |
| `finalization.filtering` | Optional routing and candidate filtering settings. |
| `finalization.publication` | Optional review and release settings. |
| `finalization.keenable_source_weight` | Optional merge weight for Keenable candidates; defaults to `0.30`. |
| `storage` | Artifact storage; `null` uses the dataset home's local storage. |

The two repository branch files use the same field names and contain only the
origins for their own branch:

| Field | Meaning |
| --- | --- |
| `revision` | Reviewed revision of the branch declaration. |
| `origins` | Nonempty list of origins. Each declares `origin_id`, `repository`, `commit`, `tree_digest`, `scope`, `license_id`, `license`, `attribution`, `source_type`, `provenance_type`, `format`, `use`, and `transform`. `included_paths`, `excluded_paths`, and `notices` are optional. Scope paths and rights paths identify files in the pinned upstream repository. |
| `origins[].transform` | Reviewed `parser_id` (`cheatsheets_front_matter`, `tldr`, `aws_cli_examples`, or `markdown_shell`), pinned `sha256`, optional `options_json` (a JSON object encoded as text), and optional `batch_size` (default `128`). Deployment code checks the pin for that ID and supplies the module and entrypoint. |
| `source_weights` | Optional overrides by origin ID in this branch. Default weights are `0.60` for a corpus and `0.90` for documentation. All weights must be finite numbers from 0 to 1. Every documentation origin must outrank every corpus, and every corpus must outrank Keenable. |
| `maximum_file_bytes`, `workers` | Optional per-branch capture size limit (default `524288`, range 1 to 8388608) and download concurrency (default `12`, range 1 to 24). |

The Keenable branch keeps its own declaration with `dataset`, `revision`,
`generation`, `generation_review`, and `cases`. Its current source-scoped
format also defines `sources`, `budget` and `search_plan`. `sources` identifies
permitted origins and rights. `budget` bounds requests and bytes; `search_plan`
selects discovery behavior within reviewed scopes. These collection settings
are separate from label-time remote lookup.

Register a new configuration after changing a declaration. Registration
resolves policy and stores the branch declarations and their digests without
local source-list or parser paths. Historical declared snapshots remain
readable. Re-register from the current template to produce current branch
settings from a registration that still contains source-list paths.
Historical inference snapshots and review decisions are not rewritten.

Change runtime settings through the normal registration/configuration workflow.
Editing the template alone doesn't reconfigure an already registered dataset.
The core pipeline owns policy invalidation, cached-result reuse and review gates.
Don't change confidence merely to reduce the human queue.

## Command embeddings on labeling workers

Provision the CPU encoder before running labeling. Use the `tokenizer.json`
and `onnx/model_quantized.onnx` files from `Xenova/all-MiniLM-L6-v2` at
revision `751bff37182d3f1213fa05d7196b954e230abad9`. Keep those relative
paths under one directory and verify their SHA-256 hashes:

| File | SHA-256 |
| --- | --- |
| `tokenizer.json` | `da0e79933b9ed51798a3ae27893d3c5fa4a201126cef75586296df9b4d2c62a0` |
| `onnx/model_quantized.onnx` | `afdb6f1a0e45b715d0bb9b11772f032c399babd23bfc31fed1c170afc848bdb1` |

Install the locked runtime dependencies and mount the directory read-only in
the worker that executes labeling. Set `CODEBENCH_COMMAND_ENCODER_DIR` to
the directory **inside that worker**. The encoder groups pending commands;
it does not index documents or supply citations. Changing batch membership
does not by itself invalidate an otherwise reusable row label.

## Local inputs and source preparation

The historical prepared template declares three companion config documents. They are
separate so the main stage settings remain readable:

- `manifests/cat3/datasets.json`
- `manifests/cat3/documentation-sources.json`
- `manifests/cat3/semantics.json`

Prepare actual source and semantic manifests through the source-preparation
workflow, with provenance, reuse eligibility, exact positions, integrity hashes,
version limits and reviewed transformation evidence. Don't create empty
placeholders to hide missing inputs or substitute a stale manifest.
Keep collected records, downloaded captures, model receipts, review decisions
and dataset output in the local dataset home or another approved untracked
data location, not among published business files.

Semantic snippets identify real operation paths or flags. Use `<default>` only
for the exact invocation of a utility without arguments, never as a wildcard
for all of its subcommands or flags. Missing operation documentation blocks
labeling; demonstrations do not fill a target's evidence gap.

The two files in `reference-examples/` aren't selected by the active template.
They preserve a small historical declaration and snippets for inspection, not a
full source inventory, fresh eligibility decision or replacement for current
candidate documentation. The source example's `local_path` now resolves to
`inputs/cat3/reference-captures/terraform-destroy.md` under this directory.
That capture is deliberately not shipped or generated. Before using the example,
acquire the pinned source through source preparation and verify its declared
hash and reuse evidence; reference snippets retain their original retrieval dates.

## Editing demonstrations

The example file is an array of operation families:
`{risk_id, operation_family, examples}`. Each example contains a full
`CandidateRecord` in `candidate`, one or more full `SemanticSnippet` records in
`documentation`, and a full `LabelDecision` in `expected`. Use the model field
names exactly, including `class` in the JSON label.

There are 13 demonstrations across six families: infrastructure teardown and
validation, namespace removal and inventory, container cleanup and disk usage,
cloud-object deletion and listing, SQL table destruction and reads, and
production image rollout and status. Each risk has a material example and an
independently benign example; CAT-3.4 covers both storage and SQL.
The deployment family also contrasts explicit production context with an
otherwise identical operation whose environment and impact are unresolved.

Candidate IDs and source identities are illustrative. `example.org` URLs mark
synthetic training fixtures, and `transformations` explicitly records authored
demonstration provenance. These examples must never enter dataset acquisition,
counts, exports, deduplication inputs or production review decisions.
Commands remain inert, including commands in examples.

Use candidate-specific official documentation snippets, paraphrase their
semantics and record retrieval dates and version limitations. Expected
`documentation_urls` must be supported by that example's relevant snippets.
At runtime, examples are selected by the target's allowed risk IDs.
Example documentation never expands the target's allowed citations or replaces
the target's own semantic documentation.

Keep the bank bounded and teach contrasts that transfer across tools. Don't add
a case for every command spelling or flag. One well-chosen evidence gap is useful
only when it teaches a decision the existing cases don't cover.

## Checking a policy edit

Check JSON syntax and typed model validation, unique candidate and family
identities, allowed routing IDs, matching operation documentation, allowed
severity names and citation membership. Keep rationales within 400 characters and
descriptions in the configured language. Every accepted example must remain
independently supported by its own evidence.

Labels carry four ordinal confidences, category, risk, severity and class,
each `low`, `medium` or `high`. They're self-reported label certainty, not
calibrated probabilities or harm severity. A default can justify high label
confidence without proving an environment safe. A material fact that could
change a plausible label must still be acknowledged.

Run the repository's normal checks through the core integration workflow after
policy or schema changes. Static validation never proves live cloud, cluster,
container or database behavior, and no example command should be executed as a
test.
