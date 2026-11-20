# Editing business configuration

This directory holds the human-editable labeling policy, runtime templates and
reference declarations. Edit the files below instead of copying policy into
Python, runtime snapshots or a second category configuration.

For CAT-3, start at `datasets/cat3.json`. This template selects the category by
ID. The loader resolves the category policy and each branch declaration by
convention. Branches create file inventories and captures at run time; source
bodies and documentation snippets do not belong in the active configuration.

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
| `dataset_id` | Dataset partition to configure. The Keenable declaration's `dataset_id` must match. |
| `category_id` | `CAT-3` selects the category policy and independently loads the corpus, documentation and Keenable declarations. |
| `revision_label` | Identifier for this dataset configuration. |
| `labeling` | Model, request-byte bound, workers and guarded batch settings. `{"from_registration":true}` inherits existing settings. |
| `filtering` | Optional routing and candidate filtering settings. |
| `review.review_columns` | Columns used for review edits and decision reuse. |
| `release.release_cap_per_risk` | Cap applied after eligibility and required approval. |
| `merge.keenable_weight` | Optional merge weight for Keenable candidates; defaults to `0.30`. |
| `storage` | Required. Declare `backend: "s3"` and `bucket`, or use `{"from_registration":true}` to inherit an existing S3 registration. |

The canonical CAT-3 template inherits its deployment's S3 configuration. If
the existing registration uses local storage, supply explicit S3 settings
in the new declaration. Set `endpoint_url` for an S3-compatible endpoint,
or omit it to use the SDK's endpoint selection. Credentials, when specified,
are named by `access_key_env` and `secret_key_env`, not stored in the declaration.
Changing storage does not copy existing artifacts or rewrite prior registrations.

The repository branch files contain only their own origins and settings.
See [repository acquisition](categories/cat3/collection/README.md) for pinned
trees, rights evidence, file selectors, parser authorization and capture limits.

The [Keenable declaration](categories/cat3/keenable/README.md) uses
`schema_version`, `dataset_id`, `generation`, `sources`, `budget` and
`search_plan`. `sources` identifies permitted origins, cases and rights.
`budget` bounds acquisition and `search_plan` selects discovery behavior within
reviewed scopes. The labeler uses verified captures from that generation.

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

## Historical CAT-3 evidence

The retired prepared-source registration used these original inputs:

- `manifests/cat3/datasets.json`
- `manifests/cat3/documentation-sources.json`
- `manifests/cat3/semantics.json`

The two files in `categories/cat3/reference-examples/` likewise preserve
historical provenance and snippets. The referenced local capture is not shipped.
Keep the original bytes, paths and hashes intact. These files do not configure
the current collection or supply current labeling evidence.

Use the [source preparation skill](../skills/codebench-source-preparation/SKILL.md)
for new declarations. Keep downloaded captures, new inventories, model receipts,
review decisions and dataset outputs outside published business files.

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
