# Editing business configuration

This directory holds the human-editable labeling policy, runtime templates and
reference declarations. Edit the files below instead of copying policy into
Python, runtime snapshots or a second category configuration.

| File | What to edit |
| --- | --- |
| `labeling-base.md` | Category-independent annotation method, evidence authority, output discipline and confidence semantics. Keep domain rules out. |
| `categories/cat3/category.json` | Current CAT-3 names, IDs, class and severity definitions, routing patterns, concise risk guidance and references to the three labeling files. Its `rules` object has no inline `policy`. |
| `categories/cat3/instructions.md` | CAT-3 membership, benign-family scope, risk boundaries, severity defaults and v0 execution-class policy. Change the general rule rather than adding one exception per tool or flag. |
| `categories/cat3/examples.json` | A small bank of documented demonstrations grouped by risk and operation family. Update expected labels when a business rule changes. |
| `datasets/cat3.json` | Initial runtime settings: model, four labeling workers, source and semantic manifests, extraction/filter limits and publication cap. |
| `categories/cat3/reference-examples/sources.jsonl` | The relocated, pinned historical source declaration, kept only as a reference example for source preparation. |
| `categories/cat3/reference-examples/semantics.jsonl` | The relocated historical Terraform semantic snippets, kept as reference examples rather than a complete current semantic manifest. |

All paths resolve relative to the file that declares them. The dataset template
binds `../categories/cat3/category.json`. That category file binds the shared
`../../labeling-base.md`, its local `instructions.md` and its local `examples.json`.
Preserve these references when moving files.

## Runtime template versus live policy

`datasets/cat3.json` is a starting template, not an instruction to overwrite a
registered dataset's settings. It retains the current native labeler,
`chatgpt-subscription/gpt-6-luna`, its 900-second timeout, four workers, the existing
limits and the configured source branches.

A registered dataset bound to a business configuration refreshes that policy
when loaded for current work. Editing the bound category, base prompt,
instructions or examples changes the compiled labeling input without replacing
the registered model, workers, caps or source settings. Historical inference
snapshots remain immutable; a policy edit doesn't rewrite old receipts or
constitute human approval. Missing or invalid policy files fail explicitly.

Change runtime settings through the normal registration/configuration workflow.
Editing the template alone doesn't reconfigure an already registered dataset.
The core pipeline owns policy invalidation, cached-result reuse and review gates.
Don't change confidence merely to reduce the human queue.

## Local inputs and source preparation

`inputs/` under this directory is local-only and ignored. The runtime template
expects these prepared manifests, none of which is shipped here:

- `inputs/cat3/datasets.json`
- `inputs/cat3/documentation-sources.json`
- `inputs/cat3/semantics.json`

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

The two files in `reference-examples/` aren't selected by the runtime template.
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
identities, allowed routing IDs, matching operation documentation, relative
references and citation membership. Keep rationales within 400 characters and
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
