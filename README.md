# CodeBench workflows

Code-only workflows for CAT-3 cloud infrastructure and database command analysis.
This repository contains no collected dataset, source captures, review ledger or
published command table. Those inputs and outputs remain local and ignored.
CAT-2 is a local multicategory example and is deliberately not shipped.

## Components

- `codebench_pipeline/`: typed Dagster collection, labeling, review and publication.
- `dagster_ui/`: native Dagster Data/Review panels and a pinned frontend builder.
- `scripts/`: reusable inert extraction, filtering, annotation and CSV tools.
- `configs/`: CAT-3 category, dataset, source-metadata and semantic-reference settings.
- `tests/`: self-contained regression tests using authored synthetic fixtures.
- `bashguard/`: standalone Go Bash AST analysis and CEL policy evaluation,
  integrated from source commit `671172026a2c0f4f6ce420f5e7bef10f53fb0732`.

Both collection branches feed one shared processing flow:

```text
Existing datasets: source collection -> command_candidates
Documentation crawl: source collection -> command_candidates
Both -> merged_command_candidates -> filtered_commands -> labeling_documentation
     -> proposed_annotations -> conservative exact dedup
     -> uncertain human queue -> one dataset_release
```

The local agent discovers sources, reads README/license/origin evidence and
prepares category-bound manifests outside Dagster. Unresolved eligibility stays
explicitly withheld in the local inventory. Both branches retrieve complete
declared files with integrity checks; neither searches for sources nor calls
source-selector or adapter models.
Each eligible command gets one isolated LLM labeling invocation with semantic docs.
There is no independent per-command LLM reviewer stage; historical
`reviewed_annotations` artifacts remain readable. Current rows don't receive
manufactured `review_verdict` or `review_confidence` fields.
After exact deduplication, any medium/low confidence among category, risk, severity,
and class requires final human review. All four
high means low uncertainty, not proof of eligibility.

Machine-eligible low-uncertainty rows need no human decision and carry
`human_reviewed=false`. Actually approved uncertain rows carry `human_reviewed=true`;
approval never overrides machine ineligibility. An empty queue lets the build
complete under a policy actor with zero human decisions and publish automatically
through the same `dataset_release`. Otherwise the single release waits for real
terminal queue decisions and explicit
completion. Unchanged model results and human decisions are reused per record.
Changed snapshots invalidate publication and require fresh completion when the
queue is nonempty. Collected shell strings are never executed.

## Install and inspect

Use Python 3.12 or 3.13 and the committed dependency lock:

```sh
uv sync --frozen
```

Prepare the ignored business inputs described below before validating or registering:

```sh
uv run python -m codebench_pipeline.dataset_cli validate \
  --config configs/datasets/cat3.json
uv run python -m codebench_pipeline.dataset_cli register \
  --config configs/datasets/cat3.json --home .codebench-datasets
```

Validation and registration do not run inference or collect commands. A fresh
home has no materialized tables. The supplied config preserves the user's model
roles and CAT-3 policy. Its native provider requires an authenticated `omo`
installation, optionally selected with `CODEBENCH_OMO_EXECUTABLE`. Alternatively,
configure each role's supported `openai_compatible` provider and endpoint;
credentials are referenced by environment-variable name, never stored here.

Build the native UI using Node >=20 and the builder's pinned Dagster/Yarn inputs:

```sh
uv run python -m codebench_pipeline.ui_build
uv run python -m codebench_pipeline.dataset_cli ui \
  --home .codebench-datasets --port 3020
```

Open `http://127.0.0.1:3020/asset-groups` for lineage. The builder needs network
access and disk space; generated frontend files are not versioned.
See `dagster_ui/BUILD.md` for exact pins and build ownership.

To inspect an existing local dataset, pass its existing home instead. Do not
register the example config over an existing registered collection merely to
view it. Build and viewer must use the same home. When reusing an already-built
bundle, the server also accepts `--bundle /path/to/dagster_ui/build`.

## Local source inputs

Use `skills/codebench-source-preparation/SKILL.md` with the requested category.
The shipped CAT-3 config expects three ignored files under `inputs/cat3/`:

- `datasets.json`: source manifest for existing corpora.
- `documentation-sources.json`: source manifest for original documentation.
- `semantics.json`: prepared operation-level references for labeling.

Keep `inventory.json` alongside them with source assessments, complete enumeration,
exclusions and preparation evidence. These business inputs are not shipped in Git.
Each source has a local path or exact remote URL, provenance, license, declared
format and SHA-256 or Git blob SHA-1. Remote acquisition rejects incomplete,
oversized, mismatched, non-UTF8 or sensitive bodies.

Optional trusted Python functions/classes implement the batch contract in
`codebench_pipeline/source_transform_contract.py`. Pin reviewed module bytes in
the source manifest; examples live in `transforms/`. Dataset strings are never
executed. Changing manifest, source or transformation bytes invalidates outputs.

Then build the registered CAT-3 collection:

```sh
uv run python -m codebench_pipeline.dataset_cli build \
  --dataset cloud_infrastructure_commands --home .codebench-datasets
```

This invokes the labeler for missing eligible rows. Declared sources load automatically; only
the final uncertainty queue requires human decisions. Inspect full persisted cells
in the native Data/Review tabs;
viewing does not require running inference again.

## Verify

```sh
uv run python -B -m unittest discover -s tests -v
uv run basedpyright
```

The tests do not require CAT-2 config, collected captures, cloud clients or model
credentials. Frontend dependency/bootstrap and test commands are in
`dagster_ui/INTEGRATION.md`. BashGuard has its own dependency lock, tests and inert
CLI examples in `bashguard/README.md`.

## Publication boundary

Keep source, tests, dependency locks, workflow documentation and CAT-3 settings
in Git. Do not force-add local datasets, captures, SQLite/Parquet stores, model
receipts, human decisions, generated reports, binaries or secrets. Synthetic
test fixtures are code verification inputs, not collected/releasable records.

This is a fresh `master` history, not a merge of earlier data-bearing branches.
The original working checkouts, CAT-2 example and existing local data are preserved
separately. See `AGENTS.md`, `docs/pipeline-stages.md`,
`docs/category-configuration.md` and `docs/reusable-review.md`.
