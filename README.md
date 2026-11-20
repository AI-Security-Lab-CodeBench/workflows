# CodeBench workflows

Code-only workflows for CAT-3 cloud infrastructure and database command analysis.
This repository contains no collected dataset, source captures, review ledger or
published command table. Those inputs and outputs remain local and ignored.
CAT-2 is a local multicategory example and is deliberately not shipped.

## Development checkout

Use `CodeBench-workflows` as the main working directory and `master` as the only
development branch. The remote is
`git@github.com:AI-Security-Lab-CodeBench/workflows.git`.
The legacy sibling `CodeBench` is local archival material, not a source checkout
for new changes. See `AGENTS.md` for verification, automatic push after authorized
commits, and the policy against co-author trailers.

## Components

- `codebench_pipeline/`: typed Dagster collection, labeling, review and publication.
- `dagster_ui/`: native Dagster Data/Review panels and a pinned frontend builder.
- `scripts/`: reusable inert extraction, filtering, annotation and CSV tools.
- `business-configs/`: human-editable base prompt, category policy, examples and dataset settings.
- `tests/`: self-contained regression tests using authored synthetic fixtures.
- `bashguard/`: standalone Go Bash AST analysis and CEL policy evaluation,
  integrated from source commit `671172026a2c0f4f6ce420f5e7bef10f53fb0732`.

Three independently selectable collection branches feed one shared processing flow:

```text
Existing datasets: pinned corpus inventory -> verified captures -> command_candidates
Documentation crawl: pinned scope and verified captures -> command_candidates
Keenable: bounded discovery -> verified origin captures -> command_candidates
All -> merged_command_candidates -> filtered_commands -> labeling_attempts
    -> proposed_annotations -> conservative exact dedup -> category eligibility
    -> uncertainty queue -> dataset_release
       labeling_attempts -> labeling_failures (audit only)
```

The repository branches acquire from reviewed pins; only Keenable searches for
new origins. Guarded batch labeling uses applicable passages from the same
verified captures and preserves independent row judgments. Unsupported outcomes
remain inspectable in `labeling_failures`; quota deferrals are tracked separately.
There is no independent per-command LLM reviewer stage; historical
`reviewed_annotations` artifacts remain readable. Current rows don't receive
manufactured `review_verdict` or `review_confidence` fields.
After exact deduplication, any medium/low confidence among category, risk, severity,
and class requires final human review. All four
high means low uncertainty, not proof of eligibility.

Machine-eligible low-uncertainty rows need no human decision and carry
`human_reviewed=false`. Actually approved uncertain rows carry `human_reviewed=true`;
approval never overrides machine ineligibility. An empty queue completes under
a policy actor with zero human decisions. A nonempty queue requires real terminal
decisions and explicit completion. Both paths use the same `dataset_release`
and remain subject to guarded publication controls. Unchanged model results
and human decisions are reused per record.
Changed snapshots invalidate publication and require fresh completion when the
queue is nonempty. Collected shell strings are never executed.

## Operate the shared deployment

Run `make remote-up` and open `http://localhost:3041`. Follow the
[operator guide](docs/orchestration.md) to inspect the selected dataset
partition, validate and apply a declaration, or run preparation and labeling
inside the existing Compose service. Viewing persisted rows does not require
registration, inference or rematerialization.

## CAT-3 configuration inputs

Use `skills/codebench-source-preparation/SKILL.md` with the requested category.
The CAT-3 template selects the category. Its collection inputs are maintained
separately: `collection/existing-datasets.json`, `collection/documentation-crawl.json`
and `keenable/declaration.json` under the category directory. The loader
embeds their validated contents in the configuration assets.
See [business configuration](business-configs/README.md) for declaration fields
and retained historical evidence. Keep new inventories, captures and run
receipts in approved local storage.

## Verify

Run these checks in the [Compose test environment](docs/test-suite.md):

```sh
make lint
make test-suite
```

`make lint` runs Ruff and strict basedpyright against the pipeline using the
locked development dependencies. It does not run or materialize assets.
`make test-suite` requires lint to pass first. The `Python quality / lint`
workflow runs the same gate on pull requests and pushes to `master`.

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
