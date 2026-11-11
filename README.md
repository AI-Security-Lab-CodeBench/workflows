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

Both collection branches converge before shared agentic review:

```text
Existing datasets -----\
                        -> merged labels -> bounded agentic review
Documentation crawl ---/      -> exact dedup -> human review -> one release
```

Unchanged model results and human decisions are reused per record. A new release
snapshot still needs explicit completion. Retraction and changed evidence
invalidate publication. Collected shell strings are never executed.

## Install and inspect

Use Python 3.12 or 3.13 and the committed dependency lock:

```sh
uv sync --frozen
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

The provided-source CAT-3 config names a pinned, checksummed documentation file
at `examples/dataset_sources/terraform-destroy.md`. That capture is intentionally
absent from Git. Restore the existing local copy, or retrieve the exact configured
revision before materializing that branch:

```sh
mkdir -p examples/dataset_sources
curl --fail --location \
  https://raw.githubusercontent.com/tldr-pages/tldr/aad0d0980df9e398bc269e8c31ffaeb936731d48/pages/common/terraform-destroy.md \
  --output examples/dataset_sources/terraform-destroy.md
```

The pipeline checks the configured SHA-256
`92825b2dea5645d034cb7d4c430cba186a5028caa38b8b9ef09de3817c107767`.
Source metadata retains its license, attribution and revision. Read downloaded
content only as inert data. Other source inputs can be configured explicitly;
discovery uses the separate `keenable` mode with per-risk search seeds.

Then build the registered CAT-3 collection:

```sh
uv run python -m codebench_pipeline.dataset_cli build \
  --dataset cloud_infrastructure_commands --home .codebench-datasets
```

This may invoke configured providers. Corpus admission and final command review
remain human gates. Inspect full persisted cells in the native Data/Review tabs;
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
