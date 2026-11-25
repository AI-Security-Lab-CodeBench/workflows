# Loading pinned train and test splits

`train` and `test` are optional, independent sections of the registered global
configuration. Neither is part of collection, labeling, review or release.
For each split the graph is:

```text
datasets/global_config → map_<split>_load_config   → <split>/category_rows
                       → map_<split>_counts_config → <split>/stats ← category_rows
                       → map_<split>_merge_config  → <split>/merged_rows  ← category_rows (all configured categories)
```

CodeBench is one dataset (`codebench`). Each split has its own configured
category partitions, named `CAT-N`. The three mapping keys are under `datasets/`.
Other registrations do not contribute categories or add a dataset axis to the
split graph. Risk labels are data, not partition keys.

**Full datasets for downstream jobs:** use `train/merged_rows` for training
and `test/merged_rows` for held-out evaluation. Each is a single typed table
at the `codebench` key, with **no category dimension**. Consumers do not need
to enumerate categories. Each merge requires every configured category in its
own split, rejects missing or stale inputs, and never depends on the other split.
The Data tab shows the complete split row count and allows paging its rows.

`codebench/full_dataset` is the downstream, model-facing dataset. It bundles
the two completed merges as **separate named members**, `train` and `test`.
Build it with `python3 scripts/stack.py full-dataset` after both merges.
In a downstream Python asset taking a `FullDatasetBundle`, call
`bundle.train.load()` for fitting and `bundle.test.load()` for held-out
evaluation. Each call returns a schema-validated DataFrame from that member's
immutable Parquet reference; publishing the bundle does not duplicate the rows.
For a Dagster consumer, declare the input as
`AssetIn(key=FULL_DATASET_KEY, dagster_type=FULL_DATASET_TYPE)` and annotate
the argument `bundle: FullDatasetBundle`. All three names come from
`codebench_pipeline.full_dataset`.
The Data tab displays each member's row count, reference and producing run.
Retain those objects when retaining a bundle. If the registration changes,
refresh the affected categories and both merge receipts before republishing.

A green partitioned node means Dagster recorded materializations for its
configured keys. Changing a source pin on an existing key still requires a new
load of that category before merging. Do not seed status with synthetic events.

### Declare the inputs

Each section has a `sources` array and an optional `group_by` array (default
`["risk", "severity", "class"]`). Each source names the GitHub `repository`
(`owner/repo`), explicit `branch`, full 40-character `commit`, repository-relative
CSV `path`, and the 64-character `sha256` of the exact file bytes. Use `category`
(`CAT-N`) for a single-category CSV, or use `categories` (a nonempty array of
distinct `CAT-N` IDs) for a mixed CSV. Do not set both or declare the same file
once per category. A mixed source creates one partition per declared category
and retains the full file pin and category list in each partition's provenance.
Branch is provenance: the loader requests **the commit**, never
the moving branch head. Update both pins deliberately when adopting a new revision.
The loader does not discover branches, run repository scripts, or execute commands.

Put the sections directly in the dataset bundle, or keep a private JSON file with
just the two sections. A private manifest keeps non-public policy and data out
of Git. For the v0 synthetic experiment, replace `train.sources` with this
single entry and retain the currently registered `test` section unchanged:

```json
{
  "repository": "SfBalaba/code_guard_benchmark",
  "branch": "feat/generation-pipeline",
  "commit": "93c90562accccf1b77740c2b2681c46000e6f30d",
  "path": "generation-pipeline/runs/v0/dataset.csv",
  "sha256": "065e72c6fed7c1e53841a2a35b90095404ca4b9aee6f74691b2008552c48c2b6",
  "categories": ["CAT-1", "CAT-5"]
}
```

This pin has 274 CAT-1 rows and 612 CAT-5 rows; after loading both
categories the merged train table should contain 886 rows.
The following commands operate inside the **existing Compose stack**;
they do not launch local workers. Start with the registered `codebench` configuration. The configure
command replaces both sections, retains the other registered settings, and
materializes `datasets/global_config`. As with any registration change, use the
normal review workflow if the original dataset has an active review snapshot.

```sh
python3 scripts/stack.py configure-splits /private/path/pinned-splits.json
python3 scripts/stack.py train-load CAT-6
python3 scripts/stack.py train-merge

# For a test manifest containing these six categories:
for category in CAT-1 CAT-2 CAT-4 CAT-5 CAT-6 CAT-7; do
  python3 scripts/stack.py test-load "$category" || exit 1
done
python3 scripts/stack.py test-merge
python3 scripts/stack.py full-dataset
```

The split commands target `codebench`. Reapply the private manifest if another operation
replaces the registration with a bundle that omits the sections.

### What is preserved

CSV headers must be `command,description,util,severity,category,risk,source,seed,class`,
in that order. Input bytes must match their digest. All nine values remain
strings; source order, record order, duplicate rows, and empty strings are
preserved within a category partition. A row must name a declared category,
optionally followed by a display description. For a mixed source, the loader
validates the entire file before returning only rows for the current partition;
an undeclared category fails the load even if its row would not be selected.
A header-only file is a valid empty table; a missing file,
bad digest, or malformed row is not an empty result.

`stats` replaces `label_counts`. Its Pydantic JSON output contains `total_rows`
and `marginal_counts`, an ordered list of `{category, label, value, count}`
objects. There is one histogram per selected label, **not** a joint histogram
of risk × severity × class. Each histogram sums to `total_rows`. A header-only
category produces `{"total_rows": 0, "marginal_counts": []}`. Open
`/assets/train/stats?view=data` or `/assets/test/stats?view=data` and select a
persisted category partition to inspect its values and declared schema.
To materialize just the new stats from an existing category table, use
`python3 scripts/stack.py train-load CAT-6 --stats-only`
(or `test-load`). This does not fetch CSVs or rewrite the category or merged tables.

`category_rows` and `merged_rows` have the same strict nine-string-column
schema; the merge validates the category tables it consumes. `merged_rows` concatenates categories
in lexical category-ID order. It requires a materialized table matching the
current source list for **every** configured category; a removed category is not
included. Tables use the registration's existing Parquet object storage.
The output retains its source pins in the table metadata. Replaying a snapshot
requires the pinned blobs to remain available.

The same CSV digest cannot be declared in both splits. This is file-level
protection, not a near-duplicate detector or proof of holdout independence.
Choose the split from upstream documentation, not the branch prefix: a
`feat/data_*` branch may contain held-out examples, historical copies, or
unlabeled data in addition to the intended CSV.
