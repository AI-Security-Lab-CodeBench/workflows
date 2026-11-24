# Loading pinned train and test splits

`train` and `test` are optional, independent sections of the registered global
configuration. Neither is part of collection, labeling, review or release.
For each split the graph is:

```text
datasets/global_config → map_<split>_load_config   → <split>/category_rows
                       → map_<split>_counts_config → <split>/stats ← category_rows
                       → map_<split>_merge_config  → <split>/merged_rows  ← category_rows (all configured categories)
```

The three mapping keys are under `datasets/`. Category rows and counts are
partitioned by registration and **category**, not by risk. Multiple risk
branches for `CAT-2`, for example, all enter a single `CAT-2` partition.
Each merged table has just the registration partition.

**Reading the lineage color:** the native graph counts every registered dataset
key, including datasets with no split sources. At the category level it counts
the Cartesian product of dataset and category keys. A partially materialized
node therefore does not, by itself, mean a configured split is missing. In the
Partitions tab, select the dataset and compare its materializations with the
categories declared for that split. Do not backfill unrelated registrations to
change the aggregate color.

### Declare the inputs

Each section has a `sources` array and an optional `group_by` array (default
`["risk", "severity", "class"]`). Each source names the GitHub `repository`
(`owner/repo`), explicit `branch`, full 40-character `commit`, repository-relative
CSV `path`, stable `category` (`CAT-N`) and the 64-character `sha256` of the
exact file bytes. Branch is provenance: the loader requests **the commit**, never
the moving branch head. Update both pins deliberately when adopting a new revision.
The loader does not discover branches, run repository scripts, or execute commands.

Put the sections directly in the dataset bundle, or keep a private JSON file with
just the two sections. A private manifest keeps non-public policy and data out
of Git. The following commands operate inside the **existing Compose stack**;
they do not launch local workers. Start with a registered dataset. The configure
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
```

Pass `--dataset <registration-id>` to any of these commands to select a
non-default registration. Reapply the private manifest if another operation
replaces the registration with a bundle that omits the sections.

### What is preserved

CSV headers must be `command,description,util,severity,category,risk,source,seed,class`,
in that order. Input bytes must match their digest. All nine values remain
strings; source order, record order, duplicate rows, and empty strings are
preserved. A row must name the declared category, optionally followed by a
display description. A header-only file is a valid empty table; a missing file,
bad digest, or malformed row is not an empty result.

`stats` replaces `label_counts`. Its Pydantic JSON output contains `total_rows`
and `marginal_counts`, an ordered list of `{category, label, value, count}`
objects. There is one histogram per selected label, **not** a joint histogram
of risk × severity × class. Each histogram sums to `total_rows`. A header-only
category produces `{"total_rows": 0, "marginal_counts": []}`. Open
`/assets/train/stats?view=data` or `/assets/test/stats?view=data` and select a
persisted category partition to inspect its values and declared schema.
To materialize just the new stats from an existing category table, use
`python3 scripts/stack.py train-load CAT-6 --dataset <registration-id> --stats-only`
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
