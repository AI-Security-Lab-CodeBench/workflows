# Independent collection, unified dataset

The current topology requires automatic eligible-source collection in independent
branches, followed by unified post-extraction processing and uncertainty-only final human review.
Earlier branch-specific releases, human corpus admission, independent per-command
LLM review and all-row human review
are superseded. Historical outputs and decisions remain evidence, not current
authorization for the combined dataset.

## Graph and admission

```text
                        category_rules
                         /           \
           Existing datasets       Documentation crawl
             manifest validation     manifest validation
             eligibility binding     eligibility binding
             full corpus collection  full corpus collection
             adapters                adapters
             extraction              extraction
                         \           /
                    merged_command_candidates
                    filtered_commands
                    labeling_documentation
                    proposed_annotations
                    exact normalized deduplication
                    uncertainty-only human review queue
                    real decisions if queue nonempty
                    one dataset release
```

`seeds` configures corpus inputs, `documentation_sources` configures original
documentation/forum inputs, and `documentation` supplies semantic references for
labeling. These are distinct contracts. The local agent reads actual README/card,
license and origin evidence, enumerates complete scopes and prepares manifests
outside Dagster. Unresolved sources stay explicitly withheld in the inventory.
Both branches retrieve complete admitted files and verify integrity. They do not
search for sources or infer parsers. See the reusable source-preparation skill
for manifest syntax, trusted batch modules and reproducibility checks.

`human_source_decisions` is
removed from the active graph. Relabeling a corpus as documentation cannot bypass
machine eligibility.

Collection remains independent through `command_candidates`. Candidates merge before
shared filtering, semantic-reference collection and one isolated LLM invocation
per eligible command. There is no independent command-review invocation or active
`reviewed_annotations` stage. Historical review artifacts remain readable;
don't manufacture `review_verdict` or `review_confidence` on current rows.
Neither branch waits for human source admission.
Final publication requires current authorized
outputs from every configured branch. An unconfigured branch contributes no
records; it is not an empty configured branch. A configured but blocked branch
cannot be silently omitted from a purported complete release.

## Shared finalization

The shared path prefixes both candidate and source IDs with `dataset:` or
`documentation:`. Its `origins` metadata preserves the original branch, candidate
ID and source ID, and `branch_metadata` retains each input table's attributes.
This prevents collisions in labeling, evidence lookup and source coverage.
Shared outputs bind the exact current candidate digest from each configured branch.
Deduplication runs after shared isolated labeling,
before human review. It removes only proven exact normalized duplicates and
retains a survivor mapping. Quoted content, shell semantics and meaningful
context or label disagreements must not be collapsed. Fuzzy siblings,
resource-name substitution and safe-counterpart construction remain deferred.

Only rows with any `medium` or `low` among category, risk, severity and class
confidence enter the final human queue. All four `high`
means low uncertainty and automatic admission if machine eligible, with no human
decision. Machine-ineligible rows remain inspectable and cannot be made eligible
by approval. Eligibility and required human approval precede the category-wide
publication cap.

There is one current `datasets/dataset_release`, not one per source family or
admission route. An empty queue completes automatically under a policy actor with
zero human decisions and lets the build release. A nonempty queue makes the same release
wait for real terminal decisions and explicit completion. Included low-uncertainty
rows have `human_reviewed=false`; actually approved uncertain rows have
`human_reviewed=true`.

## Review and invalidation

Review is a persisted external-input stage. Waiting does not occupy a running
worker. For a nonempty queue, completing the exact current snapshot signals
downstream materialization. An empty queue needs no human completion.
Decisions bind source evidence, merged/deduplicated records, configuration,
implementation and the decision generation. An upstream change or retraction
makes affected publication stale; it never revives an older automatic release.

Unchanged records retain their labels and saved human decisions when their
relevant evidence and policy fingerprints still match. Processing order
or a changed neighboring record must not invalidate them. A new or
changed record requires one labeling invocation and, if uncertain, a human decision.
A changed release snapshot requires explicit completion when its queue is
nonempty; an empty queue is revalidated automatically. Carry-forward keeps the
original reviewer and audit origin.

Unchanged collection outputs remain reusable. Reading historical tables does not
rerun inference or grant approval. A changed configured branch invalidates shared
publication, not unrelated branch collection or unchanged record decisions.

## Viewing

Use native lineage groups `existing_datasets`, `documentation_crawl`,
`dataset_finalization` and `dataset_configuration`. Collection assets retain their
`datasets/<stage>` and `datasets/documentation/<stage>` keys. The final Review
tab belongs to the shared finalization assets, with a single `annotations` stage.
Preserve the category `partition` query parameter in native links.

Use the same `CODEBENCH_HOME` and `DAGSTER_HOME` for building and viewing.
Historical branch publications remain historical, not current releases.

`build_documentation` ends at `documentation/command_candidates` and performs no
labeling. The registered `build_dataset` planner runs shared processing when every
configured branch is current. A merge may expose a typed waiting placeholder for
inspection, but shared processing cannot proceed with a waiting configured branch.
An empty configured candidate table retains its digest and configured state;
an unconfigured branch has an explicit unconfigured placeholder.

Historical `merged_proposed_annotations` and `documentation/filtered_commands`,
`documentation/labeling_documentation`, `documentation/proposed_annotations` remain
readable through the data API and CLI `show`. They are absent from the active graph
and cannot substitute for current candidate inputs. The topology migration changes
label request identities and source IDs, so the first new build can require fresh
labels and invalidates old completion authorization; no historical decisions are
manufactured or rewritten.

## Verification

Run verification on the runtime host, not the Mac checkout. The following focused
suite uses temporary synthetic homes, mocked label providers and actual Dagster
materializations; it does not execute collected command strings:

```sh
PYTHONPATH=tests uv run python -B -m unittest \
  test_dataset_graph test_unified_finalization test_independent_branches \
  test_dataset_incremental test_confidence_publication test_dataset_viewer \
  test_annotation_cache -v
```

Expected results: both branches end at candidates in the collection groups,
shared processing starts at the merge, each eligible merged row is labeled once,
colliding source IDs remain distinct, empty configured branches retain digests,
waiting configured branches cannot authorize processing, and unchanged rows reuse
labels and human decisions. Historical tables remain readable without a new run.

For the focused real Dagster materialization scenarios alone:

```sh
PYTHONPATH=tests uv run python -B -m unittest \
  test_independent_branches.IndependentBranchDatasetTests.test_both_configured_branches_prepare_without_source_human_completion \
  test_independent_branches.IndependentBranchDatasetTests.test_configured_empty_branch_retains_its_digest_instead_of_becoming_unconfigured \
  test_independent_branches.IndependentBranchDatasetTests.test_configured_waiting_branch_cannot_authorize_shared_processing -v
```

Then run `uv run python -B -m unittest discover -s tests -v`, `uv run basedpyright`
and `uv run ruff check codebench_pipeline tests`. Native UI verification must show
the selected category partition and the four named lineage groups. Inspect the
merged candidates, shared labels, duplicate mapping and review state; repeat an
unchanged registered build to confirm no new run. These are operator verification
steps, not authorization to deploy or modify a live runtime.
