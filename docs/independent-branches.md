# Independent collection, unified dataset

The November 11, 2026 clarification requires independent collection branches,
not separate released datasets. The earlier branch-specific releases and
confidence-based automatic publication are superseded. Historical outputs and
decisions remain evidence; they are not approval for the new combined dataset.

## Graph and admission

```text
                        category_rules
                         /           \
           Existing datasets       Documentation crawl
             source catalog          source catalog
             human source gate       inspected-source binding
             documents               documents
             adapters                adapters
             extraction              extraction
             filtering               filtering
             semantic references     semantic references
             isolated labeling       isolated labeling
                         \           /
                    merged proposed annotations
                    bounded incremental agentic review
                    exact normalized deduplication
                    unified human review queue
                    completed human decisions
                    one dataset release
```

`seeds` configures corpus inputs, `documentation_sources` configures original
documentation/forum inputs, and `documentation` supplies semantic references for
labeling. These are distinct contracts. Corpus admission requires actual human
source approval. Original documentation requires inspected provenance, permitted
reuse and unchanged captured bytes; relabeling a corpus as documentation cannot
bypass admission.

Collection remains independent through labeling. Agentic review runs in the
unified tail, in bounded packets containing only records without a current
review result. A waiting corpus gate does not stop documentation work.
Final publication requires current authorized
outputs from every configured branch. An unconfigured branch contributes no
records; it is not a completed human selection. A configured but blocked branch
cannot be silently omitted from a purported complete release.

## Shared finalization

The shared path retains branch-qualified record identities and original
provenance. Deduplication runs after isolated labeling and shared agentic review,
before human review. It removes only proven exact normalized duplicates and
retains a survivor mapping. Quoted content, shell semantics and meaningful
context or label disagreements must not be collapsed. Fuzzy siblings,
resource-name substitution and safe-counterpart construction remain deferred.

Every retained release candidate requires human review, regardless of model
confidence. Machine-ineligible rows remain inspectable and cannot be made
eligible merely by approving them. Human exclusions precede the category-wide
publication cap. There is one current `datasets/dataset_release`, not one per
source family, and no automatic publication bypass.

## Review and invalidation

Review is a persisted external-input stage. Waiting does not occupy a running
worker. Completing the exact current snapshot signals downstream materialization.
Decisions bind source evidence, merged/deduplicated records, configuration,
implementation and the decision generation. An upstream change or retraction
makes affected publication stale; it never revives an older automatic release.

Unchanged records retain their model reviews and saved human decisions when their
relevant evidence and policy fingerprints still match. Packet membership,
ordering or a changed neighboring record must not invalidate them. A new or
changed record requires new review; a changed release snapshot still requires
explicit completion. Carry-forward keeps the original reviewer and audit origin.

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
