# Independent collection, unified dataset

The current November 11, 2026 instruction requires automatic eligible-source
collection in independent branches and uncertainty-only final human review.
Earlier branch-specific releases, mandatory corpus admission and all-row review
are superseded. Historical outputs and decisions remain evidence, not current
authorization for the combined dataset.

## Graph and admission

```text
                        category_rules
                         /           \
           Existing datasets       Documentation crawl
             source catalog          source catalog
             eligibility binding     eligibility binding
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
                    uncertainty-only human review queue
                    real decisions if queue nonempty
                    one dataset release
```

`seeds` configures corpus inputs, `documentation_sources` configures original
documentation/forum inputs, and `documentation` supplies semantic references for
labeling. These are distinct contracts. Both collection branches automatically
crawl or load eligible sources with inspected provenance, permitted reuse,
content eligibility and unchanged captured bytes. `human_source_decisions` is
removed from the active graph. Relabeling a corpus as documentation cannot bypass
machine eligibility.

Collection remains independent through labeling. Agentic review runs in the
unified tail, in bounded packets containing only records without a current
review result. Neither branch waits for human source admission.
Final publication requires current authorized
outputs from every configured branch. An unconfigured branch contributes no
records; it is not an empty configured branch. A configured but blocked branch
cannot be silently omitted from a purported complete release.

## Shared finalization

The shared path retains branch-qualified record identities and original
provenance. Deduplication runs after isolated labeling and shared agentic review,
before human review. It removes only proven exact normalized duplicates and
retains a survivor mapping. Quoted content, shell semantics and meaningful
context or label disagreements must not be collapsed. Fuzzy siblings,
resource-name substitution and safe-counterpart construction remain deferred.

Only rows with any `medium` or `low` among category, risk, severity, class and
independent reviewer confidence enter the final human queue. All five `high`
means low uncertainty and automatic admission if machine eligible, with no human
decision. Machine-ineligible rows remain inspectable and cannot be made eligible
by approval. Eligibility and required human approval precede the category-wide
publication cap.

There is one current `datasets/dataset_release`, not one per source family or
admission route. An empty queue lets the build release automatically without
inventing human decisions or completion. A nonempty queue makes the same release
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

Unchanged records retain their model reviews and saved human decisions when their
relevant evidence and policy fingerprints still match. Packet membership,
ordering or a changed neighboring record must not invalidate them. A new or
changed record requires new model review and, if uncertain, a human decision.
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
