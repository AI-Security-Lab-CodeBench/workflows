# Independent collection, unified dataset

The [CAT-3 flow](architecture/cat3-collection-flow.md) shows the three declared
collection branches. Existing datasets enumerates pinned corpus files before
capturing them. Documentation crawl enumerates and captures pinned documentation
within its own acquisition stage. Keenable alone searches for new origins within
reviewed scopes and verifies the original pages before extracting commands.
Search hits do not authorize retention. Each branch reads only its own
declaration and produces its own provenance and exclusion evidence.

Collection remains independent through `command_candidates`. The branches use
reviewed parser bindings and preserve positions in inert source content. The
shared merge namespaces candidate and source identities by branch and preserves
the originals and the current input digests. A successfully empty branch retains
its identity and evidence. An absent or blocked configured branch cannot be
silently substituted with historical data or treated as empty.

Shared filtering precedes guarded batch labeling. The labeler checks applicable
passages from the same verified captures and emits independent outcomes for
each assessed row. Supported outcomes flow through conservative exact
deduplication and category eligibility before the shared uncertainty queue.
Failed outcomes stay inspectable and cannot enter release. The
[stage guide](pipeline-stages.md) describes each output and its admission rules.

There is one final `dataset_release` per partition. A nonempty queue waits for
real terminal decisions and explicit completion; an empty queue needs no human
completion. Current eligibility and publication controls apply in both cases.
A changed branch invalidates affected shared outputs without authorizing a stale
release. Unchanged labels and reusable decisions retain their original evidence.

Use [the operator guide](orchestration.md) to inspect the selected partition
or run the preparation-only selection. Historical prepared source manifests,
branch-specific publications and review decisions remain historical evidence.
