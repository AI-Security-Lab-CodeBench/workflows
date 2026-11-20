# CAT-3 Keenable declaration

`declaration.json` uses authoring schema version `3`. It defines the
reviewed discovery URLs for the workload and container cases, plus the
source scopes, reference URLs, search and capture budgets, license evidence,
and whether examples may be extracted or only quoted.
The same frozen generation supplies both candidate collection and labeling.

The registration loader reads the companion through
`codebench_pipeline.keenable_policy.load_declaration`.
Pass the current registered declaration and a stable directory under the
dataset home to `KeenableCollection`. The declaration's `dataset_id` must match
the selected partition. Store the root on a filesystem shared by the stage
workers that use this branch, with SQLite durability and working file locks.
The generation, rather than the Dagster run ID, identifies the persisted
receipts so interrupted runs can resume without resending calls.

| Operation | Receipt projection |
| --- | --- |
| `discover() -> CatalogReceipt` | Declaration digest, ranked leads and exclusions, frozen page decisions, and search and planning receipts. |
| `capture(catalog) -> CaptureLock` | Declaration and catalog digests, verified page hashes and metadata, withheld URLs, and origin receipts. |
| `adapters(lock) -> tuple[AdapterReceipt, ...]` | URL, capture digest, permission digest, and reviewed span parser identity. |
| `candidates(lock, adapters) -> tuple[CandidateReceipt, ...]` | Literal administrative command, origin and capture digest, exact byte offsets and span digest, attribution and rights receipt. |
| `read_body(lock, url) -> bytes` | Exact locally retained bytes, after rechecking the supplied lock, the current declaration's permission and the pinned digest. |

The receipt types live in `codebench_pipeline.keenable_receipts`. Preserve
them at stage boundaries with `model_dump_json` and reconstruct them with
`model_validate_json`; do not reconstruct inputs from the display rows.
`CallReceipt` records the slot, request digest, reservation, terminal outcome,
and available response status, digest, size and peer. Provider snippets and
withheld page bodies are not stored.

For a new live generation, declare the permitted source scope, actual license
evidence and attribution, allowed uses, and expiry. An exact reference URL
seeds capture directly and its grant must have a single case. `allow_discovery`
independently opts a bounded prefix into search and admission of search results.
Each reviewed search scope names its grant by `origin_id`. Candidate
receipts bind extracted commands to byte ranges in verified original bodies.
The labeler verifies operation passages from those captures without another
Keenable search or a separate page or operation grant. Historical revision `1`
and `2` declarations retain their original authorization semantics and receipt
serialization. Use a distinct generation when moving to schema version `3`.

`pinned_license_file` requires verified license bytes from the same immutable
repository commit. `page_notice` checks the notice in each captured HTML page
and grants quotation only; its evidence URL and digest are declared metadata.
`steward_decision` records the declared decision and evidence digest without a
runtime lookup.

CAT-3 registration loads this fixed category companion. The ledger derives the
shared search and origin byte capacity from the two call limits at 512 KiB per
reservation. The five administrative cases have a fixed order in code.
The shipped `search_plan.mode` is `fixed`. It selects one reference query per
scope in declaration order before moving to the next focus if budget remains.
For a comparison, register a companion with a new `generation` and
`search_plan.mode: "adaptive"`, with the same scopes, reviewed URLs, acquisition
budgets, and seed references. The adaptive catalog
stage must supply `GuardedSearchPlanner` to `KeenableCollection(..., planner=...)`
using the same capacity ledger as other users of that model endpoint. Discovery
fails closed without this guard; capture and grounding consume the frozen
receipts and need no planner. Pass the registered `backend_revision` into the
planner so its identity changes even when the model alias stays the same.
Use a new generation for changes to authorization or the planner binding.

Both modes use the same authored vocabulary: general reference, inventory,
output fields, maintenance scope, lifecycle, and data retention. Each option
searches a case's exact reviewed documentation URLs; result admission checks
the exact URL and the current source grant independently. The adaptive model
can choose an unused option ID or stop. It receives only the options, remaining
budget, and three counts from each prior search:

- `returned`: provider result rows considered, up to the per-call cap.
- `eligible`: distinct canonical URLs on the reviewed list with current case
  authority, including any seed references returned by search.
- `new_urls`: eligible URLs absent from declared seeds and all prior searches
  in this generation. This is catalog URL yield; it does not measure coverage,
  extracted command novelty, or successful capture.

A model turn can authorize at most one search. Model and Keenable calls each
have the configured search-count ceiling. Planning requests and accepted
responses have a separate per-call size ceiling; model output also has a token
limit. The ledger reserves the call before dispatch and retains only the
normalized decision or lead data and response digests. An invalid decision or
an ambiguous dispatch prevents a partial catalog and cannot be retried in that
generation.
